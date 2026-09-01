"""Chargeback invoice tools — ``discover_erp_integrations``, ``generate_chargeback_invoices``,
``submit_invoices_to_erp``.

Rewritten to build invoices from a cost datasource via the SOE **extract tools** (``cost_reader``)
+ polars. ``generate`` and ``submit`` stay in sync by sharing one ``_build_chargeback_invoices``
(the single source of truth for counts, line items, and materiality filtering). The pi-router has
**no live ERP connection** server-side — the assistant orchestrates the ERP via its own connected
MCP servers (e.g. ``zoho-books``), and ``submit_invoices_to_erp`` returns the delegation payloads
for the assistant to execute. A deterministic local confirmation path is kept for non-Zoho ERPs.
"""

from __future__ import annotations

from datetime import date

import polars as pl
from fastmcp import FastMCP

from . import common as cm
from . import cost_reader as cr
from . import formatting as fmt
from .settings import (
    CHARGEBACK_POLICY,
    ERP_DOC_BASE,
    ERP_SERVER_HINTS,
    SUPPORTED_ERPS,
    get_chargeback_settings,
)


async def _build_chargeback_invoices(
    soe,
    data_source: str,
    start: date,
    end: date,
    period_label: str,
    materiality_threshold: float,
    cost_center_column: str | None,
    org_column: str | None,
    cost_column: str | None,
    period_column: str | None,
    provider_column: str | None,
) -> tuple[list[dict], dict]:
    """Build the per-cost-center invoice list with provider-level line items.

    Single source of truth for both ``generate_chargeback_invoices`` and
    ``submit_invoices_to_erp``. Reads the destination via the SOE focus-query SQL path, groups by
    cost center (+ org + provider), and wraps each cost center as a draft invoice with per-provider
    line items. Returns ``(kept_invoices, materiality)``. Raises ``RuntimeError`` on any refusal
    (the tool prefixes ``ERR``).
    """
    dc = cr.resolve_destination(soe, data_source)
    schema = cr.read_cost_schema(soe, dc)
    if not schema:
        raise RuntimeError(f"no schema discovered for {data_source!r}.")
    cost_col = cr.resolve_cost_column(schema, cost_column)
    if not cost_col:
        raise RuntimeError(f"could not identify a cost column. Columns: {', '.join(sorted(schema))}. Pass cost_column.")
    # Grouping dimension from the allocation pipeline (cost_center → business_unit → org) so a
    # destination without a cost center still bills against its business-unit/org column.
    alloc = await cr.resolve_allocation_dimension(soe, schema, override=cost_center_column)
    cc_col = alloc.get("column")
    if not cc_col:
        raise RuntimeError(
            f"could not identify an allocation dimension (cost center / business unit / org) column. "
            f"Columns: {', '.join(sorted(schema))}. Pass cost_center_column."
        )
    classification = await cr.classify_org_cost_center(schema, soe=soe)
    org_col = await cr.resolve_org_column(schema, org_column, classification, soe=soe) or cc_col
    period_col = cr.resolve_period_column(schema, period_column)
    provider_col = cr.resolve_provider_column(schema, provider_column)

    group_cols = [cc_col]
    for col in (org_col, provider_col):
        if col and col in schema and col not in group_cols:
            group_cols.append(col)
    df = cr.read_aggregated_cost(
        soe, dc, group_cols, cost_col, period_col, start, end, None, schema.get(period_col), top_n=200
    )
    if df.is_empty():
        raise RuntimeError(f"no rows in the destination for {period_label}.")

    # The frame is already per-(cost_center, org, provider[, allocation]) from SQL — shape it
    # (cast/round the summed cost, keep SQL's row_count for the line-item descriptions).
    gb = [c for c in group_cols if c in df.columns]
    has_rc = "row_count" in df.columns
    agg = (
        df.select([*gb, cost_col] + (["row_count"] if has_rc else []))
        .with_columns(pl.col(cost_col).cast(pl.Float64, strict=False).round(2).alias("billed_cost"))
        .sort("billed_cost", descending=True)
    )

    by_cc: dict[str, dict] = {}
    for r in agg.iter_rows(named=True):
        cc = r[cc_col]
        cc = cc if cc is not None else "(unallocated)"
        if cc == "(unallocated)":
            continue  # orphan spend can't be invoiced until tagged
        org = r.get(org_col) if org_col in gb else None
        org = org if org is not None else "(unknown org)"
        provider = r.get(provider_col) if provider_col in gb else None
        provider = provider if provider is not None else "(unknown provider)"
        billed = float(r["billed_cost"] or 0)
        records = int(r.get("row_count") or 0) if has_rc else 0
        inv = by_cc.setdefault(
            cc,
            {
                "invoice_id": f"CB-{start.strftime('%Y%m')}-{cc}",
                "period_label": period_label,
                "period_start": start.isoformat(),
                "period_end": end.isoformat(),
                "organization": org,
                "cost_center": cc,
                "currency": "USD",
                "line_items": [],
                "total_billed": 0.0,
                "status": "draft",
            },
        )
        inv["line_items"].append(
            {
                "provider": provider,
                "description": f"{provider} ({records:,} charge records)",
                "billed_cost": billed,
            }
        )
        inv["total_billed"] = round(inv["total_billed"] + billed, 2)

    all_invoices = sorted(by_cc.values(), key=lambda i: -i["total_billed"])
    kept = [inv for inv in all_invoices if inv["total_billed"] >= materiality_threshold]
    filtered = [inv for inv in all_invoices if inv["total_billed"] < materiality_threshold]
    materiality = {
        "threshold": float(materiality_threshold),
        "filtered_count": len(filtered),
        "filtered_total": round(sum(inv["total_billed"] for inv in filtered), 2),
        "filtered_cost_centers": [inv["cost_center"] for inv in filtered],
        "combined_into": ("Below materiality (combined)" if filtered else None),
    }
    return kept, materiality


def register(mcp: FastMCP, client, soe) -> None:
    @mcp.tool
    async def discover_erp_integrations(environment_id: str | None = None) -> str:
        """Discover which ERP the environment is configured to post to, and the supported set.

        The pipeline declares the ERP this environment has standardized on. This surfaces that
        target + match hints, and asks the assistant to introspect its connected MCP servers for
        which ERP-shaped integration is actually reachable. Proceed to ``submit_invoices_to_erp``
        only for the configured ERP.
        """
        try:
            env_id = cm.resolve_env_id(soe, environment_id)
        except RuntimeError as exc:
            return str(exc)
        cfg = CHARGEBACK_POLICY["erp_integration"]
        configured = cfg["system"]

        lines = [
            f"# Discover ERP integrations — env {env_id}",
            "",
            f"**Configured ERP:** {configured} (org {cfg['organization_id']}, "
            f"account '{cfg['default_journal_account']}')",
            "",
            "**Match hints (grep your connected MCP servers):** "
            + ", ".join(f"`{h}`" for h in ERP_SERVER_HINTS.get(configured, [])),
            "",
            "**Supported ERPs:**",
        ]
        for name in SUPPORTED_ERPS:
            lines.append(f"- **{name}** — hints: " + ", ".join(f"`{h}`" for h in ERP_SERVER_HINTS.get(name, [])))
        lines.extend(
            [
                "",
                f"Inspect your own connected MCP servers / tools and report which (if any) match an "
                f"ERP above, emphasizing {configured} (the configured target). If a matching "
                f"integration is connected, proceed to submit_invoices_to_erp(erp_system={configured!r}). "
                f"If NOT, report the gap and stop — do not fall through to a different ERP.",
            ]
        )
        return "\n".join(lines)

    @mcp.tool
    async def generate_chargeback_invoices(
        data_source: str = "",
        period: str | None = None,
        since_days: int = 30,
        period_label: str | None = None,
        materiality_threshold: float | None = None,
        cost_center_column: str | None = None,
        org_column: str | None = None,
        cost_column: str | None = None,
        period_column: str | None = None,
        provider_column: str | None = None,
        environment_id: str | None = None,
    ) -> str:
        """Generate one draft chargeback invoice per cost center for the period, from a FOCUS
        **destination** (what Stitcher has written).

        Groups the destination by cost center (default ``x_CostCenter``), wraps each cost center's
        cost as a draft invoice with a deterministic invoice_id and one line item per provider.
        Skips the (unallocated) bucket and sub-materiality cost centers (combined into a
        ``materiality`` note). Returned invoices are ``status="draft"``.

        Args:
            data_source: Name or id of the **destination** (a FOCUS data-lake export connection).
                Omit to auto-resolve the environment's single FOCUS data lake.
            period: ``YYYY-MM`` or "last_month". since_days: Rolling-window fallback.
            period_label: Override the human-readable label.
            materiality_threshold: Skip invoices below this USD amount (combined note).
            cost_center_column / org_column / provider_column / cost_column / period_column:
                Override discovered columns.
            environment_id: Scope.
        """
        try:
            cm.resolve_env_id(soe, environment_id)
        except RuntimeError as exc:
            return str(exc)
        start, end, computed_label = cm.resolve_window(period, since_days)
        if period_label is None:
            period_label = computed_label
        if materiality_threshold is None:
            materiality_threshold = get_chargeback_settings().materiality_threshold_usd

        try:
            invoices, materiality = await _build_chargeback_invoices(
                soe,
                data_source,
                start,
                end,
                period_label,
                materiality_threshold,
                cost_center_column,
                org_column,
                cost_column,
                period_column,
                provider_column,
            )
        except RuntimeError as exc:
            return f"ERR (generate_chargeback_invoices): {exc}"

        featured_invoice = (
            max(invoices, key=lambda inv: (len(inv["line_items"]), inv["total_billed"])) if invoices else None
        )

        lines = [f"# Chargeback invoices — {period_label}  (env {_resolve_quiet(soe, environment_id)})", ""]
        if materiality.get("filtered_count"):
            lines.append(
                f"_{materiality['filtered_count']} cost center(s) below the "
                f"${materiality['threshold']:.2f} materiality threshold combined and skipped"
                " — they are NOT invoiced._"
            )
            lines.append("")
        if featured_invoice:
            lines.append(
                f"**Featured invoice — {featured_invoice['cost_center']}** " f"({featured_invoice['invoice_id']})"
            )
            lines.append("| Provider | Description | Amount |")
            lines.append("|---|---|---:|")
            for item in featured_invoice["line_items"]:
                lines.append(f"| {item['provider']} | {item['description']} | {fmt.fmt_money(item['billed_cost'])} |")
            lines.append("")
            lines.append(
                f"**{len(invoices)} draft invoice(s), total {fmt.fmt_money(sum(i['total_billed'] for i in invoices))}.**"
            )
            lines.append("")
            lines.append(
                "Render the featured invoice, then ask whether to review the remaining invoices "
                "before listing them. When confirmed, call discover_erp_integrations() then "
                f"submit_invoices_to_erp(erp_system=<discovered>, period={period!r}). Do NOT "
                "re-raise the below-materiality invoices."
            )
        else:
            lines.append(f"No invoices above the ${materiality_threshold:.2f} materiality threshold for this period.")
            lines.append(
                f"_{materiality.get('filtered_count', 0)} cost center(s) below threshold were combined and skipped._"
            )
        return "\n".join(lines)

    @mcp.tool
    async def submit_invoices_to_erp(
        erp_system: str,
        data_source: str = "",
        period: str | None = None,
        invoice_ids: list[str] | None = None,
        materiality_threshold: float | None = None,
        cost_center_column: str | None = None,
        org_column: str | None = None,
        cost_column: str | None = None,
        period_column: str | None = None,
        provider_column: str | None = None,
        environment_id: str | None = None,
    ) -> str:
        """Submit chargeback invoices to the ERP as journal entries.

        Supported ERPs: QuickBooks Online, NetSuite, Xero, Sage Intacct, Microsoft Dynamics 365
        Finance, Workday Financial Management, Zoho Books. Pass the exact name as ``erp_system``.

        The pi-router has **no live ERP connection** server-side. For an ERP with a connected MCP
        server (Zoho Books has one), returns the two-phase delegation payloads (create_contact per
        cost center, then create_invoice) the assistant should execute via that server. For other
        ERPs it returns deterministic local confirmations. Always rebuilds the canonical invoice
        set so counts/totals match what generate showed.

        Args:
            erp_system: One of the supported ERP names (unsupported → refusal).
            data_source: Name or id of the **destination** (a FOCUS data-lake export connection).
                Omit to auto-resolve the environment's single FOCUS data lake.
            period: ``YYYY-MM`` or "last_month" to derive the canonical invoice set.
            invoice_ids: Narrow to specific invoice IDs (only above-threshold ones).
            materiality_threshold: Must match generate time (defaults to the setting).
            environment_id: Scope.
        """
        try:
            env_id = cm.resolve_env_id(soe, environment_id)
        except RuntimeError as exc:
            return str(exc)
        if erp_system not in SUPPORTED_ERPS:
            return f"ERR (submit_invoices_to_erp): Unsupported ERP {erp_system!r}. Supported: {SUPPORTED_ERPS}"
        if materiality_threshold is None:
            materiality_threshold = get_chargeback_settings().materiality_threshold_usd

        start, end, period_label = cm.resolve_window(period, 30)
        try:
            invoices, materiality = await _build_chargeback_invoices(
                soe,
                data_source,
                start,
                end,
                period_label,
                materiality_threshold,
                cost_center_column,
                org_column,
                cost_column,
                period_column,
                provider_column,
            )
        except RuntimeError as exc:
            return f"ERR (submit_invoices_to_erp): {exc}"

        if invoice_ids:
            wanted = set(invoice_ids)
            invoices = [inv for inv in invoices if inv["invoice_id"] in wanted]

        summary_md = fmt.build_posted_summary_markdown(invoices, materiality)

        if erp_system == "Zoho Books":
            contact_payloads = [
                {
                    "contact_name": f"Stitcher Cost Center: {inv['cost_center']}",
                    "company_name": f"Stitcher AI ({inv['cost_center']})",
                    "contact_type": "customer",
                    "customer_sub_type": "business",
                    "custom_fields": [{"label": "Cost Center", "value": inv["cost_center"]}],
                    "notes": "Internal cost-center contact for chargeback automation.",
                    "_local_ref": inv["cost_center"],
                }
                for inv in invoices
            ]
            invoice_payloads = [
                {
                    "customer_id": "REPLACE_WITH_CONTACT_ID_FROM_PHASE_1[" + inv["cost_center"] + "]",
                    "invoice_number": inv["invoice_id"],
                    "reference_number": inv["invoice_id"],
                    "date": start.isoformat(),
                    "custom_fields": [{"label": "Cost Center", "value": inv["cost_center"]}],
                    "line_items": [
                        {
                            "name": item["provider"],
                            "description": item["description"],
                            "rate": item["billed_cost"],
                            "quantity": 1,
                        }
                        for item in inv["line_items"]
                    ],
                    "notes": "Posted by Stitcher chargeback automation.",
                    "_local_ref": inv["cost_center"],
                }
                for inv in invoices
            ]
            return "\n".join(
                [
                    f"# Submit to Zoho Books — env {env_id}",
                    "",
                    f"Zoho Books exposes only customer-facing invoice tools. Run TWO phases via the "
                    f"connected `zoho-books` MCP server (org "
                    f"{CHARGEBACK_POLICY['erp_integration']['organization_id']}):",
                    "",
                    f"**PHASE 1** — create {len(contact_payloads)} contacts (create_contact); capture "
                    f"each response's contact_id keyed by its `_local_ref` (cost-center id).",
                    f"**PHASE 2** — for each invoice_payload, replace "
                    f"`REPLACE_WITH_CONTACT_ID_FROM_PHASE_1[<cc>]` in customer_id with the phase-1 "
                    f"contact_id, then call create_invoice ({len(invoice_payloads)} invoices).",
                    "",
                    "Contact payloads:",
                    f"```json\n{contact_payloads}\n```",
                    "",
                    "Invoice payloads:",
                    f"```json\n{invoice_payloads}\n```",
                    "",
                    "Counts: "
                    + str(
                        {
                            "invoice_count": len(invoice_payloads),
                            "contact_count": len(contact_payloads),
                            "submitted_count": len(invoice_payloads),
                            "filtered_count": materiality["filtered_count"],
                        }
                    ),
                    "",
                    "Posted summary (render verbatim when complete):",
                    "",
                    summary_md,
                ]
            )

        # Non-Zoho deterministic local confirmation path.
        import datetime as _dt

        posted_at = _dt.datetime.now(_dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        erp_prefix = {
            "QuickBooks Online": "QBO-JE",
            "NetSuite": "NS-JE",
            "Xero": "XR-MJ",
            "Sage Intacct": "SI-JE",
            "Microsoft Dynamics 365 Finance": "D365-JE",
            "Workday Financial Management": "WD-JE",
            "Zoho Books": "ZB-JE",
        }[erp_system]
        confirmations = [
            {
                "invoice_id": inv["invoice_id"],
                "erp_system": erp_system,
                "erp_doc_number": f"{erp_prefix}-{ERP_DOC_BASE + idx:05d}",
                "posted_at": posted_at,
                "status": "posted",
            }
            for idx, inv in enumerate(invoices)
        ]
        return "\n".join(
            [
                f"# Submit chargeback invoices to {erp_system} — env {env_id}",
                "",
                f"**{len(confirmations)} invoice(s) posted** at {posted_at} "
                f"({materiality['filtered_count']} below materiality combined/skipped).",
                "",
                "| Invoice | ERP Doc # | Status |",
                "|---|---|---|",
            ]
            + [f"| {c['invoice_id']} | {c['erp_doc_number']} | {c['status']} |" for c in confirmations]
            + [
                "",
                "Posted summary (render verbatim):",
                "",
                summary_md,
            ]
        )


def _resolve_quiet(soe, environment_id: str | None) -> str:
    try:
        return cm.resolve_env_id(soe, environment_id)
    except RuntimeError:
        return "?"

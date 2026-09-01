"""Chargeback report tools — ``chargeback_by_billing_account``, ``chargeback_by_cost_center``,
``chargeback_provider_lineage``.

Read a FOCUS data-lake **destination** (what Stitcher has written) via the SOE focus-query SQL
path (``cost_reader.read_aggregated_cost`` → ``_run_focus_query`` — the GROUP BY/SUM is pushed
into the database) and shape/render with the shared ``common`` helpers. Chargeback only reads
**destinations** — never source datasources — so the column defaults are FOCUS-normalized
(``BilledCost`` / ``ChargePeriodStart`` / ``x_CostCenter`` / …). Omit ``data_source`` to
auto-resolve the environment's single FOCUS data lake.

Allocation lineage (direct / allocation-in / allocation-out) is only computed when the
destination exposes ``x_AllocationStatusSource``/``Destination``. Period resolution, materiality
rollup, and markdown rendering follow the harness house style.
"""

from __future__ import annotations

import polars as pl
from fastmcp import FastMCP

from . import common as cm
from . import cost_reader as cr
from . import formatting as fmt
from .settings import CC_NAMES, get_chargeback_settings

_DIM_DISP = {
    "cost_center": "cost center",
    "business_unit": "business unit",
    "organization": "organization",
}


def _rollup_cost_center(
    df: pl.DataFrame,
    cost_col: str,
    cc_col: str,
    org_col: str | None,
    provider_col: str | None,
    alloc: tuple[str | None, str | None],
) -> list[dict]:
    """Aggregate the SQL-aggregated frame into per-(cost_center, org, provider) rows with
    direct / allocation-in / allocation-out buckets (allocation only when the columns exist)."""
    alloc_src, alloc_dst = alloc
    if not cc_col:
        raise RuntimeError("cost-center column is required")
    gb = [cc_col]
    for col in (org_col, provider_col):
        if col and col not in gb and col in df.columns:
            gb.append(col)

    cost = pl.col(cost_col).cast(pl.Float64, strict=False)
    has_alloc = bool(alloc_src and alloc_dst and alloc_src in df.columns and alloc_dst in df.columns)
    select_cols = [*gb, cost_col] + ([alloc_src, alloc_dst] if has_alloc else [])
    if has_alloc:
        src, dst = pl.col(alloc_src), pl.col(alloc_dst)
        aggs = [
            pl.when(src.is_null() & dst.is_null() & (cost > 0))
            .then(cost)
            .otherwise(0)
            .sum()
            .round(2)
            .alias("direct_cost"),
            pl.when(((dst == "Allocated") | (src == "Allocations")) & (cost > 0))
            .then(cost)
            .otherwise(0)
            .sum()
            .round(2)
            .alias("allocation_in"),
            pl.when(cost < 0).then(cost).otherwise(0).sum().round(2).alias("allocation_out"),
        ]
    else:
        aggs = [
            cost.sum().round(2).alias("direct_cost"),
            pl.lit(0.0).alias("allocation_in"),
            pl.lit(0.0).alias("allocation_out"),
        ]
    frame = df.select(select_cols).group_by(gb).agg(aggs).sort("direct_cost", descending=True)

    out: dict[tuple, dict] = {}
    for r in frame.iter_rows(named=True):
        cc = r[cc_col]
        cc = cc if cc is not None else "(unallocated)"
        org = r.get(org_col) if org_col else None
        org = org if org is not None else "(unallocated)"
        provider = r.get(provider_col) if provider_col else None
        provider = provider if provider is not None else "(unknown provider)"
        bucket = out.setdefault(
            (cc, org, provider),
            {
                "cost_center": cc,
                "organization": org,
                "provider": provider,
                "direct_cost": 0.0,
                "allocation_in": 0.0,
                "allocation_out": 0.0,
            },
        )
        bucket["direct_cost"] = round(bucket["direct_cost"] + float(r["direct_cost"] or 0), 2)
        bucket["allocation_in"] = round(bucket["allocation_in"] + float(r["allocation_in"] or 0), 2)
        bucket["allocation_out"] = round(bucket["allocation_out"] + float(r["allocation_out"] or 0), 2)
    return list(out.values())


async def _read_cc_frame(
    soe,
    tool: str,
    data_source: str,
    environment_id: str | None,
    *,
    cost_column: str | None,
    period_column: str | None,
    cost_center_column: str | None,
    org_column: str | None,
    provider_column: str | None = None,
    cost_center: str | None = None,
    since_days: int = 30,
    period: str | None = None,
):
    """Shared read for the two cost-center tools: prelude → classification → group columns → SQL
    aggregation (with allocation columns when present). Returns
    ``(dc, schema, cost_col, df, period_label, cols, alloc, has_alloc)`` where ``cols`` is
    ``(cc_col, org_col, provider_col)``; raises :class:`cm.ToolRefusal` on every refusal boundary."""
    dc, schema, cost_col = cm.prep_read(soe, tool, data_source, environment_id, cost_column)
    # The grouping dimension comes from the allocation pipeline (cost_center → business_unit →
    # organization), so a destination WITHOUT a cost center still groups on its business-unit or
    # organization column instead of a hard cost-center refusal.
    alloc = await cr.resolve_allocation_dimension(soe, schema, override=cost_center_column)
    cc_col = alloc.get("column")
    dim_label = alloc.get("dimension") or "cost_center"
    if not cc_col:
        raise cm.ToolRefusal(
            cm.refusal(tool, "cost center / business unit / organization", schema, "cost_center_column")
        )
    classification = await cr.classify_org_cost_center(schema, soe=soe)
    org_col = await cr.resolve_org_column(schema, org_column, classification, soe=soe) or cc_col
    provider_col = cr.resolve_provider_column(schema, provider_column)
    alloc_src, alloc_dst = cr.resolve_allocation_columns(schema)
    period_col = cr.resolve_period_column(schema, period_column)
    start, end, period_label = cm.resolve_window(period, since_days)

    group_cols = [cc_col]
    for col in (org_col, provider_col):
        if col and col in schema and col not in group_cols:
            group_cols.append(col)
    filters = {cc_col: cost_center} if cost_center is not None else None
    df = cr.read_aggregated_cost(
        soe,
        dc,
        group_cols,
        cost_col,
        period_col,
        start,
        end,
        filters,
        schema.get(period_col),
        allocation_src=alloc_src,
        allocation_dst=alloc_dst,
        top_n=200,
    )
    has_alloc = bool(alloc_src and alloc_dst)
    return (dc, schema, cost_col, df, period_label, (cc_col, org_col, provider_col),
            (alloc_src, alloc_dst), has_alloc, dim_label)


def register(mcp: FastMCP, client, soe) -> None:
    @mcp.tool
    async def chargeback_by_billing_account(
        data_source: str = "",
        period: str | None = None,
        since_days: int = 30,
        top_n: int = 20,
        cost_column: str | None = None,
        period_column: str | None = None,
        billing_account_column: str | None = None,
        environment_id: str | None = None,
    ) -> str:
        """Top billing accounts (or projects) by total cost for a period, from a FOCUS **destination**.

        Reads the destination via the SOE focus-query SQL path and sums cost per billing-account
        column (default ``BillingAccountId``).

        Args:
            data_source: Name or id of the **destination** (a FOCUS data-lake export connection).
                Omit to auto-resolve the environment's single FOCUS data lake.
            period: ``YYYY-MM`` (e.g. "2026-03") or "last_month". Overrides since_days.
            since_days: Rolling-window fallback when ``period`` is omitted.
            top_n: Max accounts (capped at 200).
            cost_column: Override the cost column (default discovered: BilledCost/…).
            period_column: Override the period column (default: ChargePeriodStart).
            billing_account_column: Override the account/project dimension column.
            environment_id: Scope.
        """
        tool = "chargeback_by_billing_account"
        try:
            dc, schema, cost_col = cm.prep_read(soe, tool, data_source, environment_id, cost_column)
        except cm.ToolRefusal as exc:
            return str(exc)
        ba_col = cr.resolve_billing_account_column(schema, billing_account_column)
        if not ba_col:
            return cm.refusal(tool, "billing-account", schema, "billing_account_column")
        period_col = cr.resolve_period_column(schema, period_column)
        start, end, period_label = cm.resolve_window(period, since_days)

        try:
            df = cr.read_aggregated_cost(
                soe, dc, [ba_col], cost_col, period_col, start, end, None, schema.get(period_col), top_n=200
            )
        except Exception as e:  # noqa: BLE001
            return f"ERR ({tool}): could not read destination: {str(e)[:300]}"
        if df.is_empty():
            return f"No rows in the destination for {period_label}."

        rows, total, records = cm.cost_summary(df, [ba_col], cost_col, max(1, min(top_n, 200)))
        return cm.share_table(
            f"Chargeback by {ba_col}",
            getattr(dc, "name", data_source),
            records,
            period_label,
            [ba_col],
            total,
            rows,
        )

    @mcp.tool
    async def chargeback_by_cost_center(
        data_source: str = "",
        period: str | None = None,
        since_days: int = 30,
        include_unallocated: bool = True,
        materiality_threshold: float | None = None,
        cost_center_column: str | None = None,
        org_column: str | None = None,
        cost_column: str | None = None,
        period_column: str | None = None,
        environment_id: str | None = None,
    ) -> str:
        """Run the monthly cloud-cost chargeback / showback report — allocate cost across cost
                centers and return a rendered markdown table. PRIMARY entry point for "run chargeback for
                <month>", "monthly chargeback", "cost-center chargeback", "showback".

                Reads a FOCUS data-lake **destination** (what Stitcher has written) via the SOE focus-query
                SQL path. Groups by a cost-center column (default ``x_CostCenter``) and, when the
        destination exposes ``x_AllocationStatusSource``/``Destination``, decomposes each row into
        direct / allocation-in / allocation-out. Sub-materiality cost centers roll into a "Miscellaneous
                (below materiality)" row so totals tie out.

                Args:
                    data_source: Name or id of the **destination** (a FOCUS data-lake export connection).
                        Omit to auto-resolve the environment's single FOCUS data lake.
                    period: ``YYYY-MM`` or "last_month" — snaps to month boundaries.
                    since_days: Rolling-window fallback when ``period`` is omitted.
                    include_unallocated: Hide the orphan (untagged) bucket when False. Default True.
                    materiality_threshold: Roll cost centers below this USD amount into one "Miscellaneous"
                        row. Defaults to CHARGEBACK_MATERIALITY_THRESHOLD_USD. Pass 0 to disable.
                    cost_center_column / org_column: Override discovered cost-center / org columns.
                    cost_column / period_column: Override cost / period columns.
                    environment_id: Scope.
        """
        tool = "chargeback_by_cost_center"
        try:
            (
                dc,
                _schema,
                cost_col,
                df,
                period_label,
                (cc_col, org_col, provider_col),
                (alloc_src, alloc_dst),
                has_alloc,
                dim_label,
            ) = await _read_cc_frame(
                soe,
                tool,
                data_source,
                environment_id,
                cost_column=cost_column,
                period_column=period_column,
                cost_center_column=cost_center_column,
                org_column=org_column,
                period=period,
                since_days=since_days,
            )
        except cm.ToolRefusal as exc:
            return str(exc)
        if df.is_empty():
            return f"No rows in the destination for {period_label}."
        if materiality_threshold is None:
            materiality_threshold = get_chargeback_settings().materiality_threshold_usd

        rows = _rollup_cost_center(df, cost_col, cc_col, org_col, provider_col, (alloc_src, alloc_dst))

        # Roll (cost_center, org, provider) → per-cost-center, keeping per-provider notes.
        rollup: dict[str, dict] = {}
        for r in rows:
            cc = r["cost_center"]
            bucket = rollup.setdefault(
                cc,
                {
                    "cost_center": cc,
                    "cost_center_name": CC_NAMES.get(cc, cc),
                    "organization": r["organization"],
                    "direct_cost": 0.0,
                    "allocation_in": 0.0,
                    "allocation_out": 0.0,
                    "net_chargeback": 0.0,
                    "providers": [],
                },
            )
            bucket["direct_cost"] += r["direct_cost"]
            bucket["allocation_in"] += r["allocation_in"]
            bucket["allocation_out"] += r["allocation_out"]
            bucket["providers"].append(r)
        for bucket in rollup.values():
            bucket["direct_cost"] = round(bucket["direct_cost"], 2)
            bucket["allocation_in"] = round(bucket["allocation_in"], 2)
            bucket["allocation_out"] = round(bucket["allocation_out"], 2)
            bucket["net_chargeback"] = round(
                bucket["direct_cost"] + bucket["allocation_in"] + bucket["allocation_out"], 2
            )
            bucket["notes"] = fmt.provider_notes(bucket["providers"])

        rows_sorted = sorted(rollup.values(), key=lambda b: -abs(b["net_chargeback"]))
        if not include_unallocated:
            rows_sorted = [r for r in rows_sorted if r["cost_center"] != "(unallocated)"]

        # Materiality rollup: keep ≥ threshold, combine the rest into one Miscellaneous row.
        kept = [r for r in rows_sorted if abs(r["net_chargeback"]) >= materiality_threshold]
        below = [r for r in rows_sorted if abs(r["net_chargeback"]) < materiality_threshold]
        if below:
            kept.append(
                {
                    "cost_center": "Miscellaneous (below materiality)",
                    "cost_center_name": "Miscellaneous (below materiality)",
                    "organization": "(various)",
                    "direct_cost": round(sum(r["direct_cost"] for r in below), 2),
                    "allocation_in": round(sum(r["allocation_in"] for r in below), 2),
                    "allocation_out": round(sum(r["allocation_out"] for r in below), 2),
                    "net_chargeback": round(sum(r["net_chargeback"] for r in below), 2),
                    "notes": f"{len(below)} cost centers combined (threshold ${materiality_threshold:.2f})",
                }
            )

        body = []
        for row in kept:
            name = row["cost_center"]
            if row["cost_center_name"] and row["cost_center_name"] != name:
                name = f"{name} ({row['cost_center_name']})"
            body.append(
                "| "
                + " | ".join(
                    [
                        name,
                        row["organization"],
                        fmt.fmt_money(row["direct_cost"]),
                        fmt.fmt_money(row["allocation_in"]),
                        fmt.fmt_money(row["allocation_out"]),
                        fmt.fmt_money(row["net_chargeback"]),
                        row["notes"],
                    ]
                )
                + " |"
            )
        totals = {
            k: round(sum(r[k] for r in kept), 2)
            for k in ("direct_cost", "allocation_in", "allocation_out", "net_chargeback")
        }
        body.append(
            f"| **TOTAL** |  | **{fmt.fmt_money(totals['direct_cost'])}** | "
            f"**{fmt.fmt_money(totals['allocation_in'])}** | **{fmt.fmt_money(totals['allocation_out'])}** | "
            f"**{fmt.fmt_money(totals['net_chargeback'])}** |  |"
        )

        records = int(df.select(pl.col("row_count").sum()).item() or 0) if "row_count" in df.columns else df.height
        return "\n".join(
            [
                f"# Chargeback by {_DIM_DISP.get(dim_label, 'cost center')} — {period_label}",
                f"source: `{getattr(dc, 'name', data_source)}`  ·  {records:,} charge records in window"
                + ("" if has_alloc else "  ·  _no allocation columns — direct cost only_"),
                "",
                "| Cost Center | Org | Direct | Allocation in | Allocation out | Net Chargeback | Notes |\n"
                "|---|---|---:|---:|---:|---:|---|\n" + "\n".join(body),
                "",
                "Render the table VERBATIM — do NOT collapse the lineage columns. Negative numbers "
                "are credits (in parentheses). Summarize the TOTAL row in 1–2 sentences.",
                "",
                "Typical next steps: chargeback_provider_lineage(period=…) to drill into one cost "
                "center, then generate_chargeback_invoices(period=…).",
            ]
        )

    @mcp.tool
    async def chargeback_provider_lineage(
        data_source: str = "",
        period: str | None = None,
        since_days: int = 30,
        cost_center: str | None = None,
        cost_center_column: str | None = None,
        org_column: str | None = None,
        provider_column: str | None = None,
        cost_column: str | None = None,
        period_column: str | None = None,
        environment_id: str | None = None,
    ) -> str:
        """Per-cost-center chargeback decomposed into provider lineage.

        Groups by (cost_center, provider) and, when the destination exposes
        ``x_AllocationStatusSource``/``Destination``, splits each cost center into direct /
        allocation-in (shared services consumed) / allocation-out (credit for being a shared
        platform). Reads a FOCUS data-lake **destination** (what Stitcher has written) via the SOE
        focus-query SQL path.

        Args:
            data_source: Name or id of the **destination** (a FOCUS data-lake export connection).
                Omit to auto-resolve the environment's single FOCUS data lake.
            period: ``YYYY-MM`` or "last_month".
            since_days: Rolling-window fallback when ``period`` is omitted.
            cost_center: Filter to one cost-center value (e.g. "cc-120").
            cost_center_column / org_column / provider_column: Override discovered columns.
            cost_column / period_column: Override cost / period columns.
            environment_id: Scope.
        """
        tool = "chargeback_provider_lineage"
        try:
            dc, _schema, cost_col, df, period_label, (cc_col, org_col, provider_col), _alloc, has_alloc, dim_label = (
                await _read_cc_frame(
                    soe,
                    tool,
                    data_source,
                    environment_id,
                    cost_column=cost_column,
                    period_column=period_column,
                    cost_center_column=cost_center_column,
                    org_column=org_column,
                    provider_column=provider_column,
                    cost_center=cost_center,
                    period=period,
                    since_days=since_days,
                )
            )
        except cm.ToolRefusal as exc:
            return str(exc)
        if df.is_empty():
            return f"No rows in the destination for {period_label}."

        rows = _rollup_cost_center(df, cost_col, cc_col, org_col, provider_col, _alloc)
        for r in rows:
            r["net_chargeback"] = round(r["direct_cost"] + r["allocation_in"] + r["allocation_out"], 2)
        rows.sort(key=lambda r: -abs(r["net_chargeback"]))

        def _cells(r: dict) -> list[str]:
            cells = [r["cost_center"], r["organization"], r["provider"], fmt.fmt_money(r["direct_cost"])]
            if has_alloc:
                cells += [
                    fmt.fmt_money(r["allocation_in"]),
                    fmt.fmt_money(r["allocation_out"]),
                    fmt.fmt_money(r["net_chargeback"]),
                ]
            return cells

        header = (
            (
                "| Cost Center | Org | Provider | Direct | Allocation in | Allocation out | Net |\n"
                "|---|---|---:|---:|---:|---:|---:|"
            )
            if has_alloc
            else ("| Cost Center | Org | Provider | Direct Cost |\n|---|---|---:|---:|")
        )
        lines = [
            f"# Chargeback provider lineage — {period_label}"
            + (f"  (cost center: {cost_center})" if cost_center else ""),
            f"source: `{getattr(dc, 'name', data_source)}`  ·  {df.height:,} groups in window",
            "",
            header,
        ]
        lines.extend("| " + " | ".join(_cells(r)) + " |" for r in rows)
        lines += [
            "",
            (
                "Legend: direct = provider invoices tagged to the CC; allocation in = shared "
                "services consumed; allocation out = credit for being a shared platform "
                "(negative, in parentheses). net = direct + in + out."
                if has_alloc
                else "No x_Allocation* columns — direct cost per provider only."
            ),
        ]
        return "\n".join(lines)

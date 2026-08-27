"""Chargeback report tools — ``chargeback_by_billing_account``, ``chargeback_by_cost_center``,
``chargeback_provider_lineage``.

Read a FOCUS data-lake **destination** (what Stitcher has written) via the SOE focus-query SQL
path (``cost_reader.read_destination_dataframe`` → ``_run_focus_query``) and aggregate in polars.
Chargeback only reads **destinations** — never source datasources — so the column defaults are
FOCUS-normalized (``BilledCost`` / ``ChargePeriodStart`` / ``x_CostCenter`` / …). Omit
``data_source`` to auto-resolve the environment's single FOCUS data lake.

Allocation lineage (direct / allocation-in / allocation-out) is only computed when the
destination exposes ``x_AllocationStatusSource``/``Destination``. Period resolution, materiality
rollup, and markdown rendering follow the harness house style.
"""

from __future__ import annotations

import polars as pl
from fastmcp import FastMCP

from . import cost_reader as cr
from . import formatting as fmt
from .period import resolve_period
from .schema_tools import _resolve_env_id
from .settings import CC_NAMES, get_chargeback_settings


def _project_cost_center_name(cc: str) -> str:
    """Human name for a cost-center key (registry lookup, else the raw key)."""
    return CC_NAMES.get(cc, cc)


def _no_cost_center_refusal(tool: str, schema: dict) -> str:
    return (
        f"ERR ({tool}): could not identify a cost-center column in the datasource. "
        f"Columns: {', '.join(sorted(schema))}. Call discover_cost_schema, then pass "
        f"cost_center_column explicitly (e.g. 'x_CostCenter')."
    )


def _no_cost_column_refusal(tool: str, schema: dict) -> str:
    return (
        f"ERR ({tool}): could not identify a cost column in the datasource. "
        f"Columns: {', '.join(sorted(schema))}. Pass cost_column explicitly."
    )


def _rollup_cost_center(
    df: pl.DataFrame,
    cost_col: str,
    cc_col: str,
    org_col: str | None,
    provider_col: str | None,
    alloc: tuple[str | None, str | None],
    start,
    end,
    period_label: str,
) -> list[dict]:
    """Aggregate the (already period-filtered) frame into per-(cost_center, provider) rows with
    direct / allocation-in / allocation-out buckets (allocation only when the columns exist)."""
    alloc_src, alloc_dst = alloc
    if not cc_col:
        raise RuntimeError("cost-center column is required")
    gb = [cc_col]
    for col in (org_col, provider_col):
        if col and col not in gb and col in df.columns:
            gb.append(col)

    cost = pl.col(cost_col).cast(pl.Float64, strict=False)
    select_cols = [*gb, cost_col]
    if alloc_src is not None and alloc_dst is not None and alloc_src in df.columns and alloc_dst in df.columns:
        select_cols += [alloc_src, alloc_dst]
    if alloc_src is not None and alloc_dst is not None and alloc_src in df.columns and alloc_dst in df.columns:
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
        key = (cc, org, provider)
        bucket = out.setdefault(
            key,
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
        try:
            _resolve_env_id(soe, environment_id)
        except RuntimeError as exc:
            return str(exc)
        tool = "chargeback_by_billing_account"
        try:
            dc = cr.load_data_connection(soe, data_source)
        except Exception as e:  # noqa: BLE001
            return f"ERR ({tool}): could not load destination {data_source!r}: {str(e)[:250]}"
        schema = cr.read_cost_schema(soe, dc)
        if not schema:
            return f"ERR ({tool}): no schema discovered for destination {data_source!r}."
        cost_col = cr.resolve_cost_column(schema, cost_column)
        if not cost_col:
            return _no_cost_column_refusal(tool, schema)
        ba_col = cr.resolve_billing_account_column(schema, billing_account_column)
        if not ba_col:
            return (
                f"ERR ({tool}): could not identify a billing-account column. Columns: "
                f"{', '.join(sorted(schema))}. Pass billing_account_column explicitly."
            )
        period_col = cr.resolve_period_column(schema, period_column)
        top_n = max(1, min(top_n, 200))
        since_days = max(1, min(since_days, 365))
        start, end, period_label = resolve_period(period, since_days)

        try:
            df = cr.read_aggregated_cost(
                soe, dc, [ba_col], cost_col, period_col, start, end, None, schema.get(period_col), top_n=top_n
            )
        except Exception as e:  # noqa: BLE001
            return f"ERR ({tool}): could not read destination: {str(e)[:300]}"
        if df.is_empty():
            return f"No rows in the destination for {period_label}."

        agg = cr.aggregate_cost(df, [ba_col], cost_col, top_n)
        total = float(df.select(pl.col(cost_col).cast(pl.Float64, strict=False).sum()).item() or 0.0)

        lines = [
            f"# Chargeback by {ba_col} — {period_label}",
            f"source: `{getattr(dc, 'name', data_source)}`  ·  {df.height} charge records in window",
            "",
            "| Account / Project | Cost | Rows | % Share |",
            "|---|---|---:|---:|",
        ]
        for r in agg.iter_rows(named=True):
            dim = r.get(ba_col)
            dim_s = "" if dim is None else str(dim)
            cost = float(r.get("cost") or 0.0)
            share = (cost / total * 100.0) if total else 0.0
            lines.append(f"| {dim_s} | {fmt.fmt_money(cost)} | {r.get('row_count', 0)} | {share:.1f}% |")
        lines.append("")
        lines.append(f"**Total: {fmt.fmt_money(total)}** across {df.height} charge records.")
        return "\n".join(lines)

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
        try:
            _resolve_env_id(soe, environment_id)
        except RuntimeError as exc:
            return str(exc)
        tool = "chargeback_by_cost_center"
        try:
            dc = cr.load_data_connection(soe, data_source)
        except Exception as e:  # noqa: BLE001
            return f"ERR ({tool}): could not load destination {data_source!r}: {str(e)[:250]}"
        schema = cr.read_cost_schema(soe, dc)
        if not schema:
            return f"ERR ({tool}): no schema discovered for destination {data_source!r}."
        cost_col = cr.resolve_cost_column(schema, cost_column)
        if not cost_col:
            return _no_cost_column_refusal(tool, schema)
        classification = await cr.classify_org_cost_center(schema)
        cc_col = await cr.resolve_cost_center_column(schema, cost_center_column, classification)
        if not cc_col:
            return _no_cost_center_refusal(tool, schema)
        org_col = await cr.resolve_org_column(schema, org_column, classification) or cc_col
        alloc_src, alloc_dst = cr.resolve_allocation_columns(schema)
        has_alloc = bool(alloc_src and alloc_dst)
        period_col = cr.resolve_period_column(schema, period_column)
        since_days = max(1, min(since_days, 365))
        start, end, period_label = resolve_period(period, since_days)
        if materiality_threshold is None:
            materiality_threshold = get_chargeback_settings().materiality_threshold_usd

        provider_col = cr.resolve_provider_column(schema)
        group_cols = [cc_col]
        for col in (org_col, provider_col):
            if col and col in schema and col not in group_cols:
                group_cols.append(col)
        try:
            df = cr.read_aggregated_cost(
                soe,
                dc,
                group_cols,
                cost_col,
                period_col,
                start,
                end,
                None,
                schema.get(period_col),
                allocation_src=alloc_src,
                allocation_dst=alloc_dst,
                top_n=200,
            )
        except Exception as e:  # noqa: BLE001
            return f"ERR ({tool}): could not read destination: {str(e)[:300]}"
        if df.is_empty():
            return f"No rows in the destination for {period_label}."

        rows = _rollup_cost_center(
            df, cost_col, cc_col, org_col, provider_col, (alloc_src, alloc_dst), start, end, period_label
        )

        # Roll (cost_center, org, provider) → per-cost-center, keeping per-provider notes.
        rollup: dict[str, dict] = {}
        for r in rows:
            cc = r["cost_center"]
            bucket = rollup.setdefault(
                cc,
                {
                    "cost_center": cc,
                    "cost_center_name": _project_cost_center_name(cc),
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
            bucket["providers"].append(
                {
                    "provider": r["provider"],
                    "direct_cost": r["direct_cost"],
                    "allocation_in": r["allocation_in"],
                    "allocation_out": r["allocation_out"],
                }
            )
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

        # Materiality rollup.
        kept = [r for r in rows_sorted if abs(r["net_chargeback"]) >= materiality_threshold]
        filtered = [r for r in rows_sorted if r not in kept]
        if filtered:
            kept.append(
                {
                    "cost_center": "Miscellaneous (below materiality)",
                    "cost_center_name": "Miscellaneous (below materiality)",
                    "organization": "(various)",
                    "direct_cost": round(sum(r["direct_cost"] for r in filtered), 2),
                    "allocation_in": round(sum(r["allocation_in"] for r in filtered), 2),
                    "allocation_out": round(sum(r["allocation_out"] for r in filtered), 2),
                    "net_chargeback": round(sum(r["net_chargeback"] for r in filtered), 2),
                    "providers": [],
                    "notes": f"{len(filtered)} cost centers combined (threshold ${materiality_threshold:.2f})",
                }
            )
        rows_sorted = kept

        header = (
            "| Cost Center | Org | Direct | Allocation in | Allocation out | Net Chargeback | Notes |\n"
            "|---|---|---:|---:|---:|---:|---|"
        )
        body = []
        for row in rows_sorted:
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
            "direct": round(sum(r["direct_cost"] for r in rows_sorted), 2),
            "in": round(sum(r["allocation_in"] for r in rows_sorted), 2),
            "out": round(sum(r["allocation_out"] for r in rows_sorted), 2),
            "net": round(sum(r["net_chargeback"] for r in rows_sorted), 2),
        }
        body.append(
            "| **TOTAL** |  | "
            f"**{fmt.fmt_money(totals['direct'])}** | "
            f"**{fmt.fmt_money(totals['in'])}** | "
            f"**{fmt.fmt_money(totals['out'])}** | "
            f"**{fmt.fmt_money(totals['net'])}** |  |"
        )

        return "\n".join(
            [
                f"# Chargeback by cost center — {period_label}",
                f"source: `{getattr(dc, 'name', data_source)}`  ·  {df.height} charge records in window"
                + ("" if has_alloc else "  ·  _no allocation columns — direct cost only_"),
                "",
                header + "\n" + "\n".join(body),
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
        try:
            _resolve_env_id(soe, environment_id)
        except RuntimeError as exc:
            return str(exc)
        tool = "chargeback_provider_lineage"
        try:
            dc = cr.load_data_connection(soe, data_source)
        except Exception as e:  # noqa: BLE001
            return f"ERR ({tool}): could not load destination {data_source!r}: {str(e)[:250]}"
        schema = cr.read_cost_schema(soe, dc)
        if not schema:
            return f"ERR ({tool}): no schema discovered for destination {data_source!r}."
        cost_col = cr.resolve_cost_column(schema, cost_column)
        if not cost_col:
            return _no_cost_column_refusal(tool, schema)
        classification = await cr.classify_org_cost_center(schema)
        cc_col = await cr.resolve_cost_center_column(schema, cost_center_column, classification)
        if not cc_col:
            return _no_cost_center_refusal(tool, schema)
        org_col = await cr.resolve_org_column(schema, org_column, classification) or cc_col
        provider_col = cr.resolve_provider_column(schema, provider_column)
        alloc_src, alloc_dst = cr.resolve_allocation_columns(schema)
        has_alloc = bool(alloc_src and alloc_dst)
        period_col = cr.resolve_period_column(schema, period_column)
        since_days = max(1, min(since_days, 365))
        start, end, period_label = resolve_period(period, since_days)

        group_cols = [cc_col]
        for col in (org_col, provider_col):
            if col and col in schema and col not in group_cols:
                group_cols.append(col)
        filters = {cc_col: cost_center} if cost_center is not None else None
        try:
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
        except Exception as e:  # noqa: BLE001
            return f"ERR ({tool}): could not read destination: {str(e)[:300]}"
        # SQL already applied the period + cost_center filters; keep polars filter as a no-op safety net.
        if cost_center is not None and cc_col in df.columns:
            df = df.filter(pl.col(cc_col) == cost_center)
        if df.is_empty():
            return f"No rows in the destination for {period_label}."

        rows = _rollup_cost_center(
            df, cost_col, cc_col, org_col, provider_col, (alloc_src, alloc_dst), start, end, period_label
        )
        for r in rows:
            r["net_chargeback"] = round(r["direct_cost"] + r["allocation_in"] + r["allocation_out"], 2)
        rows.sort(key=lambda r: -abs(r["net_chargeback"]))

        header = "| Cost Center | Org | Provider | Direct | Allocation in | Allocation out | Net |"
        header += "" if has_alloc else " Direct Cost |"
        lines = [
            f"# Chargeback provider lineage — {period_label}"
            + (f"  (cost center: {cost_center})" if cost_center else ""),
            f"source: `{getattr(dc, 'name', data_source)}`  ·  {df.height} charge records in window",
            "",
            (
                (header + "\n" + "|---|---|---:|---:|---:|---:|---:|")
                if has_alloc
                else ("| Cost Center | Org | Provider | Direct Cost |\n|---|---|---:|---:|")
            ),
        ]
        for r in rows:
            if has_alloc:
                lines.append(
                    f"| {r['cost_center']} | {r['organization']} | {r['provider']} | "
                    f"{fmt.fmt_money(r['direct_cost'])} | {fmt.fmt_money(r['allocation_in'])} | "
                    f"{fmt.fmt_money(r['allocation_out'])} | {fmt.fmt_money(r['net_chargeback'])} |"
                )
            else:
                lines.append(
                    f"| {r['cost_center']} | {r['organization']} | {r['provider']} | "
                    f"{fmt.fmt_money(r['direct_cost'])} |"
                )
        lines.append("")
        if has_alloc:
            lines.append(
                "Legend: direct = provider invoices tagged to the CC; allocation in = shared "
                "services consumed; allocation out = credit for being a shared platform "
                "(negative, in parentheses). net = direct + in + out."
            )
        else:
            lines.append("No x_Allocation* columns — direct cost per provider only.")
        return "\n".join(lines)

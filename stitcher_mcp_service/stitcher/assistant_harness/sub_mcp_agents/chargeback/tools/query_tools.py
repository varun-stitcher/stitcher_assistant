"""Ad-hoc FOCUS cost query tool — ``query_focus_cost`` (synchronous, destination SQL path).

Reads a FOCUS data-lake **destination** (what Stitcher has written) via the SOE focus-query SQL
path (``cost_reader.read_destination_dataframe`` → ``_run_focus_query``) and aggregates in
polars. Chargeback only reads **destinations** — never arbitrary source datasources — so the
column defaults are FOCUS-normalized (``BilledCost`` / ``ChargePeriodStart`` / …). Omit
``data_source`` to auto-resolve the environment's single FOCUS data lake.

Unknown ``group_by`` / ``cost_column`` values are refused (never guessed); allocation-based
group_by is only offered when the destination exposes the matching ``x_Allocation*`` column.
Period + equality filters are pushed into the SQL WHERE (query-parameter bound — no injection).
"""

from __future__ import annotations

import polars as pl
from fastmcp import FastMCP

from . import cost_reader as cr
from . import formatting as fmt
from .period import resolve_period
from .schema_tools import _resolve_env_id


def register(mcp: FastMCP, client, soe) -> None:
    @mcp.tool
    async def query_focus_cost(
        data_source: str = "",
        group_by: str = "service",
        period: str | None = None,
        since_days: int = 30,
        top_n: int = 20,
        metric: str = "billed",
        cost_column: str | None = None,
        period_column: str | None = None,
        filters: dict | None = None,
        environment_id: str | None = None,
    ) -> str:
        """General-purpose cost query against a FOCUS data-lake **destination** (synchronous).

        Aggregates cost by a dimension (any column, or a short alias: service, provider,
        billing_account, region) over a period, with optional equality filters. Reads the
        destination via the SOE focus-query SQL path (BigQuery / Snowflake) and aggregates in
        polars. Chargeback only reads destinations (what Stitcher has written) — never source
        datasources.

        Args:
            data_source: Name or id of the **destination** (a FOCUS data-lake export connection).
                Omit to auto-resolve the environment's single FOCUS data lake.
            group_by: Dimension column (a literal column name or an alias: service, provider,
                billing_account, region). Default ``service``.
            period: ``YYYY-MM`` (e.g. "2026-03") or "last_month" — overrides since_days.
            since_days: Rolling-window fallback when ``period`` is omitted.
            top_n: Max rows (capped at 200).
            metric: Which cost column to sum — ``billed`` (default), ``list``, or ``effective``.
            cost_column: Override the cost column (default: discovered — BilledCost/…).
            period_column: Override the period column (default: ChargePeriodStart).
            filters: Optional ``{column: value}`` equality filters (pushed into the SQL WHERE).
            environment_id: Scope.
        """
        try:
            _resolve_env_id(soe, environment_id)
        except RuntimeError as exc:
            return str(exc)

        try:
            dc = cr.load_data_connection(soe, data_source)
        except Exception as e:  # noqa: BLE001
            return f"ERR (query_focus_cost): could not load destination {data_source!r}: {str(e)[:250]}"
        schema = cr.read_cost_schema(soe, dc)
        if not schema:
            return (
                f"ERR (query_focus_cost): no schema discovered for destination {data_source!r}."
            )

        # Resolve the cost column: explicit cost_column wins; else metric alias; else discovery.
        metric_map = {"billed": "BilledCost", "list": "ListCost", "effective": "EffectiveCost"}
        cost_col = cost_column
        metric_col = metric_map.get(metric.lower(), "")
        if not cost_col and metric_col in schema:
            cost_col = metric_col
        if not cost_col:
            cost_col = cr.resolve_cost_column(schema)
        if not cost_col:
            return (
                f"ERR (query_focus_cost): could not identify a cost column in the destination. "
                f"Columns seen: {', '.join(sorted(schema))}. Pass cost_column explicitly."
            )

        group_col = cr.resolve_group_by(schema, group_by)
        if not group_col:
            return (
                f"ERR (query_focus_cost): Invalid group_by {group_by!r}. It is not a column in "
                f"the destination nor a known alias (service, provider, billing_account, region). "
                f"Columns: {', '.join(sorted(schema))}."
            )

        period_col = cr.resolve_period_column(schema, period_column)
        top_n = max(1, min(top_n, 200))
        since_days = max(1, min(since_days, 365))
        start, end, period_label = resolve_period(period, since_days)

        # Aggregate IN SQL so we pull ~group-count rows, not the whole month.
        try:
            df = cr.read_aggregated_cost(
                soe, dc, [group_col], cost_col, period_col, start, end, filters, schema.get(period_col), top_n=top_n
            )
        except Exception as e:  # noqa: BLE001
            return f"ERR (query_focus_cost): could not read destination: {str(e)[:300]}"
        if df is None or df.is_empty():
            return f"No rows in the destination for {period_label}."

        # SQL already applied the period + equality filters; keep polars filters as a no-op safety net
        # (they no-op when the columns aren't in the frame).
        df = cr.equality_filters(df, filters)
        agg = cr.aggregate_cost(df, [group_col], cost_col, top_n)
        try:
            total = float(df.select(pl.col(cost_col).cast(pl.Float64, strict=False).sum()).item() or 0.0)
        except Exception:  # noqa: BLE001
            total = float(agg.select(pl.col("cost").sum()).item() or 0.0)

        header = [group_col, f"cost ({metric}, USD)", "rows", "% share"]
        lines = [
            f"# Cost by {group_col} — {period_label}",
            f"source: `{getattr(dc, 'name', data_source)}`  ·  {df.height} rows in window",
            "",
            "| " + " | ".join(header) + " |",
            "|" + "---|" * len(header),
        ]
        for r in agg.iter_rows(named=True):
            dim = r.get(group_col)
            dim_s = "" if dim is None else str(dim)
            cost = float(r.get("cost") or 0.0)
            share = (cost / total * 100.0) if total else 0.0
            lines.append(f"| {dim_s} | {fmt.fmt_money(cost)} | {r.get('row_count', 0)} | {share:.1f}% |")
        lines.append("")
        lines.append(f"**Total: {fmt.fmt_money(total)}** across {df.height} charge records.")
        return "\n".join(lines)

"""Ad-hoc FOCUS cost query tool — ``query_focus_cost`` (synchronous, destination SQL path).

Reads a FOCUS data-lake **destination** (what Stitcher has written) via the SOE focus-query SQL
path (``cost_reader.read_aggregated_cost`` — the GROUP BY/SUM is pushed into BigQuery/Snowflake).
Chargeback only reads **destinations** — never arbitrary source datasources — so the column
defaults are FOCUS-normalized (``BilledCost`` / ``ChargePeriodStart`` / …). Omit ``data_source``
to auto-resolve the environment's single FOCUS data lake.

Unknown ``group_by`` / ``cost_column`` values are refused (never guessed); period + equality
filters are pushed into the SQL WHERE (query-parameter bound — no injection).
"""

from __future__ import annotations

from fastmcp import FastMCP

from . import common as cm
from . import cost_reader as cr


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

        Aggregates cost by one or more dimensions (any column, or a short alias: service, provider,
        billing_account, region, cost_center, organization) over a period, with optional equality
        filters. Pass comma-separated dimensions for a cross-tab, e.g.
        ``group_by="cost_center,service"`` → one row per (cost center, service) pair. Reads the
        destination via the SOE focus-query SQL path (BigQuery / Snowflake). Chargeback only
        reads destinations (what Stitcher has written) — never source datasources.

        Args:
            data_source: Name or id of the **destination** (a FOCUS data-lake export connection).
                Omit to auto-resolve the environment's single FOCUS data lake.
            group_by: Dimension column(s) — a literal column name or an alias (service, provider,
                billing_account, region, cost_center, organization), comma-separated for a
                cross-tab. Default ``service``.
            period: ``YYYY-MM`` (e.g. "2026-03") or "last_month" — overrides since_days.
            since_days: Rolling-window fallback when ``period`` is omitted.
            top_n: Max rows (capped at 200).
            metric: Which cost column to sum — ``billed`` (default), ``list``, or ``effective``.
            cost_column: Override the cost column (default: discovered — BilledCost/…).
            period_column: Override the period column (default: ChargePeriodStart).
            filters: Optional ``{column: value}`` equality filters (pushed into the SQL WHERE).
            environment_id: Scope.
        """
        tool = "query_focus_cost"
        try:
            dc, schema, cost_col = cm.prep_read(soe, tool, data_source, environment_id, cost_column)
        except cm.ToolRefusal as exc:
            return str(exc)

        # Explicit cost_column wins; else the metric alias; else discovery.
        if not cost_col:
            metric_col = {"billed": "BilledCost", "list": "ListCost", "effective": "EffectiveCost"}.get(
                metric.lower(), ""
            )
            cost_col = metric_col if metric_col in schema else cr.resolve_cost_column(schema)
        if not cost_col:
            return cm.refusal(tool, "cost", schema)

        # Resolve every group_by token (literal column name or alias); dedupe, cap at 4 dims.
        tokens = [t.strip() for t in group_by.split(",") if t.strip()] or ["service"]
        if len(tokens) > 4:
            return f"ERR ({tool}): at most 4 group_by dimensions are supported, got {len(tokens)}."
        dim_cols: list[str] = []
        for tok in tokens:
            col = cr.resolve_group_by(schema, tok)
            if not col:
                return (
                    f"ERR ({tool}): Invalid group_by {tok!r}. It is not a column in "
                    f"the destination nor a known alias (service, provider, billing_account, region, "
                    f"cost_center, organization). Columns: {', '.join(sorted(schema))}."
                )
            if col not in dim_cols:
                dim_cols.append(col)

        period_col = cr.resolve_period_column(schema, period_column)
        start, end, period_label = cm.resolve_window(period, since_days)

        # Aggregate IN SQL so we pull ~group-count rows, not the whole month.
        try:
            df = cr.read_aggregated_cost(
                soe, dc, dim_cols, cost_col, period_col, start, end, filters, schema.get(period_col), top_n=200
            )
        except Exception as e:  # noqa: BLE001
            return f"ERR ({tool}): could not read destination: {str(e)[:300]}"
        if df.is_empty():
            return f"No rows in the destination for {period_label}."

        rows, total, records = cm.cost_summary(df, dim_cols, cost_col, max(1, min(top_n, 200)))
        return cm.share_table(
            f"Cost by {' + '.join(dim_cols)}",
            getattr(dc, "name", data_source),
            records,
            period_label,
            dim_cols,
            total,
            rows,
            metric=metric.lower() or "billed",
        )

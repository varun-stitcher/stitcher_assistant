"""Shared tool prelude for the chargeback sub-MCP tools.

One place for the ceremony every tool repeats: the env-scope check, destination load, schema
probe, cost-column resolution (+ the standard refusals), the ``since_days`` clamp + period
window, and the shared "share table" markdown renderer. Tool bodies stay thin: call
``prep_read`` (catching :class:`ToolRefusal`) and render.
"""

from __future__ import annotations

import polars as pl

from . import cost_reader as cr
from . import formatting as fmt
from .period import resolve_period


class ToolRefusal(RuntimeError):
    """A tool-level refusal, already formatted for return to the agent (starts with ``ERR``)."""


def resolve_env_id(soe, environment_id: str | None) -> str:
    """Resolve the env to operate on; raises a clear error when unscoped."""
    env_id = environment_id or soe.environment_id
    if not env_id:
        raise RuntimeError("ERR: no STITCHER_ENVIRONMENT_ID — chargeback tools are environment-scoped.")
    return env_id


def refusal(tool: str, what: str, schema: dict, override: str | None = None) -> str:
    """The standard cannot-identify-column refusal (names the schema, asks for the override)."""
    return (
        f"ERR ({tool}): could not identify a {what} column in the datasource. "
        f"Columns: {', '.join(sorted(schema))}. Pass {override or f'{what}_column'} explicitly."
    )


def resolve_window(period: str | None, since_days: int) -> tuple:
    """Clamp ``since_days`` to [1, 365] and resolve the period window (raises ValueError on a bad
    ``period`` — never guesses)."""
    return resolve_period(period, max(1, min(since_days, 365)))


def prep_read(
    soe,
    tool: str,
    data_source: str,
    environment_id: str | None,
    cost_column: str | None = None,
    require_cost: bool = True,
):
    """The shared tool prelude: env-scope check → load destination → schema → cost column.

    Returns ``(dc, schema, cost_col)``; raises :class:`ToolRefusal` (``ERR``-prefixed) for every
    refusal boundary so each tool body is a straight line after this call.
    """
    try:
        resolve_env_id(soe, environment_id)
    except RuntimeError as exc:
        raise ToolRefusal(str(exc)) from None
    try:
        dc = cr.resolve_destination(soe, data_source)
    except Exception as e:  # noqa: BLE001 — surface as a refusal, never a raw traceback
        raise ToolRefusal(f"ERR ({tool}): could not load destination {data_source!r}: {str(e)[:250]}") from e
    schema = cr.read_cost_schema(soe, dc)
    if not schema:
        raise ToolRefusal(f"ERR ({tool}): no schema discovered for destination {data_source!r}.")
    cost_col = cr.resolve_cost_column(schema, cost_column)
    if require_cost and not cost_col:
        raise ToolRefusal(refusal(tool, "cost", schema))
    return dc, schema, cost_col


def cost_summary(df: pl.DataFrame, dim_cols: list[str], cost_col: str, top_n: int):
    """Shape a cost frame into ``[dims…, cost, row_count]`` rows sorted by cost desc.

    Handles both the SQL-aggregated frame (``row_count`` is summed per dimension, preserving the
    record counts the SQL ``GROUP BY`` computed) and a raw frame (re-aggregates in polars).
    Returns ``(rows, total_cost, record_count)`` — ``total`` is over the whole frame (not just
    the top_n head) so % shares are relative to the window, not the rendered slice.
    """
    has_rc = "row_count" in df.columns
    select = [*dim_cols, cost_col] + (["row_count"] if has_rc else [])
    rows = (
        df.select(select)
        .with_columns(pl.col(cost_col).cast(pl.Float64, strict=False).alias("_cost"))
        .group_by(dim_cols)
        .agg(
            pl.col("_cost").sum().round(2).alias("cost"),
            (pl.col("row_count").sum() if has_rc else pl.len()).alias("row_count"),
        )
        .sort("cost", descending=True)
        .head(top_n)
    )
    total = float(df.select(pl.col(cost_col).cast(pl.Float64, strict=False).sum()).item() or 0.0)
    records = int(df.select(pl.col("row_count").sum()).item() or 0) if has_rc else df.height
    return rows, total, records


def share_table(
    title: str,
    source: str,
    records: int,
    period_label: str,
    dim_cols: list[str],
    total: float,
    rows: pl.DataFrame,
    metric: str = "billed",
) -> str:
    """Render the shared ``| dims… | cost | rows | % share |`` markdown table + total line
    (one row per group; ``dim_cols`` may be a cross-tab of several dimensions)."""
    lines = [
        f"# {title} — {period_label}",
        f"source: `{source}`  ·  {records:,} charge records in window",
        "",
        "| " + " | ".join(dim_cols) + f" | cost ({metric}, USD) | rows | % share |",
        "|" + "---|" * (len(dim_cols) + 3),
    ]
    for r in rows.iter_rows(named=True):
        dims = ["" if r.get(c) is None else str(r.get(c)) for c in dim_cols]
        cost = float(r.get("cost") or 0.0)
        share = (cost / total * 100.0) if total else 0.0
        lines.append(
            f"| {' | '.join(dims)} | {fmt.fmt_money(cost)} | {int(r.get('row_count') or 0):,} | {share:.1f}% |"
        )
    lines += ["", f"**Total: {fmt.fmt_money(total)}** across {records:,} charge records."]
    return "\n".join(lines)

"""Destination cost-data reader — SOE focus-query SQL path (what Stitcher has WRITTEN).

Chargeback only ever reads **destinations** — the FOCUS-normalized tables Stitcher has *written*
(the environment's BigQuery / Snowflake export destinations) — never arbitrary source datasources.
A destination is a *queryable* data-lake: a DB export whose ``connection_parameters`` resolve to
a real table reference. SOE already owns the canonical resolution + query machinery in
``focus_query._resolve_focus_connection`` / ``_run_focus_query``; this module reuses it **as-is**
(importing the activity's helpers directly — the plan's Step-1 spike confirmed that import is
Temporal-free and works when launched from ``pi_coding_agent/`` where ``.env.local`` resolves):

  - **Resolve a destination:** ``resolve_destination(soe, name_or_id)`` → load a specific
    destination connection by name/id via ``DataConnType.DESTINATIONS``, or auto-resolve the
    environment's single queryable FOCUS data lake when omitted (preferring the canonical
    ``STITCHER_AI_DB_EXPORT_V1_0`` DB export, mirroring ``_resolve_focus_connection``).
  - **List destinations:** ``list_chargeback_destinations(soe)`` → the environment's queryable
    FOCUS destinations (the grounding surface — "what Stitcher has written").
  - **Schema (cheap, no scan):** ``read_cost_schema(soe, dc)`` → columns + dtypes via the SOE
    metadata operator (the same call ``discover_cost_schema`` uses to classify columns).
  - **Data (SQL path):** ``read_destination_dataframe(soe, dc, columns, period_col, start, end,
    filters, period_dtype)`` → builds a BigQuery-flavored ``SELECT … FROM {focus_table} WHERE …``
    with ``@name`` query parameters (period window + equality filters pushed down so we never pull
    the whole table), runs it via ``_run_focus_query`` (BigQuery ADC or Snowflake+Vault), and
    returns the rows as an eager polars DataFrame. The report/invoice/query tools then aggregate
    in polars (group_by + sum the cost column).

Column mapping uses FOCUS defaults (``BilledCost`` / ``ChargePeriodStart`` / ``BillingAccountId`` /
``x_CostCenter`` …) — destinations are FOCUS-normalized, so the defaults almost always apply, but
every column stays overridable. Allocation lineage (``x_AllocationStatusSource``/``Destination``)
is computed only when the destination exposes those columns.
"""

from __future__ import annotations

from datetime import date

import polars as pl

# ── Column discovery — enum-backed FOCUS names ONLY (destinations are FOCUS-normalized) ──
#
# Chargeback reads **destinations** (what Stitcher has WRITTEN — a FOCUS DB export), so discovery
# matches only the canonical ``FocusColumnNames``. There is deliberately NO raw-export fallback
# (``cost`` / ``usage_start_time`` / ``project.name`` / … are dead weight on a destination).
# Cost-column/period/provider/service/billing-account/region are static FOCUS enums. Org and
# cost-center live under the customer ``x_*`` namespace and are discovered by the LLM classifier
# (see ``classify_org_cost_center``) — with a deterministic ``x_*`` default as the escape hatch.

from stitcher.pipeline.common.focus_column_names import FocusColumnNames as _FocusCol
from stitcher.pipeline.common.column_names.stitcher_column_names import (
    StitcherColumnNames as _StitcherCol,
)

# cost(4) — the canonical FOCUS cost columns.
_COST_CANDIDATES = [
    _FocusCol.LIST_COST.value,
    _FocusCol.CONTRACTED_COST.value,
    _FocusCol.BILLED_COST.value,
    _FocusCol.EFFECTIVE_COST.value,
]
# period(4) — charge + billing period start/end.
_PERIOD_CANDIDATES = [
    _FocusCol.CHARGE_PERIOD_START.value,
    _FocusCol.CHARGE_PERIOD_END.value,
    _FocusCol.BILLING_PERIOD_START.value,
    _FocusCol.BILLING_PERIOD_END.value,
]
# provider(1) / service(1) — NOTE: Service and Provider are SEPARATE FOCUS columns.
_PROVIDER_CANDIDATES = [_FocusCol.PROVIDER.value]  # ProviderName
_SERVICE_CANDIDATES = [_FocusCol.SERVICE_NAME.value]  # ServiceName
# billing-account(6) — account + sub-account (Id/Name/Type each).
_BILLING_ACCOUNT_CANDIDATES = [
    _FocusCol.BILLING_ACCOUNT_ID.value,
    _FocusCol.BILLING_ACCOUNT_NAME.value,
    _FocusCol.BILLING_ACCOUNT_TYPE.value,
    _FocusCol.SUB_ACCOUNT_ID.value,
    _FocusCol.SUB_ACCOUNT_NAME.value,
    _FocusCol.SUB_ACCOUNT_TYPE.value,
]
# region(2) — RegionId / RegionName.
_REGION_CANDIDATES = [_FocusCol.REGION.value, _FocusCol.REGION_NAME.value]

# Deterministic x_* defaults for org / cost-center (the no-LLM escape hatch). These are just the
# conventional FOCUS x_* names — arbitrary customer x_* columns are classified by the LLM.
_ORG_X_DEFAULTS = ["x_Organization", "x_organization"]
_COST_CENTER_X_DEFAULTS = ["x_CostCenter", "x_cost_center"]

# Provider-prefixed x_* columns (vendor tags) are NEVER cost-center/org candidates — filtered out
# before LLM classification so a vendor tag can't be mistaken for an org/cost-center dimension.
_PROVIDER_X_PREFIXES = (
    "aws",
    "gcp",
    "azure",
    "google",
    "anthropic",
    "openai",
    "twilio",
    "snowflake",
    "confluent",
    "datadog",
    "github",
    "stripe",
    "sentry",
)

# Allocation lineage (Stitcher-specific custom columns) — canonical names from the
# StitcherColumnNames enum (future-proof; the `x_` namespace is customer-defined). The snake_case
# spellings are kept as legacy fallbacks for exports that normalized to snake_case.
_ALLOC_SRC_CANDIDATES = [
    _StitcherCol.ALLOCATION_STATUS_SOURCE.value,
    "x_allocation_status_source",
]
_ALLOC_DST_CANDIDATES = [
    _StitcherCol.ALLOCATION_STATUS_DESTINATION.value,
    "x_allocation_status_destination",
]

# Short aliases → candidate list, so group_by="service" / "billing_account" / "provider" /
# "region" work without typing the full column name. A literal column name always wins. There is
# deliberately no ``cost_center`` / ``organization`` / ``project`` alias — those dimensions are
# LLM-discovered (and must be passed as a real column name or an explicit override).
_ALIASES: dict[str, list[str]] = {
    "service": _SERVICE_CANDIDATES,
    "provider": _PROVIDER_CANDIDATES,
    "billing_account": _BILLING_ACCOUNT_CANDIDATES,
    "region": _REGION_CANDIDATES,
}


def _build_data_connection_util(soe):
    """SOE ``DataConnectionUtil`` (env-scoped; init triggers a Keycloak SA-JWT — network-gated)."""
    err = soe.scope_error()
    if err:
        raise RuntimeError(err)
    ten = soe.tenant_error()
    if ten:
        raise RuntimeError(ten)
    from stitcher.operation_executor.util.data_connection_util import DataConnectionUtil

    return DataConnectionUtil(soe.environment_id, soe.auth_tenant)


# ── Destination resolution (FOCUS data lakes — what Stitcher has WRITTEN) ─────


def _import_focus_query():
    """Import the SOE focus-query activity helpers (Temporal-free; the plan's Step-1 spike
    confirmed the import works from ``pi_coding_agent/`` where ``.env.local`` resolves)."""
    from stitcher.operation_executor.workflows.assistant_workflow.activities.focus_query import (
        _build_table_ref,
        _conn_engine,
        _is_focus_destination,
        _resolve_focus_connection,
        _run_focus_query,
    )
    from stitcher.pipeline.common.dataset_type import SupportedDatasets

    return _resolve_focus_connection, _run_focus_query, _conn_engine, _build_table_ref, _is_focus_destination, SupportedDatasets


def _queryable_destinations(dcu, destinations):
    """Narrow to destinations that resolve to a real queryable table ref (BigQuery/Snowflake)."""
    _resolve, _run, _engine, _table_ref, _is_focus, _ = _import_focus_query()
    return [c for c in destinations if _engine(c) is not None and _table_ref(c, _engine(c)) is not None]


def list_chargeback_destinations(soe):
    """List the environment's queryable FOCUS data-lake **destinations** (what Stitcher has
    written), deterministically ordered (canonical DB export first, then by name).

    Returns a list of ``DataConnectionResponse`` objects. Empty when the environment has no
    queryable destination (only file exports / none configured)."""
    from stitcher.webservice.client import DataConnType

    dcu = _build_data_connection_util(soe)
    destinations = dcu.list_data_connections(DataConnType.DESTINATIONS) or []
    _resolve, _run, _engine, _table_ref, _is_focus, SupportedDatasets = _import_focus_query()
    queryable = [c for c in destinations if _engine(c) is not None and _table_ref(c, _engine(c)) is not None]
    if not queryable:
        return []
    db_export = [
        c for c in queryable
        if str(getattr(c, "dataset_name", "") or "") == SupportedDatasets.STITCHER_AI_DB_EXPORT_V1_0.value
    ]
    focus_hinted = [c for c in queryable if _is_focus(c)]
    ordered = (db_export or focus_hinted or queryable)
    # Stable, deterministic order by name (ties broken by id).
    return sorted(ordered, key=lambda c: (str(getattr(c, "name", "") or ""), str(getattr(c, "id", "") or "")))


def resolve_destination(soe, name_or_id: str = ""):
    """Resolve a FOCUS data-lake **destination** to read chargeback from.

    - With a ``name_or_id``: load that specific destination connection via
      ``DataConnType.DESTINATIONS`` (raises a clear error if it isn't a destination / is unknown).
    - Without one: auto-resolve the environment's single queryable FOCUS data lake (mirroring SOE's
      ``_resolve_focus_connection`` — prefers the canonical ``STITCHER_AI_DB_EXPORT_V1_0`` DB
      export, then any queryable FOCUS-hinted destination).

    Raises ``RuntimeError`` with an actionable message when no queryable destination exists.
    """
    dcu = _build_data_connection_util(soe)
    if name_or_id:
        from stitcher.webservice.client import DataConnType

        return dcu.get_data_connection(name_or_id, DataConnType.DESTINATIONS)
    _resolve, _run, _engine, _table_ref, _is_focus, _ = _import_focus_query()
    conn = _resolve(dcu, None)
    if conn is None:
        raise RuntimeError(
            "No FOCUS data-lake destination found for this environment. Chargeback only reads "
            "destinations (what Stitcher has written). Configure a BigQuery or Snowflake export "
            "destination and try again."
        )
    return conn


def load_data_connection(soe, name_or_id: str = ""):
    """Load a chargeback **destination** by name/id, or auto-resolve the FOCUS lake when omitted.

    (Name kept for compatibility with the tool layer + tests; this now resolves DESTINATIONS, not
    source datasources — chargeback reads what Stitcher has written.)"""
    return resolve_destination(soe, name_or_id)


def read_cost_schema(soe, dc) -> dict[str, str]:
    """Columns + dtypes for a FOCUS **destination**, via a ``LIMIT 1`` focus query (polars infers
    dtypes from the returned row). The SOE metadata operator used for source datasources doesn't
    resolve destination connections, so we probe the table directly through the same SQL path the
    data read uses (one row, then ``pl.DataFrame(rows).schema``).

    Returns ``{column: dtype_name}`` (dtype names are polars stringifications, e.g. ``Datetime`` /
    ``Float64`` / ``Utf8``); empty for an empty table when the engine can't list columns.
    """
    _resolve, _run, _engine, _table_ref, _is_focus, _ = _import_focus_query()
    engine = _engine(dc)
    table_ref = _table_ref(dc, engine) if engine else None
    if not engine or not table_ref:
        raise RuntimeError(
            f"destination {getattr(dc, 'name', '?')!r} is not a queryable FOCUS data lake "
            "(BigQuery/Snowflake DB export with a resolvable table reference)."
        )
    sql = "SELECT * FROM {focus_table} LIMIT 1"
    result = _run(
        environment_id=soe.environment_id,
        auth_tenant=soe.auth_tenant,
        sql=sql,
        connection_id=str(getattr(dc, "id", "") or getattr(dc, "name", "") or ""),
        response_id=None,
        parameters=None,
    )
    err = result.get("error")
    if err:
        raise RuntimeError(f"focus_query schema probe failed on {getattr(dc, 'name', '?')!r}: {err}")
    rows = result.get("rows") or []
    if not rows:
        # Empty table: fall back to the engine's column-name list (best-effort; Snowflake returns []).
        return {str(c): "" for c in (result.get("columns") or [])}
    return {str(c): str(t) for c, t in pl.DataFrame(rows).schema.items()}


def _quote(engine: str, col: str) -> str:
    """Quote an identifier for the destination's dialect (BigQuery backticks / Snowflake quotes)."""
    return f"`{col}`" if engine == "bigquery" else f'"{col}"'


def _focus_where(engine: str, period_col, start, end, filters, period_dtype) -> tuple[str, list]:
    """Build the ``WHERE`` clause + ``@name`` params for a focus-query read (period window +
    equality filters pushed down; shared by the raw and aggregated readers)."""
    where_parts: list[str] = []
    params: list[dict] = []
    is_date_period = bool(period_dtype) and any(
        t in str(period_dtype).lower() for t in ("datetime", "date", "time", "timestamp")
    )
    if period_col and start is not None and end is not None:
        pcol = _quote(engine, period_col)
        if is_date_period:
            where_parts.append(
                f"CAST({pcol} AS DATETIME) >= @p_start AND CAST({pcol} AS DATETIME) < @p_end"
            )
            params.append({"name": "p_start", "type": "DATETIME", "value": f"{start.isoformat()}T00:00:00"})
            params.append({"name": "p_end", "type": "DATETIME", "value": f"{end.isoformat()}T00:00:00"})
        else:  # string period (e.g. invoice.month "202506"): match YYYY-MM by prefix
            ym_start, ym_end = start.strftime("%Y%m"), end.strftime("%Y%m")
            where_parts.append(
                f"SUBSTR(CAST({pcol} AS STRING), 1, 6) >= @p_start AND SUBSTR(CAST({pcol} AS STRING), 1, 6) < @p_end"
            )
            params.append({"name": "p_start", "type": "STRING", "value": ym_start})
            params.append({"name": "p_end", "type": "STRING", "value": ym_end})
    if filters:
        for i, (col, val) in enumerate(filters.items()):
            pname = f"f_{i}"
            pcol = _quote(engine, col)
            where_parts.append(f"{pcol} = @{pname}")
            params.append({"name": pname, "type": "STRING", "value": str(val)})
    where_sql = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""
    return where_sql, params


def read_destination_dataframe(
    soe,
    dc,
    columns: list[str],
    period_col: str | None = None,
    start: date | None = None,
    end: date | None = None,
    filters: dict | None = None,
    period_dtype: str | None = None,
) -> pl.DataFrame:
    """Read a FOCUS data-lake **destination** into an eager polars DataFrame via the SOE
    focus-query SQL path (the canonical "read what Stitcher wrote" path).

    Builds a BigQuery-flavored ``SELECT <columns> FROM {focus_table} WHERE <period> AND <filters>``
    with ``@name`` query parameters (period window + equality filters pushed down into the SQL so
    the whole table is never pulled), runs it via SOE's ``_run_focus_query`` (BigQuery ADC or
    Snowflake+Vault — transpiled by sqlglot), and materializes the rows as a polars DataFrame.

    Args:
      dc: the destination ``DataConnectionResponse`` (from ``resolve_destination``).
      columns: the columns to SELECT (the ones the caller will aggregate on).
      period_col: period column for the WHERE window (datetime/date, or a string ``invoice.month``
        — selected via ``period_dtype``). When ``None`` no period filter is applied.
      start / end: half-open ``[start, end)`` window.
      filters: optional ``{column: value}`` equality filters pushed into the WHERE.
      period_dtype: the schema dtype of ``period_col`` (``"Datetime"`` / ``"Date"`` → date
        comparison; anything else → string ``YYYY-MM`` prefix match).

    Raises ``RuntimeError`` with the engine's error if the query fails — never a fabricated frame.
    """
    import asyncio  # noqa: F401  (kept for future to_thread wrap; _run_focus_query is sync)

    _resolve, _run, _engine, _table_ref, _is_focus, _ = _import_focus_query()
    if not columns:
        raise RuntimeError("read_destination_dataframe: at least one column is required.")
    engine = _engine(dc)
    table_ref = _table_ref(dc, engine) if engine else None
    if not engine or not table_ref:
        raise RuntimeError(
            f"destination {getattr(dc, 'name', '?')!r} is not a queryable FOCUS data lake "
            "(BigQuery/Snowflake DB export with a resolvable table reference)."
        )

    select_list = ", ".join(_quote(engine, c) for c in columns)
    where_sql, params = _focus_where(engine, period_col, start, end, filters, period_dtype)
    sql = f"SELECT {select_list} FROM {{focus_table}}{where_sql}"

    result = _run(
        environment_id=soe.environment_id,
        auth_tenant=soe.auth_tenant,
        sql=sql,
        connection_id=str(getattr(dc, "id", "") or getattr(dc, "name", "") or ""),
        response_id=None,
        parameters=params or None,
    )
    err = result.get("error")
    if err:
        raise RuntimeError(f"focus_query failed on destination {getattr(dc, 'name', '?')!r}: {err}")
    rows = result.get("rows") or []
    if not rows:
        return pl.DataFrame({c: [] for c in columns})
    return pl.DataFrame(rows)


def read_aggregated_cost(
    soe,
    dc,
    group_by_cols: list[str],
    cost_col: str,
    period_col: str | None = None,
    start: date | None = None,
    end: date | None = None,
    filters: dict | None = None,
    period_dtype: str | None = None,
    allocation_src: str | None = None,
    allocation_dst: str | None = None,
    top_n: int | None = 200,
) -> pl.DataFrame:
    """Aggregate a FOCUS **destination** IN SQL so only ~group-count rows are pulled.

    Unlike ``read_destination_dataframe`` (which SELECTs raw rows and aggregates in polars),
    this pushes the ``GROUP BY`` + ``SUM`` into BigQuery/Snowflake — the chargeback aggregation
    over a large simulated export then returns in seconds (a handful of rows), not by streaming
    the whole month (2.4M+ rows over HTTPS, which can stall). The sum is aliased to ``cost_col``
    and ``COUNT(*)`` to ``row_count`` so downstream polars helpers consume it unchanged.

    ``allocation_src`` / ``allocation_dst`` are additionally grouped on so the caller can still
    compute direct / allocation-in / allocation-out buckets from the per-(dimension, status) rows.

    Returns ``[group_by_cols, allocation_src?, allocation_dst?, cost_col, row_count]``.
    """
    _resolve, _run, _engine, _table_ref, _is_focus, _ = _import_focus_query()
    if not group_by_cols:
        raise RuntimeError("read_aggregated_cost: at least one group-by column is required.")
    engine = _engine(dc)
    table_ref = _table_ref(dc, engine) if engine else None
    if not engine or not table_ref:
        raise RuntimeError(
            f"destination {getattr(dc, 'name', '?')!r} is not a queryable FOCUS data lake "
            "(BigQuery/Snowflake DB export with a resolvable table reference)."
        )

    gb_cols = list(group_by_cols)
    group_cols = gb_cols + [c for c in (allocation_src, allocation_dst) if c and c not in gb_cols]
    select_parts = [_quote(engine, c) for c in group_cols] + [
        f"SUM(CAST({_quote(engine, cost_col)} AS FLOAT64)) AS {_quote(engine, cost_col)}",
        "COUNT(*) AS row_count",
    ]
    where_sql, params = _focus_where(engine, period_col, start, end, filters, period_dtype)
    group_sql = ", ".join(_quote(engine, c) for c in group_cols)
    sql = f"SELECT {', '.join(select_parts)} FROM {{focus_table}}{where_sql} GROUP BY {group_sql}"
    if top_n:
        sql += f" ORDER BY {_quote(engine, cost_col)} DESC LIMIT {int(top_n)}"

    result = _run(
        environment_id=soe.environment_id,
        auth_tenant=soe.auth_tenant,
        sql=sql,
        connection_id=str(getattr(dc, "id", "") or getattr(dc, "name", "") or ""),
        response_id=None,
        parameters=params or None,
    )
    err = result.get("error")
    if err:
        raise RuntimeError(f"focus_query aggregation failed on {getattr(dc, 'name', '?')!r}: {err}")
    rows = result.get("rows") or []
    out_cols = group_cols + [cost_col, "row_count"]
    if not rows:
        return pl.DataFrame({c: [] for c in out_cols})
    return pl.DataFrame(rows)


# ── Column discovery / resolution ──────────────────────────────────────────

# Injectable async classifier hook: ``async (candidate_x: list[str]) -> dict`` returning keys
# ``organization_column`` / ``cost_center_column`` (both optional). When set, org/cost-center
# discovery routes through it; otherwise (unit tests, the deterministic core escape hatch) it
# falls back to the conventional ``x_*`` defaults. The harness wires the real Stitcher-LLM
# classifier here in ``build_server``.
LLM_COLUMN_CLASSIFIER = None


def _is_provider_prefixed_x(col: str) -> bool:
    """True when an ``x_*`` column is a known provider/vendor tag (never an org/cc dimension)."""
    lower = col.lower()
    return any(lower.startswith(f"x_{p}") for p in _PROVIDER_X_PREFIXES)


def _pick(schema: dict[str, str], candidates: list[str], override: str | None) -> str | None:
    """First candidate present in the schema (case-insensitive exact match); override wins."""
    if override:
        return override
    lower = {k.lower(): k for k in schema}
    for c in candidates:
        if c.lower() in lower:
            return lower[c.lower()]
    return None


def resolve_cost_column(schema, override=None):
    return _pick(schema, _COST_CANDIDATES, override)


def resolve_period_column(schema, override=None):
    return _pick(schema, _PERIOD_CANDIDATES, override)


def resolve_provider_column(schema, override=None):
    return _pick(schema, _PROVIDER_CANDIDATES, override)


def resolve_service_column(schema, override=None):
    return _pick(schema, _SERVICE_CANDIDATES, override)


def resolve_billing_account_column(schema, override=None):
    return _pick(schema, _BILLING_ACCOUNT_CANDIDATES, override)


def resolve_region_column(schema, override=None):
    return _pick(schema, _REGION_CANDIDATES, override)


def _normalize_org_cc(mapping, candidate_x: list[str]) -> tuple[str | None, str | None]:
    """Validate + sanitize an LLM classifier result against the candidate ``x_*`` set.

    Refuses (returns None for) any column NOT in the (provider-filtered) ``x_*`` candidates — the
    classifier must name an actual customer ``x_*`` column, and it must be distinct. Never guesses.
    """
    if not isinstance(mapping, dict):
        return None, None
    allowed = set(candidate_x)

    def _ok(val):
        return isinstance(val, str) and val in allowed

    org = mapping.get("organization_column") if _ok(mapping.get("organization_column")) else None
    cc = mapping.get("cost_center_column") if _ok(mapping.get("cost_center_column")) else None
    if org == cc and org is not None:  # a single column can't be both → refuse both (no guessing)
        return None, None
    return org, cc


async def _llm_classify_org_cost_center(candidate_x: list[str]) -> dict:
    """Classify provider-filtered ``x_*`` column names into org / cost-center via the Stitcher LLM.

    Uses the existing gateway transport (``get_openai_client`` + ``LLMAgentProxy`` +
    ``generate_llamaindex_pydantic_program`` — the same pattern as ``custom_cost`` /
    ``config_generation``). Returns ``{"organization_column": …, "cost_center_column": …}`` with
    only columns that actually appear in ``candidate_x``. Raises when the gateway is unconfigured /
    unreachable so the caller can fall back to deterministic defaults.
    """
    from pydantic import BaseModel, Field

    class _OrgCostCenterMapping(BaseModel):
        organization_column: str | None = Field(default=None, description="Closest x_* column naming an organization / business unit / division. None if none fit.")
        cost_center_column: str | None = Field(default=None, description="Closest x_* column naming a cost center / team / project / department. None if none fit.")

    from stitcher.pipeline.common.invoice_parser.parser_settings import get_parser_settings
    from stitcher.pipeline.common.invoice_parser.utils.openai_utils import get_openai_client
    from stitcher.pipeline.common.pipeline_config_models.ai.common.ai_agent_proxy.base import LLMAgentProxy

    settings = get_parser_settings()
    effective_model = settings.plan_generation_model
    client = get_openai_client()

    examples = (
        "Examples of cost-center/org columns:\n"
        "- x_CostCenter / x_cost_center / x_team_id / x_team / x_dept -> cost_center_column\n"
        "- x_Organization / x_org / x_business_unit / x_division -> organization_column\n"
        "- x_project / x_product / x_product_line -> cost_center_column (project-as-cost-center)\n"
    )
    prompt = (
        "You are mapping a FOCUS cost export's CUSTOM (x_*) columns to FinOps dimensions.\n"
        "A FOCUS destination may carry customer columns under the x_ namespace. From the list below,"
        " pick the single best column that names an ORGANIZATION / business unit / division,"
        " and the single best column that names a COST CENTER / team / project / department."
        " A column cannot be both. Use EXACT column names from the list. Leave a field None when"
        " no column fits that dimension (do not invent columns).\n\n"
        f"{examples}\n"
        f"x_* columns available:\n- " + "\n- ".join(candidate_x) + "\n"
    )

    proxy = LLMAgentProxy(
        model=effective_model,
        client=client,
        sai_product="coordination_workflow",
        sai_product_step="chargeback_org_cc_classifier",
    )
    program = proxy.generate_llamaindex_pydantic_program(
        base_model=_OrgCostCenterMapping,
        prompt_template_str=prompt,
        model_name=effective_model,
        attributes={"purpose": "chargeback_org_cost_center_classification"},
        seed=42,
    )
    result: _OrgCostCenterMapping = await program.acall()
    return {"organization_column": result.organization_column, "cost_center_column": result.cost_center_column}


async def classify_org_cost_center(schema: dict[str, str]) -> dict:
    """LLM-classify the destination's ``x_*`` columns into org / cost-center.

    Input is LIMITED to ``x_*`` column names only (never the full schema), with provider-prefixed
    ``x_*`` tags filtered out first (they can never be an org/cost-center). Uses the injectable
    ``LLM_COLUMN_CLASSIFIER`` when set (the harness wires the real Stitcher-LLM classifier);
    otherwise (unit tests / deterministic core) falls back to the conventional ``x_*`` defaults.
    A model failure or gateway misconfiguration NEVER blocks chargeback — it degrades to the
    deterministic default. Returns ``{"organization": col|None, "cost_center": col|None}`` where
    values are real schema columns (or None).
    """
    x_cols = sorted(c for c in schema if c.startswith("x_"))
    candidate_x = [c for c in x_cols if not _is_provider_prefixed_x(c)]
    if not candidate_x:
        return {"organization": None, "cost_center": None}
    if LLM_COLUMN_CLASSIFIER is not None:
        try:
            mapping = await LLM_COLUMN_CLASSIFIER(candidate_x)
            org, cc = _normalize_org_cc(mapping, candidate_x)
            if org is not None or cc is not None:
                return {"organization": org, "cost_center": cc}
        except Exception:  # noqa: BLE001 — a model failure must never block chargeback
            pass
    return {
        "organization": _pick(schema, _ORG_X_DEFAULTS, None),
        "cost_center": _pick(schema, _COST_CENTER_X_DEFAULTS, None),
    }


async def resolve_cost_center_column(schema, override=None, classification=None):
    """Resolve a cost-center column. Explicit override wins; else the org/cc classification
    (computed via ``classify_org_cost_center`` when not supplied); else None (caller should ask)."""
    if override:
        return override
    if classification is None:
        classification = await classify_org_cost_center(schema)
    return classification.get("cost_center")


async def resolve_org_column(schema, override=None, classification=None):
    """Resolve an organization column. Explicit override wins; else the org/cc classification
    (computed via ``classify_org_cost_center`` when not supplied); else None."""
    if override:
        return override
    if classification is None:
        classification = await classify_org_cost_center(schema)
    return classification.get("organization")


def resolve_allocation_columns(schema):
    src = _pick(schema, _ALLOC_SRC_CANDIDATES, None)
    dst = _pick(schema, _ALLOC_DST_CANDIDATES, None)
    return (src, dst) if (src and dst) else (None, None)


def resolve_group_by(schema: dict[str, str], group_by: str) -> str | None:
    """Resolve a group_by token to a real column: literal column name first, then alias."""
    if group_by in schema:
        return group_by
    candidates = _ALIASES.get(group_by.lower())
    if candidates:
        return _pick(schema, candidates, None)
    return None


# ── Polars aggregation helpers ──────────────────────────────────────────────


def filter_period(df: pl.DataFrame, period_column: str | None, start: date, end: date) -> pl.DataFrame:
    """Filter rows to ``start <= period_column < end``. Handles Datetime/Date columns and the
    GCP ``invoice.month`` string column (``"202506"``) via prefix match on ``YYYY-MM``."""
    if period_column is None or period_column not in df.columns:
        return df
    col = pl.col(period_column)
    dtype = df.schema.get(period_column)
    if dtype in (pl.Datetime, pl.Date):
        return df.filter((col.cast(pl.Datetime) >= pl.lit(start)) & (col.cast(pl.Datetime) < pl.lit(end)))
    # String period (e.g. invoice.month "202506"): match the YYYY-MM window by prefix.
    ym_start = start.strftime("%Y%m")
    ym_end = end.strftime("%Y%m")
    return df.filter((col.str.slice(0, 6) >= ym_start) & (col.str.slice(0, 6) < ym_end))


def aggregate_cost(df: pl.DataFrame, group_cols: list[str], cost_column: str, top_n: int = 20) -> pl.DataFrame:
    """Group by ``group_cols``, sum ``cost_column`` (as Float64), count rows; sort by cost desc."""
    agg = (
        df.select([*group_cols, cost_column])
        .with_columns(pl.col(cost_column).cast(pl.Float64, strict=False).alias("_cost"))
        .group_by(group_cols)
        .agg(pl.col("_cost").sum().round(2).alias("cost"), pl.len().alias("row_count"))
        .sort("cost", descending=True)
        .head(top_n)
    )
    return agg


def equality_filters(df: pl.DataFrame, filters: dict | None) -> pl.DataFrame:
    """Apply ``{column: value}`` equality filters (best-effort; unknown columns ignored)."""
    if not filters:
        return df
    exprs = [pl.col(c) == v for c, v in filters.items() if c in df.columns]
    return df.filter(pl.all_horizontal(exprs)) if exprs else df


__all__ = [
    "LLM_COLUMN_CLASSIFIER",
    "aggregate_cost",
    "classify_org_cost_center",
    "equality_filters",
    "filter_period",
    "list_chargeback_destinations",
    "load_data_connection",
    "read_cost_schema",
    "read_aggregated_cost",
    "read_destination_dataframe",
    "resolve_destination",
    "resolve_allocation_columns",
    "resolve_billing_account_column",
    "resolve_cost_center_column",
    "resolve_cost_column",
    "resolve_group_by",
    "resolve_org_column",
    "resolve_period_column",
    "resolve_provider_column",
    "resolve_region_column",
    "resolve_service_column",
]

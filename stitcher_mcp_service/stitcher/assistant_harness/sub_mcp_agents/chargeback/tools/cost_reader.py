"""Destination cost-data reader — SOE focus-query SQL path (what Stitcher has WRITTEN).

Chargeback only ever reads **destinations** — the FOCUS-normalized tables Stitcher has *written*
(the environment's BigQuery / Snowflake export destinations) — never arbitrary source datasources.
SOE owns the canonical resolution + query machinery in ``focus_query._resolve_focus_connection``
/ ``_run_focus_query``; this module reuses it **as-is** (the import is Temporal-free and works
when launched from ``pi_coding_agent/`` where ``.env.local`` resolves):

  - **Resolve a destination:** ``resolve_destination(soe, name_or_id)`` → load a specific
    destination connection by name/id via ``DataConnType.DESTINATIONS``, or auto-resolve the
    environment's single queryable FOCUS data lake when omitted (preferring the canonical
    ``STITCHER_AI_DB_EXPORT_V1_0`` DB export, mirroring ``_resolve_focus_connection``).
  - **List destinations:** ``list_chargeback_destinations(soe)`` → the environment's queryable
    FOCUS destinations (the grounding surface — "what Stitcher has written").
  - **Schema (cheap, no scan):** ``read_cost_schema(soe, dc)`` → columns + dtypes via a
    ``LIMIT 1`` probe through the same SQL path the data read uses.
  - **Data (SQL path):** ``read_aggregated_cost(soe, dc, group_by_cols, cost_col, …)`` → pushes
    the ``GROUP BY`` + ``SUM`` into BigQuery/Snowflake (period window + equality filters pushed
    into the ``WHERE`` via ``@name`` parameters) so only ~group-count rows are pulled, never the
    whole table. Callers shape/render the returned frame (``common.cost_summary``).

Column mapping uses FOCUS defaults (``BilledCost`` / ``ChargePeriodStart`` / ``BillingAccountId`` /
``x_CostCenter`` …) — destinations are FOCUS-normalized, so the defaults almost always apply, but
every column stays overridable. Allocation lineage (``x_AllocationStatusSource``/``Destination``)
is computed only when the destination exposes those columns.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Coroutine
from datetime import date
from typing import Any

import polars as pl

from stitcher.pipeline.common.column_names.stitcher_column_names import StitcherColumnNames as _StitcherCol
from stitcher.pipeline.common.focus_column_names import FocusColumnNames as _FocusCol

# ── Column discovery — enum-backed FOCUS names ONLY (destinations are FOCUS-normalized) ──
#
# Chargeback reads **destinations** (what Stitcher has WRITTEN — a FOCUS DB export), so discovery
# matches only the canonical ``FocusColumnNames``. There is deliberately NO raw-export fallback
# (``cost`` / ``usage_start_time`` / ``project.name`` / … are dead weight on a destination).
# Cost-column/period/provider/service/billing-account/region are static FOCUS enums. Org and
# cost-center live under the customer ``x_*`` namespace and are discovered by the LLM classifier
# (see ``classify_org_cost_center``) — with a deterministic ``x_*`` default as the escape hatch.


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
# "region" work without typing the full column name. A literal column name always wins.
# ``cost_center`` / ``organization`` resolve via the DETERMINISTIC conventional x_* defaults only
# (never the LLM — the ad-hoc query path stays cheap and reproducible); if the customer renamed
# those columns, pass the real column name (``discover_cost_schema`` shows it).
_ALIASES: dict[str, list[str]] = {
    "service": _SERVICE_CANDIDATES,
    "provider": _PROVIDER_CANDIDATES,
    "billing_account": _BILLING_ACCOUNT_CANDIDATES,
    "region": _REGION_CANDIDATES,
    "cost_center": _COST_CENTER_X_DEFAULTS,
    "cost_centre": _COST_CENTER_X_DEFAULTS,
    "organization": _ORG_X_DEFAULTS,
    "org": _ORG_X_DEFAULTS,
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

    return (
        _resolve_focus_connection,
        _run_focus_query,
        _conn_engine,
        _build_table_ref,
        _is_focus_destination,
        SupportedDatasets,
    )


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
        c
        for c in queryable
        if str(getattr(c, "dataset_name", "") or "") == SupportedDatasets.STITCHER_AI_DB_EXPORT_V1_0.value
    ]
    focus_hinted = [c for c in queryable if _is_focus(c)]
    ordered = db_export or focus_hinted or queryable
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
            where_parts.append(f"CAST({pcol} AS DATETIME) >= @p_start AND CAST({pcol} AS DATETIME) < @p_end")
            params.append({"name": "p_start", "type": "DATETIME", "value": f"{start.isoformat()}T00:00:00"})
            params.append({"name": "p_end", "type": "DATETIME", "value": f"{end.isoformat()}T00:00:00"})
        else:  # string period — month codes ("202506") AND ISO dates ("2026-07-23T…"): strip
            # every non-digit, then compare the YYYYMM prefix. (Plain SUBSTR(1,6) mangles ISO
            # strings — "2026-07-23"[:6] = "2026-0" — silently matching NOTHING. That exact
            # silent-zero regression hit when the dev export's schema rotated to string periods.)
            digits = f"REGEXP_REPLACE(CAST({pcol} AS STRING), '[^0-9]', '')"
            ym_start, ym_end = start.strftime("%Y%m"), end.strftime("%Y%m")
            where_parts.append(
                f"SUBSTR({digits}, 1, 6) >= @p_start AND SUBSTR({digits}, 1, 6) < @p_end"
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

    Pushes the ``GROUP BY`` + ``SUM`` into BigQuery/Snowflake so the chargeback aggregation over a
    large export returns in seconds (a handful of rows), not by streaming the whole month (2.4M+
    rows over HTTPS, which can stall). The sum is aliased to ``cost_col`` and ``COUNT(*)`` to
    ``row_count`` so downstream polars helpers consume it unchanged.

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
LLM_COLUMN_CLASSIFIER: Callable[[list[str]], Coroutine[Any, Any, dict]] | None = None

# ── optional deterministic first path: file-backed ColumnSemanticIndex ─────────
# When configured (with an SOE scope), org/cost-center discovery first asks the
# index (see stitcher_assistant/column_semantic_index.py). The index returns a
# confident/ambiguous verdict per dimension: confident match or confident refusal
# is used directly (no LLM turn); an ambiguous dimension falls through to the
# LLM classifier, whose answer is persisted back (source="llm") so it is paid
# once. The index must NEVER block chargeback — every interaction is wrapped in
# try/except and degrades to the LLM / deterministic defaults unchanged.
COLUMN_INDEX: Any | None = None
COLUMN_INDEX_STORE_PATH: str = os.path.join(
    os.path.expanduser("~"), ".stitcher", "column_semantic_index.json"
)

# Injectable async allocation-dimension picker for the ambiguous case:
# ``async (candidate_x, pipeline, shortlist) -> dict`` returning {"dimension", "column"}.
# The deterministic pipeline (cost_center → business_unit → organization) resolves most
# destinations; when the top priority is only ambiguous, this hook gets a SCOPED shortlist
# (the candidate  per dimension) so the LLM makes a multiple-choice pick instead of an
# open-ended classification. The harness wires it in ``build_server``; tests keep None
# → deterministic best-ambiguous fallback.
LLM_ALLOCATION_PICKER: Callable[[list[str], tuple, list], Coroutine[Any, Any, dict]] | None = None


# lazily import the index module on first use (keeps this module import-light
# and avoids any import-time coupling to numpy for callers that never use it)
def _get_column_index() -> Any | None:
    if COLUMN_INDEX is not None:
        return COLUMN_INDEX
    return None


def __reimport_column_index() -> Any:
    """Return the ColumnSemanticIndex class for the harness to configure (build_server)."""
    from .column_semantic_index import ColumnSemanticIndex, default_backend  # noqa: PLC0415

    return ColumnSemanticIndex, default_backend


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
        organization_column: str | None = Field(
            default=None,
            description="Closest x_* column naming an organization / business unit / division. None if none fit.",
        )
        cost_center_column: str | None = Field(
            default=None,
            description="Closest x_* column naming a cost center / team / project / department. None if none fit.",
        )

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


async def classify_org_cost_center(schema: dict[str, str], soe=None) -> dict:
    """Resolve the destination's ``x_*`` columns into org / cost-center dimensions.

    Order (deterministic-first, LLM-fallback):
      1. **File-backed index** (when ``COLUMN_INDEX`` is configured and ``soe`` is
         given): the index returns a confident/ambiguous verdict per dimension. If
         BOTH dimensions are confident (a trusted match, or a trusted refusal that
         there is no such dimension), return that and skip the LLM entirely.
      2. **LLM classifier** (``LLM_COLUMN_CLASSIFIER``): resolves when any dimension
         is ambiguous (or the index is absent/unconfigured). Its answer is persisted
         back into the index store (source="llm") so the next call with the same
         columns is served from the store without another LLM turn.
      3. **Deterministic ``x_*`` defaults**: the no-LLM, no-index escape hatch.

    A model failure, index error, or gateway misconfiguration NEVER blocks
    chargeback — it degrades to the next path and finally to the deterministic
    default. Returns ``{"organization": col|None, "cost_center": col|None}`` where
    values are real schema columns (or None).
    """
    x_cols = sorted(c for c in schema if c.startswith("x_"))
    candidate_x = [c for c in x_cols if not _is_provider_prefixed_x(c)]
    if not candidate_x:
        return {"organization": None, "cost_center": None}

    # Resolve the destination once up-front (for the index cache key + LLM persist).
    dest_id = ""
    use_index = COLUMN_INDEX is not None and soe is not None
    if use_index:
        try:
            dest = resolve_destination(soe, "")
            dest_id = str(getattr(dest, "id", "") or getattr(dest, "name", ""))
        except Exception:  # noqa: BLE001 — index must never block
            pass

    # 1) Deterministic first path (both dimensions confident ⇒ done).
    if use_index and dest_id:
        try:
            r = COLUMN_INDEX.match(
                candidate_x,
                env_id=getattr(soe, "environment_id", ""),
                dest_id=dest_id,
                persist_result=True,
                path=COLUMN_INDEX_STORE_PATH,
            )
            org_m, cc_m = r.organization, r.cost_center
            org_ok = org_m is None or org_m.confident
            cc_ok = cc_m is None or cc_m.confident
            if org_ok and cc_ok:
                return {
                    "organization": org_m.column if org_m and org_m.confident else None,
                    "cost_center": cc_m.column if cc_m and cc_m.confident else None,
                }
        except Exception:  # noqa: BLE001 — index must never block
            pass

    # 2) LLM fallback (resolves ambiguous dimensions; persists its answer).
    if LLM_COLUMN_CLASSIFIER is not None:
        try:
            mapping = await LLM_COLUMN_CLASSIFIER(candidate_x)
            org, cc = _normalize_org_cc(mapping, candidate_x)
            if org is not None or cc is not None:
                if use_index and dest_id:
                    try:
                        COLUMN_INDEX.persist_llm_result(
                            candidate_x, org, cc,
                            env_id=getattr(soe, "environment_id", ""),
                            dest_id=dest_id, path=COLUMN_INDEX_STORE_PATH,
                        )
                    except Exception:  # noqa: BLE001 — persist is best-effort
                        pass
                return {"organization": org, "cost_center": cc}
        except Exception:  # noqa: BLE001 — a model failure must never block chargeback
            pass

    # 3) Deterministic escape hatch (no LLM, no index).
    return {
        "organization": _pick(schema, _ORG_X_DEFAULTS, None),
        "cost_center": _pick(schema, _COST_CENTER_X_DEFAULTS, None),
    }


async def resolve_cost_center_column(schema, override=None, classification=None, soe=None):
    """Resolve a cost-center column. Explicit override wins; else the org/cc classification
    (computed via ``classify_org_cost_center`` when not supplied); else None (caller should ask)."""
    if override:
        return override
    if classification is None:
        classification = await classify_org_cost_center(schema, soe=soe)
    return classification.get("cost_center")


async def resolve_org_column(schema, override=None, classification=None, soe=None):
    """Resolve an organization column. Explicit override wins; else the org/cc classification
    (computed via ``classify_org_cost_center`` when not supplied); else None."""
    if override:
        return override
    if classification is None:
        classification = await classify_org_cost_center(schema, soe=soe)
    return classification.get("organization")


async def _llm_pick_allocation(candidate_x: list[str], pipeline, shortlist: list | None) -> dict:
    """Multiple-choice LLM: pick ONE column to allocate cost by, from a shortlist.

    Unlike the org/cost-center classifier (which maps to two fixed dimensions), this
    asks the model to pick a single grouping key given a SCOPED candidate list and a
    dimension priority (cost_center → business_unit → organization). Scoping the
    question to the index's shortlist makes the pick easy and low-variance vs. an
    open-ended ``which column is the cost center?`` over all 30+ vendor tags.
    """
    from pydantic import BaseModel, Field

    class _AllocationPick(BaseModel):
        dimension: str = Field(
            default="",
            description="One of: cost_center, business_unit, organization (the best grouping dimension). Empty if none fit.",
        )
        column: str = Field(
            default="",
            description="EXACT column name from the candidate list to allocate cost by. Empty if none fit.",
        )

    from stitcher.pipeline.common.invoice_parser.parser_settings import get_parser_settings
    from stitcher.pipeline.common.invoice_parser.utils.openai_utils import get_openai_client
    from stitcher.pipeline.common.pipeline_config_models.ai.common.ai_agent_proxy.base import LLMAgentProxy

    settings = get_parser_settings()
    effective_model = settings.plan_generation_model
    client = get_openai_client()

    order = "> ".join(pipeline)
    lines = "\n".join(f"  - {c.get('column')}  (best fit: {c.get('dimension')}, score {c.get('score', 0):.2f})"
                     for c in (shortlist or []) if c.get("column"))
    prompt = (
        "We bill cloud cost into chargeback groups. Prefer a COST CENTER column; if none exists,\n"
        "fall back to a BUSINESS UNIT, then an ORGANIZATION. Dimension priority: " + order + ".\n"
        "Here are the candidate columns ({dim_len}) the deterministic matcher surfaced, each with its\n"
        "best-fitting dimension:\n" + (lines or "  (no candidates)") + "\n\n"
        "Pick EXACTLY ONE column from the list to allocate cost by — prefer the highest-priority\n"
        "dimension that has a clear column. Reply with the chosen dimension and the EXACT column name\n"
        "(nothing else), or leave both empty if none of them is a defensible grouping dimension."
    ).format(dim_len=len(shortlist or []) if lines else 0)

    proxy = LLMAgentProxy(
        model=effective_model,
        client=client,
        sai_product="coordination_workflow",
        sai_product_step="chargeback_allocation_pick",
    )
    program = proxy.generate_llamaindex_pydantic_program(
        base_model=_AllocationPick,
        prompt_template_str=prompt,
        model_name=effective_model,
        attributes={"purpose": "chargeback_allocation_dimension_pick"},
        seed=42,
    )
    result: _AllocationPick = await program.acall()
    return {"dimension": (result.dimension or "").strip(), "column": (result.column or "").strip()}


async def resolve_allocation_dimension(soe, schema: dict[str, str], pipeline=None, override=None) -> dict:
    """Resolve which column to allocate / group cost by, via an allocation pipeline.

    Priority: explicit override > deterministic index pipeline > LLM multiple-choice >
    best-ambiguous fallback. ``resolve``
    Returns ``{"dimension": str|None, "column": str|None, "confident": bool,
    "source": "explicit"|"index"|"llm"|"fallback"}``. A model/index failure never blocks — it
    degrades to the best deterministic candidate (or a clear None).
    """
    from .column_semantic_index import ALLOCATION_PIPELINE as _PIPE  # noqa: PLC0415

    pipeline = tuple(pipeline) if pipeline else _PIPE
    if override and override in schema:
        return {"dimension": "explicit", "column": override, "confident": True, "source": "explicit"}

    x_cols = sorted(c for c in schema if c.startswith("x_"))
    candidate_x = [c for c in x_cols if not _is_provider_prefixed_x(c)]
    if not candidate_x:
        return {"dimension": None, "column": None, "confident": True, "source": "index"}

    shortlist: list | None = None
    best: dict | None = None
    # 1) deterministic index pipeline
    if COLUMN_INDEX is not None and soe is not None:
        try:
            best = COLUMN_INDEX.pick_allocation(candidate_x, pipeline)
            cands_out = (best.get("candidates") or []) if best else []
            if best.get("confident") and best.get("column"):
                return {
                    "dimension": best["dimension"], "column": best["column"],
                    "confident": True, "source": "index",
                    "score": best.get("score", 0.0), "_candidates": cands_out,
                }
            shortlist = best.get("candidates")
        except Exception:  # noqa: BLE001 — index must never block
            shortlist = None
            cands_out = []

    # 2) LLM multiple-choice over the shortlist
    if LLM_ALLOCATION_PICKER is not None and (shortlist or candidate_x):
        try:
            pick = await LLM_ALLOCATION_PICKER(candidate_x, pipeline, shortlist)
            col = pick.get("column", "") if isinstance(pick, dict) else ""
            if col and col in candidate_x:
                return {
                    "dimension": pick.get("dimension") or best.get("dimension") if best else None,
                    "column": col, "confident": True, "source": "llm",
                    "_candidates": shortlist or [],
                }
        except Exception:  # noqa: BLE001 — a model failure must never block
            pass

    # 3) best deterministic fallback (only if we have a defensible candidate)
    if best and best.get("column"):
        return {
            "dimension": best.get("dimension"), "column": best.get("column"),
            "confident": best.get("confident", False), "source": "fallback",
            "score": best.get("score", 0.0), "_candidates": cands_out or [],
        }

    # 4) conventional x_* defaults — backward-compat escape hatch, parity with the old
    #    deterministic cost-center/org resolution used when no index/LLM is configured
    #    (tests, minimal deployments). Picks the highest-priority pipeline dimension whose
    #    conventional column is present (x_CostCenter first).
    _defaults = {"cost_center": _COST_CENTER_X_DEFAULTS, "organization": _ORG_X_DEFAULTS}
    for dim in pipeline:
        for dcol in _defaults.get(dim, []):
            if dcol in schema:
                return {
                    "dimension": dim, "column": dcol, "confident": True,
                    "source": "default", "_candidates": shortlist or [],
                }
    return {"dimension": None, "column": None, "confident": True, "source": "index", "_candidates": []}


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


__all__ = [
    "LLM_COLUMN_CLASSIFIER",
    "LLM_ALLOCATION_PICKER",
    "COLUMN_INDEX",
    "COLUMN_INDEX_STORE_PATH",
    "classify_org_cost_center",
    "resolve_allocation_dimension",
    "_llm_pick_allocation",
    "list_chargeback_destinations",
    "read_cost_schema",
    "read_aggregated_cost",
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

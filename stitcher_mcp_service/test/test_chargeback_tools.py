"""Adversarial tests for the chargeback sub-MCP tools.

Following the human-in-the-loop audit pattern (mirrors ``test_config_generation_tools.py``): every
safety boundary gets a test proving the wrong path is never taken (refusal, materiality rollup,
unknown-value refusal). The SOE focus-query SQL read is DETERMINISTIC in these tools — the only
mutable part is the destination being read — so we patch ``cost_reader.resolve_destination`` /
``read_cost_schema`` / ``read_aggregated_cost`` with a synthetic polars cost destination and assert
on the DETERMINISTIC tool logic (column mapping, aggregation shaping, materiality rollup, ERP
gating, allocation lineage). No live network.

Boundaries under test
---------------------
C1. env-scope refusal — no environment_id → ERR (never proceeds to a read).
C2. period validation — ``YYYY-MM`` / ``last_month`` accepted; ``2026-13`` / "March 2026" refused.
C3. raw GCP export mapping — ``cost`` / ``usage_start_time`` / ``project.name`` aggregate correctly.
C4. materiality rollup — sub-threshold cost centers combine into "Miscellaneous (below materiality)".
C5. ERP-system gating — ``submit_invoices_to_erp`` refuses an unsupported ERP; passes a supported one.
C6. query_focus_cost refuses an unknown ``group_by`` / missing ``cost_column``.
C7. discover_cost_schema shows an ambiguity prompt when org/cost-center have multiple candidates.
C8. allocation lineage — direct / in / out buckets computed only when x_Allocation* columns exist.
C9. invoice builder skips the (unallocated) bucket.
"""

from __future__ import annotations

import asyncio
from datetime import date, timedelta

import polars as pl
import pytest
from fastmcp import FastMCP

from stitcher.assistant_harness.sub_mcp_agents.chargeback.tools import cost_reader as cr
from stitcher.assistant_harness.sub_mcp_agents.chargeback.tools import formatting as fmt
from stitcher.assistant_harness.sub_mcp_agents.chargeback.tools import (
    invoice_tools,
)
from stitcher.assistant_harness.sub_mcp_agents.chargeback.tools import period as period_mod
from stitcher.assistant_harness.sub_mcp_agents.chargeback.tools import query_tools as qt
from stitcher.assistant_harness.sub_mcp_agents.chargeback.tools import report_tools as rt
from stitcher.assistant_harness.sub_mcp_agents.chargeback.tools import schema_tools as st
from stitcher.assistant_harness.sub_mcp_agents.chargeback.tools.invoice_tools import register as inv_register

# A raw GCP billing export (the user's datasource shape) + a FOCUS-normalized table.
_GCP_SCHEMA = {
    "cost": "Float64",
    "usage_start_time": "Datetime",
    "usage_end_time": "Datetime",
    "project.name": "String",
    "project.id": "String",
    "labels": "List",
    "service.description": "String",
    "location.region": "String",
    "invoice.month": "String",
}


# dates relative to today so the default rolling 30-day window contains them
_today = date.today()
_day1 = _today - timedelta(days=2)
_day5 = _today - timedelta(days=1)
_day30 = _today - timedelta(days=30)
_day20 = _today - timedelta(days=5)

_GCP_ROWS = pl.DataFrame(
    {
        "cost": [10.0, 5.0, 30.0, 1.27],
        "usage_start_time": [_day1, _day5, _day30, _day20],
        "project.name": ["proj-a", "proj-a", "proj-b", "proj-c"],
        "service.description": ["Compute Engine", "Cloud Storage", "Compute Engine", "Networking"],
    }
)

_FOCUS_SCHEMA = {
    "BilledCost": "Float64",
    "ChargePeriodStart": "Datetime",
    "BillingAccountId": "String",
    "ProviderName": "String",
    "x_CostCenter": "String",
    "x_Organization": "String",
    "x_AllocationStatusSource": "String",
    "x_AllocationStatusDestination": "String",
}

# A FOCUS destination WITHOUT allocation columns (for direct-cost / provider / service tests).
_FOCUS_SVC_SCHEMA = {
    "BilledCost": "Float64",
    "ChargePeriodStart": "Datetime",
    "ServiceName": "String",
    "ProviderName": "String",
    "x_CostCenter": "String",
    "x_Organization": "String",
}

_FOCUS_SVC_ROWS = pl.DataFrame(
    {
        "BilledCost": [10.0, 5.0, 30.0, 1.27],
        "ChargePeriodStart": [_day1, _day5, _day30, _day20],
        "ServiceName": ["Compute Engine", "Cloud Storage", "Compute Engine", "Networking"],
        "ProviderName": ["AWS", "AWS", "GCP", "GCP"],
        "x_CostCenter": ["cc-120", "cc-120", "cc-121", "cc-122"],
        "x_Organization": ["org-a", "org-a", "org-b", "org-b"],
    }
)


class _DummySoe:
    environment_id = "env-1234"
    auth_tenant = "acme"

    def scope_error(self):
        return ""

    def tenant_error(self):
        return ""


class _UnscopedSoe:
    environment_id = ""
    auth_tenant = ""

    def scope_error(self):
        return "ERR: no STITCHER_ENVIRONMENT_ID (or SAI_ENV_CONTEXT.environment_id) — config generation is environment-scoped."

    def tenant_error(self):
        return ""


def _patch_cost_reader(monkeypatch, schema, df):
    """Point cost_reader at a synthetic destination (the helpers the tools call are patched)."""
    dc = object()

    def fake_load(soe, name_or_id=""):
        return dc

    def fake_schema(soe, d):
        return schema

    def fake_agg(soe, d, group_by_cols, cost_col, period_col=None, start=None, end=None, filters=None,
                 period_dtype=None, allocation_src=None, allocation_dst=None, top_n=200):
        # SQL-aggregation reader is patched to return the synthetic frame verbatim (the SQL
        # GROUP BY pushdown is exercised live, not here — these tests assert the deterministic
        # polars rendering on top of it).
        return df

    monkeypatch.setattr(cr, "resolve_destination", fake_load)
    monkeypatch.setattr(cr, "read_cost_schema", fake_schema)
    monkeypatch.setattr(cr, "read_aggregated_cost", fake_agg)
    return dc


@pytest.fixture(scope="module")
def server():
    mcp = FastMCP("test-chargeback")
    st.register(mcp, None, _DummySoe())
    rt.register(mcp, None, _DummySoe())
    inv_register(mcp, None, _DummySoe())
    qt.register(mcp, None, _DummySoe())
    return mcp


@pytest.fixture(scope="module")
def unscoped_server():
    mcp = FastMCP("test-chargeback-unscoped")
    rt.register(mcp, None, _UnscopedSoe())
    qt.register(mcp, None, _UnscopedSoe())
    return mcp


def _call(server, name, args=None, monkeypatch=None, schema=_GCP_SCHEMA, df=_GCP_ROWS):
    """Run a tool with cost_reader patched to a synthetic datasource (if monkeypatch is given)."""
    if monkeypatch is None:
        # No patch → tool hits a real read; callers wanting determinism must supply monkeypatch.
        res = asyncio.run(server.call_tool(name, args or {}))
        return res.content[0].text if res.content else str(res)
    _patch_cost_reader(monkeypatch, schema, df)
    res = asyncio.run(server.call_tool(name, args or {}))
    return res.content[0].text if res.content else str(res)


# ── C1: env-scope refusal ───────────────────────────────────────────────────
def test_env_scope_refusal(unscoped_server, monkeypatch):
    _patch_cost_reader(monkeypatch, _GCP_SCHEMA, _GCP_ROWS)
    txt = _call(unscoped_server, "chargeback_by_billing_account", {"data_source": "gcp"})
    assert "ERR" in txt and "STITCHER_ENVIRONMENT_ID" in txt


def test_env_scope_refusal_query(unscoped_server, monkeypatch):
    txt = _call(
        unscoped_server, "query_focus_cost", {"data_source": "gcp", "group_by": "project.name"}, monkeypatch=monkeypatch
    )
    assert "ERR" in txt


# ── C2: period validation ───────────────────────────────────────────────────
def test_period_accepts_yyyy_mm_and_last_month():
    start, end, label = period_mod.resolve_period("2026-03", 30)
    assert start == date(2026, 3, 1) and end == date(2026, 4, 1) and label == "March 2026"


def test_period_accepts_last_month():
    start, end, label = period_mod.resolve_period("last_month", 30)
    assert start.day == 1 and end.day == 1
    assert " " in label


@pytest.mark.parametrize("bad", ["2026-13", "2026-00", "March 2026", "march"])
def test_period_rejects_invalid(bad):
    with pytest.raises(ValueError):
        period_mod.resolve_period(bad, 30)


# ── C3: FOCUS fetch mapping + aggregation (destination model) ───────────────
def test_focus_query_by_service(server, monkeypatch):
    txt = _call(
        server,
        "query_focus_cost",
        {"data_source": "focus", "group_by": "ServiceName"},
        monkeypatch=monkeypatch,
        schema=_FOCUS_SVC_SCHEMA,
        df=_FOCUS_SVC_ROWS,
    )
    assert "Compute Engine" in txt
    assert "$40.00" in txt  # Compute Engine = 10 (day1) + 30 (day30)
    assert "Cloud Storage" in txt
    assert "$5.00" in txt


def test_focus_query_by_service_alias(server, monkeypatch):
    # 'service' alias resolves to the SEPARATE FOCUS ServiceName column (not ProviderName).
    txt = _call(
        server,
        "query_focus_cost",
        {"data_source": "focus", "group_by": "service"},
        monkeypatch=monkeypatch,
        schema=_FOCUS_SVC_SCHEMA,
        df=_FOCUS_SVC_ROWS,
    )
    assert "Compute Engine" in txt
    assert "ERR" not in txt


def test_focus_query_provider_alias(server, monkeypatch):
    # 'provider' alias resolves to ProviderName.
    txt = _call(
        server,
        "query_focus_cost",
        {"data_source": "focus", "group_by": "provider"},
        monkeypatch=monkeypatch,
        schema=_FOCUS_SVC_SCHEMA,
        df=_FOCUS_SVC_ROWS,
    )
    assert "AWS" in txt and "GCP" in txt


def test_provider_alias_does_not_leak_service(server, monkeypatch):
    # Schema has ServiceName but NO ProviderName: 'provider' must NOT resolve to the service column.
    schema = {"BilledCost": "Float64", "ChargePeriodStart": "Datetime", "ServiceName": "String"}
    rows = pl.DataFrame(
        {"BilledCost": [1.0], "ChargePeriodStart": [_day1], "ServiceName": ["Compute Engine"]}
    )
    txt = _call(
        server,
        "query_focus_cost",
        {"data_source": "focus", "group_by": "provider"},
        monkeypatch=monkeypatch,
        schema=schema,
        df=rows,
    )
    assert "ERR" in txt and "Invalid group_by" in txt


def test_raw_gcp_cost_column_not_discovered(server, monkeypatch):
    """Enum-backed discovery must NOT fall back to raw-GCP names (cost/project.*) on a
    destination — chargeback reads FOCUS destinations only."""
    _patch_cost_reader(monkeypatch, _GCP_SCHEMA, _GCP_ROWS)
    res = asyncio.run(server.call_tool("query_focus_cost", {"data_source": "gcp"}))
    txt = res.content[0].text if res.content else str(res)
    assert "ERR" in txt and "cost column" in txt


def test_focus_discover_schema(server, monkeypatch):
    _patch_cost_reader(monkeypatch, _FOCUS_SVC_SCHEMA, _FOCUS_SVC_ROWS)
    res = asyncio.run(server.call_tool("discover_cost_schema", {"data_source": "focus"}))
    txt = res.content[0].text if res.content else str(res)
    assert "ERR" not in txt
    assert "BilledCost" in txt
    assert "x_CostCenter" in txt and "x_Organization" in txt  # discovered org/cost-center mapping


# ── C4: materiality rollup ──────────────────────────────────────────────────
def test_materiality_rollup(server, monkeypatch):
    txt = _call(
        server,
        "chargeback_by_cost_center",
        {"data_source": "focus", "materiality_threshold": 10.0},
        monkeypatch=monkeypatch,
        schema=_FOCUS_SVC_SCHEMA,
        df=_FOCUS_SVC_ROWS,
    )
    assert "Miscellaneous (below materiality)" in txt
    # cc-120 ($15) and cc-121 ($30) stay above threshold; cc-122 ($1.27) rolls up.
    assert "cc-120" in txt
    assert "cc-121" in txt
    assert "cc-122" not in txt  # below materiality → combined, never shown


# ── C5: ERP-system gating ───────────────────────────────────────────────────
def test_submit_rejects_unsupported_erp(server, monkeypatch):
    _patch_cost_reader(monkeypatch, _GCP_SCHEMA, _GCP_ROWS)
    res = asyncio.run(server.call_tool("submit_invoices_to_erp", {"erp_system": "Oracle Fusion", "data_source": "gcp"}))
    txt = res.content[0].text if res.content else str(res)
    assert "ERR" in txt and "Unsupported ERP" in txt


def test_submit_accepts_supported_erp(server, monkeypatch):
    _patch_cost_reader(monkeypatch, _FOCUS_SVC_SCHEMA, _FOCUS_SVC_ROWS)
    res = asyncio.run(
        server.call_tool(
            "submit_invoices_to_erp",
            {"erp_system": "QuickBooks Online", "data_source": "focus"},
        )
    )
    txt = res.content[0].text if res.content else str(res)
    assert "ERR" not in txt and "posted" in txt.lower()


# ── C6: query_focus_cost refusals ───────────────────────────────────────────
def test_query_unknown_group_by(server, monkeypatch):
    txt = _call(
        server,
        "query_focus_cost",
        {"data_source": "focus", "group_by": "bogus_dimension"},
        monkeypatch=monkeypatch,
        schema=_FOCUS_SVC_SCHEMA,
        df=_FOCUS_SVC_ROWS,
    )
    assert "ERR" in txt and "Invalid group_by" in txt


def test_query_cross_tab_cost_center_plus_service(server, monkeypatch):
    """Comma-separated group_by dims → ONE cross-tab table (cost center + service in a row),
    not two separate tables."""
    txt = _call(
        server,
        "query_focus_cost",
        {"data_source": "focus", "group_by": "x_CostCenter,ServiceName"},
        monkeypatch=monkeypatch,
        schema=_FOCUS_SVC_SCHEMA,
        df=_FOCUS_SVC_ROWS,
    )
    assert "ERR" not in txt
    assert "x_CostCenter | ServiceName" in txt  # both dims in the header
    assert "cc-120" in txt and "Compute Engine" in txt  # one row per (cc, service) pair


def test_query_cost_center_alias_resolves_x_column(server, monkeypatch):
    """The deterministic cost_center alias resolves the conventional x_CostCenter column (no LLM)."""
    txt = _call(
        server,
        "query_focus_cost",
        {"data_source": "focus", "group_by": "cost_center"},
        monkeypatch=monkeypatch,
        schema=_FOCUS_SVC_SCHEMA,
        df=_FOCUS_SVC_ROWS,
    )
    assert "ERR" not in txt
    assert "x_CostCenter" in txt  # header shows the resolved real column


def test_query_too_many_dims_refused(server, monkeypatch):
    txt = _call(
        server,
        "query_focus_cost",
        {"data_source": "focus", "group_by": "a,b,c,d,e"},
        monkeypatch=monkeypatch,
        schema=_FOCUS_SVC_SCHEMA,
        df=_FOCUS_SVC_ROWS,
    )
    assert "ERR" in txt and "at most 4" in txt


def test_query_cost_column_not_found(server, monkeypatch):
    _patch_cost_reader(monkeypatch, {"service.description": "String", "project.name": "String"}, _GCP_ROWS)
    res = asyncio.run(server.call_tool("query_focus_cost", {"data_source": "gcp", "group_by": "project.name"}))
    txt = res.content[0].text if res.content else str(res)
    assert "ERR" in txt and "cost column" in txt


# ── C7: discover_cost_schema ambiguity prompt ───────────────────────────────
def test_discover_schema_no_cost_column(server, monkeypatch):
    _patch_cost_reader(monkeypatch, {"project.name": "String", "usage_start_time": "Datetime"}, _GCP_ROWS)
    res = asyncio.run(server.call_tool("discover_cost_schema", {"data_source": "gcp"}))
    txt = res.content[0].text if res.content else str(res)
    assert "no cost column found" in txt


# ── C8: allocation lineage (FOCUS table) ────────────────────────────────────
def test_allocation_lineage_computed(server, monkeypatch):
    focus_rows = pl.DataFrame(
        {
            "BilledCost": [100.0, 50.0, -20.0],
            "ChargePeriodStart": [_day1, _day1, _day1],
            "x_CostCenter": ["cc-120", "cc-120", "Operations"],
            "x_Organization": ["R&D", "R&D", "internal-it"],
            "ProviderName": ["AWS", "IT", "AWS"],
            "x_AllocationStatusSource": [None, "Allocations", None],
            "x_AllocationStatusDestination": [None, "Allocated", "Negation"],
        }
    )
    txt = _call(
        server,
        "chargeback_by_cost_center",
        {"data_source": "focus"},
        monkeypatch=monkeypatch,
        schema=_FOCUS_SCHEMA,
        df=focus_rows,
    )
    # Direct / allocation-in / allocation-out columns present (lineage rendered).
    assert "Allocation in" in txt
    assert "$100.00" in txt


def test_allocation_not_computed_without_alloc_columns(server, monkeypatch):
    txt = _call(
        server,
        "chargeback_by_cost_center",
        {"data_source": "focus"},
        monkeypatch=monkeypatch,
        schema=_FOCUS_SVC_SCHEMA,
        df=_FOCUS_SVC_ROWS,
    )
    assert "no allocation columns" in txt  # no x_Allocation* → direct-cost note


# ── C8.5: SQL row_count must survive the render path (regression) ───────────
def test_share_table_preserves_sql_row_count(server, monkeypatch):
    """When read_aggregated_cost returns the SQL frame (with row_count), the rendered rows column
    and the 'N charge records in window' line must reflect the SQL COUNT(*) — never collapse to 1
    per group (the aggregate_cost re-group regression) or to the group count."""
    sql_frame = pl.DataFrame(
        {
            "BillingAccountId": ["acct-a", "acct-b"],
            "BilledCost": [100.0, 40.0],
            "row_count": [1500, 300],
        }
    )
    txt = _call(
        server,
        "chargeback_by_billing_account",
        {"data_source": "focus"},
        monkeypatch=monkeypatch,
        schema={
            "BilledCost": "Float64",
            "ChargePeriodStart": "Datetime",
            "BillingAccountId": "String",
        },
        df=sql_frame,
    )
    assert "1,500" in txt and "300" in txt  # per-account SQL record counts, not 1
    assert "1,800 charge records in window" in txt  # 1500 + 300
    assert "$140.00" in txt  # total across both groups


# ── C9: invoice builder sanity + formatting ─────────────────────────────────
def test_fmt_money():
    assert fmt.fmt_money(0.0) == "—"
    assert fmt.fmt_money(-5.5) == "($5.50)"
    assert fmt.fmt_money(1234.5) == "$1,234.50"


def test_invoice_skips_unallocated_bucket_synthetic(server, monkeypatch):
    """The invoice builder must not emit an invoice for x_CostCenter=None (unallocated)."""
    rows = pl.DataFrame(
        {
            "BilledCost": [50.0, 25.0],
            "ChargePeriodStart": [date(2025, 6, 1), date(2025, 6, 1)],
            "x_CostCenter": ["cc-a", None],
            "ServiceName": ["Compute Engine", "Networking"],
        }
    )
    schema = {
        "BilledCost": "Float64",
        "ChargePeriodStart": "Datetime",
        "x_CostCenter": "String",
        "ServiceName": "String",
    }
    _patch_cost_reader(monkeypatch, schema, rows)
    invoices, mat = asyncio.run(
        invoice_tools._build_chargeback_invoices(
            _DummySoe(),
            "focus",
            date(2025, 6, 1),
            date(2025, 7, 1),
            "June 2025",
            10.0,
            None,
            None,
            None,
            None,
            None,
        )
    )
    # cc-a is the only non-unallocated cost center.
    assert len(invoices) == 1
    assert invoices[0]["cost_center"] == "cc-a"


# ── C10: LLM org/cost-center classifier (x_* only, provider-tags filtered) ──


@pytest.mark.asyncio
async def test_classify_uses_llm_and_filters_provider_tags(monkeypatch):
    """The classifier input is LIMITED to x_* columns and provider-prefixed tags are filtered out
    before the model sees them."""
    seen = []

    async def fake_classifier(candidate_x):
        seen.append(list(candidate_x))
        return {"organization_column": "x_OrgUnit", "cost_center_column": "x_TeamId"}

    monkeypatch.setattr(cr, "LLM_COLUMN_CLASSIFIER", fake_classifier)
    schema = {
        "x_OrgUnit": "String",
        "x_TeamId": "String",
        "x_aws_account": "String",
        "x_gcp_project": "String",
        "BilledCost": "Float64",
    }
    res = await cr.classify_org_cost_center(schema)
    assert res["organization"] == "x_OrgUnit"
    assert res["cost_center"] == "x_TeamId"
    # provider-prefixed x_* was filtered OUT of the classifier input.
    assert seen == [["x_OrgUnit", "x_TeamId"]]


@pytest.mark.asyncio
async def test_classify_refuses_noncandidate_output(monkeypatch):
    """The classifier must never return a column outside the x_* candidates (no guessing): a
    non-x_* column and a provider-prefixed column are both refused → kept None."""

    async def fake_classifier(candidate_x):
        return {"organization_column": "BilledCost", "cost_center_column": "x_gcp_project"}

    monkeypatch.setattr(cr, "LLM_COLUMN_CLASSIFIER", fake_classifier)
    schema = {"x_OrgUnit": "String", "BilledCost": "Float64"}
    res = await cr.classify_org_cost_center(schema)
    assert res["organization"] is None
    assert res["cost_center"] is None


@pytest.mark.asyncio
async def test_classify_refuses_single_column_as_both(monkeypatch):
    """A single column cannot be both org and cost-center."""

    async def fake_classifier(candidate_x):
        return {"organization_column": "x_TeamId", "cost_center_column": "x_TeamId"}

    monkeypatch.setattr(cr, "LLM_COLUMN_CLASSIFIER", fake_classifier)
    schema = {"x_TeamId": "String"}
    res = await cr.classify_org_cost_center(schema)
    assert res["organization"] is None
    assert res["cost_center"] is None  # both were refused because they collided


@pytest.mark.asyncio
async def test_classify_falls_back_on_llm_failure(monkeypatch):
    """A model/gateway failure must NEVER block chargeback — degrade to deterministic x_* defaults."""

    async def fake_classifier(candidate_x):
        raise RuntimeError("gateway down")

    monkeypatch.setattr(cr, "LLM_COLUMN_CLASSIFIER", fake_classifier)
    schema = {"x_Organization": "String", "x_CostCenter": "String", "BilledCost": "Float64"}
    res = await cr.classify_org_cost_center(schema)
    assert res["organization"] == "x_Organization"
    assert res["cost_center"] == "x_CostCenter"


@pytest.mark.asyncio
async def test_classify_empty_x_defaults(monkeypatch):
    """No x_* columns → neither dimension discovered, classifier never called."""
    calls = []

    async def fake_classifier(candidate_x):
        calls.append(list(candidate_x))
        return {"organization_column": None, "cost_center_column": None}

    monkeypatch.setattr(cr, "LLM_COLUMN_CLASSIFIER", fake_classifier)
    schema = {"BilledCost": "Float64", "ChargePeriodStart": "Datetime"}
    res = await cr.classify_org_cost_center(schema)
    assert res["organization"] is None
    assert res["cost_center"] is None
    assert calls == []


@pytest.mark.asyncio
async def test_resolve_override_beats_classifier():
    """An explicit override always wins over the (LLM) classification."""
    schema = {"x_Organization": "String"}
    assert (await cr.resolve_org_column(schema, override="x_custom_org")) == "x_custom_org"
    assert (await cr.resolve_org_column(schema, classification={"organization": "x_Organization"})) == "x_Organization"

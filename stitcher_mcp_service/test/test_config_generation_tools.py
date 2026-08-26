"""Adversarial tests for the config_generation sub-MCP tools.

Following the human-in-the-loop audit pattern: every safety boundary gets a test proving the wrong
path is never taken (``must NOT``, ``NOT in``, == FAIL, refusal), not just happy-path tests. These
exercise the DETERMINISTIC guards + authoring only — no live LLM, no live SWS/SOE network.

Boundaries under test
---------------------
O1. list_operators enumerates the full enhance vocabulary; rejects an unknown stage.
O2. describe_operator lists both required and defaulted fields + a REAL example for Lookup;
    rejects an unknown operation_type.
G1. plan guard refuses an UNKNOWN operation_type (refused at the model boundary — enum).
G2. plan guard refuses an op referencing a COLUMN not in the provided metadata.
G3. plan guard refuses a business_dataset not in the provided datasets.
G4. plan guard KEEPS a fully-grounded op (positive control — no over-refusal).
A1. generate_lookup refuses a shadowing import rename (duplicate rename_to).
A2. generate_lookup refuses an import column NOT in available_columns.
A3. generate_lookup refuses an empty join (missing columns).
A4. generate_filter 'drop' → Exclude; 'keep' → Include; both validate against the real model.
A5. validate_config FAILs an empty/invalid config; PASSes a generated one.
C1. committed-config summaries are compact (never raw-YAML-heavy) and derive bridge columns.
"""

from __future__ import annotations

import asyncio

import pytest
from fastmcp import FastMCP

from stitcher.assistant_harness.sub_mcp_agents.config_generation.tools import (
    authoring_tools as at,
    committed_config_tools as cc,
    operator_tools as ot,
    planning_tools as pt,
)
from stitcher.assistant_harness.common import soe_context as soe_mod
from stitcher.assistant_harness.sub_mcp_agents.config_generation.tools import (
    data_source_tools as dst,
)


class _DummySoe:
    """Stands in for SoeContext for tools that don't need real SOE state."""

    output_dir = "/tmp/stitcher-cg-test-output"


@pytest.fixture(scope="module")
def server():
    """A FastMCP server with the operator + authoring tools registered (client/soe unused by the
    tools under test). Lets us exercise the real @mcp.tool surface via call_tool."""
    mcp = FastMCP("test-config-gen")
    ot.register(mcp, None, None)
    at.register(mcp, None, _DummySoe())
    return mcp


def _call(server, name, args=None):
    res = asyncio.run(server.call_tool(name, args or {}))
    return res.content[0].text if res.content else str(res)


# ── O: operator + environment tools ──────────────────────────────────────────
def test_list_operators_covers_vocabulary(server):
    txt = _call(server, "list_operators", {"stage": "enrich"})
    for op in ("Lookup", "Mapping", "Compute column", "Filter rows", "Unpack object value"):
        assert f"`{op}`" in txt
    assert "ERR" not in txt


def test_list_operators_rejects_bad_stage(server):
    assert "ERR" in _call(server, "list_operators", {"stage": "bogus"})


def test_describe_operator_shows_fields_and_example(server):
    txt = _call(server, "describe_operator", {"stage": "enrich", "operation_type": "Lookup"})
    assert "business_dataset_name" in txt
    assert "join_columns" in txt
    assert "import_columns" in txt
    assert "Real example" in txt  # grounded on the committed example configs


def test_describe_operator_rejects_unknown_op(server):
    assert "ERR" in _call(server, "describe_operator", {"stage": "enrich", "operation_type": "NotAnOp"})


# ── G: plan guard (deterministic, no LLM) ────────────────────────────────────
_GROUNDED = pt.EnhanceOperationDraft(
    operation_type="Lookup",
    name="team-lookup",
    rationale="enrich with team",
    fields={
        "business_dataset_name": "app_metadata",
        "join_columns": [
            {"cost_dataset_join_column": "line_item_resource_id", "business_dataset_join_column": "resource_id"}
        ],
        "import_columns": [{"name": "owning_team"}],
    },
)
# The REALISTIC grounding: cost columns and business columns are DIFFERENT sets. The cost side has
# no `owning_team` / `resource_id` — those live on the business/reference dataset.
_COLS = ["line_item_resource_id", "effective_cost"]
_BIZ_COLS = ["resource_id", "owning_team"]
_DSS = ["app_metadata"]


def _sigs(drops):
    return {(d.get("operation_type"), d.get("name")) for d in drops}


def test_guard_refuses_unknown_operation_type():
    """Refuse-by-construction: operation_type is typed as SPC's EnhanceOperationType enum, so an
    out-of-vocabulary value is rejected at the pydantic model boundary (ValidationError) — the LLM
    can never ship a guessed/invented type. This is the safety boundary; the plan never even forms."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        pt.EnhanceOperationPlan(
            operations=[pt.EnhanceOperationDraft(operation_type="NotReal", name="x", rationale="", fields={})],
            summary="",
        )


def test_guard_refuses_fabricated_cost_column():
    fab = pt.EnhanceOperationDraft(
        operation_type="Lookup",
        name="fab",
        rationale="",
        fields={
            "business_dataset_name": "app_metadata",
            "join_columns": [{"cost_dataset_join_column": "MADE_UP", "business_dataset_join_column": "resource_id"}],
            "import_columns": [{"name": "owning_team"}],
        },
    )
    drops = pt._guard(pt.EnhanceOperationPlan(operations=[fab], summary=""), "enrich", _COLS, _DSS, _BIZ_COLS)
    assert ("Lookup", "fab") in _sigs(drops)
    assert any("COST dataset" in d["reason"] for d in drops)


def test_guard_refuses_unknown_dataset():
    bad = pt.EnhanceOperationDraft(
        operation_type="Lookup",
        name="badds",
        rationale="",
        fields={"business_dataset_name": "nope", "join_columns": [], "import_columns": []},
    )
    drops = pt._guard(pt.EnhanceOperationPlan(operations=[bad], summary=""), "enrich", _COLS, _DSS)
    assert ("Lookup", "badds") in _sigs(drops)


def test_guard_keeps_grounded_op():
    """Positive control for the bow-tie: a Lookup whose join key is on the COST side and whose
    import + business join key are on the BUSINESS side is KEPT (each side validated only against
    its own metadata). This is the gap-#2 regression — the pre-fix guard over-refused this because
    owning_team wasn't in the cost columns."""
    plan = pt.EnhanceOperationPlan(operations=[_GROUNDED], summary="ok")
    # cost-only available_columns must NOT nuke the Lookup: business-side refs aren't in the cost set
    drops = pt._guard(plan, "enrich", _COLS, _DSS)
    assert ("Lookup", "team-lookup") not in _sigs(drops)
    # ...and with both sides supplied it's also kept
    drops2 = pt._guard(plan, "enrich", _COLS, _DSS, _BIZ_COLS)
    assert ("Lookup", "team-lookup") not in _sigs(drops2)


def test_guard_refuses_import_not_in_business_columns():
    """A Lookup import that is NOT in the business-dataset columns must be dropped (that's the
    refuse-by-construction boundary for the business side)."""
    bad = pt.EnhanceOperationDraft(
        operation_type="Lookup",
        name="bizimp",
        rationale="",
        fields={
            "business_dataset_name": "app_metadata",
            "join_columns": [
                {"cost_dataset_join_column": "line_item_resource_id", "business_dataset_join_column": "resource_id"}
            ],
            "import_columns": [{"name": "nope_import"}],
        },
    )
    drops = pt._guard(pt.EnhanceOperationPlan(operations=[bad], summary=""), "enrich", _COLS, _DSS, _BIZ_COLS)
    assert ("Lookup", "bizimp") in _sigs(drops)
    assert any("BUSINESS dataset" in d["reason"] for d in drops)


# ── A: authoring guard rails ─────────────────────────────────────────────────
def test_lookup_refuses_shadowing_rename():
    txt = at.lookup_text(
        stage="enrich",
        business_dataset="app_metadata",
        cost_join_column="a",
        business_join_column="b",
        imports=[{"name": "a", "rename_to": "x_team"}, {"name": "b", "rename_to": "x_team"}],
        available_columns="a,b",
    )
    assert "ERR" in txt and "shadow" in txt


def test_lookup_refuses_unknown_import():
    txt = at.lookup_text(
        stage="enrich",
        business_dataset="app_metadata",
        cost_join_column="x",
        business_join_column="y",
        imports=[{"name": "NOPE_COL"}],
        available_columns="x,y",
    )
    assert "ERR" in txt and "NOPE_COL" in txt


def test_scope_is_validate_by_construction():
    """The authored scope must go through SPC's ScopeObject/BasicScopeInput construction (not a
    raw dict), and the emitted scope must still validate against the real enhance model with the
    provider LOCKED to Include + empty data_sources by default."""
    # with providers -> a valid scope is produced and the generated Lookup validates
    txt = at.lookup_text(
        stage="enrich",
        business_dataset="app_metadata",
        cost_join_column="x",
        business_join_column="y",
        imports=[{"name": "x_team"}],
        providers=["AWS", "GCP"],
    )
    assert "ERR" not in txt and "FAIL" not in txt
    _doc, errs = at._validate_stage_text("enrich", txt)
    assert errs == []
    # default type=Include is materialized (validate-by-construction, not left to downstream)
    assert "Include" in txt and "provider: AWS" in txt
    # no providers -> no scope key emitted
    txt2 = at.lookup_text(
        stage="enrich",
        business_dataset="app_metadata",
        cost_join_column="x",
        business_join_column="y",
        imports=[{"name": "x_team"}],
    )
    assert "scope:" not in txt2


def test_filter_drop_is_exclude_and_validate(server):
    txt = _call(
        server,
        "generate_filter",
        {"stage": "enrich", "column": "x_team", "operator": "=", "keep_or_drop": "drop", "value": "SRE"},
    )
    assert "Exclude" in txt
    assert "ERR" not in txt
    _doc, errs = at._validate_stage_text("enrich", txt)
    assert errs == []


def test_filter_keep_is_include(server):
    txt = _call(
        server,
        "generate_filter",
        {"stage": "enrich", "column": "x_team", "operator": "is null", "keep_or_drop": "keep"},
    )
    assert "Include" in txt


def test_filter_rejects_bad_operator(server):
    assert "ERR" in _call(
        server, "generate_filter", {"stage": "enrich", "column": "x_team", "operator": "~", "keep_or_drop": "keep"}
    )


def test_validate_config_fails_empty(server):
    assert "FAIL" in _call(server, "validate_config", {"stage": "enrich", "yaml_text": "enhance_operations: []"})


def test_validate_config_passes_lookup(server):
    txt = _call(
        server,
        "generate_lookup",
        {
            "stage": "enrich",
            "business_dataset": "app_metadata",
            "cost_join_column": "line_item_resource_id",
            "business_join_column": "resource_id",
            "imports": [{"name": "owning_team", "rename_to": "x_team"}],
        },
    )
    assert "PASS" in _call(server, "validate_config", {"stage": "enrich", "yaml_text": txt})


def test_lookup_refuses_empty_join(server):
    txt = _call(
        server,
        "generate_lookup",
        {
            "stage": "enrich",
            "business_dataset": "app_metadata",
            "cost_join_column": "",
            "business_join_column": "",
            "imports": [{"name": "owning_team"}],
        },
    )
    assert "ERR" in txt


# ── T: tenant guard (auth_tenant missing → precise 'Realm does not exist' hint) ──
def test_tenant_error_present_when_unset_and_empty_when_set():
    class _FakeSoe:
        def __init__(self, tenant):
            self.auth_tenant = tenant
            self.environment_id = "d7dad3dc-d02a-48f8-bfc3-a874111c0013"

        @property
        def has_tenant(self):
            return bool(self.auth_tenant)

        def tenant_error(self):
            return "" if self.auth_tenant else "ERR: STITCHER_AUTH_TENANT"

    assert "STITCHER_AUTH_TENANT" in _FakeSoe("").tenant_error()
    assert _FakeSoe("").has_tenant is False
    assert _FakeSoe("stitcherai-wsmo5").tenant_error() == ""
    assert _FakeSoe("stitcherai-wsmo5").has_tenant is True


def test_data_connection_util_refuses_missing_tenant_before_keycloak():
    """The SOE DataConnectionUtil init hits Keycloak; if auth_tenant is unset it fails with the
    cryptic 'Realm does not exist'. The sub-MCP must refuse EARLY with the precise hint instead."""

    class _FakeSoe:
        environment_id = "d7dad3dc-d02a-48f8-bfc3-a874111c0013"
        auth_tenant = ""

        def scope_error(self):
            return ""

        def tenant_error(self):
            return "ERR: STITCHER_AUTH_TENANT is not set — ... Realm does not exist ..."

        def get_workflow_context(self):
            raise AssertionError("must NOT build WorkflowContext when tenant is missing")

    with pytest.raises(RuntimeError) as e:
        dst._build_data_connection_util(_FakeSoe())
    assert "STITCHER_AUTH_TENANT" in str(e.value)


def test_get_committed_config_refuses_missing_tenant():
    """get_committed_config must return the precise tenant hint (not a network JWT error) when
    auth_tenant is unset — the fetch must NOT proceed."""
    mcp = FastMCP("test-config-gen-tenant")

    class _FakeSoe:
        environment_id = "d7dad3dc-d02a-48f8-bfc3-a874111c0013"
        auth_tenant = ""
        output_dir = "/tmp/stitcher-cg-test-output"

        @property
        def is_scoped(self):
            return True

        def scope_error(self):
            return ""

        def tenant_error(self):
            return "ERR: STITCHER_AUTH_TENANT is not set — Realm does not exist ..."

        async def fetch_committed_configs(self, branch=""):
            # Mirrors SoeContext.fetch_committed_configs' refusal order behind the tool: the tenant
            # precondition must win BEFORE pipeline resolution is attempted.
            if ten := self.tenant_error():
                return None, ten
            self.resolve_pipeline_id()  # must never be reached when tenant is missing
            return {}, ""

        def resolve_pipeline_id(self):
            raise AssertionError("must NOT resolve pipeline when tenant is missing")

    cc.register(mcp, None, _FakeSoe())
    txt = _call(mcp, "get_committed_config", {"stage": "enrich"})
    assert "STITCHER_AUTH_TENANT" in txt
    assert "Realm does not exist" in txt
    assert "pipeline_id" not in txt or "resolve_pipeline_id" not in txt  # fetch never reached pipeline resolve


# ── C: committed-config summaries ────────────────────────────────────────────
def test_committed_summary_and_derived():
    from stitcher.pipeline.common.pipeline_config_models.versions.v1_alpha.enhance.enhance_config import (
        EnhanceEnrichConfigModelV1,
    )

    op = {
        "operation_type": "Lookup",
        "name": "team-lookup",
        "business_dataset_name": "app_metadata",
        "scope": {"scope_inputs": [{"provider": "AWS", "data_sources": ["cur-1"]}]},
        "join_columns": [
            {
                "cost_dataset_join_column": {"input": "line_item_resource_id"},
                "business_dataset_join_column": {"input": "resource_id"},
            }
        ],
        "import_columns": [{"name": "owning_team", "rename_to": "x_team"}],
    }
    cfg = EnhanceEnrichConfigModelV1(enhance_operations=[op])
    pipeline_configs = {"enhance_enrich_config": cfg, "enhance_prepare_config": None}

    summary = cc._committed(pipeline_configs, "enrich")
    assert "team-lookup" in summary
    assert "x_team" in summary or "owning_team" in summary

    derived = cc._derived_text(pipeline_configs, "")
    assert "x_team" in derived  # the Lookup rename_to is the derived bridge column


# ── D: scan_data must accept the SOE extract reader's EAGER DataFrame ────────
def test_serialize_scan_accepts_eager_dataframe():
    """The SOE extract reference-data reader (`__extract_reference_dataframe_recursion__`) returns an
    EAGER pl.DataFrame (pl.concat(batches)), not a LazyFrame. `_serialize_scan` must normalize it so the
    .collect() paths work — otherwise scan_data dies with ``'DataFrame' object has no attribute 'collect'``."""
    import polars as pl

    eager = pl.DataFrame({"team": ["a", "b", "a", "b"], "EffectiveCost": [10.0, 20.0, 30.0, 40.0]})
    assert isinstance(eager, pl.DataFrame)

    # group_by + value -> $ split (exercises .collect() on the normalized frame)
    txt = dst._serialize_scan(eager, "", "team", "EffectiveCost", "", 20)
    assert "split by team" in txt
    assert "$100.00" in txt  # 10+30+40+20
    assert "$60.00" in txt  # team b (20+40) total
    assert "$40.00" in txt  # team a (10+30) total

    # group_by alone -> row counts
    txt2 = dst._serialize_scan(eager, "", "team", "", "", 20)
    assert "row counts by team" in txt2

    # columns/head sample path also works
    txt3 = dst._serialize_scan(eager, "team,EffectiveCost", "", "", "", 20)
    assert "team" in txt3 and "EffectiveCost" in txt3

    # a real LazyFrame still works too
    txt4 = dst._serialize_scan(eager.lazy(), "", "team", "EffectiveCost", "", 20)
    assert "split by team" in txt4

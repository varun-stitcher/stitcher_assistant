"""Adversarial tests for the harness-native plan generator (plan_generation_tools).

Following the human-in-the-loop audit pattern: every safety boundary gets a test
proving the wrong path is never taken (``must NOT``, ``not in``, ``== 0``,
``is None``), not just happy-path tests. These tests exercise the DETERMINISTIC
guards only — no LLM calls are made.

Boundaries under test
---------------------
G1. Column-fidelity grounding: a mapping whose ``source_column`` is NOT a real
    column of the DataFrame is DROPPED (never fabricated / never silently
    merged) — refuse-by-construction.
G2. Duplicate focus columns: the first mapping per FOCUS column wins and
    duplicates are DROPPED and reported.
G3. SET_STATIC_VALUE is exempt from the source-column presence check (a literal
    needs no source column), but only when it carries an explicit literal —
    otherwise it is untranslated, never a silent default value.
G4. Deterministic translation emits the expected step type + fields per
    transform function (datetime_from_string carries the declared format;
    rename carries source_column; assign_timezone defaults to UTC).
G5. Every guarded drop is REPORTED (``dropped`` is non-empty and enumerates
    reasons) — nothing is silently discarded.
G6. The emitted config validates against the real ``InlineNormalizeDatasourceDto``
    pydantic model (positive control for G4).
"""

import polars as pl

from stitcher.assistant_harness.sub_mcp_agents.custom_cost.tools import plan_generation_tools as pgt
from stitcher.pipeline.common.focus_column_names import FocusColumnNames as F
from stitcher.pipeline.common.pipeline_config_models.versions.v1_alpha.normalize.normalize_config import (
    InlineNormalizeDatasourceDto,
)
from stitcher.pipeline.common.pipeline_config_models.versions.v1_alpha.normalize.transform_configs.base_config import (
    TransformFunctionNames as T,
)
from stitcher.pipeline.common.plan_generation_workflow.models import (
    FOCUSColumnMappingInput,
    MappingFunctions,
)

# ── Helpers ────────────────────────────────────────────────────────────────


def _df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "Billing period": ["2025-01-01", "2025-02-01"],
            "Amount": ["-12.5", "3.2"],
        }
    )


def _map(src: str, fn: T, fc, target: str = "string", fmt: str | None = None, hint: str | None = None) -> FOCUSColumnMappingInput:
    return FOCUSColumnMappingInput(
        source_column=src,
        conversion_function=fn,
        focus_column_name=fc,
        target_type=target,
        datetime_format=fmt,
        is_source_already_datetime=False,
        transformation_hint=hint,
    )


def _ground(df: pl.DataFrame, mappings: MappingFunctions):
    kept, dropped = pgt.ground_mappings(df, mappings)
    config, built, untranslated = pgt.mappings_to_config(mappings, kept)
    return kept, dropped, config, built, untranslated


# ── G1: fabricated source column is dropped ────────────────────────────────


def test_fabricated_source_column_is_dropped_not_fabricated():
    df = _df()
    mappings = MappingFunctions(
        billing_period_start_column=_map("RealCol", T.RENAME_COLUMN, F.BILLING_PERIOD_START),
        billing_period_end_column=_map("DoesNotExist", T.RENAME_COLUMN, F.BILLING_PERIOD_END),
        charge_period_start=_map("Billing period", T.RENAME_COLUMN, F.CHARGE_PERIOD_START),
        charge_period_end=_map("Billing period", T.RENAME_COLUMN, F.CHARGE_PERIOD_END),
        other_focus_column=[],
    )
    kept, dropped, config, built, _ = _ground(df, mappings)
    focus_cols = [m.focus_column_name.value for m in kept]
    # BillingPeriosStart uses a fabricated source → dropped; the real ones survive.
    assert "BillingPeriodEnd" not in focus_cols  # fabricated source → gone
    assert "ChargePeriodStart" in focus_cols
    assert "ChargePeriodEnd" in focus_cols
    assert any(d["reason"].startswith("source column not present") for d in dropped)
    assert pgt._mapping_col(mappings.billing_period_end_column) == "BillingPeriodEnd"
    # The dropped mapping must NOT be in the config either.
    built_cols = [fc["focus_column"] for fc in config["focus_columns"]]
    assert "BillingPeriodEnd" not in built_cols


# ── G2: duplicate focus columns are dropped (first wins) ───────────────────


def test_duplicate_focus_column_dropped():
    df = _df()
    mappings = MappingFunctions(
        billing_period_start_column=_map("Billing period", T.RENAME_COLUMN, F.BILLING_PERIOD_START),
        billing_period_end_column=_map("Billing period", T.RENAME_COLUMN, F.BILLING_PERIOD_END),
        charge_period_start=_map("Billing period", T.RENAME_COLUMN, F.CHARGE_PERIOD_START),
        charge_period_end=_map("Billing period", T.RENAME_COLUMN, F.CHARGE_PERIOD_END),
        other_focus_column=[
            _map("Amount", T.RENAME_COLUMN, F.BILLED_COST),
            _map("Amount", T.RENAME_COLUMN, F.BILLED_COST),  # duplicate
        ],
    )
    kept, dropped, config, built, _ = _ground(df, mappings)
    assert sum(1 for m in kept if m.focus_column_name.value == "BilledCost") == 1
    assert any(d["reason"] == "duplicate focus column (kept first)" for d in dropped)
    assert sum(1 for fc in config["focus_columns"] if fc["focus_column"] == "BilledCost") == 1


# ── G3: static value exemption + no silent default ─────────────────────────


def test_static_value_exempt_from_source_presence_with_literal():
    df = _df()
    mappings = MappingFunctions(
        billing_period_start_column=_map("Billing period", T.RENAME_COLUMN, F.BILLING_PERIOD_START),
        billing_period_end_column=_map("Billing period", T.RENAME_COLUMN, F.BILLING_PERIOD_END),
        charge_period_start=_map("Billing period", T.RENAME_COLUMN, F.CHARGE_PERIOD_START),
        charge_period_end=_map("Billing period", T.RENAME_COLUMN, F.CHARGE_PERIOD_END),
        other_focus_column=[
            _map("NotARealColumn", T.SET_STATIC_VALUE, F.PROVIDER, hint="AWS"),
        ],
    )
    kept, dropped, config, built, untranslated = _ground(df, mappings)
    # Static value needs no source column → kept despite the fake source.
    assert any(m.focus_column_name.value == "ProviderName" for m in kept)
    # Its step is a real set_static_value with the LLM's literal — never a default.
    provider_step = [fc for fc in config["focus_columns"] if fc["focus_column"] == "ProviderName"][0]["steps"][0]
    assert provider_step["type"] == "General.set_static_value"
    assert provider_step["static_value"] == "AWS"


def test_static_value_without_literal_is_untranslated_not_defaulted():
    df = _df()
    mappings = MappingFunctions(
        billing_period_start_column=_map("Billing period", T.RENAME_COLUMN, F.BILLING_PERIOD_START),
        billing_period_end_column=_map("Billing period", T.RENAME_COLUMN, F.BILLING_PERIOD_END),
        charge_period_start=_map("Billing period", T.RENAME_COLUMN, F.CHARGE_PERIOD_START),
        charge_period_end=_map("Billing period", T.RENAME_COLUMN, F.CHARGE_PERIOD_END),
        other_focus_column=[_map("Any", T.SET_STATIC_VALUE, F.PROVIDER, hint=None)],
    )
    _, _, config, _, untranslated = _ground(df, mappings)
    assert any(u["focus_column"] == "ProviderName" for u in untranslated)
    assert all(fc["focus_column"] != "ProviderName" for fc in config["focus_columns"])


# ── G4: deterministic translation per transform function ───────────────────


def test_datetime_from_string_carries_declared_format():
    df = _df()
    mappings = MappingFunctions(
        billing_period_start_column=_map("Billing period", T.DATETIME_FROM_STRING, F.BILLING_PERIOD_START, target="datetime", fmt="%Y-%m-%d"),
        billing_period_end_column=_map("Billing period", T.RENAME_COLUMN, F.BILLING_PERIOD_END),
        charge_period_start=_map("Billing period", T.RENAME_COLUMN, F.CHARGE_PERIOD_START),
        charge_period_end=_map("Billing period", T.RENAME_COLUMN, F.CHARGE_PERIOD_END),
        other_focus_column=[],
    )
    _, _, config, _, _ = _ground(df, mappings)
    start = [fc for fc in config["focus_columns"] if fc["focus_column"] == "BillingPeriodStart"][0]["steps"][0]
    assert start["type"] == "String.datetime_from_string"
    assert start["source_column"] == "Billing period"
    assert start["format"] == "%Y-%m-%d"


def test_assign_timezone_defaults_to_utc():
    df = _df()
    mappings = MappingFunctions(
        billing_period_start_column=_map("Billing period", T.ASSIGN_TIMEZONE, F.BILLING_PERIOD_START, target="datetime"),
        billing_period_end_column=_map("Billing period", T.RENAME_COLUMN, F.BILLING_PERIOD_END),
        charge_period_start=_map("Billing period", T.RENAME_COLUMN, F.CHARGE_PERIOD_START),
        charge_period_end=_map("Billing period", T.RENAME_COLUMN, F.CHARGE_PERIOD_END),
        other_focus_column=[],
    )
    _, _, config, _, _ = _ground(df, mappings)
    start = [fc for fc in config["focus_columns"] if fc["focus_column"] == "BillingPeriodStart"][0]["steps"][0]
    assert start["type"] == "Datetime.assign_timezone"
    assert start["tz"] == "UTC"


# ── G5: every drop is reported ─────────────────────────────────────────────


def test_dropped_mappings_are_reported():
    df = _df()
    mappings = MappingFunctions(
        billing_period_start_column=_map("Nope1", T.RENAME_COLUMN, F.BILLING_PERIOD_START),
        billing_period_end_column=_map("Nope2", T.RENAME_COLUMN, F.BILLING_PERIOD_END),
        charge_period_start=_map("Billing period", T.RENAME_COLUMN, F.CHARGE_PERIOD_START),
        charge_period_end=_map("Billing period", T.RENAME_COLUMN, F.CHARGE_PERIOD_END),
        other_focus_column=[_map("Amount", T.RENAME_COLUMN, F.BILLED_COST), _map("Amount", T.RENAME_COLUMN, F.BILLED_COST)],
    )
    _, dropped, _, _, _ = _ground(df, mappings)
    reasons = [d["reason"] for d in dropped]
    assert any(r.startswith("source column not present") for r in reasons)
    assert any(r == "duplicate focus column (kept first)" for r in reasons)
    # Nothing dropped is silently swallowed: total mapped = kept + reported drops.
    assert len(dropped) == 3  # 2 fabricated + 1 duplicate


# ── G6: emitted config validates against the real pydantic model ───────────


def test_emitted_config_validates_against_inline_model():
    df = _df()
    mappings = MappingFunctions(
        billing_period_start_column=_map("Billing period", T.RENAME_COLUMN, F.BILLING_PERIOD_START),
        billing_period_end_column=_map("Billing period", T.RENAME_COLUMN, F.BILLING_PERIOD_END),
        charge_period_start=_map("Billing period", T.RENAME_COLUMN, F.CHARGE_PERIOD_START),
        charge_period_end=_map("Billing period", T.RENAME_COLUMN, F.CHARGE_PERIOD_END),
        other_focus_column=[_map("Amount", T.RENAME_COLUMN, F.BILLED_COST)],
    )
    _, _, config, _, _ = _ground(df, mappings)
    validated = InlineNormalizeDatasourceDto.model_validate(config)
    assert validated.converter_plan_name == "harness_plan_generated"
    assert {fc.focus_column.value for fc in validated.focus_columns} == {
        "BillingPeriodStart",
        "BillingPeriodEnd",
        "ChargePeriodStart",
        "ChargePeriodEnd",
        "BilledCost",
    }

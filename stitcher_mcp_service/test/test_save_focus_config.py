"""Adversarial tests for `save_focus_config` — the persist boundary of the
"fix the normalize configs" loop. The tool refuses bad input by construction;
these tests prove the wrong path is never taken:

  * an empty / non-object / schema-invalid config is refused (never written);
  * a JSON-STRING payload — the exact shape models hand back (found by the
    live integration test UC2) — is PARSED and accepted when valid, REFUSED
    with a clear error when not (never a silent fake success);
  * a valid save round-trips through the PRODUCTION NormalizeConfigLoader.
"""

from __future__ import annotations

import asyncio
import json

import polars as pl
import pytest

from stitcher.assistant_harness.sub_mcp_agents.custom_cost.tools.conversion import (
    conversion_tools as ct,
)
from stitcher.assistant_harness.sub_mcp_agents.custom_cost.tools.plan import (
    plan_generation_tools as pgt,
)
from stitcher.pipeline.common.focus_column_names import FocusColumnNames as F
from stitcher.pipeline.common.pipeline_config_models.versions.v1_alpha.normalize.transform_configs.base_config import (
    TransformFunctionNames as T,
)
from stitcher.pipeline.common.plan_generation_workflow.models import (
    FOCUSColumnMappingInput,
    MappingFunctions,
)


def _save_focus_config(config_json, name=None):
    """Call save_focus_config the way FastMCP serves it (it is registered inside
    conversion_tools.register — exercise the registered tool, not a copy)."""
    from fastmcp import FastMCP

    mcp = FastMCP(name="conversion-test")
    ct.register(mcp)
    tool = asyncio.run(mcp.get_tool("save_focus_config"))
    kwargs = {"config_json": config_json}
    if name is not None:
        kwargs["name"] = name
    return tool.fn(**kwargs)


# ── config builders (same grounding path as the plan generator) ──────────────


def _df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "Billing period": ["2025-01-01", "2025-02-01"],
            "Amount": ["-12.5", "3.2"],
        }
    )


def _map(src: str, fn: T, fc) -> FOCUSColumnMappingInput:
    return FOCUSColumnMappingInput(
        source_column=src,
        conversion_function=fn,
        focus_column_name=fc,
        target_type="string",
        datetime_format=None,
        is_source_already_datetime=False,
        transformation_hint=None,
    )


def _valid_config() -> dict:
    """A config the real InlineNormalizeDatasourceDto accepts (via the plan
    generator's grounding — same shape as the emitted-config G6 test)."""
    mappings = MappingFunctions(
        billing_period_start_column=_map("Billing period", T.RENAME_COLUMN, F.BILLING_PERIOD_START),
        billing_period_end_column=_map("Billing period", T.RENAME_COLUMN, F.BILLING_PERIOD_END),
        charge_period_start=_map("Billing period", T.RENAME_COLUMN, F.CHARGE_PERIOD_START),
        charge_period_end=_map("Billing period", T.RENAME_COLUMN, F.CHARGE_PERIOD_END),
        other_focus_column=[_map("Amount", T.RENAME_COLUMN, F.BILLED_COST)],
    )
    kept, _ = pgt.ground_mappings(_df(), mappings)
    config, _, _ = pgt.mappings_to_config(mappings, kept)
    return config


@pytest.fixture()
def config_dir(tmp_path, monkeypatch):
    d = tmp_path / "configs"
    d.mkdir()
    monkeypatch.setenv("FOCUS_CONFIG_OUTPUT_DIR", str(d))
    return d


class TestSaveFocusConfigRefusals:
    def test_empty_dict_refused_nothing_written(self, config_dir):
        out = _save_focus_config(config_json={})
        assert out["success"] is False and "non-empty" in out["error"]
        assert list(config_dir.glob("*.yaml")) == [], "a refused config must NOT be written"

    def test_invalid_json_string_refused_with_clear_error(self, config_dir):
        out = _save_focus_config(config_json="{not json")
        assert out["success"] is False and "not valid JSON" in out["error"]
        assert list(config_dir.glob("*.yaml")) == []

    def test_json_scalar_refused(self, config_dir):
        # VALID JSON that parses to a scalar — must be refused as a non-object
        out = _save_focus_config(config_json=json.dumps("just a string"))
        assert out["success"] is False and "JSON object" in out["error"]
        assert list(config_dir.glob("*.yaml")) == []

    def test_schema_invalid_dict_refused(self, config_dir):
        out = _save_focus_config(config_json={"bogus": True})
        assert out["success"] is False and "InlineNormalizeDatasourceDto" in out["error"]
        assert list(config_dir.glob("*.yaml")) == []


class TestSaveFocusConfigAcceptance:
    def test_valid_dict_saves_yaml(self, config_dir):
        config = _valid_config()
        out = _save_focus_config(config_json=config, name="roundtrip-test")
        assert out["success"] is True, out.get("error")
        assert list(config_dir.glob("roundtrip-test.yaml")) != []

    def test_json_string_payload_is_parsed_and_accepted(self, config_dir):
        """The boundary found by live integration test UC2: models routinely hand
        back a stringified payload — parse, don't refuse."""
        out = _save_focus_config(config_json=json.dumps(_valid_config()), name="from-string")
        assert out["success"] is True, out.get("error")
        assert list(config_dir.glob("from-string.yaml")) != []

    def test_saved_yaml_loads_via_production_loader(self, config_dir):
        config = _valid_config()
        out = _save_focus_config(config_json=config, name="loader-check")
        assert out["success"] is True, out.get("error")
        from stitcher.pipeline.common.config_loaders.normalize_config_loader import (
            NormalizeConfigLoader,
        )

        loaded = NormalizeConfigLoader(base_dir=str(config_dir)).load_configs()
        names = {ds.converter_plan_name for cfg in loaded for ds in cfg.data_source_normalizers}
        assert config["converter_plan_name"] in names

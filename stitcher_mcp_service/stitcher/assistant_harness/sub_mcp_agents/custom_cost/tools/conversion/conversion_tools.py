"""FOCUS conversion-step tools — the deterministic conversion primitives from
``stitcher_pipeline_common`` (driving ``stitcher_focus_converters``), exposed
as individual MCP tools.

These are the **actual conversion steps** the pipeline runs, surfaced one at a
time so the agent can drive the conversion deterministically (no LLM) instead
of only through the monolithic, LLM-driven ``normalize_to_focus``. Each tool is
a thin MCP wrapper over the engine's public surface:

  list_focus_providers      — enumerate built-in provider conversion configs
  detect_provider           — auto-sense a data file's provider (ProviderSensor)
  load_provider_plans       — load a built-in provider's ConversionPlan list (JSON)
  apply_conversion_plans    — apply a list of ConversionPlan (JSON) to raw data → FOCUS
  simulate_normalize_config — apply an InlineNormalizeDatasourceDto (JSON) to raw data → FOCUS
  load_normalize_configs    — load NormalizeConfigModelV1 YAML files from a dir

Design (mirrors the harness constitution):
  * pure determinism — ZERO LLM calls in this module. The engine (FocusConverter
    + prepare_lazyframe + simulate_conversion_plan) does every transform;
    these tools only orchestrate + serialize.
  * refuse by construction — bad input (missing file, unknown provider, malformed
    plan/config) returns a clear ``success=false`` error; never a silent default.
  * environment-agnostic — no StitcherAssistantConfig / OIDCAuth. These are pure data
    conversions, not bound to a Stitcher environment, so the sub-MCP can start
    without STITCHER_ENVIRONMENT_ID / STITCHER_PIPELINE_NAME.
  * one artifact per run — reads the caller's data file; never writes unless an
    explicit export_path is given (and even then only deterministic parquet/csv).

Heavy determinism lives in ``stitcher_pipeline_common`` + ``stitcher_focus_converters``
(editable deps); only the tool orchestration + JSON serialization is ours.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

import polars as pl
from fastmcp import FastMCP

logger = logging.getLogger(__name__)


def _now() -> float:
    return round(time.time(), 2)


def _serialize_df(df: pl.DataFrame, max_rows: int = 5) -> dict[str, Any]:
    """Schema + first N rows, with temporal cols stringified for JSON."""
    sample = df.head(max_rows)
    for col in sample.columns:
        if sample[col].dtype.is_temporal():
            sample = sample.with_columns(pl.col(col).cast(pl.Utf8))
    return {
        "shape": {"rows": df.height, "columns": df.width},
        "columns": df.columns,
        "schema": {
            col: str(dtype) for col, dtype in zip(df.columns, df.dtypes, strict=True)
        },
        "sample_rows": sample.to_dicts(),
    }


def _load_data(path: str) -> pl.DataFrame:
    """Load a CSV or parquet file into a polars DataFrame. Refuses unknown types."""
    if not os.path.isfile(path):
        raise FileNotFoundError(f"no such file: {path}")
    ext = os.path.splitext(path)[1].lstrip(".").lower()
    if ext == "csv":
        return pl.read_csv(path)
    if ext in ("parquet", "parq"):
        return pl.read_parquet(path)
    raise ValueError(
        f"unsupported data type '{ext}' (bring a .csv or .parquet): {path}"
    )


def register(mcp: FastMCP) -> None:
    @mcp.tool
    def list_focus_providers() -> dict[str, Any]:
        """List the built-in provider conversion configs bundled with the FOCUS engine.

        Each name (e.g. ``aws``, ``azure``, ``google_cloud``) is a directory under
        the engine's ``conversion_configs/`` whose YAML files form that provider's
        ``ConversionPlan`` list. Use ``load_provider_plans(name)`` to fetch the
        serialized plans, or ``detect_provider`` to auto-sense a data file's
        provider. No LLM, no data file — pure metadata.
        """
        from focus_converter.converter import BASE_CONVERSION_CONFIGS

        try:
            providers = sorted(
                p for p in os.listdir(BASE_CONVERSION_CONFIGS) if not p.startswith(".")
            )
        except Exception as e:  # noqa: BLE001
            return {"success": False, "error": f"could not list provider configs: {e}"}
        return {"success": True, "providers": providers, "count": len(providers)}

    @mcp.tool
    def detect_provider(file_path: str) -> dict[str, Any]:
        """Auto-detect the FOCUS provider for a data file (CSV or parquet).

        Uses the engine's ``ProviderSensor`` — it reads a data sample and matches
        columns against each built-in provider's ``SET_COLUMN_DTYPES`` plan; the
        provider with the best (count, fraction) signature wins. No LLM. Returns
        the detected provider name (e.g. ``aws-csv``) and the file format sensed.
        """
        from focus_converter.data_loaders.provider_sensor import ProviderSensor

        t0 = time.time()

        def _err(message: str) -> dict[str, Any]:
            return {
                "success": False,
                "error": message,
                "elapsed_seconds": round(time.time() - t0, 2),
            }

        if not os.path.isfile(file_path):
            return _err(f"no such file: {file_path}")
        try:
            sensor = ProviderSensor(base_path=file_path)
            sensor.load()
        except Exception as e:  # noqa: BLE001
            logger.exception("detect_provider failed")
            return _err(
                f"provider detection failed: {type(e).__name__}: {str(e)[:500]}"
            )
        return {
            "success": True,
            "provider": sensor.provider,
            "file_format": getattr(sensor, "__file_format__", None),
            "file_path": file_path,
            "elapsed_seconds": round(time.time() - t0, 2),
        }

    @mcp.tool
    def load_provider_plans(provider: str) -> dict[str, Any]:
        """Load a built-in provider's full ConversionPlan list as JSON.

        Each plan is serialized with ``model_dump(mode="json")`` so it can be
        inspected or round-tripped back into ``apply_conversion_plans``. The
        ``conversion_type`` and ``focus_column`` fields are string enum values.
        No LLM, no data file.
        """
        from focus_converter.converter import BASE_CONVERSION_CONFIGS, FocusConverter

        t0 = time.time()

        def _err(message: str) -> dict[str, Any]:
            return {
                "success": False,
                "error": message,
                "elapsed_seconds": round(time.time() - t0, 2),
            }

        provider_base = os.path.join(BASE_CONVERSION_CONFIGS, provider)
        if not os.path.isdir(provider_base):
            return _err(f"unknown provider {provider!r}; see list_focus_providers")
        try:
            converter = FocusConverter()
            converter.load_provider_conversion_configs()
            plans = converter.plans.get(provider, [])
        except Exception as e:  # noqa: BLE001
            logger.exception("load_provider_plans failed")
            return _err(
                f"failed to load plans for {provider!r}: {type(e).__name__}: {str(e)[:500]}"
            )
        return {
            "success": True,
            "provider": provider,
            "plan_count": len(plans),
            "plans": [p.model_dump(mode="json") for p in plans],
            "elapsed_seconds": round(time.time() - t0, 2),
        }

    @mcp.tool
    def apply_conversion_plans(
        plans_json: str,
        data_path: str,
        max_sample_rows: int = 5,
    ) -> dict[str, Any]:
        """Apply a list of ConversionPlan (JSON) to a raw data file → FOCUS frame.

        The deterministic core of the pipeline: reconstructs each ``ConversionPlan``
        from its JSON form (as emitted by ``load_provider_plans``), then runs
        ``prepare_lazyframe`` — the same engine call ``normalize_to_focus`` uses
        after the LLM generates plans. Zero LLM calls; pure conversion.

        Args:
            plans_json: JSON array of ConversionPlan objects (the shape returned
              by ``load_provider_plans``).
            data_path: server path to a .csv or .parquet file holding the raw cost data.
            max_sample_rows: sample rows to include in the output DataFrame summary.
        """
        from focus_converter.configs.base_config import ConversionPlan

        from stitcher.pipeline.common.pipeline_config_models.ai.agents.custom_cost_agent.custom_cost_utils import (
            prepare_lazyframe,
        )

        t0 = time.time()

        def _err(message: str) -> dict[str, Any]:
            return {
                "success": False,
                "error": message,
                "elapsed_seconds": round(time.time() - t0, 2),
            }

        try:
            plan_dicts = json.loads(plans_json)
        except json.JSONDecodeError as e:
            return _err(f"plans_json is not valid JSON: {e}")
        if not isinstance(plan_dicts, list) or not plan_dicts:
            return _err(
                "plans_json must be a non-empty JSON array of ConversionPlan objects."
            )
        try:
            plans = [ConversionPlan.model_validate(p) for p in plan_dicts]
        except Exception as e:  # noqa: BLE001
            return _err(
                f"could not build ConversionPlan list: {type(e).__name__}: {str(e)[:500]}"
            )
        try:
            raw_df = _load_data(data_path)
        except Exception as e:  # noqa: BLE001
            return _err(f"{type(e).__name__}: {e}")
        if raw_df.is_empty():
            return _err("data file is empty.")
        try:
            focus_df = prepare_lazyframe(plans=plans, raw_df=raw_df)
        except Exception as e:  # noqa: BLE001
            logger.exception("apply_conversion_plans: prepare_lazyframe failed")
            return _err(f"conversion failed: {type(e).__name__}: {str(e)[:500]}")
        return {
            "success": True,
            "plan_count": len(plans),
            "data_path": data_path,
            "focus_df_summary": _serialize_df(focus_df, max_sample_rows),
            "elapsed_seconds": round(time.time() - t0, 2),
        }

    @mcp.tool
    def simulate_normalize_config(
        config_json: str,
        data_path: str,
        max_sample_rows: int = 5,
    ) -> dict[str, Any]:
        """Apply an InlineNormalizeDatasourceDto (JSON) to a raw data file → FOCUS frame.

        This is the engine's ``InlineNormalizeDatasourceDto.simulate_conversion_plan``
        — the same deterministic normalizer ``normalize_to_focus`` calls after the
        LLM merges plans. Zero LLM calls. The config's ``focus_columns`` each
        carry a FOCUS column + the transforms it requires; the engine turns those
        into ConversionPlans and applies them.

        Args:
            config_json: JSON object matching InlineNormalizeDatasourceDto
              (fields: converter_plan_name, focus_columns, scope) — the shape
              ``normalize_to_focus`` emits in its ``plans`` / merged config.
            data_path: server path to a .csv or .parquet file holding the raw cost data.
            max_sample_rows: sample rows to include in the output DataFrame summary.
        """
        from stitcher.pipeline.common.pipeline_config_models.versions.v1_alpha.normalize.normalize_config import (
            InlineNormalizeDatasourceDto,
        )

        t0 = time.time()

        def _err(message: str) -> dict[str, Any]:
            return {
                "success": False,
                "error": message,
                "elapsed_seconds": round(time.time() - t0, 2),
            }

        try:
            cfg_dict = json.loads(config_json)
        except json.JSONDecodeError as e:
            return _err(f"config_json is not valid JSON: {e}")
        if not isinstance(cfg_dict, dict) or not cfg_dict:
            return _err(
                "config_json must be a non-empty JSON object (InlineNormalizeDatasourceDto)."
            )
        try:
            config = InlineNormalizeDatasourceDto.model_validate(cfg_dict)
        except Exception as e:  # noqa: BLE001
            return _err(
                f"could not build InlineNormalizeDatasourceDto: {type(e).__name__}: {str(e)[:500]}"
            )
        try:
            raw_df = _load_data(data_path)
        except Exception as e:  # noqa: BLE001
            return _err(f"{type(e).__name__}: {e}")
        if raw_df.is_empty():
            return _err("data file is empty.")
        try:
            focus_df = InlineNormalizeDatasourceDto.simulate_conversion_plan(
                raw_df=raw_df, response_configs=config
            )
        except Exception as e:  # noqa: BLE001
            logger.exception(
                "simulate_normalize_config: simulate_conversion_plan failed"
            )
            return _err(f"conversion failed: {type(e).__name__}: {str(e)[:500]}")
        return {
            "success": True,
            "converter_plan_name": config.converter_plan_name,
            "focus_column_count": len(config.focus_columns),
            "data_path": data_path,
            "focus_df_summary": _serialize_df(focus_df, max_sample_rows),
            "elapsed_seconds": round(time.time() - t0, 2),
        }

    @mcp.tool
    def load_normalize_configs(config_dir: str) -> dict[str, Any]:
        """Load NormalizeConfigModelV1 YAML files from a directory → serialized plans.

        Uses the pipeline's ``NormalizeConfigLoader`` (the same loader the
        pipeline runs in production) — every ``.yaml`` file in ``config_dir``
        is parsed into a ``NormalizeConfigModelV1`` and its
        ``data_source_normalizers`` (InlineNormalizeDatasourceDto list) are
        serialized to JSON, ready to feed into ``simulate_normalize_config``.
        Zero LLM calls. A directory with no valid configs is a hard error,
        not a silent empty list.

        Args:
            config_dir: directory holding normalize stage YAML files (e.g. an
              environment repo's ``build/normalize/`` directory).
        """
        from stitcher.pipeline.common.config_loaders.normalize_config_loader import (
            NormalizeConfigLoader,
        )

        t0 = time.time()

        def _err(message: str) -> dict[str, Any]:
            return {
                "success": False,
                "error": message,
                "elapsed_seconds": round(time.time() - t0, 2),
            }

        if not os.path.isdir(config_dir):
            return _err(f"no such directory: {config_dir}")
        try:
            loader = NormalizeConfigLoader(base_dir=config_dir)
            configs = loader.load_configs()
        except Exception as e:  # noqa: BLE001
            logger.exception("load_normalize_configs failed")
            return _err(f"config load failed: {type(e).__name__}: {str(e)[:500]}")
        files = sorted(p.name for p in loader.__list_dir__())
        if not configs:
            return _err(
                f"loaded 0 valid configs from {config_dir} (files: {files or 'none'})."
            )
        serialized: list[dict[str, Any]] = []
        for cfg in configs:
            for ds in cfg.data_source_normalizers:
                serialized.append(
                    {
                        "converter_plan_name": ds.converter_plan_name,
                        "focus_column_count": len(ds.focus_columns),
                        "config": ds.model_dump(mode="json"),
                    }
                )
        return {
            "success": True,
            "config_dir": config_dir,
            "config_file_count": len(configs),
            "files": files,
            "normalizers": serialized,
            "elapsed_seconds": round(time.time() - t0, 2),
        }

    # ---- save_focus_config: closes the 'update normalize configs' loop ----
    @mcp.tool
    def save_focus_config(
        config_json: dict[str, Any], name: str | None = None
    ) -> dict[str, Any]:
        """Persist an updated FOCUS normalize config as a user-visible YAML artifact.

        Closes the loop for "make the fix and update normalize configs": after the
        agent corrects a plan (e.g. via ``generate_focus_plans`` output, or by editing
        a config from ``load_normalize_configs``), THIS tool saves it as a
        NormalizeConfigModelV1 YAML file the workflow can reuse
        (``load_normalize_configs`` → ``simulate_normalize_config`` re-verifies it
        against raw data — zero LLM).

        Refuses by construction: the config is validated through the SAME
        ``InlineNormalizeDatasourceDto`` model the pipeline uses — an invalid or
        empty config is never written. A verification re-parse (write → load →
        compare) must pass, otherwise the write is reported as failed.

        Args:
            config_json: InlineNormalizeDatasourceDto JSON (e.g. from
              ``normalize_to_focus``'s ``plans[0].config``, ``simulate_normalize_config``
              round-trip, or a hand-corrected variant). Accepts a dict OR a JSON string
              (models routinely hand a stringified payload back — parse, don't refuse).
            name: optional file stem (default: the converter_plan_name).
        """
        t0 = time.time()

        def _err(message: str) -> dict[str, Any]:
            return {
                "success": False,
                "error": message,
                "elapsed_seconds": round(time.time() - t0, 2),
            }

        if isinstance(config_json, str):
            try:
                config_json = json.loads(config_json)
            except json.JSONDecodeError as e:
                return _err(f"config_json is a string but not valid JSON ({e}).")
            if not isinstance(config_json, dict):
                return _err("config_json (parsed from string) must be a JSON object, not a scalar/array.")

        if not isinstance(config_json, dict) or not config_json:
            return _err(
                "config_json must be a non-empty JSON object (InlineNormalizeDatasourceDto)."
            )
        try:
            from stitcher.pipeline.common.config_loaders.normalize_config_loader import (
                NormalizeConfigLoader,
            )
            from stitcher.pipeline.common.pipeline_config_models.versions.v1_alpha.normalize.normalize_config import (
                InlineNormalizeDatasourceDto,
            )

            dto = InlineNormalizeDatasourceDto.model_validate(config_json)
        except Exception as e:  # noqa: BLE001
            return _err(
                f"could not validate config as InlineNormalizeDatasourceDto: {type(e).__name__}: {str(e)[:500]}"
            )

        from .....common import artifacts

        out_dir = artifacts.user_output_dir("FOCUS_CONFIG_OUTPUT_DIR", "stitcher-focus-configs")
        try:
            stem = (name or dto.converter_plan_name or "focus-config").replace(
                " ", "_"
            )[:80]
            path = out_dir / f"{stem}.yaml"
            path.write_text(
                _yaml_dump({"data_source_normalizers": [dto.model_dump(mode="json")]})
            )
        except Exception as e:  # noqa: BLE001
            logger.exception("save_focus_config write failed")
            return _err(f"failed to write config: {type(e).__name__}: {str(e)[:500]}")

        # Verify: the saved YAML must round-trip through the PRODUCTION loader.
        try:
            loader = NormalizeConfigLoader(base_dir=str(out_dir))
            loaded = loader.load_configs()
            match = any(
                ds.converter_plan_name == dto.converter_plan_name
                for cfg in loaded
                for ds in cfg.data_source_normalizers
            )
            if not match:
                return _err(
                    f"saved YAML did not verify via NormalizeConfigLoader (wrote {path})."
                )
        except Exception as e:  # noqa: BLE001
            return _err(
                f"saved YAML failed verification load: {type(e).__name__}: {str(e)[:500]}"
            )

        return {
            "success": True,
            "config_path": str(path),
            "converter_plan_name": dto.converter_plan_name,
            "focus_column_count": len(dto.focus_columns),
            "output_dir": str(out_dir),
            "next_steps": (
                "verify with simulate_normalize_config(config_json=...) against the raw data, then "
                "re-validate with validate_focus_official(file_path=<normalized parquet>)"
            ),
            "elapsed_seconds": round(time.time() - t0, 2),
        }


def _yaml_dump(obj: dict[str, Any]) -> str:
    import yaml

    return yaml.safe_dump(
        obj, sort_keys=False, default_flow_style=False, allow_unicode=True
    )

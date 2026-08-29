"""FOCUS plan-generation tool — the harness-native plan generator.

This is the **fuzzy** step of the custom-cost pipeline, implemented in the
harness (not hidden inside ``stitcher_pipeline_common``'s opaque multi-step +
SQL-LLM workflow). It maps a raw cost DataFrame's columns → FOCUS columns and
emits an ``InlineNormalizeDatasourceDto`` config that feeds directly into the
conversion tools (``simulate_normalize_config`` / ``apply_conversion_plans``).

Unlike the pipeline_common workflow — which makes *several* LLM calls including
LLM-generated SQL with reflection/retry — this generator keeps the fuzzy part
thin (ONE structured LLM call for the column mapping) and owns all determinism:

  1. **Grounding (deterministic)** — build a column summary of the actual raw
     DataFrame (reuses ``_build_column_summary`` from pipeline_common).
  2. **The one fuzzy call** — ask the LLM for a structured
     ``MappingFunctions`` (source column → FOCUS column + transform function),
     via ``LLMAgentProxy.generate_llamaindex_pydantic_program`` (same gateway
     the other FOCUS tools use).
  3. **Column-fidelity guard (deterministic, refuse-by-construction)** — drop
     any mapping whose ``source_column`` is NOT a real column of the DataFrame
     (no fabrication); drop duplicates per FOCUS column; drop mappings whose
     function we cannot translate deterministically. Every drop is reported,
     never silently merged.
  4. **Deterministic mapping→config translation (owned here)** — turn each
     surviving mapping into a ``FocusColumnNormalizeSteps`` using the transform
     step shapes with a direct, unambiguous definition:
       * ``General.rename_column``      (copy/rename an existing cast value)
       * ``General.set_static_value``   (literal applied to every row)
       * ``String.date_from_string``    (+ the LLM's datetime_format)
       * ``String.datetime_from_string``(+ the LLM's datetime_format)
       * ``Datetime.assign_timezone``   (UTC default)
       * ``Datetime.replace_timezone``
     Functions that would need the heavy LLM SQL-generation workflow
     (``Sql.sql_query``, ``Sql.sql_condition``, ``Temporal.*``,
     ``Datetime.date_from_timestamp``, …) are **not** fabricated here: such
     mappings are reported as untranslated so the caller knows the emitted
     config is partial, exactly as a human escape hatch.

No silent fallbacks. A mapping that fails a guard is either dropped-and-reported
or the whole call fails with a clear message — never approximated.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

import polars as pl
from fastmcp import Context, FastMCP

logger = logging.getLogger(__name__)

# Transform functions we translate deterministically → step config classes.
# Keyed by the step `type` string they produce.
_SUPPORTED_RENAME = ("General.rename_column",)
_SUPPORTED_STATIC = ("General.set_static_value",)
_SUPPORTED_DATETIME = ("String.datetime_from_string", "String.date_from_string")
_SUPPORTED_TZ = ("Datetime.assign_timezone", "Datetime.replace_timezone")


def _now() -> float:
    return round(time.time(), 2)


def _load_data(path: str) -> pl.DataFrame:
    if not __import__("os").path.isfile(path):
        raise FileNotFoundError(f"no such file: {path}")
    ext = __import__("os").path.splitext(path)[1].lstrip(".").lower()
    if ext == "csv":
        return pl.read_csv(path)
    if ext in ("parquet", "parq"):
        return pl.read_parquet(path)
    raise ValueError(f"unsupported data type '{ext}' (bring a .csv or .parquet): {path}")


def _mapping_col(m) -> str:
    return m.focus_column_name.value if hasattr(m.focus_column_name, "value") else str(m.focus_column_name)


def _mapping_fn(m) -> str:
    return m.conversion_function.value if hasattr(m.conversion_function, "value") else str(m.conversion_function)


def ground_mappings(raw_df: pl.DataFrame, mappings) -> tuple[list[Any], list[dict[str, Any]]]:
    """Deterministically ground the LLM's MappingFunctions against the DataFrame.

    Returns ``(kept, dropped)``. Refuse-by-construction:
      * source_column must be a real column of ``raw_df`` (unless the function
        is ``SET_STATIC_VALUE``, which writes a literal and needs no source);
      * one mapping per FOCUS column (first wins, duplicates dropped);
      * mappings are ordered: the 4 mandatory date columns first, then other.
    Nothing is silently merged — every drop is reported.
    """
    actual_cols = set(raw_df.columns)
    ordered: list[Any] = []
    ordered.extend(
        [
            mappings.billing_period_start_column,
            mappings.billing_period_end_column,
            mappings.charge_period_start,
            mappings.charge_period_end,
        ]
    )
    ordered.extend(mappings.other_focus_column)

    kept: list[Any] = []
    dropped: list[dict[str, Any]] = []
    seen_focus: set[str] = set()

    for m in ordered:
        fc = _mapping_col(m)
        fn = _mapping_fn(m)
        if fc in seen_focus:
            dropped.append({"focus_column": fc, "reason": "duplicate focus column (kept first)"})
            continue
        if fn not in _SUPPORTED_STATIC and m.source_column not in actual_cols:
            dropped.append(
                {
                    "focus_column": fc,
                    "source_column": m.source_column,
                    "reason": f"source column not present in data (actual columns: {sorted(actual_cols)})",
                }
            )
            continue
        seen_focus.add(fc)
        kept.append(m)
    return kept, dropped


def _build_step(m: Any) -> dict[str, Any] | None:
    """Deterministically translate a grounded mapping into one transform step dict.

    Returns the step dict (Serializable TRANSFORM_FUNCTION_CONFIG_TYPE form) or
    ``None`` when the mapping's function is not deterministically supported.
    """
    from stitcher.pipeline.common.pipeline_config_models.versions.v1_alpha.normalize.transform_configs.base_config import (
        TransformFunctionNames,
    )

    fn = _mapping_fn(m)
    plan_name = f"map_{_mapping_col(m)}"

    # rename_column / copy: keep the source value as-is under the FOCUS column.
    if fn == TransformFunctionNames.RENAME_COLUMN.value:
        return {"plan_name": plan_name, "type": "General.rename_column", "source_column": m.source_column}

    # set_static_value: write a literal. Grounded statics are the ONLY place a
    # literal is allowed — sourced from the LLM hint, never a silent default.
    if fn == TransformFunctionNames.SET_STATIC_VALUE.value:
        literal = (getattr(m, "transformation_hint", None) or "").strip()
        if not literal:
            return None
        return {"plan_name": plan_name, "type": "General.set_static_value", "static_value": literal}

    # datetime from string: requires a format the LLM declared.
    if fn in (TransformFunctionNames.DATETIME_FROM_STRING.value, TransformFunctionNames.DATE_FROM_STRING.value):
        fmt = (getattr(m, "datetime_format", None) or "").strip()
        if not fmt:
            return None
        step_type = (
            "String.datetime_from_string"
            if fn == TransformFunctionNames.DATETIME_FROM_STRING.value
            else "String.date_from_string"
        )
        return {"plan_name": plan_name, "type": step_type, "source_column": m.source_column, "format": fmt}

    # timezone assign/replace (UTC default).
    if fn == TransformFunctionNames.ASSIGN_TIMEZONE.value:
        return {
            "plan_name": plan_name,
            "type": "Datetime.assign_timezone",
            "source_column": m.source_column,
            "tz": "UTC",
        }
    if fn == TransformFunctionNames.REPLACE_TIMEZONE.value:
        tz = (getattr(m, "transformation_hint", None) or "UTC").strip()
        return {"plan_name": plan_name, "type": "Datetime.replace_timezone", "source_column": m.source_column, "tz": tz}

    return None


def mappings_to_config(mappings, kept: list[Any]) -> dict[str, Any]:
    """Build an InlineNormalizeDatasourceDto (dict form) from grounded mappings.

    Returns ``(config_dict, built_columns, untranslated)``.
    """
    focus_columns: list[dict[str, Any]] = []
    untranslated: list[dict[str, Any]] = []
    for m in kept:
        step = _build_step(m)
        if step is None:
            untranslated.append({"focus_column": _mapping_col(m), "function": _mapping_fn(m)})
            continue
        focus_columns.append({"focus_column": _mapping_col(m), "steps": [step]})

    config = {
        "converter_plan_name": "harness_plan_generated",
        "focus_columns": focus_columns,
        "scope": None,
    }
    return config, [fc["focus_column"] for fc in focus_columns], untranslated


def register(mcp: FastMCP) -> None:
    @mcp.tool
    async def generate_focus_plans(
        ctx: Context,
        data_path: str,
        provider_name: str = "unknown",
        model_name: str | None = None,
        timeout_seconds: float = 60.0,
    ) -> dict[str, Any]:
        """Generate a FOCUS normalization plan (InlineNormalizeDatasourceDto) from raw cost data.

        The harness-native plan generator: builds a deterministic column summary of
        ``data_path``, then makes ONE structured LLM call to map raw columns → FOCUS
        columns (+ transform function), then deterministically grounds the result
        (drops fabricated/duplicate mappings) and translates it into an
        ``InlineNormalizeDatasourceDto`` config. The emitted ``config_json`` feeds
        directly into ``simulate_normalize_config`` / ``apply_conversion_plans``.

        Deterministically-supported transforms: rename_column, set_static_value,
        date/datetime_from_string (with the LLM's declared format), assign/replace
        timezone. Mappings needing LLM-generated SQL are reported as
        ``untranslated`` rather than fabricated.

        Args:
            data_path: .csv or .parquet file holding the raw cost data.
            provider_name: provider hint (e.g. 'aws-csv') to guide the LLM.
            model_name: optional plan-generation model override (default: the
              gateway's plan_generation_model from parser settings).
            timeout_seconds: per-call timeout for the single LLM mapping call.
        """
        from stitcher.pipeline.common.invoice_parser.parser_settings import get_parser_settings
        from stitcher.pipeline.common.invoice_parser.utils.openai_utils import get_openai_client
        from stitcher.pipeline.common.pipeline_config_models.ai.common.ai_agent_proxy.base import LLMAgentProxy
        from stitcher.pipeline.common.plan_generation_workflow.models import MappingFunctions
        from stitcher.pipeline.common.plan_generation_workflow.plan_generation_workflow import _build_column_summary

        t0 = time.time()

        def _err(message: str, **extra: Any) -> dict[str, Any]:
            return {"success": False, "error": message, "elapsed_seconds": round(time.time() - t0, 2), **extra}

        try:
            raw_df = _load_data(data_path)
        except Exception as e:  # noqa: BLE001
            return _err(f"could not load data: {type(e).__name__}: {e}")
        if raw_df.is_empty():
            return _err("data file is empty.")

        try:
            settings = get_parser_settings()
            model_name = model_name or settings.plan_generation_model
            client = get_openai_client()
        except Exception as e:  # noqa: BLE001
            return _err(f"LLM gateway not configured: {type(e).__name__}: {e}")

        await ctx.report_progress(1, 3, "Analyzing data columns...")
        column_summary = _build_column_summary(raw_df)
        raw_csv = raw_df.limit(5).write_csv()
        column_list = "\n".join(f'- "{c}"' for c in raw_df.columns)
        prompt = (
            "You are a FinOps assistant. Map the raw cost-data columns below to FOCUS columns.\n"
            "Respond with a complete MappingFunctions object per the schema.\n"
            f"Provider hint: {provider_name}\n\n"
            f"Raw column names (use these EXACT names, case-sensitive):\n{column_list}\n\n"
            f"Column summary:\n{column_summary}\n\n"
            f"Sample rows:\n{raw_csv}\n\n"
            "Rules:\n"
            "- source_column MUST be one of the exact raw column names above.\n"
            "- For the 4 date/period columns use datetime target with "
            "DATETIME_FROM_STRING/date_from_string + datetime_format, or RENAME_COLUMN "
            "if the value is already ISO-8601/datetime (is_source_already_datetime=true).\n"
            "- ProviderName/ServiceName etc. typically COPY/RENAME from an existing column; "
            "only use SET_STATIC_VALUE when the value is a fixed constant.\n"
            "- target_type must be one of datetime|numeric|string|boolean.\n"
            "- Do not invent columns. Leave other_focus_column empty if nothing else maps."
        )

        try:
            proxy = LLMAgentProxy(
                model=model_name,
                client=client,
                sai_product="custom_cost",
                sai_product_step="plan_generation",
            )
            program = proxy.generate_llamaindex_pydantic_program(
                base_model=MappingFunctions,
                prompt_template_str=prompt,
                model_name=model_name,
                attributes={"purpose": "harness_plan_generation", "step": "single_pass"},
                seed=42,
            )
            import asyncio

            await ctx.report_progress(2, 3, "Mapping raw columns → FOCUS columns (LLM)...")
            mappings: MappingFunctions = await asyncio.wait_for(program.acall(), timeout=timeout_seconds)
        except Exception as e:  # noqa: BLE001
            logger.exception("generate_focus_plans: LLM mapping failed")
            return _err(f"plan generation LLM call failed: {type(e).__name__}: {str(e)[:500]}")

        # Deterministic grounding + translation.
        await ctx.report_progress(3, 3, "Grounding mappings + building config...")
        kept, dropped = ground_mappings(raw_df, mappings)
        config, built_columns, untranslated = mappings_to_config(mappings, kept)
        config_json = json.dumps(config)

        return {
            "success": True,
            "provider_hint": provider_name,
            "total_mappings": len(kept) + len(dropped),
            "mappings_kept": len(kept),
            "mappings_dropped": len(dropped),
            "dropped": dropped,
            "untranslated": untranslated,
            "built_focus_columns": built_columns,
            "config_json": config_json,
            "config": config,
            "elapsed_seconds": round(time.time() - t0, 2),
        }

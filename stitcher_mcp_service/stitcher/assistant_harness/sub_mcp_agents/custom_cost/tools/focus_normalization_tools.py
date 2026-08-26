"""FOCUS normalization tools — bring any PDF (or CSV) and normalize it to FOCUS.

Reimplemented in our own service code (the stitcher.assistant_harness FastMCP
server) rather than the shared ``stitcher_pipeline_common`` MCP surface. The
heavy determinism — PDF extraction, multi-step plan generation, and the
simulate_conversion_plan normalizer — lives in ``stitcher_pipeline_common``
(an editable dependency of this service) and is reused here; only the tool
orchestration + serialization is ours, so pi stays a thin caller.

Pipeline (mirrors test/test_pdf_workflow_agent/run_pdf_workflow.py):

  1. Extract raw rows from the invoice (PDF via the LLM OCR workflow, CSV directly).
  2. Generate FOCUS plans (LLM maps source cols → FOCUS cols, 4 steps).
  3. Normalize the raw rows by applying the plans → a FOCUS-shaped DataFrame.
  4. (optional) Run the FOCUS v1.2 spec validator against the normalized output.

Requires LLM API access — the underlying InvoiceParserWorkflow + plan
generation make several LLM calls. They read their endpoint/key from the
environment via ``get_openai_client`` / ``get_parser_settings`` (set
``USE_STITCHER_MODEL=true`` + ``STITCHER_MODEL_*`` to reuse the same Stitcher
gateway the pi agent uses, or ``LLM_BASE_URL`` / ``LLM_API_KEY`` for an
external provider). If no LLM is configured the tool fails gracefully with a
clear message instead of simulating.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import tempfile
import time
from typing import Any

import polars as pl
from fastmcp import Context, FastMCP

from . import kw_cache

logger = logging.getLogger(__name__)

# FOCUS columns the normalize dimension pipeline aims to populate. Used only to
# report found/missing in the result — the normalizer itself emits whatever the
# plans produce (a superset/subset of these).
_EXPECTED_FOCUS_COLUMNS = [
    "BillingPeriodStart",
    "BillingPeriodEnd",
    "ChargePeriodStart",
    "ChargePeriodEnd",
    "BilledCost",
    "BillingCurrency",
    "ProviderName",
    "ServiceName",
    "InvoiceId",
    "ChargeCategory",
]

# Advanced FOCUS columns gated behind a feature flag in the pipeline; filtered
# out of the merged config unless the flag is on (matches run_pdf_workflow.py).
_ADVANCED_FOCUS_COLUMNS = {"ChargeFrequency", "PricingCategory"}

_PROVIDER_AUTO = "auto"


def _serialize_df(df: pl.DataFrame, max_rows: int = 5) -> dict[str, Any]:
    """Schema + first N rows, with temporal cols stringified for JSON."""
    sample = df.head(max_rows)
    for col in sample.columns:
        if sample[col].dtype.is_temporal():
            sample = sample.with_columns(pl.col(col).cast(pl.Utf8))
    return {
        "shape": {"rows": df.height, "columns": df.width},
        "columns": df.columns,
        "schema": {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes, strict=True)},
        "sample_rows": sample.to_dicts(),
    }


def _serialize_plans(plans: list[Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for p in plans:
        if hasattr(p, "model_dump"):
            out.append(p.model_dump(mode="json"))
        else:
            out.append({"repr": str(p)})
    return out


def _classify_plans(plans: list[Any]) -> tuple[list[str], list[str]]:
    deterministic: list[str] = []
    llm: list[str] = []
    for plan in plans:
        for fc in plan.focus_columns:
            col_name = fc.focus_column.value
            if plan.converter_plan_name.startswith("deterministic_"):
                deterministic.append(col_name)
            else:
                llm.append(col_name)
    return deterministic, llm


def _decode_bytes(pdf_b64: str) -> bytes:
    # Accept standard or URL-safe base64; validate to catch truncation.
    return base64.b64decode(pdf_b64, validate=True)


def _ext_for(filename: str | None, dataset_type: str | None) -> str:
    if dataset_type in ("pdf", "csv"):
        return dataset_type
    if filename:
        suffix = os.path.splitext(filename)[1].lstrip(".").lower()
        if suffix in ("pdf", "csv", "parquet"):
            return suffix
    # Default: PDF (the headline use case — "bring any pdf").
    return "pdf"


def _merge_plans_to_config(plans: list[Any], *, allow_advanced: bool):
    """Merge every plan's focus_columns into one InlineNormalizeDatasourceDto,
    dedup by FOCUS column name, and (unless advanced support is on) drop the
    advanced columns the pipeline gates behind a flag. Mirrors run_pdf_workflow.
    """
    from stitcher.pipeline.common.pipeline_config_models.versions.v1_alpha.normalize.normalize_config import (
        InlineNormalizeDatasourceDto,
    )

    all_focus_columns = []
    seen: set[str] = set()
    for plan in plans:
        for fc in plan.focus_columns:
            col_name = fc.focus_column.value
            if col_name in seen:
                continue
            seen.add(col_name)
            all_focus_columns.append(fc)

    if not allow_advanced:
        all_focus_columns = [fc for fc in all_focus_columns if fc.focus_column.value not in _ADVANCED_FOCUS_COLUMNS]

    return InlineNormalizeDatasourceDto(converter_plan_name="mcp_focus_merged", focus_columns=all_focus_columns)


async def _extract_raw_df(pdf_path: str, expected_columns: list[str] | None = None) -> tuple[pl.DataFrame, str]:
    """Run the LLM invoice parser on a PDF path → (raw_df, detected_provider).

    ``expected_columns`` (optional) is threaded into the invoice parser's
    ``expected_columns`` so callers can influence WHICH columns are extracted
    (e.g. force it to capture a specific field the parser didn't pick up).
    """
    from llama_index.core.workflow import Context

    from stitcher.pipeline.common.pipeline_config_models.ai.workflows.invoice_parser_v2.invoice_parser_workflow import (
        InvoiceParserWorkflow,
    )

    workflow = InvoiceParserWorkflow(timeout=300, verbose=False, disable_validation=True)
    ctx = Context(workflow=workflow)
    result = await workflow.run(ctx=ctx, pdf_path=pdf_path, expected_columns=expected_columns)
    raw_df: pl.DataFrame = result["raw_df"]
    provider_type: str = result.get("provider_type", "")
    return raw_df, provider_type


def _validate_pdf(pdf_path: str) -> str | None:
    """Return an actionable error string if ``pdf_path`` is not a readable PDF, else None.

    Gives a clear message up front (instead of the LLM workflow's cryptic
    ``FileDataError``) when the file is missing, empty, or not actually a PDF —
    so callers fix the *file* rather than mistakenly switching to base64.
    """
    if not os.path.isfile(pdf_path):
        return f"no such file: {pdf_path}"
    if os.path.getsize(pdf_path) == 0:
        return f"file is empty (0 bytes): {pdf_path}"
    try:
        with open(pdf_path, "rb") as f:
            head = f.read(5)
    except OSError as e:  # noqa: BLE001
        return f"cannot read {pdf_path}: {e}"
    if head != b"%PDF-":
        return (
            f"{pdf_path} is not a valid PDF (missing %PDF header — it may be a "
            "plain-text/markdown file with a .pdf name). Re-create it as a real PDF, "
            "then pass its `file_path`. (No base64 needed — `file_path` is preferred.)"
        )
    try:
        import fitz  # PyMuPDF, available in this venv

        doc = fitz.open(pdf_path)
        doc.close()
    except Exception as e:  # noqa: BLE001
        return f"{pdf_path} cannot be parsed as a PDF: {type(e).__name__}: {e}"
    return None


async def _generate_and_normalize(
    raw_df: pl.DataFrame,
    provider_name: str,
    timeout_per_step: float = 90.0,
) -> tuple[list[Any], pl.DataFrame]:
    """LLM plan generation + deterministic normalization → (plans, normalized_df)."""
    from stitcher.pipeline.common.invoice_parser.parser_settings import get_parser_settings
    from stitcher.pipeline.common.invoice_parser.utils.openai_utils import get_openai_client
    from stitcher.pipeline.common.pipeline_config_models.ai.common.ai_agent_proxy.base import LLMAgentProxy
    from stitcher.pipeline.common.plan_generation_workflow.multi_step_plan_generation import (
        generate_plans_from_mappings,
        multi_step_plan_generation,
    )

    settings = get_parser_settings()
    llm_proxy = LLMAgentProxy(model=settings.plan_generation_model, client=get_openai_client())
    mappings = await multi_step_plan_generation(
        raw_df=raw_df,
        provider_name=provider_name,
        llm_proxy=llm_proxy,
        timeout_per_step=timeout_per_step,
    )
    plans = await generate_plans_from_mappings(mappings, raw_df)

    allow_advanced = bool(get_parser_settings().enable_advanced_focus_column_support)
    merged = _merge_plans_to_config(plans, allow_advanced=allow_advanced)
    normalized_df = type(merged).simulate_conversion_plan(raw_df=raw_df, response_configs=merged)
    return plans, normalized_df


def _validate_focus(df: pl.DataFrame, source: str) -> dict[str, Any] | None:
    """Run the FOCUS v1.2 spec validator; return its report dict, or None on failure."""
    try:
        from stitcher.pipeline.common.focus_column_names import FocusColumnNames
        from stitcher.pipeline.common.focus_spec_validator import validate_focus

        all_focus_names = [fc.value for fc in FocusColumnNames]
        focus_cols = [c for c in df.columns if c in all_focus_names]
        focus_df = df.select(focus_cols) if focus_cols else df
        return validate_focus(focus_df, source=source).to_dict()
    except Exception as e:  # noqa: BLE001
        logger.warning("FOCUS spec validation skipped: %s", e)
        return None


def register(mcp: FastMCP) -> None:
    @mcp.tool
    async def normalize_to_focus(
        ctx: Context,
        pdf_b64: str | None = None,
        file_path: str | None = None,
        filename: str | None = None,
        provider_name: str = _PROVIDER_AUTO,
        expected_columns: list[str] | None = None,
        use_cache: bool = True,
        validate: bool = True,
        max_sample_rows: int = 5,
    ) -> dict[str, Any]:
        """Normalize any invoice to the FOCUS v1.2 column shape.

        Simplest call: pass ``file_path`` to a PDF/CSV already on the server and
        let it auto-detect the provider — NO base64 needed:

            normalize_to_focus(file_path="samples/invoice_sample.pdf")

        Only use ``pdf_b64`` (+ ``filename``) when the bytes aren't on the server's
        disk. Everything else is optional: the provider is auto-detected and
        validation is on by default.

        Runs the full custom-cost pipeline server-side: extraction → LLM plan
        generation (source cols → FOCUS cols) → deterministic normalization →
        optional FOCUS v1.2 spec validation. Returns the raw extraction summary,
        generated plans, normalized FOCUS DataFrame summary, and the validation
        report. Requires LLM API access (several LLM calls).

        ``provider_name`` is an OPTIONAL hint and defaults to ``"auto"``. You do
        NOT need to supply it — for PDFs the extractor detects the provider
        itself, and this works for BOTH generic invoices (e.g. a hardware-store
        bill) and cloud-provider invoices. It is independent of the cloud
        providers listed by ``list_focus_providers`` (aws, azure, google_cloud,
        …) — those are for the converter tools, not required here. For CSVs (no
        detection possible) the hint is used directly as the provider label,
        defaulting to ``"unknown"`` when left at ``auto``.

        Args:
            file_path: Absolute server path to a PDF or CSV. Preferred input.
            pdf_b64: Base64-encoded PDF/CSV bytes (+ ``filename``). Only needed
              when the file is NOT already on the server's disk.
            filename: Logical filename (drives the .pdf/.csv extension) when using pdf_b64.
            provider_name: OPTIONAL provider label hint; default 'auto' detects
              from the PDF. Omit or pass 'auto' for generic or cloud invoices.
            expected_columns: OPTIONAL list of column names to influence which
              fields the extractor pulls (e.g. ["Invoice number", "Tax"]). The
              parser tries to capture these even if it would skip them.
            use_cache: when true (default), reuse cached plan-generation output
              for this (file, provider, columns) to skip the expensive LLM step,
              and save each step's output to the KW cache for reuse.
            validate: When true (default), run the FOCUS v1.2 spec validator on the output.
            max_sample_rows: Max sample rows to include in each DataFrame summary (default 5).
        """
        t0 = time.time()

        def _err(message: str) -> dict[str, Any]:
            return {"success": False, "error": message, "elapsed_seconds": round(time.time() - t0, 2)}

        if not pdf_b64 and not file_path:
            return _err("Provide either pdf_b64 (base64 file bytes) or file_path (server path).")
        if pdf_b64 and file_path:
            return _err("Pass only one of pdf_b64 or file_path, not both.")

        temp_path: str | None = None
        try:
            if pdf_b64:
                ext = _ext_for(filename, None)
                if ext not in ("pdf", "csv"):
                    return _err(f"Unsupported file type '{ext}'. Bring a PDF or CSV.")
                try:
                    raw_bytes = _decode_bytes(pdf_b64)
                except (ValueError, TypeError) as e:
                    return _err(f"pdf_b64 is not valid base64: {e}")
                with tempfile.NamedTemporaryFile(mode="wb", suffix=f".{ext}", delete=False) as tf:
                    tf.write(raw_bytes)
                    tf.flush()
                    temp_path = tf.name
                source = filename or f"upload.{ext}"
            else:
                assert file_path is not None  # guarded above
                ext = _ext_for(file_path, None)
                if ext not in ("pdf", "csv"):
                    return _err(f"Unsupported file type '{ext}'. Bring a PDF or CSV.")
                source = file_path
                temp_path = file_path  # do NOT unlink a caller-owned path

            # Stable content identity for the KW cache.
            file_data: bytes = b""
            if pdf_b64:
                file_data = raw_bytes  # decoded above (function-scoped)
            elif file_path:
                with open(file_path, "rb") as _f:
                    file_data = _f.read()

            # ── Stage 1: Extract raw DataFrame ───────────────────────────
            assert temp_path is not None
            if ext == "csv":
                raw_df = await asyncio.to_thread(pl.read_csv, temp_path)
                if raw_df.is_empty():
                    return _err("CSV file is empty.")
                detected_provider = provider_name if provider_name != _PROVIDER_AUTO else "unknown"
            else:
                await ctx.report_progress(1, 4, "Extracting rows from invoice (PDF OCR)...")
                invalid = _validate_pdf(temp_path)
                if invalid:
                    return _err(invalid)
                raw_df, detected_provider = await _extract_raw_df(temp_path, expected_columns=expected_columns)
                if not detected_provider:
                    detected_provider = provider_name if provider_name != _PROVIDER_AUTO else "unknown"
                if raw_df.is_empty():
                    return _err("PDF extraction returned no rows.")

            raw_summary = _serialize_df(raw_df, max_sample_rows)

            # Cache identity: file content + provider + column hint.
            variant = kw_cache.sha256_bytes(
                json.dumps({"provider": detected_provider, "cols": sorted(expected_columns or [])}).encode()
            )[:8]
            if use_cache:
                kw_cache.step_cache_put(
                    "extract",
                    file_data,
                    variant,
                    {
                        "source": source,
                        "provider_detected": detected_provider,
                        "columns": raw_df.columns,
                        "raw_df_summary": raw_summary,
                    },
                )

            # ── Stage 2 + 3: plan generation + normalize ────────────────
            plan_count = 0
            plans_from_cache = False
            if use_cache:
                cached_plans = kw_cache.step_cache_get("plans", file_data, variant)
                if cached_plans:
                    try:
                        from stitcher.pipeline.common.pipeline_config_models.versions.v1_alpha.normalize.normalize_config import (
                            InlineNormalizeDatasourceDto,
                        )

                        merged = InlineNormalizeDatasourceDto.model_validate(cached_plans["data"]["config"])
                        normalized_df = merged.simulate_conversion_plan(raw_df=raw_df, response_configs=merged)
                        plans = [merged]
                        plan_count = int(cached_plans["data"].get("plan_count", 1))
                        plans_from_cache = True
                        await ctx.report_progress(2, 4, "Reusing cached FOCUS plans (skipped LLM plan-gen)...")
                    except Exception:  # noqa: BLE001
                        logger.warning("cached plans unreadable; regenerating", exc_info=True)

            if not plans_from_cache:
                await ctx.report_progress(2, 4, "Generating FOCUS plans (LLM source→FOCUS mapping)...")
                plans, normalized_df = await _generate_and_normalize(raw_df, detected_provider)
                plan_count = len(plans)
                if use_cache:
                    from stitcher.pipeline.common.invoice_parser.parser_settings import get_parser_settings

                    allow_advanced = bool(get_parser_settings().enable_advanced_focus_column_support)
                    merged_cfg = _merge_plans_to_config(plans, allow_advanced=allow_advanced).model_dump(mode="json")
                    kw_cache.step_cache_put(
                        "plans",
                        file_data,
                        variant,
                        {
                            "source": source,
                            "provider": detected_provider,
                            "config": merged_cfg,
                            "plan_count": plan_count,
                        },
                    )

            deterministic, llm_cols = _classify_plans(plans)
            await ctx.report_progress(3, 4, "Normalized to FOCUS shape—validating...")
            normalized_summary = _serialize_df(normalized_df, max_sample_rows)

            focus_found = [c for c in _EXPECTED_FOCUS_COLUMNS if c in normalized_df.columns]
            focus_missing = [c for c in _EXPECTED_FOCUS_COLUMNS if c not in normalized_df.columns]

            # ── Stage 4 (optional): FOCUS spec validation ────────────────
            validation_report: dict[str, Any] | None = None
            if validate:
                validation_report = _validate_focus(normalized_df, source=source)
            await ctx.report_progress(4, 4, "Done.")

            if use_cache:
                kw_cache.step_cache_put("normalize", file_data, variant, normalized_summary)
                if validation_report is not None:
                    kw_cache.step_cache_put("validate", file_data, variant, validation_report)

            return {
                "success": True,
                "provider_detected": detected_provider,
                "provider_hint": provider_name,
                "source": source,
                "raw_df_summary": raw_summary,
                "plan_count": plan_count,
                "plans_from_cache": plans_from_cache,
                "plans": _serialize_plans(plans),
                "deterministic_focus_columns": deterministic,
                "llm_focus_columns": llm_cols,
                "normalized_df_summary": normalized_summary,
                "focus_columns_found": focus_found,
                "focus_columns_missing": focus_missing,
                "validation_report": validation_report,
                "elapsed_seconds": round(time.time() - t0, 2),
            }
        except Exception as e:  # noqa: BLE001
            logger.exception("normalize_to_focus failed")
            if isinstance(e, TimeoutError) or "TimeoutError" in type(e).__name__:
                return _err(
                    "an LLM plan-generation step timed out (the gateway was slow). Retry; "
                    "if it keeps failing on a large/generic invoice, narrow the input."
                )
            return _err(f"Pipeline failed: {type(e).__name__}: {str(e)[:500]}")
        finally:
            if temp_path and pdf_b64 and os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except OSError:
                    pass

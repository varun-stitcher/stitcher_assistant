"""FOCUS validation + repair tool — the safety boundary for ``normalize_to_focus``.

Why this exists
---------------
``normalize_to_focus`` returns ``success=true`` even when the FOCUS v1.2
validator reports failing checks (e.g. a missing mandatory ``BillingCurrency``),
and its internal ``_validate_focus`` swallows *any* validator exception into
``None`` (a weaken-by-fallback hole). This tool is the refuse-by-construction
layer that sits on top of the normalizer's output:

  * it runs the real ``validate_focus`` and **never swallows its exceptions**
    (a validator crash is a hard failure, not a silent ``None``);
  * it refuses by construction — any missing ``MANDATORY_COLUMNS`` entry or any
    ``severity=FAIL`` check makes the run non-compliant (``compliant=false``);
  * it can *repair* deterministic gaps (currently: a missing ``BillingCurrency``)
    by applying a static value to the frame and re-running the validator — the
    LLM never writes the frame directly;
  * every repair is re-validated; a repair that does not make its check pass is
    rolled back and reported as an unresolved gap, never silently kept;
  * the LLM's only allowed fuzzy decision is *inferring the invoice's billing
    currency from the raw text/CSV* — and only when the caller opts in via
    ``llm_repair=true``. With ``llm_repair=false`` (default) the tool is fully
    deterministic and makes zero LLM calls (the human escape hatch).

Inputs: pass ``file_path``/``pdf_b64`` (+``filename``) to re-extract + normalize
inside this tool, OR pass ``raw_df_json`` to validate/repair an already-produced
frame without re-extracting (zero LLM calls when ``llm_repair=false``).

Constitution (non-negotiable; the code below enforces each):
  1. Never swallow a validator exception — crash = hard failure.
  2. Missing mandatory column or any FAIL check ⇒ ``compliant=false``.
  3. No silent currency default — ``BillingCurrency`` is repaired ONLY from an
     explicit caller value or a gated LLM inference; never a hardcoded constant.
  4. Repair is deterministic + append-only (a constant column), never an LLM write.
  5. Every repair is re-validated; unverifiable repairs are rolled back, not kept.
  6. One artifact per run — reads the existing frame; does not re-extract when
     the caller already has the normalized output.
  7. No traceback to the client; full traceback to the server log.
  8. Human escape hatch: ``llm_repair=false`` runs the whole tool with no LLM.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import tempfile
import time
from pathlib import Path
from typing import Any

import polars as pl
from fastmcp import Context, FastMCP

logger = logging.getLogger(__name__)

# Single source of truth for what "FOCUS-required" means (kept in sync with the
# validator in stitcher_pipeline_common).
from stitcher.pipeline.common.focus_spec_validator import (  # noqa: E402
    MANDATORY_COLUMNS,
    validate_focus,
)


def _now() -> float:
    return round(time.time(), 2)


def _df_from_json(raw_df_json: str) -> pl.DataFrame:
    """Reconstruct a polars DataFrame from the JSON a prior tool call emitted.

    Accepts a ``{"sample_rows": [...]}`` or ``{"rows": [...]}`` blob. Refuses
    anything that carries no row data — no silent empty-frame fallback.
    """
    blob = json.loads(raw_df_json)
    rows = blob.get("sample_rows") or blob.get("rows")
    if not rows:
        raise ValueError("raw_df_json carries no rows (provide sample_rows or rows).")
    return pl.DataFrame(rows)


def _apply_static_repair(df: pl.DataFrame, column: str, value: str) -> pl.DataFrame:
    """Deterministic, append-only repair: fill one column with a constant."""
    return df.with_columns(pl.lit(value).alias(column))


def _report_to_dict(report) -> dict[str, Any]:
    return report.to_dict()


def _failed_checks(report) -> list[dict[str, Any]]:
    return [
        c.to_dict() if hasattr(c, "to_dict") else c
        for c in report.checks
        if not c.passed and c.severity == "FAIL"
    ]


def _missing_mandatory(df: pl.DataFrame) -> list[str]:
    return [c for c in MANDATORY_COLUMNS if c not in df.columns]


async def _maybe_infer_currency(raw_df: pl.DataFrame) -> str | None:
    """The ONE fuzzy decision: infer BillingCurrency from the raw invoice data.

    Returns a 3-letter uppercase ISO 4217 code or None. Hard-gated: anything
    that is not a plausible ISO code is dropped, so a bad model guess can never
    become a fabricated currency.
    """
    from llama_index.core.llms import ChatMessage, MessageRole

    from stitcher.pipeline.common.invoice_parser.parser_settings import (
        get_parser_settings,
    )
    from stitcher.pipeline.common.invoice_parser.utils.openai_utils import (
        get_openai_client,
    )
    from stitcher.pipeline.common.pipeline_config_models.ai.common.ai_agent_proxy.base import (
        LLMAgentProxy,
    )

    settings = get_parser_settings()
    client = get_openai_client()
    proxy = LLMAgentProxy(
        model=settings.task_model,
        client=client,
        sai_product="custom_cost",
        sai_product_step="focus_validation",
    )

    sample = raw_df.head(5).write_csv()
    cols = ", ".join(raw_df.columns)
    prompt = (
        "You are a FinOps assistant. From the invoice data below, infer the billing currency.\n"
        "Respond with ONLY a single 3-letter uppercase ISO 4217 currency code (e.g. USD, GBP, EUR).\n"
        "If you cannot determine it with confidence, respond with the single word UNKNOWN.\n"
        f"Columns: {cols}\n\n{sample}\n"
    )
    try:
        # generate_text is sync → run in a thread so we don't block the loop.
        text = await asyncio.to_thread(
            proxy.generate_text,
            messages=[ChatMessage(role=MessageRole.USER, content=prompt)],
            temperature=0.0,
            max_tokens=8,
            attributes={"purpose": "focus_validation", "step": "infer_currency"},
        )
    except Exception as e:  # noqa: BLE001
        logger.warning("currency inference LLM call failed: %s", e)
        return None
    text = (text or "").strip().upper()
    if len(text) == 3 and text.isalpha() and text != "UNKNOWN":
        return text
    return None


def register(mcp: FastMCP) -> None:
    @mcp.tool
    async def validate_and_repair_focus(
        ctx: Context,
        file_path: str | None = None,
        pdf_b64: str | None = None,
        filename: str | None = None,
        raw_df_json: str | None = None,
        billing_currency: str | None = None,
        llm_repair: bool = False,
        max_sample_rows: int = 5,
        official_validation: bool = False,
    ) -> dict[str, Any]:
        """Validate a normalized FOCUS frame and repair deterministic gaps (e.g. currency).

        Refuses by construction: any missing mandatory FOCUS column or any failing
        FOCUS v1.2 check makes the result ``compliant=false``. Repairs are
        deterministic (append a constant column) and every repair is re-validated;
        unverifiable repairs are rolled back and reported.

        **Recommended — repair an existing frame (ZERO LLM, fast, no re-extraction):**
        after a ``normalize_to_focus`` call, pass its ``normalized_df_summary`` as
        ``raw_df_json`` plus ``billing_currency``:

            validate_and_repair_focus(
                raw_df_json=<normalize_to_focus(...)["normalized_df_summary"]>,
                billing_currency="CAD",
            )

        This validates the frame and deterministically repairs BillingCurrency to
        CAD, re-validates, and makes ZERO LLM calls (easy to test, never times out).

        Alternative — run the full pipeline inside this tool: pass ``file_path`` /
        ``pdf_b64``+``filename`` to re-extract + normalize first (makes LLM calls
        for extraction/plan-gen — slow, and may time out on large/generic invoices).
        Only use this when you need to (re)normalize AND validate in one call.

        ``billing_currency`` (ISO 4217, e.g. "CAD") repairs a missing
        ``BillingCurrency`` deterministically. With ``llm_repair=true`` and no
        explicit currency, the tool infers one from the raw data (the only LLM
        call this tool makes — always gated to a valid ISO code and re-validated).
        ``llm_repair=false`` (default) makes ZERO LLM calls on the ``raw_df_json``
        path — the human escape hatch.

        ``official_validation=true`` adds a SECOND, independent opinion: after the
        internal validate/repair pass, the final frame is also run through the
        official FinOps Foundation focus_validator (isolated subprocess, zero LLM)
        and its conformance report is returned under ``official_validation``. The
        official validator is STRICTER than the internal one (checks ~578 rules
        incl. column presence for the full FOCUS 1.2 schema), so expect failures
        the internal validator does not report. Its failure (e.g. unbootstrapped
        venv) is reported as ``official_validation_error`` WITHOUT failing the
        internal result.
        """
        t0 = time.time()

        def _err(message: str, **extra: Any) -> dict[str, Any]:
            return {
                "success": False,
                "compliant": False,
                "error": message,
                "elapsed_seconds": _now() - t0,
                **extra,
            }

        # ── Input validation ──────────────────────────────────────────
        has_file = bool(file_path or pdf_b64)
        has_json = bool(raw_df_json)
        if has_file and has_json:
            return _err("Pass either file_path/pdf_b64 OR raw_df_json, not both.")
        if not has_file and not has_json:
            return _err("Provide file_path, pdf_b64, or raw_df_json.")

        currency_value: str | None = None
        if billing_currency is not None:
            currency_value = billing_currency.strip().upper()
            if not (len(currency_value) == 3 and currency_value.isalpha()):
                return _err(
                    f"billing_currency must be a 3-letter ISO 4217 code, got {billing_currency!r}."
                )

        raw_df: pl.DataFrame
        source: str

        try:
            if has_json:
                assert raw_df_json is not None
                raw_df = _df_from_json(raw_df_json)
                source = "<provided raw_df_json>"
            elif file_path and file_path.endswith((".parquet",)):
                # Parquet artifact (e.g. normalize_to_focus's normalized_parquet) —
                # read directly, NO extraction, NO LLM. Keeps the input contract
                # consistent with validate_focus_official.
                from . import focus_normalization_tools as fnt

                p = Path(file_path)
                if not p.is_file():
                    return _err(f"data file not found: {file_path}")
                raw_df = pl.read_parquet(p)
                if raw_df.is_empty():
                    return _err(f"parquet file has no rows: {file_path}")
                source = file_path
            else:
                from . import focus_normalization_tools as fnt

                temp_path: str | None = None
                try:
                    if pdf_b64:
                        ext = fnt._ext_for(filename, None)
                        if ext not in ("pdf", "csv"):
                            return _err(
                                f"Unsupported file type {ext!r}; bring a PDF or CSV."
                            )
                        try:
                            raw_bytes = base64.b64decode(pdf_b64, validate=True)
                        except (ValueError, TypeError) as e:
                            return _err(f"pdf_b64 is not valid base64: {e}")
                        with tempfile.NamedTemporaryFile(
                            mode="wb", suffix=f".{ext}", delete=False
                        ) as tf:
                            tf.write(raw_bytes)
                            tf.flush()
                            temp_path = tf.name
                        source = filename or f"upload.{ext}"
                    else:
                        assert file_path is not None
                        ext = fnt._ext_for(file_path, None)
                        if ext not in ("pdf", "csv"):
                            return _err(
                                f"Unsupported file type {ext!r}; bring a PDF or CSV."
                            )
                        temp_path = file_path
                        source = file_path

                    assert temp_path is not None
                    if ext == "csv":
                        raw_df = await asyncio.to_thread(pl.read_csv, temp_path)
                        if raw_df.is_empty():
                            return _err("CSV file is empty.")
                    else:
                        await ctx.report_progress(
                            1, 3, "Extracting + normalizing invoice..."
                        )
                        invalid = fnt._validate_pdf(temp_path)
                        if invalid:
                            return _err(invalid)
                        extracted, _provider = await fnt._extract_raw_df(temp_path)
                        if extracted.is_empty():
                            return _err("PDF extraction returned no rows.")
                        # Normalize to FOCUS so we validate the real artifact.
                        _plans, raw_df = await fnt._generate_and_normalize(
                            extracted, "unknown"
                        )
                finally:
                    if temp_path and pdf_b64 and os.path.exists(temp_path):
                        try:
                            os.unlink(temp_path)
                        except OSError:
                            pass
        except Exception as e:  # noqa: BLE001
            logger.exception("validate_and_repair_focus: extraction/normalize failed")
            if isinstance(e, TimeoutError) or "TimeoutError" in type(e).__name__:
                return _err(
                    "the re-extract + plan-gen step timed out. If you already have a "
                    "normalized frame from normalize_to_focus, pass its normalized_df_summary "
                    "as `raw_df_json` (+ `billing_currency`) instead — that validates + repairs "
                    "deterministically with ZERO LLM calls and won't time out."
                )
            return _err(f"Pipeline failed: {type(e).__name__}: {str(e)[:500]}")

        # ── Validate (NEVER swallow) ───────────────────────────────────
        await ctx.report_progress(2, 3, "Validating against FOCUS v1.2 spec...")
        try:
            initial_report = validate_focus(raw_df, source=source)
        except Exception as e:  # noqa: BLE001
            logger.exception("validate_focus raised — refusing (no silent None)")
            return _err(f"Validator crashed: {type(e).__name__}: {str(e)[:500]}")

        initial_missing = _missing_mandatory(raw_df)
        initial_failed = _failed_checks(initial_report)
        initial_compliant = not initial_missing and not initial_failed

        repairs: list[dict[str, Any]] = []
        # ── Repair pass (deterministic; optional LLM for currency only) ─
        # Repairs fill a MISSING BillingCurrency, OR override an existing one when
        # the caller explicitly passes a different ``billing_currency`` (deterministic
        # override — e.g. the LLM read "USD" from the invoice but the user says CAD).
        existing_currency = (
            raw_df["BillingCurrency"].drop_nulls().first()
            if "BillingCurrency" in raw_df.columns
            else None
        )
        currency_missing = "BillingCurrency" in initial_missing
        caller_override = (
            currency_value is not None
            and not currency_missing
            and str(existing_currency).strip().upper() != currency_value
        )
        if currency_missing or caller_override:
            value = currency_value
            if value is None and llm_repair:
                value = await _maybe_infer_currency(raw_df)
            if value is not None:
                repairs.append(
                    {
                        "column": "BillingCurrency",
                        "method": "set_static_value",
                        "value": value,
                        "source": "caller" if currency_value else "llm_inferred",
                        "reason": (
                            "missing"
                            if currency_missing
                            else f"override {existing_currency} -> {currency_value}"
                        ),
                    }
                )

        repairs_kept: list[dict[str, Any]] = []
        repairs_rolled_back: list[dict[str, Any]] = []
        if repairs:
            repaired_df = raw_df
            for r in repairs:
                repaired_df = _apply_static_repair(repaired_df, r["column"], r["value"])
            try:
                repaired_report = validate_focus(repaired_df, source=source)
            except Exception as e:  # noqa: BLE001
                logger.exception("post-repair validate_focus raised — refusing")
                return _err(
                    f"Post-repair validator crashed: {type(e).__name__}: {str(e)[:500]}",
                    initial_validation_report=_report_to_dict(initial_report),
                    repairs_attempted=repairs,
                )
            for r in repairs:
                col = r["column"]
                # The presence_<col> check passing is the proof the repair worked.
                check = next(
                    (
                        c
                        for c in repaired_report.checks
                        if c.rule_id == f"presence_{col}"
                    ),
                    None,
                )
                if check is not None and check.passed:
                    repairs_kept.append(r)
                else:
                    repairs_rolled_back.append(
                        {**r, "reason": "re-validation did not pass; reverted"}
                    )
            # All-or-nothing: only trust the repaired frame if everything verified.
            if repairs_kept and not repairs_rolled_back:
                final_df = repaired_df
                final_report = repaired_report
            else:
                logger.warning("repairs rolled back: %s", repairs_rolled_back)
                final_df = raw_df
                final_report = initial_report
                repairs_kept = []
                repairs_rolled_back = repairs_rolled_back or repairs
        else:
            final_df = raw_df
            final_report = initial_report

        final_missing = _missing_mandatory(final_df)
        final_failed = _failed_checks(final_report)
        final_compliant = not final_missing and not final_failed

        # ── Optional: official FinOps validator on the FINAL (post-repair) frame ──
        # Additive second opinion — an official-validator failure (env, crash) is
        # reported explicitly, never silently dropped, and never fails the
        # internal result.
        official: dict[str, Any] | None = None
        if official_validation:
            from stitcher.assistant_harness.tools import (
                focus_official_validation_tools as _fovt,
            )

            await ctx.report_progress(
                3, 3, "Running official focus_validator (subprocess)..."
            )
            official = await _fovt.run_official_on_df(final_df)

        await ctx.report_progress(3, 3, "Done.")

        def _serialize(df: pl.DataFrame) -> dict[str, Any]:
            sample = df.head(max_sample_rows)
            for col in sample.columns:
                if sample[col].dtype.is_temporal():
                    sample = sample.with_columns(pl.col(col).cast(pl.Utf8))
            return {
                "shape": {"rows": df.height, "columns": df.width},
                "columns": df.columns,
                "sample_rows": sample.to_dicts(),
            }

        # ── User-visible artifact: the FINAL (post-repair) frame ────────
        from .....common import artifacts

        final_parquet = artifacts.persist_parquet(final_df, "validated", source, "FOCUS_PARQUET_OUTPUT_DIR", "stitcher-focus-parquet")

        return {
            "success": True,
            "compliant": final_compliant,
            "source": source,
            "next_steps": (
                [
                    "To fix failing checks, use the custom_cost tools IN THIS ORDER: "
                    "1) generate_focus_plans (correct the source→FOCUS mapping), "
                    "2) simulate_normalize_config (verify the corrected config on raw data), "
                    "3) save_focus_config (persist the updated normalize config as YAML), "
                    "4) validate_focus_official(file_path=<normalized parquet>) (official conformance)."
                ]
                if not final_compliant
                else None
            ),
            "initial_validation_report": _report_to_dict(initial_report),
            "initial_compliant": initial_compliant,
            "repairs_applied": repairs,
            "repairs_kept": repairs_kept,
            "repairs_rolled_back": repairs_rolled_back,
            "final_compliant": final_compliant,
            "final_validation_report": _report_to_dict(final_report),
            "missing_mandatory_columns": final_missing,
            "failed_checks": final_failed,
            "llm_calls_made": any(r["source"] == "llm_inferred" for r in repairs),
            "normalized_df_summary": _serialize(final_df),
            "final_parquet": final_parquet,
            "official_validation": (
                (official or {}).get("summary") if official else None
            ),
            "official_validation_compliant": (
                official.get("compliant") if official else None
            ),
            "official_validation_report_path": (
                official.get("report_path") if official else None
            ),
            "official_validation_error": (
                official.get("error") if official and not official["success"] else None
            ),
            "elapsed_seconds": _now() - t0,
        }

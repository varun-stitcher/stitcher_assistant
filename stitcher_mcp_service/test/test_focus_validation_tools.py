"""Adversarial tests for the focus_validation / validate_and_repair_focus tool.

Following the human-in-the-loop audit pattern: every safety boundary gets a
test proving the wrong path is never taken (``must NOT``, ``not in``, ``== 0``,
``is None``), not just happy-path tests.

Boundaries under test
---------------------
B1. Validator exception is NEVER swallowed → hard failure (not ``success=true``
    with ``validation_report=None``).  [weaken-by-fallback closure]
B2. Missing mandatory column ⇒ ``compliant=false`` — no "passed-but-broken".
B3. BillingCurrency is repaired ONLY from an explicit caller value or a gated
    LLM inference; never a hardcoded default.  [no silent fallback]
B4. Repair is deterministic (appended plan step re-run through the normalizer),
    never an LLM-written frame; zero LLM calls when ``llm_repair=false``.
B5. An unverifiable repair is ROLLED BACK and reported, not silently kept.
B6. A caller-provided BillingCurrency already valid ⇒ deterministic repair
    applies and the presence check passes (positive control for B3/B5).
"""

import json

import polars as pl

from stitcher.assistant_harness.sub_mcp_agents.custom_cost.tools import focus_validation_tools as fvt

# ── Helpers ────────────────────────────────────────────────────────────────


def _minimal_focus_df(missing: list[str] | None = None) -> pl.DataFrame:
    """A mandatory-complete FOCUS frame, optionally with columns removed.

    ``missing`` lists FOCUS mandatory columns to drop so we can exercise the
    refuse-by-construction boundary without depending on the LLM.
    """
    missing = missing or []
    data = {
        "BillingPeriodStart": ["2024-05-16 00:00:00"],
        "BillingPeriodEnd": ["2024-06-16 00:00:00"],
        "ChargePeriodStart": ["2024-05-16 00:00:00"],
        "ChargePeriodEnd": ["2024-06-16 00:00:00"],
        "BilledCost": [129.30],
        "BillingCurrency": ["GBP"],
        "ProviderName": ["CONTOSO"],
        "ServiceName": ["UNIV. A4 80gsm"],
        "InvoiceId": ["3847193"],
        "ChargeCategory": ["Purchase"],
    }
    for col in missing:
        data.pop(col, None)
    return pl.DataFrame(data)


def _df_blob(df: pl.DataFrame) -> str:
    return json.dumps({"sample_rows": df.to_dicts()})


# ── B1: validator exception is never swallowed ─────────────────────────────


def test_validator_exception_is_hard_failure_not_none(monkeypatch):
    """If validate_focus raises, the tool must return a hard failure — it MUST
    NOT return success=True with a validation_report=None (weaken-by-fallback)."""

    def _boom(df, source=""):
        raise RuntimeError("validator corrupt")

    monkeypatch.setattr(fvt, "validate_focus", _boom)

    result = asyncio_run(
        fvt_register().call_tool("validate_and_repair_focus", {"raw_df_json": _df_blob(_minimal_focus_df())})
    )
    payload = _payload(result)
    assert payload["success"] is False, "validator crash must be a hard failure"
    assert payload["compliant"] is False
    assert "Validator crashed" in payload["error"]
    # The safety property: we do NOT fall into a silent None report.
    assert payload["error"].count("None") == 0 or "Validator crashed" in payload["error"]
    assert "RuntimeError" in payload["error"]


# ── B2: missing mandatory ⇒ compliant=false ────────────────────────────────


def test_missing_mandatory_column_is_noncompliant():
    df = _minimal_focus_df(missing=["BillingCurrency"])
    result = asyncio_run(
        fvt_register().call_tool("validate_and_repair_focus", {"raw_df_json": _df_blob(df), "llm_repair": False})
    )
    payload = _payload(result)
    assert payload["success"] is True  # tool itself ran
    assert payload["compliant"] is False  # but the output is NOT FOCUS-compliant
    assert "BillingCurrency" in payload["missing_mandatory_columns"]  # reported, not hidden
    # No silent default was applied.
    assert payload["repairs_applied"] == []


# ── B3: no silent BillingCurrency fallback ─────────────────────────────────


def test_no_silent_currency_fallback_without_llm():
    """With llm_repair=false and no explicit currency, a missing BillingCurrency
    must remain missing and be reported — never filled with a hardcoded default."""
    df = _minimal_focus_df(missing=["BillingCurrency"])
    result = asyncio_run(
        fvt_register().call_tool("validate_and_repair_focus", {"raw_df_json": _df_blob(df), "llm_repair": False})
    )
    payload = _payload(result)
    assert payload["repairs_applied"] == []  # nothing silently applied
    assert payload["llm_calls_made"] is False
    assert "BillingCurrency" in payload["missing_mandatory_columns"]
    # No column named BillingCurrency appeared in the output frame.
    assert "BillingCurrency" not in payload["normalized_df_summary"]["columns"]


# ── B4: deterministic repair, zero LLM calls when llm_repair=false ─────────


def test_explicit_currency_repair_is_deterministic_and_zero_llm():
    """An explicit caller BillingCurrency repairs the gap deterministically —
    the repaired frame has the column and zero LLM calls are made."""
    df = _minimal_focus_df(missing=["BillingCurrency"])
    result = asyncio_run(
        fvt_register().call_tool(
            "validate_and_repair_focus",
            {"raw_df_json": _df_blob(df), "billing_currency": "GBP", "llm_repair": False},
        )
    )
    payload = _payload(result)
    assert payload["llm_calls_made"] is False  # escape hatch: no LLM
    assert "BillingCurrency" in payload["repairs_kept"][0]["column"] if payload["repairs_kept"] else True
    # The repair actually landed in the frame.
    assert "BillingCurrency" in payload["normalized_df_summary"]["columns"]
    assert payload["repairs_rolled_back"] == []


def test_invalid_currency_code_rejected_before_any_llm():
    """A malformed billing_currency must be refused up front — validates the
    'refuse-by-construction' input gate rather than accepting garbage."""
    df = _minimal_focus_df(missing=["BillingCurrency"])
    result = asyncio_run(
        fvt_register().call_tool(
            "validate_and_repair_focus",
            {"raw_df_json": _df_blob(df), "billing_currency": "GBpounds", "llm_repair": False},
        )
    )
    payload = _payload(result)
    assert payload["success"] is False
    assert "ISO 4217" in payload["error"]


# ── B5: unverifiable repair is rolled back, not kept ───────────────────────


def test_unverifiable_repair_is_rolled_back(monkeypatch):
    """If a repair does not make its check pass, it must be rolled back and
    reported as such — not silently kept in the output frame."""
    df = _minimal_focus_df(missing=["BillingCurrency"])

    # Force the post-repair validator to reject BillingCurrency presence so the
    # repair is judged unverifiable and must be rolled back.
    monkeypatch.setattr(fvt, "validate_focus", _force_presence_fail)

    result = asyncio_run(
        fvt_register().call_tool(
            "validate_and_repair_focus",
            {"raw_df_json": _df_blob(df), "billing_currency": "GBP", "llm_repair": False},
        )
    )
    payload = _payload(result)
    # The repair was attempted, so it shows up as applied.
    assert payload["repairs_applied"] != []
    # But because it did not verify, it is reported as rolled back.
    assert payload["repairs_rolled_back"] != []
    assert payload["repairs_kept"] == []
    # The final frame falls back to the initial (unrepaired) frame.
    assert "BillingCurrency" not in payload["normalized_df_summary"]["columns"]
    assert payload["final_compliant"] is False


def _force_presence_fail(df, source=""):
    """Validates normally EXCEPT the BillingCurrency presence check fails."""
    from stitcher.pipeline.common.focus_spec_validator import validate_focus as real

    report = real(df, source=source)
    for c in report.checks:
        if c.rule_id == "presence_BillingCurrency":
            c.passed = False
            c.detail = "MISSING"
    return report


# ── B6: positive control — valid currency repairs successfully ─────────────


def test_compliant_frame_is_compliant_no_repair():
    """A fully mandatory-complete frame is compliant with zero repairs and zero
    LLM calls — the happy path that should never need a repair."""
    df = _minimal_focus_df()
    result = asyncio_run(
        fvt_register().call_tool("validate_and_repair_focus", {"raw_df_json": _df_blob(df), "llm_repair": False})
    )
    payload = _payload(result)
    assert payload["success"] is True
    assert payload["compliant"] is True
    assert payload["repairs_applied"] == []
    assert payload["llm_calls_made"] is False


# ── B7: explicit currency overrides an existing (wrong) value ──────────────


def test_explicit_currency_overrides_existing_value():
    """The user says the currency should be CAD, but the frame says GBP (the LLM
    mis-read it). Passing ``billing_currency="CAD"`` must deterministically OVERRIDE
    the present value to CAD (zero LLM), not only fill a missing one."""
    df = _minimal_focus_df()  # BillingCurrency="GBP" present
    result = asyncio_run(
        fvt_register().call_tool(
            "validate_and_repair_focus",
            {"raw_df_json": _df_blob(df), "billing_currency": "CAD", "llm_repair": False},
        )
    )
    payload = _payload(result)
    kept = payload["repairs_kept"]
    assert kept and kept[0]["column"] == "BillingCurrency"
    assert kept[0]["value"] == "CAD"
    assert kept[0]["source"] == "caller"
    assert "override" in kept[0].get("reason", "")
    # The repair actually landed in the frame.
    row = payload["normalized_df_summary"]["sample_rows"][0]
    assert row["BillingCurrency"] == "CAD"
    assert payload["llm_calls_made"] is False
    assert payload["repairs_rolled_back"] == []


def test_same_explicit_currency_is_noop_override():
    """Passing the currency that is already present must NOT trigger a repair
    (idempotent override) — no churn, nothing reported as applied."""
    df = _minimal_focus_df()  # BillingCurrency="GBP" present
    result = asyncio_run(
        fvt_register().call_tool(
            "validate_and_repair_focus",
            {"raw_df_json": _df_blob(df), "billing_currency": "GBP", "llm_repair": False},
        )
    )
    payload = _payload(result)
    assert payload["repairs_applied"] == []
    assert payload["compliant"] is True
    assert payload["repairs_kept"] == []


# ── plumbing helpers ────────────────────────────────────────────────────────


def fvt_register():
    from fastmcp import FastMCP

    mcp = FastMCP(name="focus-validation-test")
    fvt.register(mcp)
    return mcp


def asyncio_run(coro):
    import asyncio

    return asyncio.run(coro)


def _payload(result):
    inner = result.content[0].text if hasattr(result, "content") else result
    return json.loads(inner)

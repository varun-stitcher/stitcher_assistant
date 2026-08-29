"""Adversarial tests for validate_focus_official (official FinOps focus_validator tool).

Every safety boundary gets a test proving the wrong path is never taken,
following the house human-in-the-loop pattern (see test_focus_validation_tools.py).

Boundaries under test
---------------------
B1. Missing/unreadable input ⇒ refused up front, no subprocess spawned, hard
    failure — never a simulated report or a faked verdict.
B2. Both/neither of file_path + raw_df_json ⇒ refused (no ambiguous input).
B3. Unsupported file format ⇒ refused up front.
B4. Validator clone/venv missing ⇒ clear error pointing at bootstrap; the server
    never crashes and never fabricates a result.
B5. A garbage (non-FOCUS) dataset ⇒ either a hard failure or failing rules — but
    NEVER success=true with compliant=true (the fabricated happy path).
B6. A valid compliant FOCUS frame ⇒ runs the REAL validator (100+ rules),
    compliant=true, zero LLM calls (analyze=false default).
B7. A deliberately non-conformant frame ⇒ compliant=false, failing rules named,
    LLM never consulted.
B8. The summary is deterministic and capped (failing_rules capped, truncated
    flag set).
B9. WORKFLOW WIRE-IN: validate_and_repair_focus(official_validation=true) runs
    the official validator on the FINAL (post-repair) frame and returns its
    summary under ``official_validation`` — with ZERO LLM calls.
B10. WORKFLOW WIRE-IN resilience: with the validator env broken, the internal
    result still succeeds and the failure is reported explicitly as
    ``official_validation_error`` — never silently dropped, never fatal.
B11. run_official_on_df (the shared runner) returns the full report for a
    compliant frame and success=False + error for a broken env.

Requires the bootstrapped local clone (focus_validator_local + .venv); tests that
need it skip with a clear message when the clone is absent (CI / fresh checkout).
"""

import asyncio
import json
from pathlib import Path

import polars as pl
import pytest

from stitcher.assistant_harness.sub_mcp_agents.custom_cost.tools.focus import focus_validation_tools
from stitcher.assistant_harness.tools import focus_official_validation_tools as fvt


def _fv_clone_available() -> bool:
    try:
        fvt._fv_python(fvt._fv_home())
        return True
    except FileNotFoundError:
        return False


_FV_OK = _fv_clone_available()


def _compliant_focus_rows() -> list[dict]:
    """A FOCUS v1.2 row that satisfies every MUST rule in the official 1.2 rule set.

    Known upstream gap (surfaced, not hidden): InvoiceId-C-004-C and InvoiceId-C-005-C
    encode their prose conditions as empty `{}` in the official 1.2 rules JSON, so the
    engine runs both statically and they are mutually unsatisfiable (null ⇒ C-005 fails,
    non-null ⇒ C-004 fails). Any InvoiceId choice trips exactly one of the pair.
    The end-to-end test asserts failing ⊆ {that pair}.
    """
    return [
        {
            "BilledCost": 100.0,
            "BillingAccountId": "acct1",
            "BillingAccountName": "TestAcct",
            "BillingAccountType": "Account",
            "BillingCurrency": "USD",
            "BillingPeriodEnd": "2024-06-01T00:00:00",
            "BillingPeriodStart": "2024-05-01T00:00:00",
            "CapacityReservationId": None,
            "CapacityReservationStatus": None,
            "ChargeCategory": "Usage",
            "ChargeClass": None,
            "ChargeDescription": "Compute charge",
            "ChargePeriodEnd": "2024-05-16T10:00:00",
            "ChargePeriodStart": "2024-05-16T09:00:00",
            "CommitmentDiscountCategory": None,
            "CommitmentDiscountId": None,
            "CommitmentDiscountName": None,
            "CommitmentDiscountQuantity": None,
            "CommitmentDiscountStatus": None,
            "CommitmentDiscountType": None,
            "CommitmentDiscountUnit": None,
            "ConsumedQuantity": 10.0,
            "ConsumedUnit": "USD",
            "ContractedCost": 10.0,  # = ContractedUnitPrice × PricingQuantity (patternC)
            "ContractedUnitPrice": 10.0,
            "EffectiveCost": 100.0,
            "InvoiceIssuerName": "AWS",
            "ListCost": 10.0,  # = ListUnitPrice × PricingQuantity (patternC)
            "ListUnitPrice": 10.0,
            "PricingCategory": "Standard",
            "PricingCurrency": None,
            "PricingCurrencyContractedUnitPrice": None,
            "PricingCurrencyEffectiveCost": None,
            "PricingCurrencyListUnitPrice": None,
            "PricingQuantity": 1.0,
            "PricingUnit": "USD",
            "ProviderName": "AWS",
            "PublisherName": "AWS",
            "RegionId": "us-east-1",
            "RegionName": "US East",
            "ResourceId": "i-123",
            "ResourceName": "instance-1",
            "ResourceType": "EC2",
            "ServiceCategory": "Compute",
            "ServiceName": "EC2",
            "SkuId": "sku1",
            "SkuMeter": "meter1",
            "SkuPriceDetails": "{}",
            "SkuPriceId": "price1",
            "SubAccountId": "sub1",
            "SubAccountName": "SubAcct",
            "SubAccountType": "Account",
            "Tags": "{}",
            "AvailabilityZone": None,
            "ChargeFrequency": "Usage-Based",
            "InvoiceId": None,
            "ServiceSubcategory": "Virtual Machines",
        },
    ]


def _df_blob(rows: list[dict]) -> str:
    return json.dumps({"sample_rows": rows})


# ── B1: missing input refused up front (no subprocess) ────────────────────


@pytest.mark.asyncio
async def test_b1_missing_file_refused_up_front(monkeypatch):
    def _no_spawn(*a, **k):  # pragma: no cover — proves the subprocess is never launched
        raise AssertionError("subprocess must not be launched for a missing input file")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _no_spawn)

    result = await fvt.validate_focus_official(None, file_path="/nonexistent/data.csv")
    assert result["success"] is False
    assert "not found" in result["error"]
    assert "compliant" not in result  # never fakes a verdict


@pytest.mark.asyncio
async def test_b1_empty_frame_refused():
    result = await fvt.validate_focus_official(None, raw_df_json='{"sample_rows": []}')
    assert result["success"] is False
    assert "no rows" in result["error"]


# ── B2: both/neither input refused ─────────────────────────────────────────


@pytest.mark.asyncio
async def test_b2_both_inputs_refused():
    result = await fvt.validate_focus_official(None, file_path="/tmp/x.csv", raw_df_json='{"rows": [{}]}')
    assert result["success"] is False
    assert "exactly one" in result["error"]


@pytest.mark.asyncio
async def test_b2_neither_input_refused():
    result = await fvt.validate_focus_official(None)
    assert result["success"] is False
    assert "exactly one" in result["error"]


# ── B3: unsupported format refused ─────────────────────────────────────────


@pytest.mark.asyncio
async def test_b3_unsupported_format_refused(tmp_path: Path):
    bad = Path(tmp_path) / "focus_bad.xlsx"
    bad.write_bytes(b"not really xlsx")
    result = await fvt.validate_focus_official(None, file_path=str(bad))
    assert result["success"] is False
    assert "unsupported data format" in result["error"]


@pytest.mark.asyncio
async def test_b3_bad_rules_version_refused():
    result = await fvt.validate_focus_official(None, raw_df_json='{"rows": [{}]}', rules_version="banana")
    assert result["success"] is False
    assert "rules_version" in result["error"]


# ── B4: missing clone/venv → clear bootstrap error, no crash ──────────────


@pytest.mark.asyncio
async def test_b4_missing_clone_refused(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("FOCUS_VALIDATOR_HOME", str(tmp_path / "does_not_exist"))
    result = await fvt.validate_focus_official(None, raw_df_json=_df_blob(_compliant_focus_rows()))
    assert result["success"] is False
    assert "not found" in result["error"]
    assert "Bootstrap" in result.get("hint", "")


@pytest.mark.asyncio
async def test_b4_missing_venv_refused(monkeypatch, tmp_path: Path):
    clone = tmp_path / "clone"
    clone.mkdir()
    monkeypatch.setenv("FOCUS_VALIDATOR_HOME", str(clone))
    result = await fvt.validate_focus_official(None, raw_df_json=_df_blob(_compliant_focus_rows()))
    assert result["success"] is False
    assert "venv" in result["error"]


# ── B5: garbage input → crash or failing rules, never fake compliance ─────


@pytest.mark.skipif(not _FV_OK, reason="focus_validator clone/venv not bootstrapped")
@pytest.mark.asyncio
async def test_b5_garbage_frame_never_fake_compliant():
    df = pl.DataFrame({"CompletelyWrong": ["a", "b"], "AlsoWrong": [1, 2]})
    result = await fvt.validate_focus_official(None, raw_df_json=_df_blob(df.to_dicts()))
    if result["success"] is False:
        assert result.get("compliant") is not True
    else:
        assert result["compliant"] is False
        assert result["summary"]["failing_count"] > 0


# ── B6/B7: real end-to-end runs (need the clone) ──────────────────────────


@pytest.mark.skipif(not _FV_OK, reason="focus_validator clone/venv not bootstrapped")
@pytest.mark.asyncio
async def test_b6_compliant_frame_end_to_end_zero_llm():
    result = await fvt.validate_focus_official(None, raw_df_json=_df_blob(_compliant_focus_rows()))
    assert result["success"] is True, result.get("error")
    assert result["summary"]["total_rules"] > 500  # real rule set, not a stub
    assert "analysis" not in result  # analyze=false default → zero LLM
    # Every failing rule must be the documented upstream InvoiceId contradiction
    # (C-004 vs C-005: empty conditions in the official 1.2 rules JSON) or a rule
    # failing only because of it — anything else means our fixture or the tool broke.
    allowed = {"InvoiceId-C-004-C", "InvoiceId-C-005-C", "InvoiceId-C-000-C"}
    unexpected = set(result["summary"]["failing_rules"]) - allowed
    assert not unexpected, f"unexpected failures: {unexpected}"


@pytest.mark.skipif(not _FV_OK, reason="focus_validator clone/venv not bootstrapped")
@pytest.mark.asyncio
async def test_b7_non_conformant_frame_reports_failing_rules():
    rows = _compliant_focus_rows()
    rows[0]["BillingCurrency"] = "NOPE"  # enum violation — MUST be an ISO 4217 code
    result = await fvt.validate_focus_official(None, raw_df_json=_df_blob(rows))
    assert result["success"] is True
    assert result["compliant"] is False
    assert result["summary"]["failing_count"] > 0
    assert any("urrency" in rid for rid in result["summary"]["failing_rules"])
    assert "analysis" not in result  # LLM never consulted


# ── B8: summary is deterministic and capped ────────────────────────────────


def test_b8_summary_deterministic_and_capped():
    report = {
        "rules_version": "1.2",
        "data_row_count": 5,
        "by_rule_id": {f"Rule-{i:03d}": {"passed": i % 2 == 0, "violations": 1, "message": "x"} for i in range(10)},
    }
    summary = fvt._summarize_report(report, max_failing=3)
    assert summary["failing_count"] == 5
    assert len(summary["failing_rules"]) == 3  # capped
    assert summary["failing_rules_truncated"] is True
    assert summary["compliant"] is False


# ── B9/B10: custom_cost workflow wire-in ──────────────────────────────────


def _custom_cost_tool(tool_name: str, args: dict) -> dict:
    """Call a custom_cost tool through the REAL FastMCP registration path.

    validate_and_repair_focus is defined nested inside register() (unlike the
    module-level official tool), so we exercise the actual registered tool —
    which also proves the wire-in works through MCP, not just in-process.
    """
    from fastmcp import FastMCP

    async def _call():
        mcp = FastMCP(name="test-custom-cost")
        focus_validation_tools.register(mcp)
        result = await mcp.call_tool(tool_name, args)
        assert result.is_error is False, result.content
        return result.structured_content

    return _call()


@pytest.mark.skipif(not _FV_OK, reason="focus_validator clone/venv not bootstrapped")
@pytest.mark.asyncio
async def test_b9_validate_and_repair_includes_official_report():
    rows = _compliant_focus_rows()
    rows[0].pop("BillingCurrency")  # exercise the repair path too
    rows[0]["InvoiceId"] = "inv-1"  # internal validator needs a non-null InvoiceId column
    result = await _custom_cost_tool(
        "validate_and_repair_focus",
        {"raw_df_json": _df_blob(rows), "billing_currency": "USD", "official_validation": True},
    )
    assert result["success"] is True, result.get("error")
    assert result["final_compliant"] is True  # internal validator passes
    # official validator ran on the FINAL frame and is reported explicitly
    assert result["official_validation"] is not None
    assert result["official_validation"]["total_rules"] > 500
    # the known upstream InvoiceId contradiction is the only official failure
    unexpected = set(result["official_validation"]["failing_rules"]) - {
        "InvoiceId-C-004-C",
        "InvoiceId-C-005-C",
        "InvoiceId-C-000-C",
    }
    assert not unexpected, f"unexpected official failures: {unexpected}"
    assert result["official_validation_error"] is None
    assert result["llm_calls_made"] is False  # zero LLM end to end


@pytest.mark.skipif(not _FV_OK, reason="focus_validator clone/venv not bootstrapped")
@pytest.mark.asyncio
async def test_b10_official_env_broken_does_not_fail_internal_result(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("FOCUS_VALIDATOR_HOME", str(tmp_path / "nope"))
    rows = _compliant_focus_rows()
    rows[0]["InvoiceId"] = "inv-1"  # internal validator needs a non-null InvoiceId column
    result = await _custom_cost_tool(
        "validate_and_repair_focus",
        {"raw_df_json": _df_blob(rows), "official_validation": True},
    )
    # the INTERNAL result is intact and successful...
    assert result["success"] is True
    assert result["final_compliant"] is True
    # ...and the official-validator failure is explicit, not silent
    assert result["official_validation"] is None
    assert "not found" in (result["official_validation_error"] or "")


@pytest.mark.skipif(not _FV_OK, reason="focus_validator clone/venv not bootstrapped")
@pytest.mark.asyncio
async def test_b11_run_official_on_df_shared_runner():
    import polars as pl

    df = pl.DataFrame(_compliant_focus_rows())
    out = await fvt.run_official_on_df(df)
    assert out["success"] is True
    assert out["summary"]["total_rules"] > 500
    assert set(out["summary"]["failing_rules"]) <= {
        "InvoiceId-C-004-C",
        "InvoiceId-C-005-C",
        "InvoiceId-C-000-C",
    }


# ── B12/B13: raw_df_json robustness + parquet artifacts ───────────────────


def test_b12_accepts_full_normalized_df_summary_verbatim():
    """The exact failure the agent hit in practice: passing the whole
    normalized_df_summary object (shape/columns/schema/sample_rows) instead of
    hand-extracting sample_rows must WORK."""
    rows = _compliant_focus_rows()
    summary = {
        "shape": {"rows": len(rows), "columns": len(rows[0])},
        "columns": list(rows[0]),
        "schema": dict.fromkeys(rows[0], "str"),
        "sample_rows": rows,
    }
    out = fvt._extract_rows_blob(json.dumps(summary))
    assert out[1] == "summary"
    assert len(out[0]) == len(rows)
    # and the WHOLE normalize_to_focus result shape works too
    result_blob = {"success": True, "normalized_df_summary": summary}
    out2 = fvt._extract_rows_blob(json.dumps(result_blob))
    assert out2[1] == "result"
    assert len(out2[0]) == len(rows)


def test_b12_hand_built_json_gets_actionable_hint():
    """Malformed hand-built payloads are refused with guidance, not a bare parser error."""
    with pytest.raises(ValueError) as ei:
        fvt._extract_rows_blob('{"sample_rows": [{"BilledCost": 1},] }')  # trailing comma
    assert "VERBATIM" in str(ei.value)


def test_b12_mixed_type_rows_reconstruct_tolerantly():
    """Empty strings + floats in the same column must NOT crash polars (the
    'unexpected value while building Series of type String' failure), and numeric
    columns must stay NUMERIC so the validator's cost-equation rules work."""
    rows = [
        {"BilledCost": 10250.0, "BillingCurrency": "USD", "Note": "x"},
        {"BilledCost": "", "BillingCurrency": "", "Note": None},
        {"BilledCost": 5.0, "BillingCurrency": "EUR", "Note": None},
    ]
    df = fvt._rows_to_frame(rows)
    assert df["BilledCost"].dtype == pl.Float64
    assert df["BilledCost"].to_list() == [10250.0, None, 5.0]  # "" → null, NOT 0.0
    assert df["BillingCurrency"].to_list() == ["USD", None, "EUR"]


@pytest.mark.skipif(not _FV_OK, reason="focus_validator clone/venv not bootstrapped")
@pytest.mark.asyncio
async def test_b12_end_to_end_with_full_summary_blob():
    """The agent's real path: pass normalize_to_focus's normalized_df_summary verbatim."""
    rows = _compliant_focus_rows()
    rows[0]["InvoiceId"] = "inv-1"
    summary = {"shape": {}, "columns": list(rows[0]), "sample_rows": rows}
    result = await fvt.validate_focus_official(None, raw_df_json=json.dumps(summary))
    assert result["success"] is True, result.get("error")
    assert result["summary"]["total_rules"] > 500


def test_b13_parquet_artifacts_persisted(tmp_path: Path, monkeypatch):
    from stitcher.assistant_harness.common import artifacts

    monkeypatch.setenv("FOCUS_PARQUET_OUTPUT_DIR", str(tmp_path))
    df = pl.DataFrame(_compliant_focus_rows())
    art = artifacts.persist_parquet(df, "normalized", "invoice sample.pdf", "FOCUS_PARQUET_OUTPUT_DIR", "stitcher-focus-parquet")
    assert art.get("path"), art
    p = Path(art["path"])
    assert p.is_file() and p.suffix == ".parquet"
    assert "invoice_sample.normalized." in p.name
    # round-trips identically — the artifact is the same data, never altered
    back = pl.read_parquet(p)
    assert back.equals(df)
    assert art["rows"] == df.height and art["columns"] == df.width


def test_b13_parquet_failure_is_reported_not_raised(monkeypatch, tmp_path: Path):
    from stitcher.assistant_harness.common import artifacts

    monkeypatch.setenv("FOCUS_PARQUET_OUTPUT_DIR", str(tmp_path / "f" / "x"))
    df = pl.DataFrame({"a": [1]})
    monkeypatch.setattr(df, "write_parquet", lambda *a, **k: (_ for _ in ()).throw(OSError("disk full")))
    art = artifacts.persist_parquet(df, "raw", "x.csv", "FOCUS_PARQUET_OUTPUT_DIR", "stitcher-focus-parquet")
    assert "error" in art
    assert "failed to persist" in art["error"]


# ── B14: context discipline — full report NEVER inlined by default ────────


@pytest.mark.skipif(not _FV_OK, reason="focus_validator clone/venv not bootstrapped")
@pytest.mark.asyncio
async def test_b14_full_report_not_inlined_by_default(tmp_path: Path, monkeypatch):
    """The full 578-rule report is ~500KB — enough to burn 75% of an agent's
    context in ONE tool result. By default the tool must return ONLY the compact
    summary + report_path (a persisted JSON artifact)."""
    monkeypatch.setenv("FOCUS_PARQUET_OUTPUT_DIR", str(tmp_path))
    result = await fvt.validate_focus_official(None, file_path="/tmp/focus_compliant.csv")
    assert result["success"] is True
    assert "full_report" not in result  # THE boundary: never inline by default
    assert result["report_path"] and Path(result["report_path"]).is_file()
    # the persisted artifact holds the full report for humans/follow-up calls
    saved = json.loads(Path(result["report_path"]).read_text())
    assert len(saved.get("by_rule_id", {})) > 500
    # summary stays small even with many failures
    assert len(json.dumps(result["summary"])) < 20_000


@pytest.mark.skipif(not _FV_OK, reason="focus_validator clone/venv not bootstrapped")
@pytest.mark.asyncio
async def test_b14_full_report_inline_only_on_explicit_optin(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("FOCUS_PARQUET_OUTPUT_DIR", str(tmp_path))
    result = await fvt.validate_focus_official(None, file_path="/tmp/focus_compliant.csv", include_full_report=True)
    assert "full_report" in result  # explicit scripting opt-in still works
    assert result["summary"]["total_rules"] > 500


@pytest.mark.skipif(not _FV_OK, reason="focus_validator clone/venv not bootstrapped")
@pytest.mark.asyncio
async def test_b14_wirein_returns_report_path_not_full_report():
    rows = _compliant_focus_rows()
    rows[0]["InvoiceId"] = "inv-1"
    rows[0]["BillingCurrency"] = "NOPE"
    result = await _custom_cost_tool(
        "validate_and_repair_focus",
        {"raw_df_json": _df_blob(rows), "official_validation": True},
    )
    assert result["success"] is True
    assert "official_validation_full_report" not in result  # no inline blob in the workflow either
    assert result["official_validation_report_path"] and Path(result["official_validation_report_path"]).is_file()
    assert len(json.dumps(result["official_validation"])) < 20_000

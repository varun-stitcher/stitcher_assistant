"""Full-pipeline integration tests: prompt → pi agent → real MCP tools → outcome.

Each test runs ONE live agent turn against the real combined MCP server
(AgentRunner), then validates the OUTCOME with signal-level assertions:

  * deterministic evidence first — transcript tool calls/results, parquet
    artifacts read back with polars, report files on disk;
  * LLM prose checked only via any-match regex signals (the wording varies by
    model; the KIND of information must be there);
  * never exact numbers from prose, never exact phrasing.

Marked `integration` (live LLM); opt-in via STITCHER_INTEGRATION=1 — see conftest.
"""

from __future__ import annotations

import json
import re

import pytest
from signals import (
    assert_any_signal,
    assert_markdown_table,
    assert_no_traceback,
    assert_parquet_artifact,
    newest_file,
    tool_calls,
    tool_results,
    transcript_events,
)

pytestmark = pytest.mark.integration

# timeout for one full agent turn (extract → plan → normalize → validate → answer).
# Generous on purpose: a turn that ends with a correct captured result but a slow
# finish is a PASS on outcome; the turn-count signal below catches flailing.
TURN_TIMEOUT = 600
# a converged turn should be done in a few dozen tool calls; hundreds mean the
# agent is flailing (a steering regression the suite must catch, not mask)
MAX_SANE_TURNS = 120


def _evidence(result) -> str:
    """Everything deterministic the turn produced: prose + structured result + tool results.
    Signal assertions run against this, so a correct structured result with empty prose
    still passes — but nothing the agent merely *claims* in prose is trusted."""
    events = transcript_events(result.run_dir)
    return "\n".join([result.text or ""] + [json.dumps(result.result) if result.result else ""] + tool_results(events))


def _run_prompt(runner, agent_model: str, scope: dict[str, str], prompt: str):
    result = runner.run(prompt, timeout=TURN_TIMEOUT, model=agent_model, **scope)
    # The run dir holds the transcript we assert on; keep it even on failure.
    assert result.run_dir, "runner produced no run_dir (cannot forensically inspect the turn)"
    print(f"\n[turn] status={result.status} turns={result.turns} elapsed={result.elapsed}s run_dir={result.run_dir}")
    print(f"[answer]\n{(result.text or '(empty prose)')[:2000]}")
    if result.result:
        print(f"[structured] {json.dumps(result.result)[:800]}")
    return result


def _assert_converged(result, what: str):
    """The turn ended with SOME deliverable (prose or a captured structured result) and
    did not flail. A timeout with neither is a failure; hundreds of turns are a failure."""
    assert result.status in ("ok", "no_structured_output"), f"{what}: turn failed: {result.error}"
    assert (result.text or "").strip() or result.result, f"{what}: no prose AND no structured result"
    assert (
        result.turns <= MAX_SANE_TURNS
    ), f"{what}: agent flailed ({result.turns} tool calls > {MAX_SANE_TURNS}) — steering regression"


# ──────────────────────────────────────────────────────────────────────────────
# UC1 — "convert to FOCUS": the flagship signal the user cares about — the
# final answer SHOWS A TABLE OF CONVERTED DATA, and the parquet artifact on
# disk is real, readable, FOCUS-shaped data.
# ──────────────────────────────────────────────────────────────────────────────


def test_convert_to_focus_shows_table(runner, agent_model, scope, focus_csv, artifact_dir):
    result = _run_prompt(
        runner,
        agent_model,
        scope,
        f"Convert the cost data in {focus_csv} to FOCUS format and show me the converted data as a markdown table.",
    )

    # turn must have completed with an answer (prose-only answers are acceptable;
    # submit_result is not required for this task)
    _assert_converged(result, "convert-to-FOCUS")

    # DETERMINISTIC: the conversion tool actually ran and did not error
    events = transcript_events(result.run_dir)
    norm_calls = tool_calls(events, "normalize_to_focus")
    assert norm_calls, (
        "normalize_to_focus was never called — the agent did not use the conversion pipeline "
        f"(tools used: {[c.get('name') for c in tool_calls(events)]})"
    )
    norm_results = tool_results(events, "normalize_to_focus")
    assert norm_results, "normalize_to_focus was called but produced no tool result"
    assert_no_traceback("\n".join(norm_results), "normalize_to_focus tool result")

    # DETERMINISTIC: the parquet artifact on disk is real FOCUS data
    parquet_dir = artifact_dir / "parquet"
    artifact = newest_file(parquet_dir, "*.parquet")
    assert artifact is not None, f"no parquet artifact written under {parquet_dir}"
    rows = assert_parquet_artifact(
        artifact,
        required_columns=["BilledCost", "BillingCurrency", "ChargeCategory", "ServiceName"],
        min_rows=1,
    )
    print(f"[artifact] {artifact.name} ({rows} rows)")

    # SIGNAL: the final answer SHOWS the converted data as a table (not just "done!")
    # — this one MUST be in the prose: "show me the converted data" is the user's ask
    assert_markdown_table(
        result.text or "",
        min_data_rows=1,
        header_signals=[r"BilledCost", r"BillingCurrency", r"EffectiveCost", r"ChargeCategory"],
    )
    assert_no_traceback(result.text or "", "final answer")


# ──────────────────────────────────────────────────────────────────────────────
# UC2 — non-compliant input: BillingCurrency removed → the agent must surface
# the VIOLATION and steer toward the repair path (plan / repair / fix), not
# silently declare success.
# ──────────────────────────────────────────────────────────────────────────────


def test_missing_billing_currency_reports_violation_and_next_steps(
    runner, agent_model, scope, focus_csv_missing_currency, artifact_dir
):
    result = _run_prompt(
        runner,
        agent_model,
        scope,
        f"Validate the cost data in {focus_csv_missing_currency} against FOCUS and fix any problems.",
    )

    _assert_converged(result, "validate-and-fix")
    evidence = _evidence(result)

    events = transcript_events(result.run_dir)
    # the validation/repair tooling was engaged (any of the validation entry points)
    validation_tools = {
        c.get("name")
        for c in tool_calls(events)
        if c.get("name") in ("validate_and_repair_focus", "validate_focus", "validate_focus_official")
    }
    assert (
        validation_tools
    ), f"no validation tool was called (tools used: {[c.get('name') for c in tool_calls(events)]})"

    # SIGNAL: the missing column and the fix are surfaced — in prose OR the structured
    # result (any of the acceptable phrasings, never a bare "all good")
    assert_any_signal(
        evidence,
        [
            r"BillingCurrency",
            r"violat\w+",  # violation(s)/violated
            r"non-?compliant",
            r"missing.{0,40}column",
        ],
        "the missing BillingCurrency / violations were surfaced",
    )
    assert_any_signal(
        evidence,
        [r"repair", r"fix", r"generate.{0,30}plan", r"next steps", r"simulat\w+", r"added"],
        "a next step / repair path was offered",
    )
    # a successful turn must still end with a user-facing answer (not just a capture)
    assert_no_traceback(result.text or "", "final answer")


# ──────────────────────────────────────────────────────────────────────────────
# UC3 — official validator as a second opinion: the official report lands on
# disk as an artifact (never inlined in full) and the answer references it.
# ──────────────────────────────────────────────────────────────────────────────


def test_official_validation_report_artifact(runner, agent_model, scope, focus_csv, artifact_dir):
    result = _run_prompt(
        runner,
        agent_model,
        scope,
        f"Run the official FOCUS validation on {focus_csv} and tell me whether it passes.",
    )

    _assert_converged(result, "official-validation")

    events = transcript_events(result.run_dir)
    assert tool_calls(
        events, "validate_focus_official"
    ), f"validate_focus_official was never called (tools used: {[c.get('name') for c in tool_calls(events)]})"
    results = "\n".join(tool_results(events, "validate_focus_official"))
    # DETERMINISTIC (tool result text is machine-generated): a report path was produced
    assert re.search(
        r"report[_ ]?path|focus-report", results, re.IGNORECASE
    ), f"no report_path in tool result:\n{results[:1000]}"

    # DETERMINISTIC: the report file exists and is a JSON artifact
    report = newest_file(artifact_dir / "parquet", "*.focus-report*.json")
    assert report is not None, "official report JSON was not persisted as an artifact"
    assert report.stat().st_size > 0

    # SIGNAL: a pass/fail-style verdict — prose OR structured result
    assert_any_signal(
        _evidence(result),
        [r"pass\w*", r"compliant", r"violat\w*", r"fail\w*", r"\d+\s+(rules|violations)"],
        "a pass/fail verdict was given",
    )
    assert_no_traceback(result.text or "", "final answer")


# ──────────────────────────────────────────────────────────────────────────────
# UC4 — bad input: a nonexistent file must produce a GRACEFUL refusal (the
# agent explains, no traceback, no invented data) — the no-silent-fallback
# boundary, tested end-to-end.
# ──────────────────────────────────────────────────────────────────────────────


def test_missing_file_is_gracefully_refused(runner, agent_model, scope, tmp_path):
    nonexistent = tmp_path / "does_not_exist.csv"
    result = _run_prompt(runner, agent_model, scope, f"Convert the cost data in {nonexistent} to FOCUS format.")

    _assert_converged(result, "missing-file-refusal")
    # SIGNAL: an honest failure surfaced (any of the acceptable phrasings)…
    assert_any_signal(
        result.text or "",
        [
            r"not found",
            r"does not exist",
            r"no such file",
            r"could not",
            r"unable to",
            r"error",
            r"fail",
        ],
        "the missing file was reported honestly",
    )
    # …and no traceback leak, and no fabricated success table for data that doesn't exist
    assert_no_traceback(result.text or "", "final answer")


# ──────────────────────────────────────────────────────────────────────────────
# UC4b — unscoped call: deterministic refusal BEFORE any subprocess/LLM spend.
# Not marked integration — it must pass everywhere, instantly, with no creds.
# ──────────────────────────────────────────────────────────────────────────────


def test_unscoped_turn_refused_without_llm(tmp_path):
    from stitcher.assistant_harness.agent_gateway.agent_runner import AgentRunner

    runner = AgentRunner()
    result = runner.run("convert something", environment_id="", pipeline_name="", timeout=10)
    assert result.status == "unscoped"
    assert "environment_id" in result.error
    assert result.turns == 0 and not result.text  # nothing ran

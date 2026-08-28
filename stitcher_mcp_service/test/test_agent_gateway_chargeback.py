"""P1 acceptance tests — chargeback tasks on the agent gateway (plans/chargeback-agent-service.md).

Covers:
  * the orchestrator MCP exposes `prepare_chargeback_report` + `explore_cost_data`;
  * the OpenAI surface: scope refusal, LIVE progress streaming on the `stitcher` extension,
    and the terminal `delta.ui` = {kind: "chargeback_report", ...} mapping;
  * AgentRunner's unscoped refusal (no subprocess spawned).

The runner is stubbed (FakeRunner) for the HTTP tests — these are L1-level seams; live L1
SSE capture against the real gateway is `pi_coding_agent/plans/` eval territory (T5).
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import pytest
from fastapi.testclient import TestClient

from stitcher.assistant_harness import agent_runner, openai_server
from stitcher.assistant_harness.agent_mcp_server import build_server
from stitcher.assistant_harness.agent_runner import AgentResult
from stitcher.assistant_harness.openai_server import _extract_scope, _terminal_ui

# ── pure mappings ────────────────────────────────────────────────────────────────────────────


def test_extract_scope_requires_environment():
    _, err = _extract_scope({"stitcher": {"pipeline_name": "finops"}})
    assert "environment_id" in err


def test_extract_scope_ok():
    scope, err = _extract_scope(
        {"stitcher": {"environment_id": "env-1", "pipeline_name": "finops", "auth_tenant": "t"}}
    )
    assert err == ""
    assert scope == {"environment_id": "env-1", "pipeline_name": "finops", "auth_tenant": "t"}


def test_terminal_ui_maps_chargeback_report():
    ui = _terminal_ui(
        {
            "task": "chargeback_report",
            "status": "completed",
            "period": "2026-07",
            "destination": "BigQuery Export",
            "markdown": "| Cost Center | Net |",
        }
    )
    assert ui == {
        "kind": "chargeback_report",
        "status": "completed",
        "period": "2026-07",
        "destination": "BigQuery Export",
        "markdown": "| Cost Center | Net |",
    }


def test_terminal_ui_passthrough_and_empty():
    other = {"task": "generate_enhance_config", "config_yaml": "a: 1"}
    assert _terminal_ui(other) is other
    assert _terminal_ui({}) is None
    assert _terminal_ui(None) is None


# ── runner refusal (no subprocess) ───────────────────────────────────────────────────────────


def test_agent_runner_unscoped_refused():
    res = agent_runner.AgentRunner().run("hi", environment_id="", pipeline_name="")
    assert res.status == "unscoped"
    assert "environment_id" in res.error


def test_message_tool_names_matches_pi_session_format():
    """Regression: pi writes assistant turns as type="message" + message.role — the old
    type=="assistant" filter matched NOTHING, so progress streams stayed silent and turns
    counted 0 (caught live, T1.2)."""
    line = {
        "type": "message",
        "id": "m1",
        "message": {
            "role": "assistant",
            "content": [
                {"type": "text", "text": "grounding"},
                {"type": "toolCall", "name": "activate_sub_mcp"},
                {"type": "toolCall", "name": "chargeback_by_cost_center"},
            ],
        },
    }
    assert agent_runner._message_tool_names(line) == ["activate_sub_mcp", "chargeback_by_cost_center"]
    # non-assistant / non-message lines carry no tool calls
    assert agent_runner._message_tool_names({"type": "message", "message": {"role": "user", "content": []}}) == []
    assert agent_runner._message_tool_names({"type": "toolResult"}) == []


def test_harvest_chargeback_markdown(tmp_path):
    """When the agent never calls submit_result, the deterministic tool's transcript output IS the
    artifact — harvested with provenance, never the model's prose re-summary."""
    sdir = tmp_path / "_session"
    sdir.mkdir()
    lines = [
        {
            "type": "message",
            "message": {
                "role": "toolResult",
                "toolName": "discover_cost_schema",
                "content": [{"type": "text", "text": "# Cost schema — cols"}],
            },
        },
        {
            "type": "message",
            "message": {
                "role": "toolResult",
                "toolName": "chargeback_by_cost_center",
                "content": [
                    {
                        "type": "text",
                        "text": "# Chargeback by cost center — July 2026\n| Cost Center | Net |\n|---|---:|",
                    }
                ],
            },
        },
        {
            "type": "message",
            "message": {
                "role": "toolResult",
                "toolName": "query_focus_cost",
                "content": [{"type": "text", "text": "# Cost by x_CostCenter + ServiceName\n| a | b |\n|---|---|"}],
            },
        },
    ]
    (sdir / "20260828T_xxx_gateway-s.jsonl").write_text("\n".join(json.dumps(ln) for ln in lines))
    out = agent_runner.harvest_chargeback_markdown(str(tmp_path))
    assert out["task"] == "chargeback_report" and out["status"] == "completed"
    assert out["source"] == "harvested_from_transcript"
    assert out["markdown"].startswith("# Cost by")  # the LAST report tool result wins

    # refusals are never harvested
    sdir2 = tmp_path / "no_report" / "_session"
    sdir2.mkdir(parents=True)
    refusal = {
        "type": "message",
        "message": {
            "role": "toolResult",
            "toolName": "chargeback_by_cost_center",
            "content": [{"type": "text", "text": "ERR (tool): no queryable FOCUS destination."}],
        },
    }
    (sdir2 / "t.jsonl").write_text(json.dumps(refusal))
    assert agent_runner.harvest_chargeback_markdown(str(tmp_path / "no_report")) == {}


# ── orchestrator MCP: the chargeback task tools exist ────────────────────────────────────────


@pytest.mark.asyncio
async def test_agent_mcp_exposes_chargeback_tasks():
    mcp = build_server()
    tools = {t.name: t for t in await mcp.list_tools()}
    for name in ("prepare_chargeback_report", "explore_cost_data"):
        assert name in tools, f"missing orchestrator task tool {name}"
    desc = tools["prepare_chargeback_report"].description or ""
    assert "chargeback_by_cost_center" in desc and "destination" in desc.lower()
    xtab = tools["explore_cost_data"].description or ""
    assert "cost_center,service" in xtab  # the cross-tab guidance is in the tool spec itself


# ── OpenAI surface: progress streaming + terminal ui (stubbed runner) ────────────────────────


class FakeRunner:
    """Stands in for AgentRunner: emits 3 progress events, returns a submitted chargeback_report."""

    def __init__(self) -> None:
        self.calls: list[dict] = []

    def run(self, prompt: str, on_event: Callable[[dict], None] | None = None, **scope: Any) -> AgentResult:
        self.calls.append({"prompt": prompt, **scope})
        for i in range(3):
            if on_event:
                on_event({"stage": "orchestrating", "message": f"tool: step-{i}", "tool": f"t{i}", "turn": i + 1})
        return AgentResult(
            status="ok",
            text="Report ready.",
            result={
                "task": "chargeback_report",
                "status": "completed",
                "period": "2026-07",
                "destination": "BigQuery Export",
                "markdown": "| Cost Center | Net Chargeback |\n|---|---:|",
            },
            turns=3,
            elapsed=1.2,
        )


@pytest.fixture()
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    from fastapi import FastAPI

    fake = FakeRunner()
    monkeypatch.setattr(openai_server, "_get_runner", lambda: fake)
    app = FastAPI()
    app.include_router(openai_server.router)
    return TestClient(app)


def _sse_chunks(lines: list[str]) -> list[dict]:
    return [json.loads(ln[len("data: ") :]) for ln in lines if ln.startswith("data: ") and ln != "data: [DONE]"]


def test_stream_progress_and_terminal_ui(client: TestClient):
    with client.stream(
        "POST",
        "/v1/chat/completions",
        json={
            "model": "stitcher-config-agent",
            "stream": True,
            "messages": [{"role": "user", "content": "chargeback for July"}],
            "stitcher": {"environment_id": "env-1", "pipeline_name": "finops"},
        },
    ) as resp:
        assert resp.status_code == 200
        lines = [ln for ln in resp.iter_lines() if ln.strip()]
    chunks = _sse_chunks(lines)
    # start marker + ≥3 LIVE progress events streamed on the stitcher extension
    progress = [c for c in chunks if c.get("stitcher", {}).get("kind") == "tool"]
    assert len(progress) >= 3
    assert "step-0" in progress[0]["stitcher"]["msg"]
    # terminal chunk carries **delta.ui** kind=chargeback_report (the SPC transport reads delta.ui)
    terminal = chunks[-1]
    assert terminal["choices"][0]["finish_reason"] == "stop"
    ui = terminal["choices"][0]["delta"].get("ui") or {}
    assert ui.get("kind") == "chargeback_report"
    assert "Cost Center" in ui.get("markdown", "")
    assert lines[-1] == "data: [DONE]"


def test_non_stream_carries_ui(client: TestClient):
    resp = client.post(
        "/v1/chat/completions",
        json={
            "messages": [{"role": "user", "content": "chargeback"}],
            "stitcher": {"environment_id": "env-1", "pipeline_name": "finops"},
        },
    )
    assert resp.status_code == 200
    msg = resp.json()["choices"][0]["message"]
    assert msg["ui"]["kind"] == "chargeback_report"


def test_scope_refused(client: TestClient):
    resp = client.post("/v1/chat/completions", json={"messages": [{"role": "user", "content": "hi"}]})
    assert resp.status_code == 400
    assert "environment_id" in resp.json()["error"]["message"]


def test_chat_ui_served(client: TestClient):
    """The minimal chat UI (T1 demo surface) is served at / and /chat."""
    for path in ("/", "/chat"):
        resp = client.get(path)
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]
        assert "chargeback_report" in resp.text  # renders the delta.ui artifact
        assert "/v1/chat/completions" in resp.text  # and posts to the streaming contract

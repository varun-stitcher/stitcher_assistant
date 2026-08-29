"""OpenAI-compatible endpoint for the stitcher pi agent, served by the agent gateway (agent_gateway/gateway.py).

`POST /v1/chat/completions` drives one headless pi turn (sharing the gateway's `AgentRunner`) and
returns an OpenAI-shaped completion. Per-call env scope arrives on the non-standard ``stitcher``
extension of the request body ``{environment_id, pipeline_name, auth_tenant}`` — mirroring the
proven ``pi_agent_coding_harness/server/sse_server.py`` contract. Streaming is supported (OpenAI
SSE chunks); the structured result the agent produced (via ``submit_result``) is forwarded on the
terminal chunk's ``delta.ui``, and live progress on the ``stitcher`` extension.

Env-scoped and refusal-safe: a request without ``stitcher.environment_id`` / ``pipeline_name`` is
refused with a clear message — never a silent simulated fallback.

Endpoints:
  POST /v1/chat/completions   OpenAI-compatible turn (stream + non-stream)
  GET  /v1/models             OpenAI model list
  GET  /health                liveness
"""

from __future__ import annotations

import asyncio
import json
import logging
import pathlib
import time
import uuid
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

from .agent_runner import AgentRunner

logger = logging.getLogger(__name__)

router = APIRouter()

_runner: AgentRunner | None = None

#: The minimal chat UI served at ``/`` — a test/demo surface for the streaming contract
#: (progress on the stitcher extension, terminal delta.ui rendered as a report artifact).
_CHAT_HTML_PATH = pathlib.Path(__file__).resolve().parent / "gateway_chat.html"
_chat_html_cache: str | None = None


def _chat_html() -> str:
    global _chat_html_cache
    if _chat_html_cache is None:
        _chat_html_cache = _CHAT_HTML_PATH.read_text()
    return _chat_html_cache


def _get_runner() -> AgentRunner:
    global _runner
    if _runner is None:
        _runner = AgentRunner()
    return _runner


_MODEL_ID = "stitcher-config-agent"


def _fold_conversation(convo: list[dict]) -> str:
    """Render prior turns as a labelled transcript prepended to the new user message (mirrors
    ``sse_server._fold_conversation``). The OpenAI path is stateless per HTTP call; the caller
    threads the full conversation in ``messages``, and pi is a single-shot prompt."""
    if not convo:
        return ""
    new_message = convo[-1].get("content", "")
    prior = convo[:-1]
    if not prior:
        return new_message
    lines = ["# Conversation so far (earlier turns — context only, do NOT re-answer these)"]
    for m in prior:
        role = "User" if m.get("role") == "user" else "Assistant"
        content = (m.get("content") or "").strip()
        if content:
            lines.append(f"{role}: {content}")
    lines.append("")
    lines.append("# The user's new message — respond to THIS:")
    lines.append(new_message)
    return "\n".join(lines)


def _extract_scope(body: dict) -> tuple[dict, str]:
    """Pull the per-call env scope from the ``stitcher`` extension. Returns (scope, error)."""
    st: dict = {}
    st_raw = body.get("stitcher")
    if isinstance(st_raw, dict):
        st = st_raw
    environment_id = (st.get("environment_id") or "").strip()
    pipeline_name = (st.get("pipeline_name") or "").strip()
    auth_tenant = (st.get("auth_tenant") or "").strip()
    if not environment_id or not pipeline_name:
        return {}, (
            "stitcher extension must include environment_id and pipeline_name — "
            "{'stitcher': {'environment_id': ..., 'pipeline_name': ...}}"
        )
    return {"environment_id": environment_id, "pipeline_name": pipeline_name, "auth_tenant": auth_tenant}, ""


def _format_progress(ev: dict) -> str:
    """One line of human-readable progress from a runner transcript event."""
    stage = str(ev.get("stage") or "working")
    message = str(ev.get("message") or "").replace("\n", " ")[:160]
    turn = ev.get("turn")
    prefix = f"[{turn}] " if isinstance(turn, int) and turn else ""
    return f"{prefix}{stage}: {message}".strip()


def _terminal_ui(result: dict[str, Any] | None) -> dict[str, Any] | None:
    """Map the agent's submitted result (the ``submit_result`` capture) to the terminal chunk's
    ``delta.ui``. Chargeback reports get the typed ``kind: "chargeback_report"`` shape the SPC
    transport/plugin can render; any other structured result passes through verbatim."""
    if not isinstance(result, dict) or not result:
        return None
    if str(result.get("task") or "") == "chargeback_report":
        ui: dict[str, Any] = {"kind": "chargeback_report"}
        for k in ("period", "markdown", "destination", "status", "question", "error"):
            if result.get(k):
                ui[k] = result[k]
        return ui
    return result


def _openai_chunk(delta: dict, model: str, finish: str | None = None, stitcher: dict | None = None) -> str:
    chunk: dict[str, Any] = {
        "id": f"chatcmpl-{uuid.uuid4().hex[:24]}",
        "created": int(time.time()),
        "model": model,
        "object": "chat.completion.chunk",
        "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
    }
    if stitcher is not None:
        chunk["stitcher"] = stitcher
    return f"data: {json.dumps(chunk)}\n\n"


@router.get("/health")
async def health() -> dict:
    return {"ok": True, "model": _MODEL_ID}


@router.get("/")
@router.get("/chat")
async def chat_ui() -> Response:
    """The minimal chat UI (gateway_chat.html) — test/demo surface for this agent's streaming."""
    return Response(content=_chat_html(), media_type="text/html")


@router.get("/v1/models")
async def models() -> dict:
    return {"object": "list", "data": [{"id": _MODEL_ID, "object": "model", "owned_by": "stitcher"}]}


@router.post("/v1/chat/completions")
async def chat_completions(request: Request) -> Any:
    try:
        body: dict = await request.json()
    except Exception:  # noqa: BLE001
        return JSONResponse({"error": {"message": "bad JSON", "type": "invalid_request_error"}}, status_code=400)

    messages = body.get("messages", [])
    stream = bool(body.get("stream", False))
    model = _MODEL_ID

    convo = [
        {"role": m["role"], "content": m.get("content", "")}
        for m in messages
        if isinstance(m, dict) and m.get("role") in ("user", "assistant") and m.get("content")
    ]
    if not convo:
        return JSONResponse(
            {"error": {"message": "no user/assistant messages", "type": "invalid_request_error"}},
            status_code=400,
        )
    message = _fold_conversation(convo)

    scope, scope_err = _extract_scope(body)
    if scope_err:
        return JSONResponse({"error": {"message": scope_err, "type": "invalid_request_error"}}, status_code=400)

    if not stream:
        runner = _get_runner()
        res = await asyncio.to_thread(runner.run, message, **scope)
        msg: dict[str, Any] = {"role": "assistant", "content": res.text}
        ui = _terminal_ui(res.result)
        if ui:
            msg["ui"] = ui
        return {
            "id": f"chatcmpl-{uuid.uuid4().hex[:24]}",
            "created": int(time.time()),
            "model": model,
            "object": "chat.completion",
            "choices": [{"index": 0, "message": msg, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
        }

    # Streaming — SSE chunks: start marker, LIVE progress on the `stitcher` extension (one chunk
    # per tool call the agent makes, streamed as it happens — the runner's transcript tail feeds
    # on_event), then the terminal answer + structured ui (kind: "chargeback_report" for report
    # tasks) and [DONE].
    async def _stream():
        runner = _get_runner()
        loop = asyncio.get_running_loop()
        events: asyncio.Queue[dict] = asyncio.Queue()

        def _on_event(ev: dict) -> None:
            loop.call_soon_threadsafe(events.put_nowait, ev)

        yield _openai_chunk({"role": "assistant"}, model, stitcher={"kind": "status", "msg": "Thinking…"})

        task = asyncio.create_task(asyncio.to_thread(runner.run, message, on_event=_on_event, **scope))
        while not task.done():
            try:
                ev = await asyncio.wait_for(events.get(), timeout=0.25)
            except TimeoutError:
                continue
            yield _openai_chunk({}, model, stitcher={"kind": "tool", "msg": _format_progress(ev)})
        res = await task
        # Final drain — events queued right before the turn finished must still stream.
        while not events.empty():
            yield _openai_chunk({}, model, stitcher={"kind": "tool", "msg": _format_progress(events.get_nowait())})

        ui = _terminal_ui(res.result)
        # Contract (mirrors config-gen sse_server): the human text is one delta.content chunk, and
        # the terminal chunk carries **delta.ui** (finish_reason=stop) — the SPC transport reads
        # delta.ui / message.ui, NOT the stitcher extension.
        delta: dict[str, Any] = {"content": res.text}
        if ui:
            delta["ui"] = ui
        yield _openai_chunk(delta, model, finish="stop")
        yield "data: [DONE]\n\n"

    return StreamingResponse(_stream(), media_type="text/event-stream")

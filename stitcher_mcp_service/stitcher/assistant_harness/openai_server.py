"""OpenAI-compatible endpoint for the stitcher pi agent, served by the agent gateway.

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
import time
import uuid
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, StreamingResponse

from .agent_runner import AgentRunner

logger = logging.getLogger(__name__)

router = APIRouter()

_runner: AgentRunner | None = None


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

    async def _run() -> Any:
        runner = _get_runner()
        return await asyncio.to_thread(runner.run, message, **scope)

    if not stream:
        res = await _run()
        msg: dict[str, Any] = {"role": "assistant", "content": res.text}
        if res.result:
            msg["ui"] = res.result
        return {
            "id": f"chatcmpl-{uuid.uuid4().hex[:24]}",
            "created": int(time.time()),
            "model": model,
            "object": "chat.completion",
            "choices": [{"index": 0, "message": msg, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
        }

    # Streaming — SSE chunks: a start marker, then the terminal answer + structured ui.
    async def _stream():
        yield _openai_chunk({"role": "assistant"}, model, stitcher={"kind": "status", "msg": "Thinking…"})
        res = await _run()
        delta: dict[str, Any] = {"content": res.text}
        terminal: dict[str, Any] = {"content": res.text}
        if res.result:
            terminal["ui"] = res.result
        yield _openai_chunk(delta, model)
        yield _openai_chunk({}, model, finish="stop", stitcher=terminal)
        yield "data: [DONE]\n\n"

    return StreamingResponse(_stream(), media_type="text/event-stream")

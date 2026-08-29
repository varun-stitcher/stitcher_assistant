"""Result-capture tool — the structured-output channel for the agent gateway.

The agent gateway (`gateway.py`) drives a headless pi turn per orchestrator call. The agent
orchestrates (grounding → planning → authoring → validating) and, at the end, calls
``submit_result`` with a JSON payload that becomes the **structured output** returned to the
caller (Claude Code / Claude Desktop over MCP, or an OpenAI-compatible client via ``delta.ui``).

This tool is **gated**: it is registered on the per-call tool MCP ONLY when the gateway sets
``STITCHER_ENABLE_RESULT_CAPTURE=1`` (and a per-call ``STITCHER_RESULT_CAPTURE`` file path). The
interactive ``run.sh`` agent never sets the flag, so its tool list stays pristine — no pollution
of the human-facing agent surface.

Determinism contract: the server owns the write (it parses + persists the JSON); the agent only
supplies the payload string. On a bad payload the tool returns a clear error so the agent can
retry, never a silent default. The AgentRunner reads the capture file after the turn; an absent
file is reported honestly as ``status: no_structured_output`` (the config task also cross-checks
against the filesystem-harvested saved YAML, which is the authoritative artifact).
"""

from __future__ import annotations

import json
import os

from fastmcp import FastMCP


def register(mcp: FastMCP) -> None:
    """Register ``submit_result`` on the shared FastMCP instance (gated by the gateway)."""

    @mcp.tool
    def submit_result(payload: str) -> str:
        """Submit this task's structured result as a JSON string. Call EXACTLY ONCE, at the very
        end, after every orchestration step is complete. The JSON you submit is returned to the
        caller as the task's structured output. If the payload is not valid JSON the call fails
        with a clear error — fix the JSON and call again. Do NOT submit before the work is done."""
        path = os.environ.get("STITCHER_RESULT_CAPTURE")
        if not path:
            return "ERR: result capture is not configured on this server (STITCHER_RESULT_CAPTURE unset)."
        try:
            obj = json.loads(payload)
        except json.JSONDecodeError as e:
            return f"ERR: payload is not valid JSON ({e}). Submit a single JSON object."
        if not isinstance(obj, dict):
            return "ERR: payload must be a JSON object ({{...}}), not an array or scalar."
        # Best-effort atomic write so a crashed turn never leaves a half-written file.
        tmp = f"{path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(obj, f)
        os.replace(tmp, path)
        return (
            f"captured: {len(payload)} bytes -> {path}. SUCCESS — the result is recorded. "
            "Do NOT call submit_result again. Your FINAL message is the only thing the caller "
            "sees: repeat the full user-facing deliverable there (tables, numbers, paths), then "
            "end the turn."
        )

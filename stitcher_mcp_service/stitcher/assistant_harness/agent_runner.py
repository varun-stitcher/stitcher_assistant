"""AgentRunner — drive one headless pi turn per orchestrator call (the gateway's core).

The agent gateway (`gateway.py`) exposes the stitcher pi agent as a higher-order MCP server
(Claude Code / Claude Desktop) and an OpenAI-compatible endpoint. Both surfaces share this
runner. Per call it:

  1. spawns the **existing combined tool MCP** (`mcp_server.py`) on an EPHEMERAL port, scoped to
     THIS call's `environment_id` / `pipeline_name` / `auth_tenant` (env-scoped, never a silent
     default) — and with result capture enabled (`STITCHER_ENABLE_RESULT_CAPTURE=1` +
     `STITCHER_RESULT_CAPTURE=<file>`);
  2. spawns `pi -p` (print mode) with the stitcher extension, pointed at that tool MCP, runs ONE
     turn, and captures stdout (the agent's final answer);
  3. tails the pi session transcript for tool-call progress (best-effort → `on_event`);
  4. reads the capture file the agent wrote via `submit_result` → the structured result;
  5. tears down both subprocesses.

This mirrors the proven `pi_agent_coding_harness/server/sse_server.py` pattern (subprocess per
turn, transcript tail, timeout, `BrokenPipeError` swallow) but is per-call scoped + concurrency-
safe (distinct ephemeral ports) so one gateway serves many environments.

Determinism / safety:
  * The agent only *supplies* the structured payload; the server owns the write (parses + persists
    JSON). A bad payload fails loudly so the agent can retry — never a silent default.
  * An absent capture file is reported honestly as `status: "no_structured_output"` (the config
    task additionally cross-checks the filesystem-harvested saved YAML, which is authoritative).
  * Missing `environment_id` / `pipeline_name` is refused before any subprocess is spawned.
"""

from __future__ import annotations

import json
import logging
import os
import pathlib
import shutil
import signal
import socket
import subprocess
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

# ── paths (anchored to this module so the runner is CWD-independent) ──────────────────────────
_PKG = pathlib.Path(__file__).resolve().parent  # .../assistant_harness/
_MCP_SERVICE = _PKG.parent.parent  # .../stitcher_mcp_service/
ASSISTANT_ROOT = _MCP_SERVICE.parent  # .../stitcher_assistant/
PIA_DIR = ASSISTANT_ROOT / "pi_coding_agent"  # home of the pi extension + .env.local symlinks
EXT = PIA_DIR / "pi_extension" / "index.ts"
SYSTEM_FILE = PIA_DIR / "AGENT_SYSTEM.md"
RUNS_DIR = PIA_DIR / ".output" / "gateway-runs"

DEFAULT_MODEL = os.environ.get("STITCHER_MODEL_NAME", "qwen3.6-27b-mtp")
DEFAULT_TIMEOUT = 600  # a full authoring turn (discover → plan → author → validate → save) is long


@dataclass
class AgentResult:
    """The typed outcome of one orchestrator turn."""

    status: str  # "ok" | "no_structured_output" | "timed_out" | "error" | "unscoped"
    text: str = ""  # the agent's final natural-language answer (pi stdout)
    result: dict[str, Any] = field(default_factory=dict)  # the structured payload the agent submitted
    turns: int = 0  # best-effort tool-call count from the transcript
    elapsed: float = 0.0
    error: str = ""  # non-empty on error/timeout/unscoped
    run_dir: str = ""  # the per-call run directory (session transcript + capture file live here)


def _free_port() -> int:
    """Pick an ephemeral port the OS confirms free (small race; caller retries on bind failure)."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_mcp(port: int, timeout_s: float = 40.0) -> bool:
    """Poll the tool MCP's Streamable-HTTP endpoint until it serves /mcp/.

    FastMCP redirects the trailing-slash ``/mcp/`` to ``/mcp`` with a 307, which urllib does not
    follow for POST — so treat ANY HTTP response (2xx or 3xx) as liveness proof that uvicorn is
    listening and FastMCP is routing. The real MCP SDK client follows the redirect itself, so this
    conservative probe never needs to return the initialize result."""
    import urllib.request

    deadline = time.time() + timeout_s
    body = json.dumps(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-03-26",
                "capabilities": {},
                "clientInfo": {"name": "agent-runner", "version": "0"},
            },
        }
    ).encode()
    while time.time() < deadline:
        try:
            req = urllib.request.Request(
                f"http://127.0.0.1:{port}/mcp/",
                data=body,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=2) as resp:  # noqa: S310 — localhost probe
                if resp.status in (200, 202, 204, 307, 308):
                    return True
        except urllib.error.HTTPError as e:
            # 3xx redirects (307) surface here when the HTTPRedirectHandler refuses the POST;
            # any 3xx still proves the server is up and routing — treat it as live.
            if e.code in (200, 202, 204, 307, 308):
                return True
        except Exception:  # noqa: BLE001 — not up yet
            pass
        time.sleep(0.5)
    return False


def _base_env() -> dict[str, str]:
    """The STITCHER_* infra vars the tool MCP + pi both need (creds/api/ssl), inherited from the
    gateway process env. Per-call scope is layered on top in `run()`."""
    env = dict(os.environ)
    # Ensure the stitcher_mcp_service package is importable as `stitcher.assistant_harness`.
    py_path = str(_MCP_SERVICE)
    env["PYTHONPATH"] = py_path + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    # `pi` + node live under homebrew on macOS; make sure the pi subprocess can find them.
    if "/opt/homebrew/bin" not in env.get("PATH", ""):
        env["PATH"] = "/opt/homebrew/bin:" + env.get("PATH", "")
    return env


class AgentRunner:
    """Spawn a per-call scoped tool MCP + a headless `pi -p` turn, return the structured result.

    One instance per gateway process; `run()` is safe to call concurrently (each call gets its own
    ephemeral port + run directory + subprocess pair)."""

    def __init__(self, py: str | None = None) -> None:
        # The python that has `fastmcp` + the stitcher siblings (the stitcher_mcp_service venv).
        self.py = py or str(_MCP_SERVICE / ".venv" / "bin" / "python")
        if not pathlib.Path(self.py).exists():
            raise RuntimeError(
                f"python with fastmcp not found at {self.py} — set STITCHER_PY / run `uv sync` in "
                "stitcher_mcp_service"
            )

    def run(
        self,
        prompt: str,
        *,
        environment_id: str,
        pipeline_name: str,
        auth_tenant: str = "",
        model: str = "",
        timeout: int = DEFAULT_TIMEOUT,
        on_event: Callable[[dict], None] | None = None,
    ) -> AgentResult:
        """Run one headless pi turn scoped to the given environment. Returns an `AgentResult`.

        `on_event` (if supplied) receives best-effort progress dicts `{stage, message, tool, turn}`
        as the agent works — the MCP surface forwards these to `ctx.report_progress`."""
        if not environment_id or not pipeline_name:
            return AgentResult(
                status="unscoped",
                error="environment_id and pipeline_name are required (config generation is environment-scoped).",
            )
        if shutil.which("pi") is None:
            return AgentResult(status="error", error="`pi` CLI not on PATH (npm i -g @earendil-works/pi-coding-agent).")

        t0 = time.time()
        stamp = time.strftime("%Y%m%d-%H%M%S")
        run_dir = RUNS_DIR / f"{stamp}-{os.getpid()}-{_free_port()}"
        run_dir.mkdir(parents=True, exist_ok=True)
        capture_file = run_dir / "result.json"
        session_dir = run_dir / "_session"
        session_dir.mkdir(parents=True, exist_ok=True)
        sid = f"gateway-{stamp}"
        out_f = run_dir / "_pi_stdout.txt"
        err_f = run_dir / "_pi_stderr.txt"

        # ── 1. spawn the per-call scoped tool MCP on an ephemeral port ────────────────────
        mcp_port = _free_port()
        mcp_env = _base_env()
        mcp_env.update(
            {
                "STITCHER_ENVIRONMENT_ID": environment_id,
                "STITCHER_PIPELINE_NAME": pipeline_name,
                "STITCHER_ENABLE_RESULT_CAPTURE": "1",
                "STITCHER_RESULT_CAPTURE": str(capture_file),
            }
        )
        if auth_tenant:
            mcp_env["STITCHER_AUTH_TENANT"] = auth_tenant
        mcp_log = open(run_dir / "_mcp.log", "w")
        mcp_proc = subprocess.Popen(
            [self.py, "-m", "stitcher.assistant_harness.mcp_server", "--http", str(mcp_port)],
            cwd=str(PIA_DIR),
            env=mcp_env,
            stdout=mcp_log,
            stderr=subprocess.STDOUT,
        )
        try:
            if not _wait_mcp(mcp_port):
                return AgentResult(
                    status="error",
                    error=f"per-call tool MCP did not come up on :{mcp_port} (see {run_dir / '_mcp.log'}).",
                    elapsed=time.time() - t0,
                    run_dir=str(run_dir),
                )

            # ── 2. spawn `pi -p` pointed at the per-call tool MCP ─────────────────────────
            pi_env = _base_env()
            pi_env.update(
                {
                    "STITCHER_ENVIRONMENT_ID": environment_id,
                    "STITCHER_PIPELINE_NAME": pipeline_name,
                    "STITCHER_MCP_URL": f"http://127.0.0.1:{mcp_port}/mcp/",
                    "STITCHER_SUB_MCP_URLS": json.dumps(
                        {
                            "custom_cost": f"http://127.0.0.1:{mcp_port}/sub_mcp_agents/custom_cost/mcp/",
                            "config_generation": f"http://127.0.0.1:{mcp_port}/sub_mcp_agents/config_generation/mcp/",
                            "chargeback": f"http://127.0.0.1:{mcp_port}/sub_mcp_agents/chargeback/mcp/",
                        }
                    ),
                }
            )
            if auth_tenant:
                pi_env["STITCHER_AUTH_TENANT"] = auth_tenant
            system_prompt = SYSTEM_FILE.read_text() if SYSTEM_FILE.exists() else ""
            cmd = [
                "pi",
                "-p",
                "--model",
                f"stitcher/{model or DEFAULT_MODEL}",
                "-e",
                str(EXT),
                "-nbt",
                "--session-id",
                sid,
                "--session-dir",
                str(session_dir),
            ]
            if system_prompt:
                cmd += ["--system-prompt", system_prompt]
            cmd += [prompt]

            with open(out_f, "w") as of, open(err_f, "w") as ef:
                proc = subprocess.Popen(cmd, cwd=str(PIA_DIR), env=pi_env, stdout=of, stderr=ef, text=True)

            # ── 3. tail the session transcript for progress while pi runs ──────────────────
            timed_out = False
            transcript_file = session_dir / f"{sid}.jsonl"

            def _tail() -> int:
                """Best-effort: count tool calls + forward progress events. Returns tool-call count."""
                turns = 0
                pos = 0
                last_emit = 0.0
                while proc.poll() is None:
                    if transcript_file.exists():
                        try:
                            with open(transcript_file, "rb") as tf:
                                tf.seek(pos)
                                chunk = tf.read()
                                pos += len(chunk)
                            for line in chunk.splitlines():
                                if not line.strip():
                                    continue
                                try:
                                    ev = json.loads(line)
                                except Exception:  # noqa: BLE001
                                    continue
                                etype = ev.get("type")
                                if etype == "assistant":
                                    # a toolCall content block = one orchestration step
                                    content = ev.get("content") or []
                                    if isinstance(content, list):
                                        for b in content:
                                            if isinstance(b, dict) and b.get("type") == "toolCall":
                                                turns += 1
                                                if on_event and time.time() - last_emit >= 0.5:
                                                    on_event(
                                                        {
                                                            "stage": "orchestrating",
                                                            "message": f"tool: {b.get('name', '?')}",
                                                            "tool": b.get("name", ""),
                                                            "turn": turns,
                                                        }
                                                    )
                                                    last_emit = time.time()
                        except Exception:  # noqa: BLE001 — transcript may be mid-write
                            pass
                    time.sleep(0.2)
                return turns

            tail_thread = threading.Thread(target=_tail, daemon=True)
            tail_thread.start()

            try:
                proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                proc.kill()
                timed_out = True
            tail_thread.join(timeout=2)

            answer = out_f.read_text().strip()
            turns = 0  # recompute from transcript (tail thread's count is local)
            if transcript_file.exists():
                for line in transcript_file.read_text().splitlines():
                    try:
                        ev = json.loads(line)
                    except Exception:  # noqa: BLE001
                        continue
                    if ev.get("type") == "assistant" and isinstance(ev.get("content"), list):
                        turns += sum(1 for b in ev["content"] if isinstance(b, dict) and b.get("type") == "toolCall")

            # ── 4. read the capture file the agent wrote via submit_result ────────────────
            result: dict[str, Any] = {}
            status = "ok"
            error = ""
            if capture_file.exists():
                try:
                    result = json.loads(capture_file.read_text())
                    if not isinstance(result, dict):
                        result = {}
                except Exception as e:  # noqa: BLE001
                    error = f"capture file was not valid JSON: {e}"
                    status = "error"
            elif timed_out:
                status = "timed_out"
                error = f"pi turn timed out after {timeout}s."
            else:
                status = "no_structured_output"
                error = "the agent did not call submit_result (no structured output produced)."

            return AgentResult(
                status=status,
                text=answer,
                result=result,
                turns=turns,
                elapsed=round(time.time() - t0, 2),
                error=error if status != "ok" else "",
                run_dir=str(run_dir),
            )
        finally:
            # ── 5. tear down the per-call tool MCP ───────────────────────────────────────
            if mcp_proc.poll() is None:
                try:
                    mcp_proc.send_signal(signal.SIGTERM)
                    mcp_proc.wait(timeout=5)
                except Exception:  # noqa: BLE001
                    try:
                        mcp_proc.kill()
                    except Exception:  # noqa: BLE001
                        pass
            try:
                mcp_log.close()
            except Exception:  # noqa: BLE001
                pass


def harvest_saved_config(run_dir: str, stage: str) -> dict[str, Any]:
    """Cross-check the agent's submitted config against the filesystem: `save_config` is the only
    persist, so the newest YAML under `.output/enhance/<stage>/` is the authoritative artifact.
    Returns {} when nothing was saved (the caller reports that honestly)."""
    if not run_dir:
        return {}
    # save_config writes to soe.output_dir/enhance/<stage> (anchored to pi_coding_agent/.output
    # by soe_context when STITCHER_OUTPUT_DIR is unset). Harvest the newest file there.
    base = PIA_DIR / ".output" / "enhance" / stage
    if not base.exists():
        return {}
    candidates = sorted(base.glob("*.yaml"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not candidates:
        return {}
    path = candidates[0]
    return {"saved_path": str(path), "config_yaml": path.read_text()}

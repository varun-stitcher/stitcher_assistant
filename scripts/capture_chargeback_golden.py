#!/usr/bin/env python
"""Capture chargeback golden outputs (plan: plans/chargeback-agent-service.md, T0.1).

Runs the three verified flows against the LIVE simulated environment and writes the
markdown outputs as golden files under
``stitcher_mcp_service/test/golden/chargeback/``. Asserts the golden totals before
writing — a capture that does not match the expected numbers is an ERROR (no silent
golden drift).

Flows:
  1. direct_runner        — scripts/run_chargeback_direct.py 2026-07 (escape hatch, no LLM)
  2. cross_tab            — query_focus_cost(group_by="cost_center,service", period="2026-07")
  3. cost_center_report   — chargeback_by_cost_center(period="2026-07")

Usage (same posture as run_chargeback_direct.py):
    cd pi_coding_agent
    PYTHONPATH=../stitcher_mcp_service ../stitcher_mcp_service/.venv/bin/python \
        ../scripts/capture_chargeback_golden.py [--out ../stitcher_mcp_service/test/golden/chargeback]

Env: reads STITCHER_* defaults from run.local.sh in the CWD (never sources it).
"""

from __future__ import annotations

import asyncio
import os
import pathlib
import re
import subprocess
import sys

HERE = pathlib.Path(__file__).resolve().parent
RUN_LOCAL_SH = HERE.parent / "pi_coding_agent" / "run.local.sh"
DEFAULT_OUT = HERE.parent / "stitcher_mcp_service" / "test" / "golden" / "chargeback"

PERIOD = "2026-07"
GOLDEN_RECORDS = 3_883_510
GOLDEN_TOTAL_USD = "$14,475.15"


def _load_env_defaults() -> dict[str, str]:
    """Parse ``export VAR="${VAR:-default}"`` lines from run.local.sh (no sourcing)."""
    env: dict[str, str] = {}
    text = RUN_LOCAL_SH.read_text()
    for match in re.finditer(r'export\s+(STITCHER_[A-Z_]+)="\$\{[A-Z_]+:-(.*?)\}"\s*$', text, re.M):
        name, default = match.group(1), match.group(2)
        env[name] = os.environ.get(name) or default
    return env


def _required_env() -> dict[str, str]:
    env = _load_env_defaults()
    missing = [
        k
        for k in ("STITCHER_API_URL", "STITCHER_ENVIRONMENT_ID", "STITCHER_AUTH_TENANT", "STITCHER_MODEL_API_KEY")
        if not env.get(k)
    ]
    if missing:
        raise SystemExit(f"ERR: missing STITCHER_* env defaults in {RUN_LOCAL_SH}: {missing}")
    return env


def _flow_direct_runner(env: dict[str, str]) -> str:
    """Flow 1 — the deterministic escape hatch (no LLM)."""
    proc = subprocess.run(
        [sys.executable, str(HERE / "run_chargeback_direct.py"), PERIOD],
        cwd=HERE.parent / "pi_coding_agent",
        env={**os.environ, **env, "PYTHONPATH": "../stitcher_mcp_service"},
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    if proc.returncode != 0:
        raise SystemExit(f"ERR: direct runner failed ({proc.returncode}):\n{proc.stderr[-800:]}")
    return proc.stdout


def _mcp_tool(tool: str, args: dict) -> str:
    """Call a chargeback tool through a real FastMCP instance (same as the sub-MCP)."""
    from fastmcp import FastMCP

    from stitcher.assistant_harness.common.config import StitcherAssistantConfig
    from stitcher.assistant_harness.common.client import StitcherClient
    from stitcher.assistant_harness.common.oidc_auth import OIDCAuth
    from stitcher.assistant_harness.common.soe_context import build_soe_context
    from stitcher.assistant_harness.sub_mcp_agents.chargeback.tools import query_tools, report_tools

    settings = StitcherAssistantConfig()
    settings.require_scope()
    settings.export_llm_env()
    auth = OIDCAuth(settings, HERE.parent / "stitcher_mcp_service" / "stitcher" / "assistant_harness")
    client = StitcherClient(settings, auth)
    soe = build_soe_context(settings, auth, client)

    mcp = FastMCP("golden-capture")
    query_tools.register(mcp, client, soe)
    report_tools.register(mcp, client, soe)

    async def _call() -> str:
        result = await mcp.call_tool(tool, args)
        return result.content[0].text

    return asyncio.run(_call())


def _assert_golden(name: str, text: str) -> None:
    problems = []
    if f"{GOLDEN_RECORDS:,}" not in text:
        problems.append(f"records {GOLDEN_RECORDS:,} missing")
    if GOLDEN_TOTAL_USD not in text:
        problems.append(f"total {GOLDEN_TOTAL_USD} missing")
    if problems:
        raise SystemExit(f"ERR: golden mismatch in {name}: {'; '.join(problems)}.\n--- captured ---\n{text[:1200]}")


def main() -> None:
    env = _required_env()
    os.environ.update(env)  # in-process flows (FastMCP) read os.environ, not the parsed dict
    out_dir = pathlib.Path(sys.argv[sys.argv.index("--out") + 1]) if "--out" in sys.argv else DEFAULT_OUT
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"[1/3] direct runner ({PERIOD})…", flush=True)
    direct = _flow_direct_runner(env)

    print("[2/3] query_focus_cost cross-tab…", flush=True)
    cross_tab = _mcp_tool("query_focus_cost", {"group_by": "cost_center,service", "period": PERIOD, "top_n": 20})

    print("[3/3] chargeback_by_cost_center…", flush=True)
    report = _mcp_tool("chargeback_by_cost_center", {"period": PERIOD})

    stamp = f"<!-- golden capture · period {PERIOD} · env {env['STITCHER_ENVIRONMENT_ID']} · capture_chargeback_golden.py -->"
    flows = {
        "direct_runner": direct,
        "cross_tab_cost_center_service": cross_tab,
        "chargeback_by_cost_center": report,
    }
    for name, text in flows.items():
        _assert_golden(name, text)
        (out_dir / f"{name}_{PERIOD}.md").write_text(f"{stamp}\n\n{text}")
        print(f"  wrote {out_dir / (name + '_' + PERIOD + '.md')} ({len(text)} chars) — golden match ✔")

    print("\nT0.1 DONE: 3 goldens captured and totals verified.")


if __name__ == "__main__":
    main()

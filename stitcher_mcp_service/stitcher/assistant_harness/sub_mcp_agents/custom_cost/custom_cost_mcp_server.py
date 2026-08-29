"""custom_cost sub-MCP server — the FOCUS normalization + validation tools.

A **sub-MCP**: a standalone FastMCP server hosting a bundle of heavy,
domain-specific tools that the pi agent does NOT broadcast by default. The pi
extension discovers this server's tools and registers them as **inactive**
(they sit in ``pi.getAllTools()`` but are absent from the active set). The
agent activates them on demand by calling the top-level ``activate_sub_mcp``
loader tool, which calls ``pi.setActiveTools([...active, ...sub])`` — pi's
native Dynamic Tool Loading. This keeps the top-level tool list pristine.

Why a separate server (not just more tools on the top-level server):
  * the top-level ``listTools()`` only returns the lightweight coordinator
    tools, so the agent's *initial* system prompt stays small;
  * the heavy FOCUS pipeline (LLM extraction, plan generation, validation)
    is isolated in its own process — a crash or slow LLM call there can never
    wedge the coordinator tools (auth, stitcher API, file);
  * it is environment-agnostic: this server does NOT instantiate
    ``StitcherAssistantConfig`` / ``OIDCAuth``. custom-cost is pure LLM normalization
    and is not bound to a Stitcher environment, so it must not require
    ``STITCHER_ENVIRONMENT_ID`` / ``STITCHER_PIPELINE_NAME`` to start.

Heavy determinism lives in ``stitcher_pipeline_common`` (an editable dep);
only the tool orchestration + serialization is ours, so pi stays a thin caller.
Two of the tools are **harness-native** rather than re-exports:

  * ``generate_focus_plans`` (plan_generation_tools) — the plan generator is
    implemented here: one structured LLM mapping call + deterministic
    grounding/guards + deterministic mapping→config translation.
  * ``conversion_tools`` — the deterministic conversion steps (provider
    detection, apply_conversion_plans, simulate_normalize_config, …) as thin
    wrappers over the engine's public surface.

Run standalone (stdio):  python -m stitcher.assistant_harness.sub_mcp_agents.custom_cost.custom_cost_mcp_server
Run over HTTP (run.sh):   ... --http 8792
"""

from __future__ import annotations

import argparse

from fastmcp import FastMCP

from ...common.config import StitcherAssistantConfig
from ...tools import focus_official_validation_tools
from .tools.conversion import conversion_tools
from .tools.extraction import extract_tools
from .tools.focus import focus_normalization_tools, focus_validation_tools
from .tools.plan import plan_generation_tools

# Server name advertised over MCP. The pi extension keys sub-MCP activation off
# the ``STITCHER_SUB_MCP_URLS`` registry (name -> URL), not off this name, but
# keeping it namespaced makes MCP client logs readable.
SERVER_NAME = "custom_cost"


def build_server() -> FastMCP:
    # Reuse the Stitcher LLM gateway for the FOCUS tools' LLM calls instead of
    # failing on a missing external LLM key — mirrors the top-level server.
    # Only default it on; an explicit USE_STITCHER_MODEL=false is still honored.
    StitcherAssistantConfig().export_llm_env()

    mcp = FastMCP(name=f"stitcher-pi-tools/{SERVER_NAME}")
    conversion_tools.register(mcp)
    extract_tools.register(mcp)
    focus_normalization_tools.register(mcp)
    focus_validation_tools.register(mcp)
    plan_generation_tools.register(mcp)
    # Official FinOps focus_validator — same module the top-level MCP serves, also
    # registered here so the custom_cost bundle is self-contained (an agent that
    # activated only this sub-MCP still gets the standalone official-validation tool).
    focus_official_validation_tools.register(mcp)
    return mcp


def main() -> None:
    ap = argparse.ArgumentParser(description="stitcher-pi custom_cost sub-MCP server")
    ap.add_argument(
        "--http",
        type=int,
        nargs="?",
        const=8792,
        default=None,
        help="serve over Streamable HTTP on the given port (default 8792)",
    )
    ap.add_argument("--host", default="127.0.0.1", help="HTTP bind host (default 127.0.0.1)")
    args = ap.parse_args()

    mcp = build_server()
    if args.http is None:
        mcp.run(transport="stdio")
    else:
        mcp.run(transport="http", host=args.host, port=args.http)


if __name__ == "__main__":
    main()

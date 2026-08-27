"""FastMCP tool server for the stitcher-pi wrapper — thin coordinator.

One process, one port. Builds a single FastAPI app that mounts the **top-level**
coordinator MCP plus every **sub-MCP** as an ASGI sub-app:

    /mcp                                             top-level coordinator (common tools)
    /sub_mcp_agents/custom_cost/mcp                  custom_cost sub-MCP
    /sub_mcp_agents/config_generation/mcp            config_generation sub-MCP
    /sub_mcp_agents/chargeback/mcp                   chargeback sub-MCP

Tools live in dedicated modules, each of which registers onto the shared FastMCP
instance here (``build_server``):

    file_tools                 — ping / now_utc / system_info / list_directory / read_text_file
    stitcher_tools             — stitcher_context / list_connections / get_connection / get_pipeline
    auth_tools                 — auth_get_url / auth_environments / auth_status / auth_set_token
    data_source_tools          — list_data_sources / get_data_source_metadata / scan_data
    committed_config_tools     — get_committed_config / derived_columns

The top-level stays small/pristine: heavy, domain-specific tools (e.g. the FOCUS
normalization + validation pipeline, enhance config authoring) live in the
**sub-MCP servers** under ``sub_mcp_agents/``, served on the same port as mounted
apps. Each sub-app's tools are held *inactive* and activated on demand via
``activate_sub_mcp`` (pi's native Dynamic Tool Loading) so the agent's initial
tool list stays minimal.

Each ``mcp.http_app()`` is a Starlette app whose StreamableHTTPSessionManager is
started in its **lifespan**. Starlette does not run the lifespans of mounted
sub-apps automatically, so the parent app runs a combined lifespan (an
``AsyncExitStack``) that enters every sub-app's lifespan — otherwise requests
fail with "StreamableHTTPSessionManager task group was not initialized".

The pi extension discovers the sub-MCPs via ``STITCHER_SUB_MCP_URLS`` (name ->
URL), where each URL is ``http://<host>:<port>/sub_mcp_agents/<name>/mcp/``.

Run (the combined server, what run.sh uses):   python mcp_server.py --http 8791
"""

from __future__ import annotations

import argparse
import os
import pathlib
from contextlib import AsyncExitStack, asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastmcp import FastMCP

from .common.client import StitcherClient
from .common.config import StitcherAssistantConfig
from .common.oidc_auth import OIDCAuth
from .common.soe_context import build_soe_context
from .sub_mcp_agents.chargeback import chargeback_mcp_server
from .sub_mcp_agents.config_generation import config_generation_mcp_server
from .sub_mcp_agents.custom_cost import custom_cost_mcp_server
from .tools import (
    auth_tools,
    committed_config_tools,
    data_source_tools,
    file_tools,
    result_capture,
    stitcher_tools,
)


def build_server() -> FastMCP:
    """Build the top-level coordinator MCP (the common tool surface)."""
    settings = StitcherAssistantConfig()
    settings.require_scope()  # refuses to start without STITCHER_* scope (env-scoped coordinator)
    # Reuse the Stitcher LLM gateway for the custom-cost / FOCUS tools' LLM calls instead of failing
    # on a missing external LLM key — ``export_llm_env`` sets USE_STITCHER_MODEL=true for the external
    # SPC LLM config (honoring an explicit USE_STITCHER_MODEL=false).
    settings.export_llm_env()

    auth = OIDCAuth(settings, pathlib.Path(__file__).resolve().parent)
    client = StitcherClient(settings, auth)
    soe = build_soe_context(settings, auth, client)
    mcp = FastMCP(name="stitcher-pi-tools")
    file_tools.register(mcp)
    stitcher_tools.register(mcp, client, settings)
    auth_tools.register(mcp, auth)
    # Common grounding tools (live on the top-level MCP, not a sub-MCP): the live SWS
    # datasource catalog + SOE metadata/scan, and the committed git-config summary +
    # derived columns. Both exercise SOE functions as-is via `soe`.
    data_source_tools.register(mcp, client, soe)
    committed_config_tools.register(mcp, client, soe)
    # The agent gateway (gateway.py) drives a headless pi turn per orchestrator call and the
    # agent submits the structured result through this tool. Gated so the interactive run.sh
    # agent (flag unset) never sees `submit_result` in its tool list.
    if os.environ.get("STITCHER_ENABLE_RESULT_CAPTURE") == "1":
        result_capture.register(mcp)
    # NOTE: heavy domain tools (focus_normalization_tools, focus_validation_tools,
    # conversion_tools, plan_generation_tools) are NOT registered here — they live
    # in the custom_cost sub-MCP server's own tools/ package
    # (sub_mcp_agents/custom_cost/tools/) and are activated on demand via
    # `activate_sub_mcp`. Keeping this surface small keeps the
    # agent's initial system prompt / tool list pristine.
    return mcp


def build_app() -> FastAPI:
    """Build the single FastAPI app with the top-level MCP + every sub-MCP mounted as a sub-app."""
    top = build_server()
    custom_cost = custom_cost_mcp_server.build_server()
    config_gen = config_generation_mcp_server.build_server()
    chargeback = chargeback_mcp_server.build_server()

    top_app = top.http_app(path="/mcp")
    custom_cost_app = custom_cost.http_app(path="/mcp")
    config_gen_app = config_gen.http_app(path="/mcp")
    chargeback_app = chargeback.http_app(path="/mcp")
    sub_apps = (custom_cost_app, config_gen_app, chargeback_app, top_app)

    @asynccontextmanager
    async def lifespan(app: FastAPI):  # noqa: ARG001
        # Start every mounted MCP app's lifespan so their StreamableHTTPSessionManager task
        # groups are initialized (Starlette does not run mounted sub-app lifespans itself).
        async with AsyncExitStack() as stack:
            for sub in sub_apps:
                await stack.enter_async_context(sub.lifespan(sub))
            yield

    app = FastAPI(title="stitcher-pi-tools", lifespan=lifespan)
    # Mount the sub-MCPs first so their specific prefixes match before the root mount.
    app.mount(f"/sub_mcp_agents/{custom_cost_mcp_server.SERVER_NAME}", custom_cost_app)
    app.mount(f"/sub_mcp_agents/{config_generation_mcp_server.SERVER_NAME}", config_gen_app)
    app.mount(f"/sub_mcp_agents/{chargeback_mcp_server.SERVER_NAME}", chargeback_app)
    # Top-level coordinator exposed at /mcp (its internal route is /mcp; mounted at root).
    app.mount("/", top_app)
    return app


def main() -> None:
    ap = argparse.ArgumentParser(description="stitcher-pi-tools FastMCP server (top-level + mounted sub-MCPs)")
    ap.add_argument(
        "--http",
        type=int,
        nargs="?",
        const=8791,
        default=None,
        help="serve the combined MCP app over HTTP on the given port (default 8791)",
    )
    ap.add_argument("--host", default="127.0.0.1", help="HTTP bind host (default 127.0.0.1)")
    args = ap.parse_args()
    if args.http is None:
        raise SystemExit("mcp_server requires --http PORT")
    uvicorn.run(build_app(), host=args.host, port=args.http, log_level="info")


if __name__ == "__main__":
    main()

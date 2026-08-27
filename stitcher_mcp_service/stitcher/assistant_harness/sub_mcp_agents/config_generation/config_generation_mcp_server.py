"""config_generation sub-MCP server — enhance/enrich config generation tools.

A **sub-MCP** (mirrors ``custom_cost``): a standalone FastMCP server hosting a bundle of heavy,
domain-specific tools the pi agent does NOT broadcast by default. The pi extension discovers this
server's tools and registers them as **inactive**; the agent activates them on demand via the
top-level ``activate_sub_mcp("config_generation")`` loader tool (pi's native Dynamic Tool Loading),
keeping the top-level tool list pristine.

Unlike ``custom_cost`` (env-agnostic), this server is **environment-scoped**: config generation
always operates on a customer environment, so ``build_server()`` instantiates
``StitcherSettings`` / ``OIDCAuth`` / ``StitcherClient`` (same as the top-level ``mcp_server.py``)
and refuses to start without ``STITCHER_ENVIRONMENT_ID`` / ``STITCHER_PIPELINE_NAME``.

The flow (per the plan): understand the user requirement → figure out the best operations → call
tools to list data sources, inspect their metadata (columns) via the SOE metadata operator, scan/read
real data by connection parameters, read the prior checked-in git configs via SOE
``get_vsc_commit_dir`` → author / validate / save the enhance config. All grounding determinism is
reused from SPC + SOE **as-is** (no vendored copies); pi stays a thin caller.

Tool modules (each ``register(mcp, client, soe)``):
  - operator_tools        — list_operators / describe_operator / environment_context
  - planning_tools        — plan_enhance_operations (LLM-assisted)
  - authoring_tools       — generate_lookup / generate_filter / validate_config / save_config

Note: the datasource/metadata/scan tools (data_source_tools) and the committed-config tools
(committed_config_tools) now live on the TOP-LEVEL MCP (``assistant_harness/tools/``) and are
always available — they are NOT part of this sub-MCP any more.

Run standalone (stdio):  python -m stitcher.assistant_harness.sub_mcp_agents.config_generation.config_generation_mcp_server
Run over HTTP (run.sh):   ... --http 8793
"""

from __future__ import annotations

import argparse
import os
import pathlib

from fastmcp import FastMCP

from ...common.client import StitcherClient
from ...common.config import StitcherSettings
from ...common.oidc_auth import OIDCAuth
from ...common.soe_context import build_soe_context
from .tools import (
    authoring_tools,
    operator_tools,
    planning_tools,
)

# Server name advertised over MCP. The pi extension keys sub-MCP activation off the
# ``STITCHER_SUB_MCP_URLS`` registry (name -> URL), not off this name, but keeping it
# namespaced makes MCP client logs readable.
SERVER_NAME = "config_generation"


def build_server() -> FastMCP:
    # Reuse the Stitcher LLM gateway for the planning tool's LLM call instead of failing on a
    # missing external LLM key — mirrors the top-level + custom_cost servers. Only default it on;
    # an explicit USE_STITCHER_MODEL=false is still honored.
    if os.environ.get("STITCHER_MODEL_API_KEY") and "USE_STITCHER_MODEL" not in os.environ:
        os.environ["USE_STITCHER_MODEL"] = "true"

    settings = StitcherSettings()  # refuses to start without STITCHER_* scope (env-scoped, like top-level)
    # Share the top-level assistant_harness/ state dir so the OIDC token the agent mints via the
    # top-level `auth_get_url` is reused here (one login, both servers).
    state_dir = pathlib.Path(__file__).resolve().parents[2]  # .../assistant_harness/
    auth = OIDCAuth(settings, state_dir)
    client = StitcherClient(settings, auth)
    soe = build_soe_context(settings, auth, client)

    mcp = FastMCP(name=f"stitcher-pi-tools/{SERVER_NAME}")
    operator_tools.register(mcp, client, soe)
    planning_tools.register(mcp, client, soe)
    authoring_tools.register(mcp, client, soe)
    return mcp


def main() -> None:
    ap = argparse.ArgumentParser(description="stitcher-pi config_generation sub-MCP server")
    ap.add_argument(
        "--http",
        type=int,
        nargs="?",
        const=8793,
        default=None,
        help="serve over Streamable HTTP on the given port (default 8793)",
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

"""chargeback sub-MCP server — cost-query + chargeback report/invoice tools.

A **sub-MCP** (mirrors ``config_generation`` / ``custom_cost``): a standalone FastMCP server
hosting the chargeback / cost-query tool bundle. The pi extension discovers this server's
tools and registers them as **inactive**; the agent activates them on demand via the top-level
``activate_sub_mcp("chargeback")`` loader (pi's native Dynamic Tool Loading).

**Environment-scoped** (like ``config_generation``): chargeback always operates on a customer
environment's REAL cost datasource. ``build_server()`` instantiates
``StitcherAssistantConfig`` / ``OIDCAuth`` / ``StitcherClient`` + ``build_soe_context`` and
refuses to start without ``STITCHER_ENVIRONMENT_ID`` / ``STITCHER_PIPELINE_NAME``. Cost data is
read via the SOE **extract tools** **as-is** (``ExtractRefDataSubOperator`` +
``MetadataConsolidateOperator`` — the same machinery ``scan_data`` uses) and aggregated in
polars — NOT the SWS gateway (whose standalone ``focus_query`` method is a stub outside SWS).
Launch from ``pi_coding_agent/`` (where ``.env.local`` / ``.env.local.dev`` are symlinked) so
``ExecutorConfig()`` resolves at import, exactly as the config-gen sub-MCP does.

Run standalone (stdio):  python -m stitcher.assistant_harness.sub_mcp_agents.chargeback.chargeback_mcp_server
Run over HTTP (run.sh):   ... --http 8794
"""

from __future__ import annotations

import argparse

from fastmcp import FastMCP

from ...common.client import StitcherClient
from ...common.config import StitcherAssistantConfig
from ...common.oidc_auth import OIDCAuth
from ...common.soe_context import build_soe_context
from .tools import (
    cost_reader,
    invoice_tools,
    query_tools,
    report_tools,
    schema_tools,
)

SERVER_NAME = "chargeback"


def build_server() -> FastMCP:
    settings = StitcherAssistantConfig()
    settings.require_scope()  # env-scoped — refuse to start without STITCHER_* scope
    settings.export_llm_env()

    # Wire the real Stitcher-LLM org/cost-center classifier when the gateway is configured.
    # ``_llm_classify_org_cost_center`` itself degrades to a clear raise if the gateway is
    # unreachable and ``classify_org_cost_center`` falls back to deterministic x_* defaults, so
    # wiring it unconditionally here is safe either way. Tests (which never call build_server)
    # keep the module default ``None`` → deterministic default path.
    if settings.use_stitcher_model and settings.model_api_key:
        cost_reader.LLM_COLUMN_CLASSIFIER = cost_reader._llm_classify_org_cost_center

    # Share the top-level assistant_harness/ state dir so the OIDC token minted via the top-level
    # `auth_get_url` is reused here (one login, both servers).
    state_dir = OIDCAuth.default_state_dir()  # ~/.stitcher/ (user-level, shared login)
    auth = OIDCAuth(settings, state_dir)
    client = StitcherClient(settings, auth)
    soe = build_soe_context(settings, auth, client)

    mcp = FastMCP(name=f"stitcher-pi-tools/{SERVER_NAME}")
    schema_tools.register(mcp, client, soe)
    report_tools.register(mcp, client, soe)
    invoice_tools.register(mcp, client, soe)
    query_tools.register(mcp, client, soe)
    return mcp


def main() -> None:
    ap = argparse.ArgumentParser(description="stitcher-pi chargeback sub-MCP server")
    ap.add_argument(
        "--http",
        type=int,
        nargs="?",
        const=8794,
        default=None,
        help="serve over Streamable HTTP on the given port (default 8794)",
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

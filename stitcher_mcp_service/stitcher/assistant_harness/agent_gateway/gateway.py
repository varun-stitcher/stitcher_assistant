"""The stitcher pi agent gateway — one process exposing TWO surfaces:

  * **higher-order MCP server** (the agent as an orchestrator) on ``STITCHER_MCP_PORT`` (default
    8792) — Claude Code / Claude Desktop register this and call the task-typed tools
    (``generate_enhance_config`` / ``normalize_invoice_to_focus`` / ``explore_environment``).
  * **OpenAI-compatible endpoint** on ``STITCHER_OPENAI_PORT`` (default 8880) —
    ``POST /v1/chat/completions`` (+ ``/v1/models``, ``/health``).

Both share the gateway's single ``AgentRunner`` (per-call headless pi turns, per-call scoped tool
MCP). The two servers run on one event loop (``asyncio.gather`` of two ``uvicorn.Server``); each
orchestrator call spawns its own ephemeral-port tool MCP + pi subprocess, so the gateway is
concurrency-safe and per-call env-scoped.

Run:
    python -m stitcher.assistant_harness.agent_gateway.gateway --mcp-port 8792 --openai-port 8880
"""

from __future__ import annotations

import argparse
import asyncio
import logging

import uvicorn

from . import agent_mcp_server, openai_server

logger = logging.getLogger(__name__)


def build_agent_mcp_app():
    """The higher-order MCP app (Streamable HTTP at /mcp) with the task-typed orchestrator tools."""
    mcp = agent_mcp_server.build_server()
    return mcp.http_app(path="/mcp")


def build_openai_app():
    """The OpenAI-compatible FastAPI app (router from openai_server)."""
    from fastapi import FastAPI

    app = FastAPI(title="stitcher-pi-agent-openai")
    app.include_router(openai_server.router)
    return app


async def _serve(app, port: int, host: str) -> None:
    config = uvicorn.Config(app, host=host, port=port, log_level="warning")
    server = uvicorn.Server(config)
    await server.serve()


async def _main(mcp_port: int, openai_port: int, host: str) -> None:
    mcp_app = build_agent_mcp_app()
    oai_app = build_openai_app()
    await asyncio.gather(_serve(mcp_app, mcp_port, host), _serve(oai_app, openai_port, host))


def main() -> None:
    import os

    logging.basicConfig(level=os.environ.get("STITCHER_GATEWAY_LOG", "INFO"))
    ap = argparse.ArgumentParser(description="stitcher pi agent gateway (MCP + OpenAI)")
    ap.add_argument("--mcp-port", type=int, default=int(os.environ.get("STITCHER_MCP_PORT", "8792")))
    ap.add_argument("--openai-port", type=int, default=int(os.environ.get("STITCHER_OPENAI_PORT", "8880")))
    ap.add_argument("--host", default="127.0.0.1", help="bind host (default 127.0.0.1 — local auth)")
    args = ap.parse_args()
    asyncio.run(_main(args.mcp_port, args.openai_port, args.host))


if __name__ == "__main__":
    main()

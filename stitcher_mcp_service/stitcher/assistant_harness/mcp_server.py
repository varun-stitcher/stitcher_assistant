"""FastMCP tool server for the stitcher-pi wrapper — thin coordinator.

Only wires pieces together. Tools live in dedicated modules, each of which
registers onto the shared FastMCP instance here:

    file_tools     — ping / now_utc / system_info / list_directory / read_text_file
    stitcher_tools — stitcher_context / list_connections / get_connection / get_pipeline
    auth_tools     — auth_get_url / auth_environments / auth_status / auth_set_token

State lives in the ``OIDCAuth`` and ``StitcherClient`` instances owned here — no
module globals. Add a tool by writing it in the right module (or new module) and
calling its ``register(mcp, ...)``; the pi extension discovers it automatically.

Run as stdio:                    python mcp_server.py
Run as Streamable HTTP (run.sh): python mcp_server.py --http 8791
"""
from __future__ import annotations

import argparse
import pathlib

from fastmcp import FastMCP

from .common.config import StitcherSettings
from .common.oidc_auth import OIDCAuth
from .common.client import StitcherClient
from .tools import file_tools
from .tools import stitcher_tools
from .tools import auth_tools


def build_server() -> FastMCP:
    settings = StitcherSettings()          # refuses to start without STITCHER_* scope
    auth = OIDCAuth(settings, pathlib.Path(__file__).resolve().parent)
    mcp = FastMCP(name="stitcher-pi-tools")
    file_tools.register(mcp)
    stitcher_tools.register(mcp, StitcherClient(settings, auth))
    auth_tools.register(mcp, auth)
    return mcp


def main() -> None:
    ap = argparse.ArgumentParser(description="stitcher-pi-tools FastMCP server")
    ap.add_argument("--http", type=int, nargs="?", const=8791, default=None,
                    help="serve over Streamable HTTP on the given port (default 8791)")
    ap.add_argument("--host", default="127.0.0.1", help="HTTP bind host (default 127.0.0.1)")
    args = ap.parse_args()

    mcp = build_server()
    if args.http is None:
        mcp.run(transport="stdio")
    else:
        mcp.run(transport="http", host=args.host, port=args.http)


if __name__ == "__main__":
    main()

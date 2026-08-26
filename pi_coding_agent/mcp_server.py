"""Minimal FastMCP tool server for the stitcher-pi wrapper.

Thin by design: this is the ONLY place you add tools. Add an ``@mcp.tool`` function
and the pi agent picks it up automatically — the pi extension discovers every tool
this server exposes over MCP at startup and proxies it to the agent (no per-tool pi
code, no restart of the pi side).

Run as stdio (for ``pi`` that spawns it in-process):
    python mcp_server.py
Run as Streamable HTTP (for the standalone bridge — the default for run.sh):
    python mcp_server.py --http 8791

Requires ``fastmcp`` (present in the stitcher_mcp_service/.venv) + the stdlib only.
"""
from __future__ import annotations

import argparse
import datetime
import os
import pathlib
import platform

from fastmcp import FastMCP

mcp = FastMCP(name="stitcher-pi-tools")


@mcp.tool
def ping() -> str:
    """Liveness check for the tool bridge."""
    return "pong"


@mcp.tool
def now_utc() -> str:
    """Current UTC timestamp (ISO-8601)."""
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


@mcp.tool
def system_info() -> dict:
    """Non-secret host info: platform, python version, cwd, user."""
    return {
        "platform": platform.platform(),
        "python": platform.python_version(),
        "cwd": str(pathlib.Path.cwd()),
        "user": os.environ.get("USER", ""),
    }


@mcp.tool
def list_directory(path: str = ".") -> list[str]:
    """List the top-level entries of a directory (names only)."""
    try:
        return sorted(p.name for p in pathlib.Path(path).iterdir())
    except Exception as e:  # noqa: BLE001
        return [f"ERR: {e}"]


@mcp.tool
def read_text_file(path: str, max_chars: int = 4000) -> str:
    """Read a UTF-8 text file, truncated to max_chars."""
    p = pathlib.Path(path)
    if not p.is_file():
        return f"ERR: no such file: {path}"
    try:
        return p.read_text(encoding="utf-8")[:max_chars]
    except Exception as e:  # noqa: BLE001
        return f"ERR: {e}"


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="stitcher-pi-tools FastMCP server")
    ap.add_argument("--http", type=int, nargs="?", const=8791, default=None,
                    help="serve over Streamable HTTP on the given port (default 8791)")
    ap.add_argument("--host", default="127.0.0.1", help="HTTP bind host (default 127.0.0.1)")
    args = ap.parse_args()
    if args.http is None:
        mcp.run(transport="stdio")
    else:
        mcp.run(transport="http", host=args.host, port=args.http)

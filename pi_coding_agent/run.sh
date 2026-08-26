#!/usr/bin/env bash
# stitcher-pi — launch the minimal pi coding agent wired to the Stitcher LLM
# endpoint and a FastMCP tool server.
#
#   1. starts the FastMCP tool server (mcp_server.py) over Streamable HTTP
#   2. execs `pi` with the extension that registers the Stitcher provider from
#      env and proxies every FastMCP tool into the agent.
#
# Env (required):
#   STITCHER_MODEL_BASE_URL   default https://app.dev.stitcher.ai/llm/v1
#   STITCHER_MODEL_API_KEY    required
#   STITCHER_MODEL_NAME       default qwen3.6-27b-mtp
# Optional:
#   STITCHER_MCP_PORT         FastMCP HTTP port (default 8791)
#   STITCHER_PY               python with fastmcp (default ../stitcher_mcp_service/.venv/bin/python)
set -euo pipefail
cd "$(dirname "$0")"

PY="${STITCHER_PY:-../stitcher_mcp_service/.venv/bin/python}"
PORT="${STITCHER_MCP_PORT:-8791}"
MODEL="${STITCHER_MODEL_NAME:-qwen3.6-27b-mtp}"
BASE="${STITCHER_MODEL_BASE_URL:-https://app.dev.stitcher.ai/llm/v1}"

: "${STITCHER_MODEL_API_KEY:?set STITCHER_MODEL_API_KEY}"
command -v pi >/dev/null 2>&1 || { echo "!! 'pi' CLI not on PATH (npm i -g @earendil-works/pi-coding-agent)" >&2; exit 1; }
[[ -x "$PY" ]] || { echo "!! no python with fastmcp at $PY (set STITCHER_PY)" >&2; exit 1; }

export STITCHER_MODEL_BASE_URL="$BASE"
export STITCHER_MCP_URL="http://127.0.0.1:${PORT}/mcp/"

# (re)start the FastMCP tool server in the background. Misses on an existing
# server are harmless; the curl wait loop below ties progress to reachability.
"$PY" "$PWD/mcp_server.py" --http "$PORT" > /tmp/stitcher-pi-mcp.log 2>&1 &
MCP_PID=$!
trap 'kill "$MCP_PID" 2>/dev/null || true' EXIT
for _ in $(seq 1 40); do
  curl -sf -X POST "http://127.0.0.1:${PORT}/mcp/" -H 'Content-Type: application/json' \
    -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"probe","version":"0"}}}' \
    >/dev/null 2>&1 && break
  sleep 0.5
done

echo "stitcher-pi — model ${MODEL}, base ${BASE}, MCP tools via ${STITCHER_MCP_URL}"
pi --model "stitcher/${MODEL}" -e "$PWD/pi_extension/index.ts"
status=$?
kill "$MCP_PID" 2>/dev/null || true
exit "$status"

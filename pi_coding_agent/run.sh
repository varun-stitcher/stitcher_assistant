#!/usr/bin/env bash
# stitcher-pi — launch the minimal pi coding agent wired to the Stitcher LLM
# endpoint and a FastMCP tool server.
#
#   1. starts the FastMCP tool server (mcp_server.py) over Streamable HTTP
#   2. execs `pi` with the extension that registers the Stitcher provider from
#      env and proxies every FastMCP tool into the agent.
#
# Env (REQUIRED — the agent never starts without knowing its Stitcher scope/creds):
#   STITCHER_MODEL_BASE_URL   LiteLLM / OpenAI-compatible base URL
#   STITCHER_MODEL_API_KEY    gateway key
#   STITCHER_MODEL_NAME       model id (e.g. qwen3.6-27b-mtp)
#   STITCHER_API_URL          Stitcher web service base (e.g. https://app.dev.stitcher.ai/v1)
#   STITCHER_ENVIRONMENT_ID   the environment UUID the agent operates on
#   STITCHER_PIPELINE_NAME    the pipeline the agent is bound to
# Optional (infra only):
#   STITCHER_API_TOKEN        SWS bearer token (default: falls back to STITCHER_MODEL_API_KEY)
#   STITCHER_MCP_PORT         FastMCP HTTP port (default 8791)
#   STITCHER_PY               python with fastmcp (default ../stitcher_mcp_service/.venv/bin/python)
set -euo pipefail
cd "$(dirname "$0")"
PIA_DIR="$PWD"
REPO_ROOT="$(cd .. && pwd)"   # stitcher_assistant — home of the stitcher_mcp_service package

PY="${STITCHER_PY:-$REPO_ROOT/stitcher_mcp_service/.venv/bin/python}"
PORT="${STITCHER_MCP_PORT:-8791}"
OAUTH_PORT="${STITCHER_OAUTH_CALLBACK_PORT:-8086}"

# Free the ports the server (and its local OAuth callback) bind, so a STALE server
# from a prior run — which would serve OLD code and cause "state mismatch" — can't
# linger and steal the connections. If these fail (no lsof), that's fine.
_kill_port() { # $1 = port
  local p
  p=$(lsof -tiTCP:"$1" -sTCP:LISTEN 2>/dev/null | head -1) || true
  [ -n "$p" ] && kill "$p" 2>/dev/null || true
}
_kill_port "$PORT"
_kill_port "$OAUTH_PORT"
sleep 1

# Every STITCHER_* runtime/config variable is REQUIRED — never start without
# knowing where the agent points and whom it is scoped to. No defaults, no fallback.
: "${STITCHER_MODEL_BASE_URL:?set STITCHER_MODEL_BASE_URL (e.g. https://app.dev.stitcher.ai/llm/v1)}"
: "${STITCHER_MODEL_API_KEY:?set STITCHER_MODEL_API_KEY (gateway key)}"
: "${STITCHER_MODEL_NAME:?set STITCHER_MODEL_NAME (e.g. qwen3.6-27b-mtp)}"
: "${STITCHER_API_URL:?set STITCHER_API_URL (e.g. https://app.dev.stitcher.ai/v1)}"
: "${STITCHER_ENVIRONMENT_ID:?set STITCHER_ENVIRONMENT_ID (environment UUID)}"
: "${STITCHER_PIPELINE_NAME:?set STITCHER_PIPELINE_NAME (pipeline name)}"

command -v pi >/dev/null 2>&1 || { echo "!! 'pi' CLI not on PATH (npm i -g @earendil-works/pi-coding-agent)" >&2; exit 1; }
[[ -x "$PY" ]] || { echo "!! no python with fastmcp at $PY (set STITCHER_PY)" >&2; exit 1; }

export STITCHER_MODEL_BASE_URL STITCHER_MODEL_API_KEY STITCHER_MODEL_NAME
export STITCHER_API_URL STITCHER_ENVIRONMENT_ID STITCHER_PIPELINE_NAME
export STITCHER_MCP_URL="http://127.0.0.1:${PORT}/mcp/"

# (re)start the FastMCP tool server in the background (the stitcher.assistant_harness
# module, run from the stitcher_mcp_service dir so the local `stitcher` namespace
# shadows any installed one). Misses on an existing server are harmless; the curl
# wait loop below ties progress to reachability.
cd "$REPO_ROOT/stitcher_mcp_service"
"$PY" -m stitcher.assistant_harness.mcp_server --http "$PORT" > /tmp/stitcher-pi-mcp.log 2>&1 &
MCP_PID=$!
trap 'kill "$MCP_PID" 2>/dev/null || true' EXIT
cd "$PIA_DIR"
for _ in $(seq 1 40); do
  curl -sf -X POST "http://127.0.0.1:${PORT}/mcp/" -H 'Content-Type: application/json' \
    -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"probe","version":"0"}}}' \
    >/dev/null 2>&1 && break
  sleep 0.5
done

echo "stitcher-pi — model ${STITCHER_MODEL_NAME}, base ${STITCHER_MODEL_BASE_URL}, env ${STITCHER_ENVIRONMENT_ID}, pipeline ${STITCHER_PIPELINE_NAME}, MCP via ${STITCHER_MCP_URL}"
pi --model "stitcher/${STITCHER_MODEL_NAME}" -e "$PIA_DIR/pi_extension/index.ts"
status=$?
kill "$MCP_PID" 2>/dev/null || true
exit "$status"

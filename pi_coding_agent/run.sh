#!/usr/bin/env bash
# stitcher-pi — launch the minimal pi coding agent wired to the Stitcher LLM
# endpoint and a FastMCP tool server.
#
#   1. starts the FastMCP tool server (mcp_server.py; top-level + sub-MCPs mounted) over HTTP
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

# Free the port the server (and its local OAuth callback) binds, so a STALE server
# from a prior run — which would serve OLD code and cause "state mismatch" — can't
# linger and steal the connections. If these fail (no lsof), that's fine.
# A stuck uvicorn can ignore SIGTERM, so escalate to SIGKILL and then verify the
# port is actually free before proceeding (otherwise the new server fails to bind
# with EADDRINUSE and run.local.sh appears "not working").
_kill_port() { # $1 = port
  local p
  p=$(lsof -tiTCP:"$1" -sTCP:LISTEN 2>/dev/null | head -1) || true
  [ -z "$p" ] && return 0
  kill "$p" 2>/dev/null || true
  for _ in 1 2 3 4 5; do
    lsof -tiTCP:"$1" -sTCP:LISTEN >/dev/null 2>&1 || return 0
    sleep 0.3
  done
  # still held after SIGTERM — force it
  p=$(lsof -tiTCP:"$1" -sTCP:LISTEN 2>/dev/null | head -1) || true
  [ -n "$p" ] && kill -9 "$p" 2>/dev/null || true
  sleep 0.3
}
_kill_port "$PORT"
_kill_port "$OAUTH_PORT"

# Every STITCHER_* runtime/config variable is REQUIRED. StitcherAssistantConfig
# (stitcher_mcp_service/.../common/config.py) reads these from the environment
# (inherited from run.local.sh / the caller) or .env.local / .env.local.dev, and its
# require_scope() refuses to start without a scope. The :? guards below are a friendly
# preflight for the pi model arg + MCP server (no defaults, no silent fallback).
: "${STITCHER_MODEL_BASE_URL:?set STITCHER_MODEL_BASE_URL (e.g. https://app.dev.stitcher.ai/llm/v1)}"
: "${STITCHER_MODEL_API_KEY:?set STITCHER_MODEL_API_KEY (gateway key)}"
: "${STITCHER_MODEL_NAME:?set STITCHER_MODEL_NAME (e.g. qwen3.6-27b-mtp)}"
: "${STITCHER_API_URL:?set STITCHER_API_URL (e.g. https://app.dev.stitcher.ai/v1)}"
: "${STITCHER_ENVIRONMENT_ID:?set STITCHER_ENVIRONMENT_ID (environment UUID)}"
: "${STITCHER_PIPELINE_NAME:?set STITCHER_PIPELINE_NAME (pipeline name)}"

command -v pi >/dev/null 2>&1 || { echo "!! 'pi' CLI not on PATH (npm i -g @earendil-works/pi-coding-agent)" >&2; exit 1; }
[[ -x "$PY" ]] || { echo "!! no python with fastmcp at $PY (set STITCHER_PY)" >&2; exit 1; }

# config_generation SOE-env: the config_generation sub-MCP is launched from pi_coding_agent
# (PIA_DIR), where .env.local / .env.local.dev are symlinked, so ExecutorConfig /
# WebserviceCommonSettings read them from the CWD exactly as SOE runs it.
# config_generation SOE-auth preflight (non-fatal — but without it scan_data / get_data_source_metadata /
# get_committed_config fail with Keycloak 'Realm does not exist'). Set STITCHER_AUTH_TENANT to this
# environment's Keycloak realm / org id (see run.local.sh for the finops-main dev env).
if [ -z "${STITCHER_AUTH_TENANT:-}" ]; then
  echo "!! config_generation: STITCHER_AUTH_TENANT unset — SOE reads/metadata/scan/git-config will fail" >&2
  echo "   with Keycloak 'Realm does not exist'. Set STITCHER_AUTH_TENANT to this env's org/tenant realm." >&2
fi

# Publish the top-level MCP endpoint to the pi extension. The scope/auth vars are
# already in the environment (inherited + read by StitcherAssistantConfig), so only
# the constructed MCP URL needs to be (re)exported here.
export STITCHER_MCP_URL="http://127.0.0.1:${PORT}/mcp/"

# Sub-MCP registry handed to the pi extension (name -> MCP endpoint URL). Every sub-MCP is served
# by the SAME combined process, mounted under /sub_mcp_agents/<name>/mcp on STITCHER_MCP_PORT.
# Add more sub-MCP servers here as they're created under sub_mcp_agents/.
export STITCHER_SUB_MCP_URLS="{\"custom_cost\":\"http://127.0.0.1:${PORT}/sub_mcp_agents/custom_cost/mcp/\",\"config_generation\":\"http://127.0.0.1:${PORT}/sub_mcp_agents/config_generation/mcp/\",\"chargeback\":\"http://127.0.0.1:${PORT}/sub_mcp_agents/chargeback/mcp/\"}"

# (re)start the combined FastMCP server in the background: ONE process on ONE port serves the
# top-level coordinator (common tools) AND every sub-MCP mounted as an ASGI sub-app at
# /sub_mcp_agents/<name>/mcp. Runs from pi_coding_agent (PIA_DIR) where .env.local /
# .env.local.dev are symlinked, so ExecutorConfig / WebserviceCommonSettings read them from the
# CWD exactly as SOE does. PYTHONPATH keeps the local `stitcher` (assistant_harness) namespace
# importable from this CWD. Misses on an existing server are harmless; the curl wait loop below
# ties progress to reachability.
cd "$PIA_DIR"
export PYTHONPATH="$REPO_ROOT/stitcher_mcp_service${PYTHONPATH:+:$PYTHONPATH}"
"$PY" -m stitcher.assistant_harness.mcp_server --http "$PORT" > /tmp/stitcher-pi-mcp.log 2>&1 &
MCP_PID=$!
trap 'kill "$MCP_PID" 2>/dev/null || true' EXIT
# Wait for the combined server to answer initialize before launching pi (all MCP endpoints are
# on this one port).
_wait_mcp() { # $1 = port
  for _ in $(seq 1 40); do
    curl -sf -X POST "http://127.0.0.1:$1/mcp/" -H 'Content-Type: application/json' \
      -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"probe","version":"0"}}}' \
      >/dev/null 2>&1 && return 0
    sleep 0.5
  done
  return 1
}
_wait_mcp "$PORT" || { echo "!! combined MCP server did not come up on $PORT" >&2; exit 1; }

echo "stitcher-pi — model ${STITCHER_MODEL_NAME}, base ${STITCHER_MODEL_BASE_URL}, env ${STITCHER_ENVIRONMENT_ID}, pipeline ${STITCHER_PIPELINE_NAME}, MCP via ${STITCHER_MCP_URL}"
# -nbt / --no-builtin-tools: disable pi's built-in read/write/bash/edit so the agent
# cannot touch source code or the filesystem directly — it may only use the Stitcher MCP
# tools proxied in by the extension (list_directory / read_text_file / read_pdf / the
# chargeback / config-gen / custom-cost bundles). Keeps the agent tool-bound, not a shell.
pi --model "stitcher/${STITCHER_MODEL_NAME}" -nbt -e "$PIA_DIR/pi_extension/index.ts"
status=$?
kill "$MCP_PID" 2>/dev/null || true
exit "$status"

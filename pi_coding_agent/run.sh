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
# Sub-MCP servers (heavy domain tools, activated on demand by `activate_sub_mcp`).
# Each runs on its own port; STITCHER_SUB_MCP_URLS is a JSON map of name -> URL
# consumed by the pi extension. Override ports via env if 8792/8793/8794 are taken.
CUSTOM_COST_PORT="${STITCHER_CUSTOM_COST_MCP_PORT:-8792}"
CONFIG_GEN_PORT="${STITCHER_CONFIG_GEN_MCP_PORT:-8793}"
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
_kill_port "$CUSTOM_COST_PORT"
_kill_port "$CONFIG_GEN_PORT"
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

# config_generation SOE-env preflight (non-fatal — common/soe_context falls back to the SOE dir):
# the config_generation sub-MCP grounds on SOE .env.local / .env.local.dev so ExecutorConfig /
# WebserviceCommonSettings resolve. Copy them from stitcher_operation_executor/ into .soe-env/ once.
if [ -z "${STITCHER_SOE_ENV_DIR:-}" ] && [ ! -d ".soe-env" ]; then
  echo "!! config_generation: no .soe-env/ here — SOE functions will fall back to stitcher_operation_executor/" >&2
  echo "   To make it self-contained: mkdir -p .soe-env && cp ../stitcher_operation_executor/.env.local* .soe-env/ (gitignored)" >&2
fi
# config_generation SOE-auth preflight (non-fatal — but without it scan_data / get_data_source_metadata /
# get_committed_config fail with Keycloak 'Realm does not exist'). Set STITCHER_AUTH_TENANT to this
# environment's Keycloak realm / org id (see run.local.sh for the finops-main dev env).
if [ -z "${STITCHER_AUTH_TENANT:-}" ]; then
  echo "!! config_generation: STITCHER_AUTH_TENANT unset — SOE reads/metadata/scan/git-config will fail" >&2
  echo "   with Keycloak 'Realm does not exist'. Set STITCHER_AUTH_TENANT to this env's org/tenant realm." >&2
fi

export STITCHER_MODEL_BASE_URL STITCHER_MODEL_API_KEY STITCHER_MODEL_NAME
export STITCHER_API_URL STITCHER_ENVIRONMENT_ID STITCHER_PIPELINE_NAME
export STITCHER_API_TOKEN STITCHER_SSL_CA_CERTIFICATE_PATH STITCHER_AUTH_TENANT STITCHER_PIPELINE_ID STITCHER_GIT_BRANCH
export STITCHER_MCP_URL="http://127.0.0.1:${PORT}/mcp/"

# Sub-MCP registry handed to the pi extension (name -> MCP endpoint URL).
# Add more sub-MCP servers here as they're created under sub_mcp_agents/.
export STITCHER_SUB_MCP_URLS="{\"custom_cost\":\"http://127.0.0.1:${CUSTOM_COST_PORT}/mcp/\",\"config_generation\":\"http://127.0.0.1:${CONFIG_GEN_PORT}/mcp/\"}"

# (re)start the FastMCP tool server in the background (the stitcher.assistant_harness
# module, run from the stitcher_mcp_service dir so the local `stitcher` namespace
# shadows any installed one). Misses on an existing server are harmless; the curl
# wait loop below ties progress to reachability.
cd "$REPO_ROOT/stitcher_mcp_service"
# Top-level coordinator server (lightweight tools only).
"$PY" -m stitcher.assistant_harness.mcp_server --http "$PORT" > /tmp/stitcher-pi-mcp.log 2>&1 &
MCP_PID=$!
# custom_cost sub-MCP server (FOCUS normalization + validation — heavy, LLM-bound).
# Activated on demand from the agent via `activate_sub_mcp("custom_cost")`.
"$PY" -m stitcher.assistant_harness.sub_mcp_agents.custom_cost.custom_cost_mcp_server --http "$CUSTOM_COST_PORT" \
  > /tmp/stitcher-pi-mcp-custom-cost.log 2>&1 &
# config_generation sub-MCP server (enhance/enrich config generation — SOE-as-is grounding).
# Activated on demand via `activate_sub_mcp("config_generation")`. SOE env files are read from
# .soe-env/ (copied from SOE, gitignored) by common/soe_context so ExecutorConfig resolves as-is.
"$PY" -m stitcher.assistant_harness.sub_mcp_agents.config_generation.config_generation_mcp_server --http "$CONFIG_GEN_PORT" \
  > /tmp/stitcher-pi-mcp-config-gen.log 2>&1 &
SUB_MCP_PIDS="$!"
trap 'kill "$MCP_PID" 2>/dev/null || true; for p in $SUB_MCP_PIDS; do kill "$p" 2>/dev/null || true; done' EXIT
cd "$PIA_DIR"
# Wait for BOTH servers to answer initialize before launching pi.
_wait_mcp() { # $1 = port
  for _ in $(seq 1 40); do
    curl -sf -X POST "http://127.0.0.1:$1/mcp/" -H 'Content-Type: application/json' \
      -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"probe","version":"0"}}}' \
      >/dev/null 2>&1 && return 0
    sleep 0.5
  done
  return 1
}
_wait_mcp "$PORT"   || { echo "!! top-level MCP server did not come up on $PORT" >&2; exit 1; }
_wait_mcp "$CUSTOM_COST_PORT" || echo "!! custom_cost sub-MCP did not come up on $CUSTOM_COST_PORT (agent will run without it)" >&2
_wait_mcp "$CONFIG_GEN_PORT" || echo "!! config_generation sub-MCP did not come up on $CONFIG_GEN_PORT (agent will run without it)" >&2

echo "stitcher-pi — model ${STITCHER_MODEL_NAME}, base ${STITCHER_MODEL_BASE_URL}, env ${STITCHER_ENVIRONMENT_ID}, pipeline ${STITCHER_PIPELINE_NAME}, MCP via ${STITCHER_MCP_URL}"
pi --model "stitcher/${STITCHER_MODEL_NAME}" -e "$PIA_DIR/pi_extension/index.ts"
status=$?
kill "$MCP_PID" 2>/dev/null || true
for p in $SUB_MCP_PIDS; do kill "$p" 2>/dev/null || true; done
exit "$status"

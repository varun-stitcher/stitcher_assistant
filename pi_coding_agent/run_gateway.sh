#!/usr/bin/env bash
# stitcher-pi agent gateway — expose the pi agent as (1) a higher-order MCP server (Claude Code /
# Claude Desktop) and (2) an OpenAI-compatible endpoint.
#
#   * MCP   : http://127.0.0.1:${STITCHER_MCP_PORT:-8792}/mcp/   (task-typed orchestrator tools)
#   * OpenAI: http://127.0.0.1:${STITCHER_OPENAI_PORT:-8880}/v1  (/v1/chat/completions, /v1/models)
#
# Both are served by ONE process (gateway.py); each orchestrator call spawns its own per-call
# scoped tool MCP + headless pi turn on ephemeral ports, so the gateway is concurrency-safe and
# per-call env-scoped (the caller passes environment_id + pipeline_name per call — never set here).
#
# REQUIRED env (the runtime/config vars the gateway inherits):
#   STITCHER_MODEL_BASE_URL   LiteLLM / OpenAI-compatible base URL
#   STITCHER_MODEL_API_KEY    gateway key
#   STITCHER_MODEL_NAME       model id (e.g. qwen3.6-27b-mtp)
#   STITCHER_API_URL          Stitcher web service base (e.g. https://app.local.stitcher.ai/v1)
# Optional (infra only):
#   STITCHER_MCP_PORT         higher-order MCP port (default 8792)
#   STITCHER_OPENAI_PORT      OpenAI endpoint port (default 8880)
#   STITCHER_PY               python with fastmcp (default ../stitcher_mcp_service/.venv/bin/python)
#   STITCHER_OUTPUT_DIR       where saved enhance configs are written (default pi_coding_agent/.output)
set -euo pipefail
cd "$(dirname "$0")"
PIA_DIR="$PWD"
REPO_ROOT="$(cd .. && pwd)"   # stitcher_assistant — home of the stitcher_mcp_service package

PY="${STITCHER_PY:-$REPO_ROOT/stitcher_mcp_service/.venv/bin/python}"
MCP_PORT="${STITCHER_MCP_PORT:-8792}"
OAI_PORT="${STITCHER_OPENAI_PORT:-8880}"

# Free the gateway ports so a STALE server from a prior run (serving OLD code) can't linger and
# steal the connections. If these fail (no lsof), that's fine.
_kill_port() { # $1 = port
  local p
  p=$(lsof -tiTCP:"$1" -sTCP:LISTEN 2>/dev/null | head -1) || true
  [ -n "$p" ] && kill "$p" 2>/dev/null || true
}
_kill_port "$MCP_PORT"
_kill_port "$OAI_PORT"
sleep 1

# Preflight (no silent fallback): the env-scoped gateway refuses to start without these.
: "${STITCHER_MODEL_BASE_URL:?set STITCHER_MODEL_BASE_URL (e.g. https://app.dev.stitcher.ai/llm/v1)}"
: "${STITCHER_MODEL_API_KEY:?set STITCHER_MODEL_API_KEY (gateway key)}"
: "${STITCHER_MODEL_NAME:?set STITCHER_MODEL_NAME (e.g. qwen3.6-27b-mtp)}"
: "${STITCHER_API_URL:?set STITCHER_API_URL (e.g. https://app.local.stitcher.ai/v1)}"

command -v pi >/dev/null 2>&1 || { echo "!! 'pi' CLI not on PATH (npm i -g @earendil-works/pi-coding-agent)" >&2; exit 1; }
[[ -x "$PY" ]] || { echo "!! no python with fastmcp at $PY (set STITCHER_PY)" >&2; exit 1; }

# PYTHONPATH keeps the local `stitcher` (assistant_harness) namespace importable from this CWD.
export PYTHONPATH="$REPO_ROOT/stitcher_mcp_service${PYTHONPATH:+:$PYTHONPATH}"

echo "stitcher-pi gateway — MCP http://127.0.0.1:${MCP_PORT}/mcp/  OpenAI http://127.0.0.1:${OAI_PORT}/v1  model ${STITCHER_MODEL_NAME}"
"$PY" -m stitcher.assistant_harness.agent_gateway.gateway --mcp-port "$MCP_PORT" --openai-port "$OAI_PORT" > /tmp/stitcher-pi-gateway.log 2>&1 &
GW_PID=$!
trap 'kill "$GW_PID" 2>/dev/null || true' EXIT

# Wait for the OpenAI surface to answer /health before reporting ready (the MCP surface listens on
# the same process, so one health probe covers both).
for _ in $(seq 1 40); do
  if curl -sf --noproxy '*' -o /dev/null "http://127.0.0.1:${OAI_PORT}/health" 2>/dev/null; then
    echo "gateway ready — MCP :${MCP_PORT}, OpenAI :${OAI_PORT} (log: /tmp/stitcher-pi-gateway.log)"
    wait "$GW_PID"
    exit 0
  fi
  sleep 0.5
done
echo "!! gateway did not come up on :${MCP_PORT} / :${OAI_PORT} (see /tmp/stitcher-pi-gateway.log)" >&2
exit 1

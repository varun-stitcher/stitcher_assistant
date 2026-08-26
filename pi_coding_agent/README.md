# stitcher-pi — minimal pi coding agent

A thin pi coding agent wrapper. Pi ([@earendil-works/pi-coding-agent]) is the
loop. Two pieces wrap it:

1. **`stitcher_mcp_service/`** — the Python MCP tool server (the
   `stitcher.assistant_harness` package). Tools live in `tools/`; shared state
   (`config.py`, `oidc_auth.py`, `client.py`) lives in `common/`.
2. **`pi_extension/`** — a pi extension that (a) registers the **Stitcher LLM
   endpoint** as a provider from env vars and (b) **discovers every FastMCP tool**
   over MCP and proxies it into pi.

```
pi (the loop) ──MCP──▶ stitcher.assistant_harness.mcp_server (FastMCP)
   stitcher model         ├─ tools/file_tools.py     (dev/file tools)
                          ├─ tools/stitcher_tools.py (Stitcher API)
                          ├─ tools/auth_tools.py     (OIDC login)
                          └─ common/                 (OIDCAuth, StitcherClient, StitcherSettings)
```

Shared state lives in `OIDCAuth` (`common/oidc_auth.py`) and `StitcherClient`
(`common/client.py`) in the `stitcher.assistant_harness` package, not globals.

## Run

Set the required env vars, then launch:

```bash
export STITCHER_MODEL_BASE_URL=https://app.dev.stitcher.ai/llm/v1
export STITCHER_MODEL_API_KEY=...          # required
export STITCHER_MODEL_NAME=qwen3.6-27b-mtp
export STITCHER_API_URL=https://app.dev.stitcher.ai/v1
export STITCHER_ENVIRONMENT_ID=<env uuid>  # required
export STITCHER_PIPELINE_NAME=finops-main  # required
./run.sh
```

Or use the gitignored `run.local.sh`, which already sets these.

### One-time setup

```bash
cd pi_extension && npm ci          # MCP SDK
npm i -g @earendil-works/pi-coding-agent   # `pi` CLI
```

The FastMCP server runs from the venv that has `fastmcp`
(`../stitcher_mcp_service/.venv`).

### Other transports

Run from the `stitcher_mcp_service` dir:

```bash
python -m stitcher.assistant_harness.mcp_server                 # stdio
python -m stitcher.assistant_harness.mcp_server --http 8791     # Streamable HTTP (what run.sh uses)
```

## Env vars

| Var | Default | Purpose |
|---|---|---|
| `STITCHER_MODEL_BASE_URL` | `https://app.dev.stitcher.ai/llm/v1` | LiteLLM / OpenAI-compatible base URL |
| `STITCHER_MODEL_API_KEY` | *(required)* | gateway key (pi interpolates its `$STITCHER_MODEL_API_KEY` reference) |
| `STITCHER_MODEL_NAME` | `qwen3.6-27b-mtp` | model id (must exist as an alias on the gateway) |
| `STITCHER_API_URL` | `https://app.dev.stitcher.ai/v1` | Stitcher web service base URL for the API tools |
| `STITCHER_ENVIRONMENT_ID` | *(none)* | environment UUID the Stitcher tools operate on |
| `STITCHER_PIPELINE_NAME` | *(none)* | pipeline context for the Stitcher tools |
| `STITCHER_API_TOKEN` | *(falls back to* `STITCHER_MODEL_API_KEY`*)* | static bearer token for SWS calls |
| `STITCHER_AUTH_URL` | *(= api_url origin)* | Keycloak base for OIDC login (e.g. `https://app.dev.stitcher.ai`) |
| `STITCHER_OIDC_REALM` | `stitcher` | Keycloak realm |
| `STITCHER_OIDC_CLIENT_ID` | `stitcher-harness-login` | public OIDC client (PKCE, no secret) |
| `STITCHER_OAUTH_CALLBACK_PORT` | `8086` | local port Keycloak redirects back to |
| `STITCHER_SSL_CA_CERTIFICATE_PATH` | `../local/certs/ca.crt` | CA bundle for a self-signed local/dev Keycloak+SWS. If unset and no file exists, TLS verify is skipped for dev. |
| `STITCHER_MCP_PORT` | `8791` | FastMCP HTTP port |
| `STITCHER_PY` | `../stitcher_mcp_service/.venv/bin/python` | python with `fastmcp` |

`sai_*` / `STITCHER_*` scope is read once at startup through a pydantic
`BaseSettings` model (`config.py`, `env_prefix="STITCHER_"`) — it also reads
`.env.local` / `.env.local.dev` if present.

## Authenticating to the Stitcher API

If `list_connections` / `get_pipeline` return **401 Unauthorized**, authenticate
via the local-port OIDC flow:

1. Run **`auth_get_url`** — it starts a callback listener on
   `http://127.0.0.1:8086/callback` and opens your browser to the Keycloak login.
2. Sign in to the `stitcher` realm.
3. Keycloak redirects to `http://127.0.0.1:8086/callback?code=...&state=...`; the
   agent exchanges the code for an access (+ refresh) token, stores it, and uses
   it for all subsequent Stitcher tool calls. Restarts and expiry are handled
   automatically via the persisted refresh token.

Alternatives:

```bash
auth_set_token <token>   # paste a token you already have
auth_status              # check whether a (live) token is present
auth_environments        # which environments the token can access
```

For a self-signed local dev Keycloak (`app.local.stitcher.ai`), set
`STITCHER_SSL_CA_CERTIFICATE_PATH`; when no bundle exists the agent skips TLS
verification for dev. If SWS still returns `401 Not authenticated`, re-run
`auth_get_url` to mint a fresh token.

## Adding a tool

Write it in the matching module under `stitcher_mcp_service/stitcher/assistant_harness/tools/`
(`file_tools.py` / `stitcher_tools.py` / `auth_tools.py`) and it's exposed
automatically — no per-tool pi code. Tools that need Stitcher state take the
`client`/`auth` instance from their module's `register(mcp, ...)` signature.

```python
@mcp.tool
def my_tool(arg: str) -> str:
    """What it does."""
    return something(arg)
```

All determinism belongs server-side; pi only makes the calls.

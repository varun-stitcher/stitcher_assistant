# stitcher-pi — minimal pi coding agent

A thin pi coding agent wrapper. Pi ([@earendil-works/pi-coding-agent]) is the
loop. Two pieces wrap it:

1. **`stitcher_mcp_service/`** — the Python MCP tool servers (the
   `stitcher.assistant_harness` package). Lightweight coordinator tools live
   in `tools/`; shared state (`config.py`, `oidc_auth.py`, `client.py`) lives in
   `common/`. Heavy domain tools live in their own **sub-MCP servers** under
   `sub_mcp_agents/` (see [Sub-MCP servers](#sub-mcp-servers-keeping-the-tool-list-pristine)).
2. **`pi_extension/`** — a pi extension that (a) registers the **Stitcher LLM
   endpoint** as a provider from env vars and (b) **discovers every FastMCP tool**
   over MCP and proxies it into pi. Top-level tools are active by default;
   sub-MCP tools are registered but held **inactive** and activated on demand
   via the `activate_sub_mcp` loader tool (pi's native Dynamic Tool Loading).

```
pi (the loop) ──MCP──▶ stitcher.assistant_harness.mcp_server (FastMCP, top-level)
   stitcher model         ├─ tools/file_tools.py     (dev/file tools)
                          ├─ tools/stitcher_tools.py (Stitcher API)
                          ├─ tools/auth_tools.py     (OIDC login)
                          └─ common/                 (OIDCAuth, StitcherClient, StitcherSettings)
          ┌──MCP──▶ sub_mcp_agents/custom_cost/  (custom_cost_mcp_server — FastMCP, sub-MCP)
          │             └─ tools/                       (the sub-MCP's own tool modules)
          │                ├─ plan_generation_tools.py  (generate_focus_plans — harness-native)
          │                ├─ conversion_tools.py       (detect_provider / apply_conversion_plans / …)
          │                ├─ focus_normalization_tools.py (normalize_to_focus)
          │                └─ focus_validation_tools.py     (validate_and_repair_focus)
          └─ activated on demand via the `activate_sub_mcp("custom_cost")` pi tool
```

Shared state lives in `OIDCAuth` (`common/oidc_auth.py`) and `StitcherClient`
(`common/client.py`) in the `stitcher.assistant_harness` package, not globals.

## Sub-MCP servers — keeping the tool list pristine

The agent's visible tool list is kept deliberately small: only the lightweight
coordinator tools (file / stitcher API / auth) plus two meta-tools are
broadcast. Heavy, domain-specific tool bundles (e.g. the FOCUS normalization +
validation pipeline) live in **sub-MCP servers** — separate FastMCP processes on
their own HTTP port. The pi extension discovers each sub-MCP and registers its
tools as **inactive** (they're in `pi.getAllTools()` but absent from the active
set). The agent "switches into" a sub-MCP by calling the `activate_sub_mcp`
loader tool, which calls `pi.setActiveTools([...active, ...sub])` — purely
additive, so the prompt prefix stays cached. See pi's *Dynamic Tool Loading*
in `docs/extensions.md`.

Visible to the agent at startup:

- top-level coordinator tools (active) — `ping`, `now_utc`, `list_directory`,
  `read_text_file`, `read_pdf`, `stitcher_context`, `stitcher_capabilities`,
  `list_connections`, `get_connection`, `get_pipeline`, `auth_get_url`,
  `auth_environments`, `auth_status`, `auth_set_token`

  `stitcher_capabilities` is the always-active discoverability aid: it lists
  every sub-MCP bundle, the tools it hosts (e.g. `normalize_to_focus`), and the
  exact `activate_sub_mcp(<name>)` call to bring them online. Call it whenever
  a task needs a capability that isn't in your active tools.
- `list_sub_mcp_servers` — list available sub-MCPs and their tool counts
- `activate_sub_mcp(name)` — activate a sub-MCP's tools for the rest of the
  session (e.g. `activate_sub_mcp("custom_cost")` exposes the FOCUS tool
  bundle: `extract_invoice`, `cache_*`, `generate_focus_plans`, `detect_provider`,
  `apply_conversion_plans`, `simulate_normalize_config`, `normalize_to_focus`,
  `validate_and_repair_focus`, …)

#### Influence extracted columns + step-artifact cache (`extract_invoice` / `cache_*`)

* `extract_invoice(file_path=…, expected_columns=[…])` runs ONLY the extraction
  step and lets you **influence which columns are pulled** (e.g. `expected_columns=["Tax", "Region"]`
  makes the parser capture fields it might otherwise skip).
* Each pipeline step saves its output to a **KW (knowledge-work) cache** — a
  persistent key-value store at `~/.stitcher/kw-cache` (env `STITCHER_STEP_CACHE_DIR`),
  keyed by source-file content hash + step + variant. This lets you **add, update,
  and reuse** intermediate artifacts across calls without re-running the expensive LLM steps.
  * `cache_list(prefix)` / `cache_get(key)` / `cache_put(key, payload_json)` /
    `cache_clear(prefix)` — inspect & manage the cache.
  * `extract_invoice` and `normalize_to_focus(use_cache=True)` **reuse** cached
    extraction / plan-gen output on repeat calls (zero LLM).

A sub-MCP is environment-agnostic — it does **not** instantiate
`StitcherSettings`/`OIDCAuth`, so it can start without `STITCHER_ENVIRONMENT_ID`
/ `STITCHER_PIPELINE_NAME`. Only the LLM gateway env (`STITCHER_MODEL_*`) is
needed for the custom_cost tools' LLM calls.

### `config_generation` sub-MCP (enhance/enrich config generation)

The **exception** to the env-agnostic rule. `config_generation` is an
**environment-scoped** sub-MCP that drives enhance/prepare + enhance/enrich
config generation, grounding on the REAL environment by exercising **SOE
functions as-is** (no vendored copies):

1. `list_operators` / `describe_operator` — the enhance operator vocabulary
   (Lookup, Mapping, Compute, Filter rows, Unpack, …) with the full field spec
   and a REAL example, grounded on the SPC enhance models + example configs.
2. `list_data_sources` / `get_data_source_metadata` / `scan_data` — the live
   SWS datasource catalog, then columns + dtypes and real $ splits **by
   connection parameters** via the SOE metadata operator
   (`MetadataConsolidateOperator` + `ExtractRefDataSubOperator`).
3. `get_committed_config` / `derived_columns` — the prior checked-in git configs
   for this environment + pipeline via SOE `get_vsc_commit_dir` (compact per-op
   summary + the `x_*` bridge columns the committed configs create).
4. `plan_enhance_operations` — ONE LLM call (Stitcher gateway) mapping a
   requirement to the best operation(s), then a deterministic guard that refuses
   unknown operation types and references to columns/datasets not in the
   gathered metadata. Columns are validated **per side**: pass
   `available_columns` (cost dataset) and `business_dataset_columns` (the
   business/reference dataset) so a Lookup's imports + business join keys are
   checked against the BUSINESS side — passing only cost columns no longer nukes
   valid Lookup imports.
5. `generate_lookup` / `generate_filter` / `validate_config` / `save_config` —
   deterministic, validate-by-construction authoring against the real SPC
   enhance models (correct filter polarity; shadowing/unknown-import refusal).

Activate with `activate_sub_mcp("config_generation")`.

**SOE env files.** To exercise SOE functions as-is, this server needs SOE's
`.env.local` / `.env.local.dev` (so `ExecutorConfig()` /
`WebserviceCommonSettings()` resolve). Copy them from
`<repo_root>/stitcher_operation_executor/` into `<this dir>/.soe-env/`
(gitignored) once:

```bash
mkdir -p .soe-env && cp ../stitcher_operation_executor/.env.local .soe-env/
cp ../stitcher_operation_executor/.env.local.dev .soe-env/
```

`common/soe_context` looks there first (override with `STITCHER_SOE_ENV_DIR`),
then falls back to the SOE dir directly. It loads a whitelisted scalar subset
into `os.environ` and absolutizes relative path vars, so `ExecutorConfig`
validates from any CWD.

**SOE service-account auth.** `get_data_source_metadata` / `scan_data` /
`get_committed_config` authenticate at Keycloak as a service account keyed by
`(environment_id, auth_tenant)` (the Keycloak realm / org id). **If
`STITCHER_AUTH_TENANT` is unset, these fail with the cryptic `Realm does not
exist`.** Set it to this environment's org/tenant realm — for the `finops-main`
dev env (`d7dad3dc…`) that is `stitcherai-wsmo5` (see `run.local.sh`).
`STITCHER_PIPELINE_ID` is additionally needed for `get_committed_config` (the
git-branch fetch); when unset, `common/soe_context` attempts a best-effort lookup from
the pipeline name, but an explicit id is more reliable. `environment_context`
surfaces whether `auth_tenant` is set.

### Adding a sub-MCP server

1. Drop a new server under `sub_mcp_agents/<name>/<name>_mcp_server.py` with a
   `build_server()`, and host its tool modules in a sibling `tools/` package
   (`sub_mcp_agents/<name>/tools/`) that you import and register there. Mirror
   `sub_mcp_agents/custom_cost/` (server + own `tools/`).
2. Add it to `STITCHER_SUB_MCP_URLS` (name -> URL) and start it on its own port
   in `run.sh`.

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
# Sub-MCP servers run the same way, each on its own port:
python -m stitcher.assistant_harness.sub_mcp_agents.custom_cost.custom_cost_mcp_server --http 8792
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
| `STITCHER_MCP_PORT` | `8791` | top-level FastMCP HTTP port |
| `STITCHER_CUSTOM_COST_MCP_PORT` | `8792` | custom_cost sub-MCP HTTP port |
| `STITCHER_CONFIG_GEN_MCP_PORT` | `8793` | config_generation sub-MCP HTTP port |
| `STITCHER_SOE_ENV_DIR` | `.soe-env/` | dir with SOE `.env.local` / `.env.local.dev` the config_generation server loads |
| `STITCHER_OUTPUT_DIR` | `<pi_coding_agent>/.output` | where `save_config` writes authored configs |
| `STITCHER_AUTH_TENANT` | *(unset → Keycloak 'Realm does not exist')* | Keycloak realm / org id (e.g. `stitcherai-wsmo5`) the SOE data/metadata/scan/git reads authenticate with |
| `STITCHER_PIPELINE_ID` | *(resolve from name)* | pipeline UUID for `get_committed_config`'s git fetch |
| `STITCHER_GIT_BRANCH` | `main` | git branch for the committed-config fetch |
| `STITCHER_SUB_MCP_URLS` | *(set by run.sh)* | JSON map of `name -> URL` the pi extension reads to discover sub-MCPs |
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

**Top-level (coordinator) tool** — write it in the matching module under
`stitcher_mcp_service/stitcher/assistant_harness/tools/`
(`file_tools.py` / `stitcher_tools.py` / `auth_tools.py`) and it's exposed
automatically — no per-tool pi code. Tools that need Stitcher state take the
`client`/`auth` instance from their module's `register(mcp, ...)` signature.
Keep this surface small; it is what the agent sees at startup.

```python
@mcp.tool
def my_tool(arg: str) -> str:
    """What it does."""
    return something(arg)
```

**Sub-MCP (domain bundle) tool** — put heavy / domain-specific tools in a sub-MCP
server under `sub_mcp_agents/<name>/` and register them from that server's
`build_server()`. They are invisible to the agent until it calls
`activate_sub_mcp("<name>")`. See [Sub-MCP servers](#sub-mcp-servers-keeping-the-tool-list-pristine).

All determinism belongs server-side; pi only makes the calls.

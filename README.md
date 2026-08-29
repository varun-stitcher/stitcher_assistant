# stitcher-assistant

pi coding agent + MCP service, built on `stitcher_operation_executor` (SOE).

uv-managed. SOE (and its stitcher siblings) resolve **local-editable** in this
worktree via `[tool.uv.sources]` in `pyproject.toml`, and fall back to the
private `stitcher_pypi` index (by the version constraints in `dependencies`)
when built elsewhere.

## Dev setup

Auth the private index once per session, then sync:

```bash
export UV_INDEX_STITCHER_PYPI_USERNAME=oauth2accesstoken
export UV_INDEX_STITCHER_PYPI_PASSWORD=$(gcloud auth print-access-token)
uv sync
```

`uv sync` builds `.venv/` with SOE installed editable from
`../stitcher_operation_executor` (edits there are picked up live).

To pull SOE (or any sibling) from the published wheel instead of local source,
comment its line out of `[tool.uv.sources]` and re-run `uv sync`.

## Code quality (QA)

A `Makefile` at the repo root gates formatting, linting, and type-checking for
both the Python service and the Node extension:

```bash
make check    # read-only gate: lint + typecheck + format-check (both sides)
make format   # auto-fix formatting (black + isort + ruff-imports, prettier)
make qa       # format, then run the full check gate
```

Python side (`stitcher_mcp_service`) — **ruff** (lint), **isort** (imports),
**black** (format), **mypy** (types). Config lives in `pyproject.toml` under
`[tool.ruff]` / `[tool.isort]` / `[tool.black]` / `[tool.mypy]`, following the
house style in `stitcher_pipeline_common/setup.cfg` (120-char lines, isort
parenthesized trailing-comma style, `ignore_missing_imports`).

Node side (`pi_coding_agent/pi_extension`) — **eslint** (flat config
`eslint.config.js`), **prettier** (`.prettierrc.json`, 120 cols / single
quotes), **tsc** (`tsconfig.json`, `--noEmit`). Run via npm scripts too:
`npm run lint / format / format:check / typecheck`.

The Python QA tools are standalone (`uv tool install ruff isort black mypy`);
the Node tools live in `pi_extension/node_modules` (devDependencies in
`pi_coding_agent/pi_extension/package.json`).

## Expose the agent: higher-order MCP server + OpenAI endpoint

The pi agent (`pi_coding_agent/`) can be exposed as an orchestrator behind two surfaces so
Claude Code / Claude Desktop and OpenAI-compatible clients can drive it: a **higher-order MCP
server** (task-typed tools — `generate_enhance_config`, `normalize_invoice_to_focus`,
`explore_environment`) and an **OpenAI-compatible endpoint** (`/v1/chat/completions`). Both are
served by one gateway process (`stitcher_mcp_service/stitcher/assistant_harness/agent_gateway/gateway.py`) — see `docs/harness/README.md` for the module map and workflows
that shares a single `AgentRunner` (per-call headless pi turns, per-call scoped tool MCP).

```bash
cd pi_coding_agent && ./run_gateway.local.sh
# MCP  :  http://127.0.0.1:8792/mcp/   (register in Claude Code / Claude Desktop)
# OpenAI: http://127.0.0.1:8880/v1     (/v1/chat/completions, /v1/models, /health)
```

Every call supplies its own `environment_id` + `pipeline_name` (env-scoped, per-call);
Claude Code / Desktop config snippets, the task-typed tool schemas, and the structured-output
contract are documented in [`pi_coding_agent/README.md`](./pi_coding_agent/README.md).

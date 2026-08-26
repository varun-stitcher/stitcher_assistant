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

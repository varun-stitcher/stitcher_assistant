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

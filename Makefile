# Code-quality gates for the stitcher-pi agent.
#
# Python side  (stitcher_mcp_service) -> ruff, isort, black, mypy
# Node side    (pi_coding_agent/pi_extension) -> eslint, prettier, tsc
#
# Python tools are standalone (`uv tool install ruff isort black mypy`); Node
# tools ship in the pi_extension's own node_modules. Run the whole QA suite with
# `make qa` (formats then checks).

PY_SRC     = stitcher_mcp_service/stitcher
PY_DIR     = stitcher_mcp_service
# ruff/isort/black/mypy read their [tool.*] config from the pyproject.toml in
# PY_DIR, so run each from there with a repo-root-relative source path. The
# tools are standalone (uv tool), or use the project venv if you prefer via PY.
NODE_DIR   = pi_coding_agent/pi_extension
NODE_BIN   = $(NODE_DIR)/node_modules/.bin

.PHONY: format lint typecheck check qa node-format node-lint node-typecheck python-format python-lint python-typecheck

## ---------- format (mutating) ----------
format: python-format node-format
	@echo "✓ formatted"

python-format:
	cd $(PY_DIR) && black $(subst $(PY_DIR)/,,$(PY_SRC))
	cd $(PY_DIR) && isort $(subst $(PY_DIR)/,,$(PY_SRC))
	cd $(PY_DIR) && ruff check --select I --fix $(subst $(PY_DIR)/,,$(PY_SRC))

node-format:
	cd $(NODE_DIR) && ./node_modules/.bin/prettier --write "index.ts" "mcpClient.mjs" "eslint.config.js"

## ---------- lint (read-only) ----------
lint: python-lint node-lint
	@echo "✓ lint clean"

python-lint:
	cd $(PY_DIR) && ruff check $(subst $(PY_DIR)/,,$(PY_SRC))
	cd $(PY_DIR) && isort --check-only $(subst $(PY_DIR)/,,$(PY_SRC))
	cd $(PY_DIR) && black --check $(subst $(PY_DIR)/,,$(PY_SRC))

node-lint:
	cd $(NODE_DIR) && ./node_modules/.bin/eslint .

## ---------- typecheck (read-only) ----------
typecheck: python-typecheck node-typecheck
	@echo "✓ types check"

python-typecheck:
	cd $(PY_DIR) && mypy $(subst $(PY_DIR)/,,$(PY_SRC))

node-typecheck:
	cd $(NODE_DIR) && ./node_modules/.bin/tsc --noEmit

## ---------- format-check (read-only) ----------
format-check:
	cd $(NODE_DIR) && ./node_modules/.bin/prettier --check "index.ts" "mcpClient.mjs" "eslint.config.js"

## ---------- combined gates ----------
check: lint typecheck format-check
	@echo "✓ all checks pass"

qa: format check
	@echo "✓ QA done"

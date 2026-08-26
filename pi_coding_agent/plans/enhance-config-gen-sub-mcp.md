# Plan — `config_generation` sub-MCP for the enhance/enrich stage

## Context

`stitcher-pi` (`stitcher_assistant/pi_coding_agent`) is a thin pi wrapper. Heavy domain tools
live in **sub-MCP servers** under `stitcher_mcp_service/.../sub_mcp_agents/<name>/` (see
`custom_cost`), each on its own HTTP port, discovered by the pi extension from
`STITCHER_SUB_MCP_URLS` and activated on demand via `activate_sub_mcp(name)`. Pi's extension
proxies each tool over MCP — no per-tool pi code.

Today this wrapper has **no config-generation capability**. Enhance/enrich config generation
exists only in two *other* places (both out-of-reach for a clean in-wrapper sub-MCP):
- `pi_agent_coding_harness/server/sai_config_mcp.py` — the Pi-loop harness's 21 sub-tools,
  backed by vendored deterministic code under `_vendor/`. It grounds on `SAI_EXISTING_DIR`
  (simulated fixtures) and resolves committed configs via the sibling `config-gen-harness`
  (`soe_env_tools` / `env_context`).
- SPC `generate_config` one-tool — forwards to that harness over HTTP (service).

We want a **new `config_generation` sub-MCP in this wrapper** so the pi agent can drive
enhance/enrich config generation directly, grounded in the **real** environment by reusing
**SOE functions as-is** (not vendored copies). Per the user's ask, the flow is:

1. Understand the user requirement and **figure out the best operations** (LLM-assisted).
2. Depending on that, call tools to **list data sources**, **inspect their metadata (columns)**
   via the SOE metadata operator, and **scan/read data** by connection parameters via SOE
   extract/read functions.
3. **Look at the prior checked-in git configs** for the environment + pipeline via the SOE
   `get_vsc_commit_dir` path.
4. Author / validate / save the enhance config (full scope).

A hard requirement: **exercise SOE functions as-is** — the sub-MCP must build a real SOE
`WorkflowContext` + `ExecutorConfig` and call the SOE functions directly (metadata operator,
extract reference-data reads, `get_vsc_commit_dir`). To make `ExecutorConfig()` resolve, we
copy SOE's `.env.local` + `.env.local.dev` into the pi_coding_agent and load them into
`os.environ` at server start (mirroring `env_context._load_soe_env_files`).

## Approach

### 1. New sub-MCP `config_generation` (mirrors `custom_cost/`)
- `sub_mcp_agents/config_generation/__init__.py`
- `sub_mcp_agents/config_generation/config_generation_mcp_server.py` — `build_server()` + `main()`
  (same shape as `custom_cost_mcp_server.py`: `--http` arg, default port).
- `sub_mcp_agents/config_generation/tools/__init__.py` + tool modules.

**Environment-scoped (unlike `custom_cost`):** config-gen always operates on a customer
environment, so `build_server()` instantiates `StitcherSettings` / `OIDCAuth` /
`StitcherClient` (same as the top-level `mcp_server.py`) and refuses to start without
`STITCHER_ENVIRONMENT_ID` / `STITCHER_PIPELINE_NAME`. A shared `SoeContext` helper (see §3)
is constructed once and passed to the tools' `register(mcp, client, soe)`.

### 2. SOE-as-is grounding (the load-bearing piece)
- `sub_mcp_agents/config_generation/tools/soe_context.py` — builds and caches:
  - loads the copied `.env.local` / `.env.local.dev` into `os.environ` (port of
    `env_context._load_soe_env_files`) **before** constructing `ExecutorConfig()`;
  - a hand-built `WorkflowContext` (`stitcher.operation_executor.models.workflow_context`) — a
    pydantic `BaseModel`, constructible outside Temporal — from `StitcherSettings`
    (environment_id, pipeline_name, auth_tenant, a `SimpleDateRange`/`month` for "today",
    default `OrgInternalConfigSchema`/`OrgExternalConfigSchema`/`WorkflowConfigSchema`);
  - resolves the live data-connection map via `StitcherClient` (top-level already does this).
- All SOE calls take a `workflow_context` parameter, so we pass ours in directly — no Temporal
  sandbox, no workflow.info(). (`get_vsc_commit_dir` is `async`; the read/schema functions are
  plain `def`, run via `asyncio.to_thread`.)

> Key implementation note / risk to confirm at build time: some SOE helpers *downstream* of the
> ones we call may reach for `temporalio.workflow.info()` or an activity context (e.g. logging /
> progress). We scope to the pure-data entry points (`__read_business_dataset_schema__`,
> `read_database_schema`, `__extract_reference_dataframe_recursion__`, `get_vsc_commit_dir`,
> `StageConfigHelpers.load_stage_config`) which take `workflow_context` explicitly. If any
> transitive call hits a Temporal-only API, we route it through `asyncio.to_thread` and, if
> needed, a thin sync shim that swaps `workflow.info()` for our context — recorded as the first
> thing to validate in Step 1 (a 30-min spike), with a fallback to the harness's vendored
> equivalents if a given SOE function is irreducibly Temporal-bound.

### 3. Tool catalog (full scope)
Grouped by phase; each is a thin MCP wrapper over a reusable function (SOE or SPC). Progress
events via `ctx.report_progress()` like `custom_cost`'s `normalize_to_focus`.

**Grounding — operators & environment**
- `list_operators(stage="enrich")` / `describe_operator(stage, operation_type)` — enumerate the
  enhance operators (Lookup, Mapping, Compute/AddColumn, Filter rows, Unpack, RemoveColumn,
  CostSimulator, AiAssistedMapping) + fields + a real example. Reuse SPC
  `stage_registry`/`enhance_config` models + example configs
  (`config_generation_agent.example_configs/enhance`).
- `environment_context()` — env id, pipeline, branch, auth tenant, whether SOE env loaded.

**Grounding — data sources & metadata (SOE-as-is)**
- `list_data_sources()` — real SWS connections via `StitcherClient.list_connections` (reuse
  top-level client), plus the registered business datasets (from committed config / SOE
  `__get_business_data_connections__`).
- `get_data_source_metadata(name_or_id)` — columns/schema via the SOE metadata operator:
  `MetadataConsolidateOperator.__read_business_dataset_schema__` /
  `__read_business_dataset_schema_for_datasource__` (which dispatch to
  `ExtractRefDataSubOperator.read_database_schema` for DB-backed connectors or
  `__extract_reference_dataframe_recursion__` for object-store). Returns `{columns, dtypes,
  connector, provider, dataset}`.
- `scan_data(name_or_id, columns="", group_by="", value="", where="", limit=50)` — read REAL
  data by connection parameters via SOE extract/read functions (polars, projection pushdown).
  For DB connectors use `ExtractRefDataSubOperator.read_database_schema` + a cheap read; for
  object-store use `__extract_reference_dataframe_recursion__`. Surfaces the dollar split /
  sample rows grounding needs (the `read_data` shape from `sai_config_mcp`).

**Grounding — prior committed configs (SOE-as-is)**
- `get_committed_config(branch="", stage="")` — call SOE `get_vsc_commit_dir(workflow_context,
  environment_id, pipeline_id, git_branch)` then `_load_all_stage_configs` /
  `StageConfigHelpers.load_stage_config` to get the committed pipeline configs; return a
  compact per-stage summary (existing Lookups' join keys / imported columns / provider scope,
  filters, derived columns) — never raw YAML. Degrades to a clear "unscoped / SWS-unreachable"
  note (mirrors `merge_git_configs`).
- `derived_columns(contains="")` — derived `x_*` bridge columns from the committed tree.

**Planning (LLM-assisted / fuzzy — Q4a)**
- `plan_enhance_operations(stage, requirement)` — one structured LLM call (Stitcher gateway via
  `LLMAgentProxy` / `OpenAILike`, same env as `custom_cost`'s LLM calls) that maps the natural-
  language requirement to the best enhance operation(s) + the required fields the operator
  needs (join keys, imports, condition, etc.), grounded by the catalog from `list_operators`
  and the data-source metadata already gathered. Returns a typed plan the agent then turns into
  config via the authoring tools. Deterministic guard: refuse if the suggested operation is
  unknown for the stage or references columns not in the metadata.

**Authoring / validation / save (full scope — Q2)**
- `generate_lookup(business_dataset, cost_join_column, business_join_column, imports, providers,
  name="")` — deterministic assembler reusing SPC `enhance_lookup_rule` /
  `enrich_assembler`-style construction (validate-by-construction; refuse unknown import /
  shadowing rename).
- `generate_filter(column, operator, keep_or_drop, value=None, providers=None, name="",
  stage="enrich")` — deterministic, correct Exclude/Keep polarity by construction (reuse
  `ConfigGenerationCommonUtils.correct_filter_inversion` shape).
- `validate_config(stage, yaml_text)` — against SPC `EnhancePrepareConfigModelV1` /
  `EnhanceEnrichConfigModelV1` + grounding audit (`ConfigGenerationCommonUtils.ground_audit`).
- `save_config(stage, name, yaml_text)` — write to an output dir + validate (the only persist).

### 4. Wiring (`run.sh` + env), no pi-extension changes
- Add `CONFIG_GEN_PORT="${STITCHER_CONFIG_GEN_MCP_PORT:-8793}"`, start the server, add to
  `STITCHER_SUB_MCP_URLS`: `"config_generation":"http://127.0.0.1:${CONFIG_GEN_PORT}/mcp/"`.
- `_kill_port "$CONFIG_GEN_PORT"` + `_wait_mcp "$CONFIG_GEN_PORT"` (mirror custom_cost).
- Copy SOE env files into `pi_coding_agent/.soe-env/.env.local` + `.env.local.dev`
  (gitignored), and have `soe_context` load them (path resolved relative to the package so it
  works regardless of CWD). Document the copy step in README + run.sh preflight.

## Files to modify / create
- **New** `stitcher_mcp_service/stitcher/assistant_harness/sub_mcp_agents/config_generation/__init__.py`
- **New** `.../config_generation/config_generation_mcp_server.py` (`build_server`, `main`)
- **New** `.../config_generation/tools/__init__.py`
- **New** `.../config_generation/tools/soe_context.py` (env load + `WorkflowContext` + `ExecutorConfig`)
- **New** `.../config_generation/tools/operator_tools.py` (`list_operators`, `describe_operator`, `environment_context`)
- **New** `.../config_generation/tools/data_source_tools.py` (`list_data_sources`, `get_data_source_metadata`, `scan_data`)
- **New** `.../config_generation/tools/committed_config_tools.py` (`get_committed_config`, `derived_columns`)
- **New** `.../config_generation/tools/planning_tools.py` (`plan_enhance_operations`)
- **New** `.../config_generation/tools/authoring_tools.py` (`generate_lookup`, `generate_filter`, `validate_config`, `save_config`)
- **New** `stitcher_mcp_service/test/test_config_generation_tools.py`
- **Edit** `pi_coding_agent/run.sh` (start server, port, `STITCHER_SUB_MCP_URLS`, preflight copy of SOE env)
- **Edit** `pi_coding_agent/.gitignore` (ignore `.soe-env/`)
- **Edit** `pi_coding_agent/README.md` (document the new sub-MCP + SOE env copy)
- (optional) `stitcher_mcp_service/.../common/config.py` add `STITCHER_CONFIG_GEN_MCP_PORT`

## Reuse (existing, with paths)
- `common/client.py::StitcherClient`, `common/config.py::StitcherSettings`,
  `common/oidc_auth.py::OIDCAuth` — env scope + SWS connections (top-level pattern).
- `pi_agent_coding_harness/server/env_context.py::_load_soe_env_files` — the pattern for
  loading `.env.local`/`.env.local.dev` into `os.environ` so `ExecutorConfig()` resolves (port
  into `soe_context.py`; the harness restricts to `_NEEDED_VARS` to avoid the multi-line K8S
  blob — keep that).
- SOE **metadata operator**: `operator/metadata/metadata_consolidate_operator.py`
  `__read_business_dataset_schema__` / `__read_business_dataset_schema_for_datasource__` →
  `operator/extract/sub_operators/extract_ref_data.py::ExtractRefDataSubOperator`
  `read_database_schema` / `__extract_reference_dataframe_recursion__` / `supports_database_connector`.
- SOE **committed configs**: `common/vcs_repo.py::get_vsc_commit_dir` +
  `vcs_repo._load_all_stage_configs` / `git_helpers/stage_config_helpers.py::StageConfigHelpers`.
- SOE **workflow context**: `models/workflow_context.py::WorkflowContext` (pydantic — build by
  hand) + `schema/date_input.SimpleDateRange`, `schema/user_defined_configs.*ConfigSchema`.
- SPC **enhance models**: `pipeline_config_models/versions/v1_alpha/enhance/enhance_config.py`
  (`EnhancePrepareConfigModelV1` / `EnhanceEnrichConfigModelV1`, `TRANSFORM_UNION_TYPE`) and
  `sub_models/*` (Lookup, Mapping, Compute, Filter, Unpack, RemoveColumn, CostSimulator,
  AiAssistedMapping).
- SPC **utils**: `pipeline_config_models/ai/config_generation_agent/common.py::ConfigGenerationCommonUtils`
  (`is_display_query_intent`, `describe_business_datasets_for_user`, `correct_filter_inversion`,
  `ground_audit`, serializers) + `example_configs/enhance/*.yaml`.
- SPC **stage registry / operators catalog** (or mirror `sai_config_mcp`'s `_vendor/stage_registry.py`).
- LLM: `pipeline_config_models/ai/common/ai_agent_proxy` + `invoice_parser/utils/openai_utils.get_openai_client`
  / `parser_settings.get_parser_settings` (reuse the Stitcher gateway, like `custom_cost`).

## Steps
- [x] **Step 1 — SOE-as-is spike (30 min).** DONE. Spike script: `/tmp/spike_soe_as_is.py`
  (run from superrepo root with `stitcher_mcp_service/.venv/bin/python`). Results:
  - `ExecutorConfig()` constructs with `.env.local`+`.env.local.dev` loaded (sws_url, base_path,
    executor resolve). `WorkflowContext()` hand-builds cleanly (env/pipeline/month/agg_sql/dir_fmt).
  - All target SOE functions import with **no Temporal side effects**; the function bodies we'll
    call (`ExtractRefDataSubOperator.read_database_schema`, `__extract_reference_dataframe_recursion__`,
    `__load_data_connection__`) do NOT touch `workflow.info`/`activity.logger`/`activity.defn`.
    `get_vsc_commit_dir` is `async`, signature confirmed. **No shim / harness-vendored fallback needed.**
  - 📌 Refinement for Step 2: the env loader must ALSO load `WebserviceCommonSettings` vars —
    `VAULT_ROLE_TEMPLATE_CLASS`, `OPENID_SA_CLIENT_ID`, `OPENID_SA_CLIENT_SECRET`, `KEYCLOAK_URL`,
    `VAULT_URL`, `APP_BASE_URL`, `aws_role_name_suffix` — because `DataConnectionUtil`/`get_vsc_commit_dir`
    construct `WebserviceCommonSettings()` (the harness's `_NEEDED_VARS` was scoped to `ExecutorConfig`
    only). Live `DataConnectionUtil`/`get_vsc_commit_dir` calls are network-gated (Keycloak JWT +
    GitHub App auth) and need a real environment — verified constructibility only.
- [x] **Step 2 — `soe_context.py`.** DONE. `tools/soe_context.py` loads the broadened SOE env var set
  (ExecutorConfig + WebserviceCommonSettings) from `.soe-env/` (copied from SOE, gitignored) →
  `ExecutorConfig` → caches a hand-built `WorkflowContext`; resolves `pipeline_id` lazily via the
  client. Smoke-tested: 19 vars loaded, ExecutorConfig + WorkflowContext build, pipeline_id resolves.
- [x] **Step 3 — server skeleton.** DONE. `config_generation_mcp_server.py` (`build_server`/`main`,
  env-scoped via StitcherSettings/OIDCAuth/StitcherClient + shared assistant_harness token dir,
  default port 8793). Stub `tools/` modules (register(mcp, client, soe)). Wired into `run.sh`:
  `CONFIG_GEN_PORT` (8793), started + added to `STITCHER_SUB_MCP_URLS`, `_kill_port`/`_wait_mcp`
  (verified curl exit 0 on the MCP 307 like custom_cost). Server boots over HTTP; tool list is
  empty until Steps 4–8 register tools.
- [x] **Step 4 — operator + environment tools.** DONE. `operator_tools.py`: `list_operators`
  (prepares the full `EnhanceOperationType` vocabulary + purposes), `describe_operator` (full field
  spec + a REAL example from `example_configs/enhance/<stage>/`), `environment_context`
  (`soe.summary()`). Verified via `mcp.call_tool`.
- [x] **Step 5 — data source + metadata + scan tools.** DONE. `data_source_tools.py`: `list_data_sources`
  (SWS catalog via client), `get_data_source_metadata` (SOE `MetadataConsolidateOperator.__read_business_dataset_schema__`
  → columns+dtypes), `scan_data` (SOE `ExtractRefDataSubOperator.__extract_reference_dataframe_recursion__` /
  `read_database_schema`; group_by+value → $ split, columns → head sample). Fixed env loader to absolutize
  relative path vars (`SSL_CA_CERTIFICATE_PATH` etc.) against the SOE dir so `ExecutorConfig()` validates from any CWD.
  Verified wired end-to-end (reaches the Keycloak SA-JWT step; live-cred verification is network-gated).
- [x] **Step 6 — committed-config tools.** DONE. `committed_config_tools.py`: `get_committed_config`
  (SOE `get_vsc_commit_dir` + committed enhance config objects → compact per-stage op summary:
  type/name/scope/Lookup join keys+imports/filters) and `derived_columns` (index of columns the
  committed configs CREATE: Lookup import rename_to, Mapping/Compute column_name, AI-assisted
  target_column — the `x_*` bridge keys). `_committed`/`_derived_text` verified against a real
  `EnhanceEnrichConfigModelV1` (lookup -> `x_team` bridge found). Live fetch is network-gated.
- [x] **Step 7 — planning tool.** DONE. `planning_tools.py`: `plan_enhance_operations(stage,
  requirement, available_columns, business_datasets)` — ONE LLM call via `LLMAgentProxy`
  (`generate_llamaindex_pydantic_program`) → `EnhanceOperationPlan` (list of op drafts with
  fields), then a deterministic `_guard` that refuses unknown operation types and any reference to
  a column/business-dataset not in the provided metadata (every drop reported, never merged).
  Guard unit-tested (grounded op survives, fabricated dataset + unknown type refused).
- [x] **Step 8 — authoring tools.** DONE. `authoring_tools.py`: `generate_lookup` (validated
  Lookup via real SPC enhance model; refuses empty join / shadowing rename / unknown import),
  `generate_filter` (correct Include/Exclude polarity from keep_or_drop intent; null ops supported),
  `validate_config` (SPC model PASS/FAIL), `save_config` (validate + write to `.output/`, gitignored;
  added `soe.output_dir`). Manually verified: valid configs validate PASS; shadowing/unknown-import/
  empty-ops refuse; save round-trips. Adversarial tests written in `test/test_config_generation_tools.py`.
- [x] **Step 9 — docs + preflight.** DONE. README sub-MCP section documents the `config_generation`
  bundle (all 5 tool groups), the SOE-env copy steps, `activate_sub_mcp("config_generation")`;
  env-var table rows for `STITCHER_CONFIG_GEN_MCP_PORT` / `STITCHER_SOE_ENV_DIR` /
  `STITCHER_OUTPUT_DIR`. `run.sh` preflight warns if `.soe-env/` is missing (non-fatal). `.soe-env/`
  + `.output/` gitignored. `.soe-env` seeded with the two SOE env files. Final: 13 tools registered;
  `bash -n run.sh` OK. (Full `./run.sh` manual run needs `pi` CLI + real env creds — component-level
  e2e verified.)

## Verification
- **Unit/Adversarial tests** (`stitcher_mcp_service/test/test_config_generation_tools.py`):
  - `list_operators`/`describe_operator` cover all enhance operators for prepare + enrich.
  - `get_data_source_metadata` refuses an unknown dataset; returns columns for a known one
    (mock `WorkflowContext`/`ExtractRefDataSubOperator` where a live SWS call is unwanted).
  - `get_committed_config` returns a graceful "unscoped" note when env/pipeline missing; never
    raw YAML; summary includes existing Lookups' join keys.
  - `plan_enhance_operations` guard refuses an unknown operation / a column not in metadata
    (no live LLM — deterministic guard path).
  - `generate_lookup` refuses a shadowing rename and an unknown import; `generate_filter`
    produces correct Exclude/Keep polarity for `keep` vs `drop`; `validate_config` FAILs a
    malformed config; `save_config` round-trips through `validate_config`.
- **SOE-as-is spike**: Step 1 script runs against a real dev environment and prints a business
  dataset's schema + the committed enhance configs (gated behind `STITCHER_ENVIRONMENT_ID`).
- **End-to-end manual**: `./run.sh`, then in pi: `activate_sub_mcp("config_generation")` →
  `list_operators("enrich")` → `list_data_sources` → `get_data_source_metadata("<name>")` →
  `get_committed_config(stage="enrich")` → `plan_enhance_operations("enrich", "enrich my AI
  spend with the owning team from the app metadata table")` → `generate_lookup(...)` →
  `validate_config("enrich", ...)` → `save_config("enrich", "130_team_mapping", ...)`.
- **Sub-MCP pristine-list check**: at startup only top-level tools + `list_sub_mcp_servers` +
  `activate_sub_mcp` are active; the config_generation tools appear only after activation
  (verify via `list_sub_mcp_servers` tool counts).

## OPEN QUESTIONS (resolved)
- Q1 name → `config_generation`. Q2 full scope (grounding + planning + authoring/validate/save).
- Q3 metadata via SOE metadata operator; scan/read via SOE extract/read by connection params.
- Q4 (a) LLM-assisted `plan_enhance_operations`. Q5 reuse SOE functions for committed state;
  copy `.env.local`/`.env.local.dev` from SOE into the agent so `ExecutorConfig()` works as-is.

## Fix-session notes (post-handoff)
### Gap #1 — auth_tenant / pipeline plumbing — DONE
The transcript spiral ("scan_data → Realm does not exist", then unbounded source-grep exploration) was
driven by `auth_tenant` being unset: SOE `DataConnectionUtil` authenticates at Keycloak as
`org_id=auth_tenant`, and a missing/fallback (`"config-gen"`) realm yields the cryptic `Realm does not
exist`. Applied:
- `tools/soe_context.py`: added `has_tenant` + `tenant_error()` (precise, actionable message naming
  `STITCHER_AUTH_TENANT`); `summary()` flags `auth_tenant: (UNSET → … will FAIL…)`.
- `tools/data_source_tools.py` `_build_data_connection_util` + `tools/committed_config_tools.py`
  `_fetch`: refuse EARLY with the tenant hint instead of reaching Keycloak.
- `run.local.sh`: `STITCHER_AUTH_TENANT=stitcherai-wsmo5` (this env's realm, from sai-plugin-e2e skill)
  + optional `STITCHER_PIPELINE_ID`.
- `run.sh`: exports `STITCHER_AUTH_TENANT` / `STITCHER_PIPELINE_ID` / `STITCHER_GIT_BRANCH`; preflight
  warns (non-fatal) when `STITCHER_AUTH_TENANT` unset.
- README: SOE-auth paragraph + env-var rows for the three vars.
- Tests: 3 new adversarial boundaries — `tenant_error` present/empty, `_build_data_connection_util`
  refuses-before-Keycloak, `get_committed_config` returns the tenant hint without reaching pipeline
  resolve (20 total, green).

### Gap #2 — plan guard over-refused Lookup imports — DONE
`plan_enhance_operations._guard` validated ALL referenced columns (join keys + lookup imports +
target/condition) against a single `available_columns` (the COST side). Because Lookup imports live
on the BUSINESS dataset, valid imports (`owning_team`, `resource_id`) were dropped whenever only cost
columns were passed. The whole `condition` string was also exact-matched (over-refused any filter).
Applied:
- `_guard` now takes a `business_dataset_columns` arg and validates **per side**:
  * cost side (`cost_dataset_join_column`, Mapping/Compute `column_name`/`source_column`) against
    `available_columns`;
  * business side (`business_dataset_join_column`, `import_columns[].name`) against
    `business_dataset_columns`.
  A side's refs are only validated when that side's column set was provided, so cost-only input no
  longer nukes valid imports. Filter `condition` (free-form expression) and op OUTPUT columns
  (`rename_to`, Mapping/Compute/AI target) are not forced into an input allowlist.
- `plan_enhance_operations` takes + wires `business_dataset_columns` (into the LLM prompt, the guard,
  and the returned `guard` block).
- Tests: `_COLS`/`_BIZ_COLS` now reflect the realistic split; new regression tests — kept Lookup when
  the import is only on the business side (the gap-#2 proof), refused import not in business columns,
  refused fabricated COST column with reason scoped to 'COST dataset' (21 total, green).

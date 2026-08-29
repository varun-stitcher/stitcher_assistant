# stitcher_mcp_service — module map & workflows

One process (`mcp_server.py`, port 8791) serves the top-level coordinator MCP plus three
sub-MCPs mounted as ASGI sub-apps. The pi agent (`pi_coding_agent/run.sh`) is the only
intended consumer; Claude Code / any MCP client can also point at `/mcp/`.

## Layout

```
stitcher/assistant_harness/
├── mcp_server.py            # builds the FastAPI app: top-level MCP + every sub-MCP mount
├── common/                  # shared infra (no MCP tools)
│   ├── config.py            #   StitcherAssistantConfig (STITCHER_* env scope)
│   ├── client.py            # StitcherClient (SWS API)
│   ├── oidc_auth.py         # Keycloak login + token refresh (state in ~/.stitcher/)
│   ├── artifacts.py         # user-visible parquet/JSON artifact writers (never raise)
│   └── soe_context.py       # SOE metadata/scan/committed-config bridge
├── tools/                   # TOP-LEVEL coordinator tools (small, always-on surface)
│   ├── file_tools.py        #   ping / list_directory / read_text_file / read_pdf
│   ├── auth_tools.py        #   auth_get_url / auth_status / auth_set_token
│   ├── stitcher_tools.py    #   stitcher_context / connections / pipeline
│   ├── data_source_tools.py #   list_data_sources / get_data_source_metadata / scan_data
│   ├── committed_config_tools.py  # get_committed_config / derived_columns
│   ├── result_capture.py    #   submit_result (gated: STITCHER_ENABLE_RESULT_CAPTURE=1)
│   └── focus_official_validation_tools.py  # validate_focus_official (official validator)
├── sub_mcp_agents/          # heavy domain bundles — INACTIVE until activated on demand
│   ├── custom_cost/         #   FOCUS: extract → convert → normalize → validate → save
│   ├── config_generation/   #   enhance/enrich stage config authoring
│   └── chargeback/          #   chargeback task tools
└── agent_gateway/           # the pi-agent gateway (separate process, run_gateway.sh)
    ├── gateway.py           #   two surfaces: MCP (:8792/mcp) + OpenAI (:8880/v1)
    ├── agent_mcp_server.py  #   task-typed orchestrator tools
    ├── openai_server.py     #   /v1/chat/completions + /v1/models + /chat UI
    ├── agent_runner.py      #   headless pi turn driver + per-call tool-MCP spawn
    └── gateway_chat.html    #   demo chat UI (served by openai_server.py)
```

## The custom_cost FOCUS workflow

```
normalize_to_focus(file_path=…)                    # extract → LLM plan-gen → normalize
  ├─ raw_parquet / normalized_parquet  → $FOCUS_PARQUET_OUTPUT_DIR (user artifacts)
  ├─ validate=true → internal FOCUS v1.2 checker (fast, ~10 mandatory columns)
  └─ official_validate=true → validate_focus_official on the normalized frame
        └─ FinOps Foundation focus_validator (578 rules) in an isolated subprocess
           (focus_validator_local/, Python 3.11 patch — see HANDOFF.md)
           → compact summary in-band, FULL report on disk (report_path)

validate_and_repair_focus(raw_df_json | file_path, billing_currency, official_validation)
  └─ deterministic repair (static-value columns only) + re-validation + rollback

Loop for fixing failures (the tools tell you this via `next_steps`):
  1. generate_focus_plans          — correct the source→FOCUS mapping
  2. simulate_normalize_config     — verify the corrected config on raw data (zero LLM)
  3. save_focus_config             — persist as verified YAML ($FOCUS_CONFIG_OUTPUT_DIR)
  4. validate_focus_official       — official conformance (compact summary + report_path)
```

## Artifacts (where files land)

| Env var | Default | Contents |
|---|---|---|
| `FOCUS_PARQUET_OUTPUT_DIR` | `<tmp>/stitcher-artifacts/stitcher-focus-parquet` | raw/normalized/validated parquet + official conformance reports (JSON) |
| `FOCUS_CONFIG_OUTPUT_DIR` | `<tmp>/stitcher-artifacts/stitcher-focus-configs` | saved normalize configs (YAML, loader-verified) |
| `FOCUS_VALIDATOR_HOME` | sibling `focus_validator_local/` | official validator clone + venv |

Artifacts are timestamped (reruns never overwrite), writers never raise, and tool
results carry only paths + compact summaries (context discipline: the full 578-rule
report is ~500KB and must never be inlined into an agent conversation).

## Auth state

OIDC tokens live in `~/.stitcher/` (user level) — one login shared by the top-level
server and all sub-MCPs, survives worktree switches, never packaged.

## Testing

```bash
cd stitcher_mcp_service && .venv/bin/python -m pytest test/ -q
```

Transcript forensics (why did the agent do X?):

```bash
python3 pi_coding_agent/scripts/eval_focus_workflow_transcript.py <session.jsonl>
```

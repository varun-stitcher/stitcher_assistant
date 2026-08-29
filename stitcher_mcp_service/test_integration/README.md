# Full-pipeline integration tests (prompt → agent → tools → outcome)

These tests exercise the REAL end-to-end pipeline the way a user does: a natural-language
prompt goes through a headless pi agent turn (`AgentRunner`), which spawns the real combined
MCP server, calls the real tools, and produces a final answer. They exist to catch
**regressions in the whole loop** — tool wiring, agent steering, artifact persistence — that
unit tests on individual tools cannot see.

## How assertions work (the contract)

Because the final output is LLM-generated, prose is **never asserted exactly**. The suite
asserts, in order of strength:

1. **Deterministic evidence** (asserted exactly):
   - the session **transcript** proves which tools ran and what they *returned*
     (tool results are machine-generated — their fields can be asserted);
   - **artifacts on disk**: parquet files are read back with polars and checked for real
     FOCUS columns + row counts; official-validation report JSONs must exist;
   - **absence checks**: no raw traceback in any user-facing text; no fabricated success
     for missing data.
2. **Signal-level prose checks** (any-of regexes): the answer must *show a table of
   converted data* (markdown table with a FOCUS column header), must *surface violations
   and offer next steps*, must give a *pass/fail verdict* — the wording may vary freely.

## Running

```bash
# creds + scope (gitignored run.local.sh exports)
set -a; source <(grep '^export ' ../pi_coding_agent/run.local.sh); set +a

STITCHER_INTEGRATION=1 .venv/bin/python -m pytest test_integration/ -v -s
```

Optional: `STITCHER_INTEGRATION_MODEL` overrides the agent model for these turns
(verbatim if it contains `/`, e.g. `router/glm53-flash` — a local router profile).
This matters: model quality directly affects convergence (the default gateway model
flailed — 300+ tool calls on inputs a stronger model finished in 4). The turn-count
signal (`MAX_SANE_TURNS`) catches exactly that steering regression.

The suite is **opt-in**: without `STITCHER_INTEGRATION=1` (or with missing `STITCHER_*`
scope vars) every live test *skips with the reason named* — it never silently passes or
runs. `test_unscoped_turn_refused_without_llm` runs everywhere with no creds (it asserts
the deterministic refusal before any LLM spend).

## What each test covers

| Test | Prompt | Key signals |
|---|---|---|
| `test_convert_to_focus_shows_table` | "convert … and show me the converted data" | `normalize_to_focus` ran without error; parquet artifact readable with FOCUS columns; **answer renders a markdown table of converted data** |
| `test_missing_billing_currency_reports_violation_and_next_steps` | data with `BillingCurrency` removed | validation tool engaged; violation surfaced; repair/next-steps offered (adversarial: must NOT report success) |
| `test_official_validation_report_artifact` | "run the official FOCUS validation" | `validate_focus_official` ran; `report_path` in the tool result; report JSON persisted; verdict in prose |
| `test_missing_file_is_gracefully_refused` | nonexistent file | honest "not found"-class signal; **no traceback leak; no fabricated table** |
| `test_unscoped_turn_refused_without_llm` | no environment scope | deterministic `unscoped` refusal, zero turns (no LLM) |

**Bugs these tests have already caught** (each now fixed + covered by unit tests):
- transcript parser vs real pi format (`toolName` on the message, not the block);
- `save_focus_config` broken relative import (post-regroup) — every live save crashed;
- `save_focus_config` refusing JSON-string payloads (models hand back stringified JSON);
- `pi -p` answer-surface blindness (model believes tool output is visible to the caller →
  final-message-must-contain-the-deliverable steering in AGENT_SYSTEM.md).

## Adding a new use case

1. Add the prompt + the deterministic checks first (which tool MUST have run, which
   artifact MUST exist). If you can't name the tool, the use case isn't specified yet.
2. Then add prose signals — write 3+ acceptable regexes, and ask: "would a *correct*
   answer from a different model match?" If not, the signal is over-fit.
3. Adversarial inputs (bad data, missing files) get a *must-NOT-succeed* assertion.

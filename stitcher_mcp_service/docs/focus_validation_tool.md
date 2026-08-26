# `validate_and_repair_focus` — the FOCUS validation + repair tool

Safety boundary between "the normalizer produced *something*" and "the normalizer
produced *FOCUS-compliant data*". Lives in
`stitcher_assistant/stitcher_mcp_service/stitcher/assistant_harness/tools/focus_validation_tools.py`,
registered alongside `normalize_to_focus` in `mcp_server.py`. It is the
refuse-by-construction layer that closes the two gaps found in the audit of
`normalize_to_focus`:

1. **Missing FOCUS column / failing check was reported as `success=true`.**
   This tool returns `compliant=false` on any missing `MANDATORY_COLUMNS` entry
   or any `severity=FAIL` check. No "passed-but-broken".
2. **Validator exceptions were swallowed into `None` (weaken-by-fallback).**
   This tool **never swallows** a `validate_focus` exception — a validator crash
   is a hard failure with the exception name in the client error and the full
   traceback in the server log.

## Two layers (fuzzy vs deterministic)

* **Fuzzy (LLM) — one decision only:** `_maybe_infer_currency(raw_df)` infers the
  invoice's billing currency from the raw data, and only when the caller sets
  `llm_repair=true`. Its output is hard-gated to a 3-char uppercase ISO 4217 code;
  anything else is dropped (`UNKNOWN`). No LLM call happens unless that path runs.
* **Deterministic (machine):** everything else — running the real `validate_focus`,
  deciding what "mandatory" means (`MANDATORY_COLUMNS` from the validator),
  applying a static-value repair (`_apply_static_repair` = a `pl.lit` constant
  column; the LLM never writes the frame), re-validating the repair, and rolling
  it back if it doesn't verify.

With `llm_repair=false` (the default) the tool makes **zero** LLM calls — the human
escape hatch to run the deterministic core standalone.

## Repair policy (refuse-by-construction)

* `BillingCurrency` is the only repairable gap today. Pass `billing_currency="GBP"`
  (explicit, deterministic) or enable `llm_repair` (gated inference).
* **No silent default:** a missing currency with neither an explicit value nor a
  confident inference stays missing and is reported. We never hardcode `USD`.
* Every repair is re-validated; the `presence_<col>` check passing is the proof.
  If it does not verify, the repair is **rolled back** (`repairs_rolled_back`) and
  the frame reverts to the initial one — a partial repair is never silently kept.
* Non-static gaps (e.g. a failing `ListCost = ListUnitPrice × PricingQuantity`
  check) are **not fabricated** — they remain unresolved and keep `compliant=false`.

## Inputs

* `file_path` or `pdf_b64`+`filename` → re-extract + normalize inside the tool.
* `raw_df_json` → validate/repair an existing normalized frame (from a prior
  `normalize_to_focus` `..._df_summary` / sample rows), with zero LLM calls when
  `llm_repair=false`.

## Running the tests / escape hatch

```bash
USE_STITCHER_MODEL=true STITCHER_MODEL_API_KEY=<key> \
  .venv/bin/python -m pytest test/test_focus_validation_tools.py
```

The tests are **adversarial**, one per boundary (see the docstrings):
B1 validator crash ⇒ hard failure (not `None`); B2 missing mandatory ⇒ non-compliant;
B3 no silent currency fallback; B4 explicit repair is deterministic + zero LLM +
invalid code rejected up front; B5 unverifiable repair rolled back; B6 compliant frame
needs no repair.

## Known upstream gap surfaced by this tool

`validate_focus`'s cost-equation check (`patternC_*`) fails with
`mul operation not supported for dtypes str and str` when the normalizer emits a
FOCUS numeric column as a String (common). This is a real validator limitation in
`stitcher_pipeline_common/focus_spec_validator.py` (`_col_float` only casts
numeric/Decimal, not String). This tool *surfaces* it as an unresolved gap rather
than hiding it; hardening `_col_float` to cast string numerics is a follow-up in
that repo.

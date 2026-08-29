"""FOCUS official-validator tool — runs the FinOps Foundation ``focus_validator``.

Why this exists
---------------
``validate_and_repair_focus`` (focus_validation_tools.py) validates against the
*internal* FOCUS v1.2 checker in stitcher_pipeline_common. This tool adds the
**official** FinOps Foundation validator (github.com/finopsfoundation/focus_validator,
v2.2.1) as a second, independent opinion — and, unlike the internal validator, it
returns the FULL conformance report (every rule, every check) suitable for LLM
analysis.

Architecture — isolated subprocess (deliberate, do not "simplify")
------------------------------------------------------------------
focus-validator 2.2.1 pins ``numpy<2`` while the main service venv runs
numpy 2.x (SOE's transitive requirement) — installing it in-process would
downgrade numpy across the shared stack. The library also needs a small
Python 3.11 compatibility patch (PEP 701 f-string + package-data path), which
lives in a local clone at ``FOCUS_VALIDATOR_HOME`` (default
``<worktree>/../focus_validator_local``, gitignored). The tool therefore:

  1. materializes the input (file_path, or raw_df_json → temp CSV/Parquet)
  2. spawns ``<fv-venv>/bin/python fv_driver.py <job.json>`` as a subprocess
  3. parses the JSON conformance report the driver emits
  4. (gated, analyze=true) sends a COMPACT deterministic summary to the LLM
     for analysis. With analyze=false (default) the tool makes ZERO LLM calls
     — the human escape hatch / deterministic core runs standalone.

Constitution (enforced by the code below):
  1. Never swallow a subprocess/validator crash — hard failure with a clear
     client error; full traceback only in the server log.
  2. Missing input / unreadable file / unsupported format ⇒ refused up front,
     never a simulated report.
  3. The LLM only *analyzes* the deterministic report; it never alters,
     fabricates, or re-runs validation. analyze=false ⇒ zero LLM calls.
  4. One artifact per run — an existing frame (raw_df_json) is written to a
     temp file as-is; no re-extraction, no hidden normalization.
  5. Failures degrade to a clear error, never a fake "compliant" result.

Usage
-----
    validate_focus_official(raw_df_json=<normalize_to_focus output>)   # zero LLM
    validate_focus_official(file_path="/data/focus.parquet", analyze=true)
    validate_focus_official(file_path="s3://bucket/sample.csv")         # s3fs-fs
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import tempfile
import time
from pathlib import Path
from typing import Any

import polars as pl
from fastmcp import Context, FastMCP

logger = logging.getLogger(__name__)

# Max characters of the compact report summary handed to the LLM (deterministic truncation).
_MAX_LLM_REPORT_CHARS = 12_000
# Max violating-rule entries included in the compact summary.
_MAX_LLM_FAILING_RULES = 30
# Subprocess timeout in seconds (the validator loads 500+ rules; allow slack).
_VALIDATOR_TIMEOUT_SECONDS = 300


def _now() -> float:
    return round(time.time(), 2)


# ── Environment resolution (refuse loudly, never simulate) ──────────────────


def _fv_home() -> Path:
    """Directory of the local focus_validator clone (driver + venv live here)."""
    env = os.environ.get("FOCUS_VALIDATOR_HOME")
    if env:
        home = Path(env)
    else:
        # default: sibling of the stitcher_assistant worktree
        # tools/ → assistant_harness → stitcher → stitcher_mcp_service → stitcher_assistant → <worktree>
        home = Path(__file__).resolve().parents[5] / "focus_validator_local"
    if not home.is_dir():
        raise FileNotFoundError(
            f"FOCUS validator clone not found at {home} — set FOCUS_VALIDATOR_HOME or bootstrap it "
            "(git clone finopsfoundation/focus_validator + the 3.11 patches + fv_driver.py)."
        )
    return home


def _fv_python(home: Path) -> Path:
    py = Path(os.environ.get("FOCUS_VALIDATOR_PYTHON", str(home / ".venv" / "bin" / "python")))
    if not py.is_file():
        raise FileNotFoundError(f"FOCUS validator venv python not found at {py} — run its bootstrap venv first.")
    return py


# ── Deterministic core: input materialization ───────────────────────────────


def _rows_to_frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    """Tolerant frame reconstruction from JSON rows (agent-supplied, mixed types).

    Hand-assembled sample_rows routinely mix empty strings with numbers in the
    same column (and polars 1.x is strict when a column mixes str and float).
    Rebuild column-by-column:
      * ``""``/whitespace-only → ``null``;
      * all values numeric → Float64 (keeps the validator's cost-equation rules
        working — a String column would make them fail);
      * anything else (or any mix with a string) → Utf8, numbers stringified.
    Never fabricates data: empty cells become null, not a default value.
    """
    if not isinstance(rows, list) or not rows:
        raise ValueError("raw_df_json carries no rows (provide sample_rows or rows).")
    if not all(isinstance(r, dict) for r in rows):
        raise ValueError("raw_df_json rows must be JSON objects (list of {column: value}).")
    keys: list[str] = []
    for r in rows:
        for k in r:
            if k not in keys:
                keys.append(k)
    columns: dict[str, pl.Series] = {}
    for k in keys:
        vals = [r.get(k) for r in rows]
        cleaned = [None if v is None or (isinstance(v, str) and v.strip() == "") else v for v in vals]
        types = {type(v) for v in cleaned if v is not None and not isinstance(v, bool)}
        if types and types <= {int, float}:
            columns[k] = pl.Series(k, cleaned, dtype=pl.Float64, strict=False)
        else:
            columns[k] = pl.Series(k, [None if v is None else str(v) for v in cleaned], dtype=pl.Utf8)
    return pl.DataFrame(columns)


def _extract_rows_blob(raw_df_json: str) -> tuple[list[dict[str, Any]], str]:
    """Parse ``raw_df_json`` and locate the row list, tolerating several shapes.

    Accepted (in order):
      * ``{"sample_rows": [...]}``                — the documented contract;
      * ``{"rows": [...]}``;
      * the FULL ``normalized_df_summary`` object — ``{"shape":…, "columns":…,
        "sample_rows": […]}`` — pass it verbatim from normalize_to_focus;
      * the FULL ``normalize_to_focus`` result     — ``{…, "normalized_df_summary":
        {…, "sample_rows": […]}}`` — also pass verbatim; we find the rows.
    Returns (rows, shape_name) — shape only for better error messages.
    """
    try:
        blob = json.loads(raw_df_json)
    except json.JSONDecodeError as e:
        raise ValueError(
            f"raw_df_json is not valid JSON: {e}. Do NOT hand-build the payload — pass the "
            "normalized_df_summary (or the whole normalize_to_focus result) VERBATIM as a JSON string."
        ) from e
    if isinstance(blob, list):
        return blob, "list"
    if not isinstance(blob, dict):
        raise ValueError("raw_df_json must be a JSON object (or array of row objects).")
    for shape, keys in (
        ("result", ("normalized_df_summary",)),
        ("summary", ("sample_rows",)),
    ):
        node: Any = blob
        for key in keys:
            node = node.get(key) if isinstance(node, dict) else None
            if node is None:
                break
        if isinstance(node, dict) and (node.get("sample_rows") or node.get("rows")):
            return node.get("sample_rows") or node.get("rows"), shape
        if isinstance(node, list) and node:
            return node, shape
    rows = blob.get("sample_rows") or blob.get("rows")
    if not rows:
        raise ValueError(
            "raw_df_json carries no rows. Provide the normalized_df_summary from normalize_to_focus "
            'VERBATIM (it already contains sample_rows), or {"sample_rows": [...]} / {"rows": [...]}.'
        )
    return rows, "blob"


def _write_input(raw_df_json: str | None, file_path: str | None, workdir: str) -> tuple[str, str, bool]:
    """Materialize the input dataset to a file the validator can read.

    Returns (path, data_format, is_temp). Refuses anything that carries no data —
    no silent empty-file fallback.
    """
    if raw_df_json:
        rows, _shape = _extract_rows_blob(raw_df_json)
        df = _rows_to_frame(rows)
        if df.is_empty():
            raise ValueError("raw_df_json carries no rows (provide sample_rows or rows).")
        out = os.path.join(workdir, "input.parquet")
        df.write_parquet(out)
        return out, "parquet", True

    assert file_path  # caller validates
    if file_path.startswith(("s3://", "gs://", "abfs://", "az://")):
        # Datalake sample: read via polars (s3fs/gcsfs already deps of the service)
        df = pl.read_parquet(file_path)
        if df.is_empty():
            raise ValueError(f"datalake sample at {file_path} has no rows.")
        out = os.path.join(workdir, "input.parquet")
        df.write_parquet(out)
        return out, "parquet", True

    p = Path(file_path)
    if not p.is_file():
        raise FileNotFoundError(f"data file not found: {file_path}")
    suffix = p.suffix.lower()
    if suffix == ".parquet":
        return str(p), "parquet", False
    if suffix == ".csv":
        return str(p), "csv", False
    raise ValueError(f"unsupported data format {suffix!r} (expected .csv or .parquet)")


def _summarize_report(report: dict[str, Any], max_failing: int) -> dict[str, Any]:
    """Deterministic COMPACT summary: counts + failing rules only (never fabricated).

    The official validator's by_rule_id entries carry a boolean ``ok`` flag (the
    internal validator uses ``passed`` — both accepted here so the summary works
    against either report shape).

    Context discipline: the full report is ~500KB for a real rule set — enough to
    burn most of an agent's context window in one tool result. This summary keeps
    ONLY the failing rules, and even those in COMPACT form (rule_id → violations
    + message, truncated; no children/timing/sql). The complete report is
    persisted to a JSON file and returned as ``report_path``.
    """

    def _failed(entry: Any) -> bool:
        return isinstance(entry, dict) and (entry.get("ok") is False or entry.get("passed") is False)

    def _compact(entry: dict[str, Any]) -> dict[str, Any]:
        details = entry.get("details") or {}
        message = details.get("message") or entry.get("message") or ""
        return {
            "violations": details.get("violations"),
            "message": (message[:300] + "…") if len(message) > 300 else message,
        }

    by_rule = report.get("by_rule_id", {})
    failing = {rid: _compact(e) for rid, e in by_rule.items() if _failed(e)}
    skipped = {
        rid: e
        for rid, e in by_rule.items()
        if isinstance(e, dict) and not _failed(e) and (e.get("details", {}) or {}).get("skipped")
    }
    return {
        "rules_version": report.get("rules_version"),
        "model_version": report.get("model_version"),
        "focus_dataset": report.get("focus_dataset"),
        "rows_validated": report.get("data_row_count"),
        "total_rules": len(by_rule),
        "failing_rules": dict(list(failing.items())[:max_failing]),
        "failing_rules_truncated": len(failing) > max_failing,
        "failing_count": len(failing),
        "skipped_count": len(skipped),
        "skipped_rules_sample": dict(list(skipped.items())[:10]),
        "compliant": len(failing) == 0,
    }


# ── Gated LLM analysis (the ONE fuzzy step) ─────────────────────────────────


async def _analyze_report(summary: dict[str, Any]) -> dict[str, Any]:
    """LLM analysis of the conformance report. Gated: caller opts in via analyze=true.

    The ENTIRE LLM path (settings, client, proxy, call) is inside the try: an
    analysis failure (missing API key, gateway down, timeout) degrades to
    ``analysis_error`` on an otherwise-intact deterministic result — it must
    never crash the tool or corrupt the report.
    """
    try:
        from llama_index.core.llms import ChatMessage, MessageRole

        from stitcher.pipeline.common.invoice_parser.parser_settings import get_parser_settings
        from stitcher.pipeline.common.invoice_parser.utils.openai_utils import get_openai_client
        from stitcher.pipeline.common.pipeline_config_models.ai.common.ai_agent_proxy.base import LLMAgentProxy

        settings = get_parser_settings()
        client = get_openai_client()
        proxy = LLMAgentProxy(
            model=settings.task_model,
            client=client,
            sai_product="custom_cost",
            sai_product_step="focus_official_validation",
        )
        report_text = json.dumps(summary, default=str)[:_MAX_LLM_REPORT_CHARS]
        prompt = (
            "You are a FinOps data quality analyst. Below is the conformance report from the official\n"
            "FOCUS (FinOps Open Cost and Usage Specification) validator for a cost dataset.\n"
            "Analyze it: (1) state whether the dataset is FOCUS-conformant; (2) group the failing rules\n"
            "by root cause (missing/misnamed columns, wrong types, enum violations, cross-column logic);\n"
            "(3) list concrete remediation steps, most impactful first; (4) note any skipped rules and why\n"
            "they may matter. Be factual: do NOT invent rules or claim checks that are not in the report.\n\n"
            f"Report: {report_text}\n"
        )
        text = await asyncio.to_thread(
            proxy.generate_text,
            messages=[ChatMessage(role=MessageRole.USER, content=prompt)],
            temperature=0.2,
            max_tokens=1500,
            attributes={"purpose": "focus_official_validation", "step": "analyze_report"},
        )
        return {"analysis": (text or "").strip() or None}
    except Exception as e:  # noqa: BLE001 — analysis failure must NOT corrupt the deterministic result
        logger.warning("FOCUS report analysis LLM call failed: %s", e)
        return {"analysis": None, "analysis_error": str(e)}


# ── Registration ────────────────────────────────────────────────────────────


async def _run_validator_subprocess(
    fv_home: Path, fv_python: Path, data_file: str, fmt: str, rules_version: str, workdir: str
) -> dict[str, Any]:
    """Run fv_driver.py in the isolated venv and return {'success', 'report'} or an error dict.

    Never raises: every failure mode (env missing, launch failure, timeout, crash,
    unparsable report) comes back as ``success=False`` with a clear error — the
    caller decides whether that is fatal (standalone tool) or additive
    (workflow hook reports it as ``official_validation_error``).
    """
    output_file = os.path.join(workdir, "result.json")
    job = {
        "data_file": data_file,
        "data_format": fmt,
        "rules_version": rules_version,
        "focus_dataset": "CostAndUsage",
        "output_file": output_file,
        "show_violations": True,
        "block_download": True,
    }
    job_file = os.path.join(workdir, "job.json")
    Path(job_file).write_text(json.dumps(job))

    try:
        proc = await asyncio.create_subprocess_exec(
            str(fv_python),
            str(fv_home / "fv_driver.py"),
            job_file,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=str(fv_home),
        )
    except FileNotFoundError as e:
        return {"success": False, "error": f"failed to launch FOCUS validator subprocess: {e}"}

    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=_VALIDATOR_TIMEOUT_SECONDS)
    except TimeoutError:
        proc.kill()
        return {"success": False, "error": f"FOCUS validator timed out after {_VALIDATOR_TIMEOUT_SECONDS}s."}

    if proc.returncode != 0:
        detail = (stdout or b"").decode(errors="replace")[-2000:]
        return {
            "success": False,
            "error": f"FOCUS validator crashed (exit {proc.returncode}). Full traceback is in the server log.",
            "validator_stderr": detail,
        }
    if not Path(output_file).is_file():
        return {"success": False, "error": "FOCUS validator produced no report (driver exit 0 but no result file)."}
    try:
        report = json.loads(Path(output_file).read_text())
    except json.JSONDecodeError as e:
        return {"success": False, "error": f"FOCUS validator report is not valid JSON: {e}"}
    return {"success": True, "report": report}


def _persist_report(report: dict[str, Any], source: str) -> str | None:
    """Persist the FULL conformance report as a JSON artifact; return its path.

    The complete 578-rule report is far too large to inline in a tool result
    (~500KB — would burn most of an agent's context). It goes to a user-visible
    file next to the parquet artifacts ($FOCUS_PARQUET_OUTPUT_DIR, default
    <tmp>/stitcher-focus-parquet) so users — and a follow-up tool call — can
    read it without it ever entering the conversation. Never raises.
    """
    from ..common import artifacts

    return artifacts.persist_json(report, "focus-report", source, "FOCUS_PARQUET_OUTPUT_DIR", "stitcher-focus-parquet")


async def run_official_on_df(
    df: pl.DataFrame,
    rules_version: str = "1.2",
    max_failing_rules: int = 30,
) -> dict[str, Any]:
    """Deterministic official validation of an in-memory polars frame.

    The workflow entry point: custom_cost tools (``normalize_to_focus`` stage 5,
    ``validate_and_repair_focus`` post-repair) call this on their final frame.
    Zero LLM calls. Never raises — a missing/broken validator environment comes
    back as ``success=False`` + ``error`` so callers can report it explicitly
    without losing their own deterministic result.
    """
    try:
        fv_home = _fv_home()
        fv_python = _fv_python(fv_home)
    except FileNotFoundError as e:
        return {"success": False, "error": str(e), "hint": "Bootstrap the focus_validator clone+venv."}

    with tempfile.TemporaryDirectory(prefix="focus_official_") as workdir:
        data_file = os.path.join(workdir, "input.parquet")
        df.write_parquet(data_file)
        run = await _run_validator_subprocess(fv_home, fv_python, data_file, "parquet", rules_version, workdir)

    if not run["success"]:
        return run
    summary = _summarize_report(run["report"], max_failing_rules)
    return {
        "success": True,
        "validator": "finopsfoundation/focus_validator (local 3.11 clone, v2.2.1)",
        "compliant": summary["compliant"],
        "summary": summary,
        "report_path": _persist_report(run["report"], source="workflow"),
    }


async def validate_focus_official(
    ctx: Context,
    file_path: str | None = None,
    raw_df_json: str | None = None,
    rules_version: str = "1.2",
    data_format: str | None = None,
    max_failing_rules: int = 30,
    analyze: bool = False,
    include_full_report: bool = False,
) -> dict[str, Any]:
    """Validate a FOCUS dataset with the OFFICIAL FinOps Foundation focus_validator and report conformance.

    Deterministic core (zero LLM by default): materializes the input, runs the
    official validator (FOCUS 1.2 rule set) in an isolated subprocess, and returns
    the full per-rule conformance report plus a compact summary.

    Inputs (exactly one):
      * file_path   — path to a .csv or .parquet file (also s3:// / gs:// URIs;
                      datalake objects are sampled to a local temp parquet first).
      * raw_df_json — pass the ``normalized_df_summary`` from ``normalize_to_focus``
                      VERBATIM (it already contains ``sample_rows``), or the WHOLE
                      normalize_to_focus result, or ``{"sample_rows": [...]}`` /
                      ``{"rows": [...]}``. Mixed-type samples are tolerated:
                      empty strings → null, numeric columns → Float64, else string.

    Set analyze=true to have the LLM analyze the conformance report (root causes +
    remediation). analyze=false (default) keeps the call fully deterministic.

    CONTEXT DISCIPLINE: the result contains only the COMPACT summary (failing
    rules, truncated messages). The full 578-rule report (~500KB) is persisted
    to ``report_path`` — read it with a file tool if needed. Set
    include_full_report=true ONLY for scripting, never from an agent: inlining
    it consumes the conversation's context window.

    Refuses by construction: missing/unreadable input or a validator crash is a
    hard failure — never a fabricated or "simulated" report.
    """
    t0 = time.time()

    def _err(message: str, **extra: Any) -> dict[str, Any]:
        return {"success": False, "error": message, "elapsed_seconds": _now() - t0, **extra}

    # ── Input validation (refuse up front, no subprocess wasted) ──
    if bool(file_path) == bool(raw_df_json):
        return _err("Provide exactly one of file_path or raw_df_json.")
    if rules_version and not rules_version.replace(".", "").isdigit():
        return _err(f"rules_version must look like '1.2', got {rules_version!r}.")
    if data_format and data_format.lower() not in ("csv", "parquet"):
        return _err(f"data_format must be 'csv' or 'parquet', got {data_format!r}.")

    # Cheap input checks BEFORE env/subprocess resolution, so bad input never
    # depends on (or is masked by) the validator environment.
    if raw_df_json:
        try:
            rows, _shape = _extract_rows_blob(raw_df_json)
        except ValueError as e:
            return _err(str(e))
        if not rows:
            return _err("raw_df_json carries no rows (provide sample_rows or rows).")
    elif file_path and not file_path.startswith(("s3://", "gs://", "abfs://", "az://")):
        p = Path(file_path)
        if not p.is_file():
            return _err(f"data file not found: {file_path}")
        suffix = p.suffix.lower()
        if suffix not in (".csv", ".parquet"):
            return _err(f"unsupported data format {suffix!r} (expected .csv or .parquet)")

    try:
        fv_home = _fv_home()
        fv_python = _fv_python(fv_home)
    except FileNotFoundError as e:
        return _err(str(e), hint="Bootstrap the focus_validator clone+venv (see tool docstring).")

    with tempfile.TemporaryDirectory(prefix="focus_official_") as workdir:
        try:
            data_file, fmt, is_temp = _write_input(raw_df_json, file_path, workdir)
        except (FileNotFoundError, ValueError) as e:
            return _err(str(e))
        if data_format:
            fmt = data_format.lower()

        run = await _run_validator_subprocess(fv_home, fv_python, data_file, fmt, rules_version, workdir)
        if not run["success"]:
            return _err(run["error"], **{k: v for k, v in run.items() if k not in ("success", "error")})
        report = run["report"]

    summary = _summarize_report(report, max_failing_rules)
    result: dict[str, Any] = {
        "success": True,
        "validator": "finopsfoundation/focus_validator (local 3.11 clone, v2.2.1)",
        "compliant": summary["compliant"],
        "summary": summary,
        "report_path": _persist_report(report, source=str(file_path or "workflow")),
        "elapsed_seconds": _now() - t0,
    }
    if include_full_report:
        result["full_report"] = report
    if analyze:
        result.update(await _analyze_report(summary))
    return result


def register(mcp: FastMCP) -> None:
    """Expose the module-level tool function on the given FastMCP instance."""
    mcp.tool(validate_focus_official)

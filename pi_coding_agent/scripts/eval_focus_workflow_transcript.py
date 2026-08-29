#!/usr/bin/env python3
"""Evaluate a pi session transcript for custom_cost FOCUS workflow failures.

Parses a pi session .jsonl, reconstructs the user → tool-call → tool-result
chain for the FOCUS/normalize/validate tools, and flags failure patterns:

  * failed tool calls (isError or success:false)
  * input-shape confusion (hand-built raw_df_json, wrong file types)
  * "wandering" — tool calls unrelated to the task after a failure
  * invented/throwaway configs (the 'test: true' smell)

Usage:
    python eval_focus_workflow_transcript.py <session.jsonl> [more.jsonl ...]
"""

from __future__ import annotations

import glob
import json
import sys

FOCUS_TOOLS = {
    "normalize_to_focus",
    "validate_focus_official",
    "validate_and_repair_focus",
    "generate_focus_plans",
    "load_normalize_configs",
    "load_provider_plans",
    "apply_conversion_plans",
    "simulate_normalize_config",
}
WANDER_TOOLS = {
    "list_directory",
    "read_text_file",
    "stitcher_capabilities",
    "environment_context",
    "list_data_sources",
    "get_committed_config",
    "list_operators",
    "describe_operator",
    "cache_list",
    "cache_get",
}


def _content_text(content) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return " ".join(c.get("text", "") for c in content if isinstance(c, dict))
    return ""


def evaluate(path: str) -> dict:
    with open(path) as fh:
        entries = [json.loads(l) for l in fh if l.strip()]
    results = {}
    for e in entries:
        m = e.get("message") or {}
        if m.get("role") == "toolResult":
            results[m.get("toolCallId")] = {
                "tool": m.get("toolName"),
                "isError": bool(m.get("isError")),
                "text": _content_text(m.get("content")),
            }

    report: dict = {"session": path, "calls": [], "failed": [], "flags": []}
    for e in entries:
        m = e.get("message") or {}
        if m.get("role") == "assistant" and isinstance(m.get("content"), list):
            for c in m["content"]:
                if c.get("type") not in ("toolCall", "tool_call"):
                    continue
                name = c.get("name") or (c.get("toolCall") or {}).get("name")
                tid = c.get("id") or (c.get("toolCall") or {}).get("id")
                args = (
                    c.get("arguments")
                    or (c.get("toolCall") or {}).get("arguments")
                    or {}
                )
                r = results.get(
                    tid, {"tool": name, "isError": False, "text": "<no result>"}
                )
                ok = (not r["isError"]) and '"success":false' not in r["text"].replace(
                    " ", ""
                )
                rec = {"tool": name, "args": args, "result": r["text"][:600], "ok": ok}
                report["calls"].append(rec)
                if not ok:
                    report["failed"].append(rec)
                # flags
                raw = json.dumps(args)
                if (
                    "raw_df_json" in raw
                    and "sample_rows" not in raw
                    and "normalized_df_summary" not in raw
                ):
                    report["flags"].append(
                        f"{name}: raw_df_json without sample_rows (wrong shape)"
                    )
                if name == "validate_and_repair_focus" and "parquet" in raw:
                    report["flags"].append(
                        f"{name}: parquet file_path (tool only accepts pdf/csv)"
                    )
                if name == "validate_focus_official" and str(
                    args.get("file_path", "")
                ).endswith(".pdf"):
                    report["flags"].append(
                        "validate_focus_official: PDF input (only csv/parquet/json)"
                    )
                if name in ("save_config", "validate_config") and "test: true" in raw:
                    report["flags"].append(
                        f"{name}: throwaway 'test: true' config — agent flailing"
                    )
                if name in WANDER_TOOLS and report["failed"]:
                    report["flags"].append(f"wandering: {name} called after failures")
    return report


def main() -> None:
    paths: list[str] = []
    for a in sys.argv[1:]:
        paths.extend(sorted(glob.glob(a)) or [a])
    for p in paths:
        rep = evaluate(p)
        print("=" * 100)
        print(p)
        calls = rep.get("calls", [])
        print(f"tool calls: {len(calls)} | failed: {len(rep['failed'])}")
        for rec in rep["failed"]:
            err = ""
            try:
                parsed = json.loads(rec["result"])
                err = parsed.get("error") or ""
            except json.JSONDecodeError:
                err = rec["result"][:150]
            print(f"  FAILED {rec['tool']}: {err[:180]}")
        seen = set()
        for flag in rep["flags"]:
            if flag not in seen:
                seen.add(flag)
                print(f"  FLAG: {flag}")


if __name__ == "__main__":
    main()

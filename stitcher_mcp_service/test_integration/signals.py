"""Signal-level assertion helpers for the stitcher integration suite.

PHILOSOPHY (the integration-test constitution):

1. The pipeline's output is LLM-generated, so its PROSE is never asserted exactly.
   We assert *signals*: "a markdown table is present", "a FOCUS cost column name
   appears", "the word 'violation' or 'compliant' appears". Any-match passes.

2. The STRONG assertions are deterministic, not linguistic:
     - the session transcript proves WHICH TOOLS ran and what they RETURNED
       (tool results are machine-generated — their structured fields can be
       asserted exactly);
     - artifacts on disk (parquet / report JSON) are read back with polars and
       asserted on real columns / row counts;
     - the final prose is only checked for soft signals and the absence of
       raw tracebacks.

3. Never assert a number the model wrote in prose (it re-summarizes numbers it
   did not compute). Numbers are only trusted from tool results or artifacts.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

# ── transcript parsing (pi session JSONL) ────────────────────────────────────────


def transcript_events(run_dir: str) -> list[dict]:
    """All message events from the pi session transcripts of one runner turn, in order."""
    events: list[dict] = []
    session_dir = Path(run_dir) / "_session"
    if not session_dir.exists():
        return events
    for tf in sorted(session_dir.glob("*.jsonl"), key=lambda p: p.stat().st_mtime):
        for line in tf.read_text().splitlines():
            if not line.strip():
                continue
            try:
                ev = json.loads(line)
            except Exception:  # noqa: BLE001 — mid-write line
                continue
            if ev.get("type") == "message":
                events.append(ev)
    return events


def _blocks(events: list[dict], role: str) -> list[dict]:
    out = []
    for ev in events:
        msg = ev.get("message") or {}
        if msg.get("role") == role:
            out.extend(msg.get("content") or [])
    return out


def tool_calls(events: list[dict], name: str | None = None) -> list[dict]:
    """Assistant toolCall blocks (optionally filtered by tool name), in call order."""
    calls = [b for b in _blocks(events, "assistant") if b.get("type") == "toolCall"]
    if name is None:
        return calls
    return [c for c in calls if c.get("name") == name]


def tool_results(events: list[dict], name: str | None = None, errors: bool = False) -> list[str]:
    """Tool-result TEXT blocks (tool outputs are deterministic — safe to assert on).

    NOTE: ``toolName``/``isError`` live on the toolResult *message* level in pi
    transcripts, not on the content block.
    """
    out = []
    for ev in events:
        msg = ev.get("message") or {}
        if msg.get("role") != "toolResult":
            continue
        if errors:
            if not msg.get("isError"):
                continue
        elif msg.get("isError"):
            continue
        if name is not None and msg.get("toolName") != name:
            continue
        for block in msg.get("content") or []:
            if isinstance(block, dict) and block.get("type") == "text" and (block.get("text") or "").strip():
                out.append(str(block["text"]))
    return out


# ── signal assertions (LLM-tolerant) ─────────────────────────────────────────────


def assert_any_signal(text: str, patterns: list[str], label: str) -> None:
    """PASS if ANY regex in `patterns` matches `text` (case-insensitive, DOTALL).

    LLM prose varies run to run — we assert that the RIGHT KIND of information is
    present, never a specific phrasing. Every pattern is an acceptable signal;
    requiring all of them would over-fit one model's wording.
    """
    for p in patterns:
        if re.search(p, text, re.IGNORECASE | re.DOTALL):
            return
    pytest.fail(
        f"signal missing: {label}\n  none of the accepted patterns matched:\n"
        + "\n".join(f"    - {p}" for p in patterns)
        + f"\n  --- actual text (first 1500 chars) ---\n{text[:1500]}"
    )


def assert_absent(text: str, pattern: str, label: str) -> None:
    """PASS if the regex does NOT match — for 'the wrong signal must not appear'."""
    m = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
    if m:
        pytest.fail(f"forbidden signal present: {label}\n  pattern: {pattern}\n" f"  matched: {m.group(0)[:300]!r}")


def assert_markdown_table(text: str, *, min_data_rows: int = 1, header_signals: list[str] | None = None) -> None:
    """The answer renders an actual markdown table (pipe-delimited, with a separator row).

    `header_signals`: regexes at least ONE of which must appear in the header row —
    e.g. a FOCUS cost column, so "a table" can't be satisfied by any random table.
    """
    rows = [ln.strip() for ln in text.splitlines() if ln.strip().startswith("|")]
    assert len(rows) >= min_data_rows + 2, (
        f"expected a markdown table (>= {min_data_rows + 2} pipe rows: header + separator + data), "
        f"found {len(rows)}\n--- text ---\n{text[:1500]}"
    )
    # separator row like |---|---| proves it's a rendered table, not a bullet list
    assert any(
        re.fullmatch(r"\|[\s:|-]+\|", r) for r in rows
    ), f"table rows found but no |---| separator row:\n{text[:800]}"
    if header_signals:
        table_text = "\n".join(rows)
        assert any(
            re.search(p, table_text, re.IGNORECASE) for p in header_signals
        ), f"table lacks any expected column signal {header_signals}:\n{text[:800]}"


def assert_no_traceback(text: str, what: str) -> None:
    """A user-facing answer must never leak a raw Python traceback (principle 6)."""
    assert_absent(text, r"Traceback \(most recent call last\)", f"{what}: raw traceback leaked to the user")
    assert_absent(text, r'File "[^"]+\.py", line \d+', f"{what}: internal file/line leak")


# ── artifact assertions (deterministic — the strongest checks we have) ──────────


def newest_file(directory: str | Path, pattern: str = "*") -> Path | None:
    """Newest matching file in a directory (reruns are timestamped, newest wins), or None."""
    d = Path(directory)
    if not d.exists():
        return None
    files = [p for p in d.glob(pattern) if p.is_file()]
    return max(files, key=lambda p: p.stat().st_mtime) if files else None


def assert_parquet_artifact(path: str | Path, *, required_columns: list[str], min_rows: int = 1) -> int:
    """The persisted parquet is REAL, readable, has the FOCUS columns and actual data."""
    import polars as pl  # local import: only integration tests pay for the import

    p = Path(path)
    assert p.exists(), f"expected parquet artifact at {p}"
    df = pl.read_parquet(p)
    assert df.height >= min_rows, f"{p.name}: expected >= {min_rows} rows, got {df.height}"
    missing = [c for c in required_columns if c not in df.columns]
    assert not missing, f"{p.name}: missing FOCUS columns {missing} (has {len(df.columns)} columns)"
    return df.height

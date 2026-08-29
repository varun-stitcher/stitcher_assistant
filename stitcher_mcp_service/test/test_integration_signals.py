"""Unit tests for the integration suite's transcript parser + signal helpers.

These pin the REAL pi transcript format (toolName/isError live on the toolResult
message level, not the content block — a regression here would silently disable
every integration assertion) and the any-match signal contract.
"""

from __future__ import annotations

import json
import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "test_integration"))

from signals import (  # noqa: E402
    assert_absent,
    assert_any_signal,
    assert_markdown_table,
    assert_no_traceback,
    tool_calls,
    tool_results,
    transcript_events,
)


def _events() -> list[dict]:
    """A miniature transcript in the EXACT shape pi writes (verified against a
    live gateway run): assistant toolCall + toolResult with message-level
    toolName/isError."""
    return [
        {
            "type": "message",
            "message": {
                "role": "assistant",
                "content": [
                    {"type": "text", "text": "converting"},
                    {"type": "toolCall", "name": "normalize_to_focus", "arguments": {}},
                ],
            },
        },
        {
            "type": "message",
            "message": {
                "role": "toolResult",
                "toolName": "normalize_to_focus",
                "isError": False,
                "content": [{"type": "text", "text": '{"success":true,"rows":1}'}],
            },
        },
        {
            "type": "message",
            "message": {
                "role": "toolResult",
                "toolName": "normalize_to_focus",
                "isError": True,
                "content": [{"type": "text", "text": "ERR: bad input"}],
            },
        },
        {
            "type": "message",
            "message": {
                "role": "toolResult",
                "toolName": "other_tool",
                "isError": False,
                "content": [{"type": "text", "text": "unrelated"}],
            },
        },
    ]


class TestTranscriptParsing:
    def test_parses_real_pi_shape(self):
        events = _events()
        calls = tool_calls(events, "normalize_to_focus")
        assert len(calls) == 1 and calls[0]["name"] == "normalize_to_focus"

    def test_tool_results_filter_by_name_and_error(self):
        events = _events()
        ok = tool_results(events, "normalize_to_focus")
        assert ok == ['{"success":true,"rows":1}'], "non-error result of the named tool only"
        errs = tool_results(events, "normalize_to_focus", errors=True)
        assert errs == ["ERR: bad input"], "error result surfaced via errors=True"
        assert tool_results(events, "other_tool") == ["unrelated"]

    def test_unparsable_lines_are_skipped_not_fatal(self):
        import tempfile

        with tempfile.TemporaryDirectory() as d:
            session = pathlib.Path(d) / "_session"
            session.mkdir()
            (session / "x.jsonl").write_text(
                json.dumps(_events()[0]) + "\n{broken json\n" + json.dumps(_events()[1]) + "\n"
            )
            events = transcript_events(d)
            assert len(events) == 2

    def test_missing_session_dir_is_empty_not_crash(self):
        assert transcript_events("/nonexistent/run") == []


class TestSignalAssertions:
    def test_any_signal_passes_on_any_alternative(self):
        assert_any_signal("It went fine", [r"never matches", r"fine"], "label")

    def test_any_signal_fails_naming_all_patterns(self):
        with pytest.raises(pytest.fail.Exception, match="signal missing"):  # helper uses pytest.fail
            assert_any_signal("nothing here", [r"a", r"b"], "the label")

    def test_absent_fails_when_present(self):
        with pytest.raises(pytest.fail.Exception, match="forbidden signal"):
            assert_absent("Traceback (most recent call last)", r"Traceback", "leak check")

    def test_markdown_table_requires_separator_and_header_signal(self):
        good = "| BilledCost | USD |\n|---|---|\n| 100 | USD |"
        assert_markdown_table(good, header_signals=[r"BilledCost"])
        with pytest.raises(AssertionError, match="separator"):
            # 3+ pipe rows (passes the row-count check) but no |---| separator row
            assert_markdown_table("| a | b |\n| c | d |\n| 1 | 2 |")
        with pytest.raises(AssertionError, match="column signal"):
            assert_markdown_table(good, header_signals=[r"NeverPresent"])
        # a key-value table (| Column | Value |) also counts: the FOCUS signal may
        # appear anywhere in the table, not only in the header row
        assert_markdown_table("| Column | Value |\n|---|---|\n| BilledCost | 100.0 |", header_signals=[r"BilledCost"])

    def test_no_traceback_catches_file_line_leak(self):
        assert_no_traceback("all good", "answer")
        with pytest.raises(pytest.fail.Exception, match="file/line leak"):
            assert_no_traceback('File "/srv/app.py", line 42', "answer")

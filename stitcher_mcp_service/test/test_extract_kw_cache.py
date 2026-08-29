"""Tests for the KW cache + extract_invoice tool (deterministic paths — no LLM for CSV).

Covers:
  C1. kw_cache put/get round-trip (JSON payload).
  C2. make_key sanitizes identity parts (rejects path traversal / hostile chars).
  C3. cache_list / cache_clear with prefix scoping.
  C4. extract_invoice on a CSV: first run not-from-cache, repeat run IS from-cache
      (zero LLM), and a different ``expected_columns`` hint → distinct artifact.
  C5. normalize_to_focus reuses a seeded ``plans`` cache artifact
      (``plans_from_cache=True``, no LLM plan-gen, correct normalized output).
  C6. cache_* MCP tools (put/get/list/clear) round-trip through the tool surface.
"""

import json
import os

os.environ.setdefault("STITCHER_STEP_CACHE_DIR", "/tmp/kw-test-suite")

from stitcher.assistant_harness.sub_mcp_agents.custom_cost.tools import kw_cache  # noqa: E402
from stitcher.assistant_harness.sub_mcp_agents.custom_cost.tools.extraction.extract_tools import (  # noqa: E402
    register as extract_register,
)
from stitcher.assistant_harness.sub_mcp_agents.custom_cost.tools.focus.focus_normalization_tools import (  # noqa: E402
    register as fnorm_register,
)


def _server():
    from fastmcp import FastMCP

    mcp = FastMCP(name="kw-test")
    extract_register(mcp)
    fnorm_register(mcp)
    return mcp


def _call(method, params):
    import asyncio

    result = asyncio.run(_server().call_tool(method, params))
    text = result.content[0].text if hasattr(result, "content") else result
    return json.loads(text)


def _write_csv(tmp_path, content: str):
    p = tmp_path / "inv.csv"
    p.write_text(content)
    return str(p)


# ── C1: put/get round-trip ────────────────────────────────────────────────


def test_cache_put_get_roundtrip():
    key = kw_cache.make_key("extract", "abc123")
    kw_cache.cache_put(key, {"a": [1, 2, 3], "b": "x"})
    got = kw_cache.cache_get(key)
    assert got == {"a": [1, 2, 3], "b": "x"}
    assert kw_cache.cache_get("nope::missing") is None
    kw_cache.cache_clear(key)


# ── C2: make_key sanitization ─────────────────────────────────────────────


def test_make_key_sanitizes_and_rejects_hostile():
    # Short safe token kept verbatim.
    assert kw_cache.make_key("extract", "abc") == "extract:abc"
    # Hostile separators / traversal are hashed, never passed through.
    k = kw_cache.make_key("extract", "../etc/passwd")
    assert "/" not in k.split(":")[1]
    assert ".." not in k.split(":")[1]
    # cache_put refuses a key with a slash (defensive).
    import pytest

    with pytest.raises(ValueError):
        kw_cache.cache_put("bad/key", {})


# ── C3: list / clear with prefix ──────────────────────────────────────────


def test_cache_list_and_clear_prefix():
    kw_cache.cache_clear()  # isolation from other tests' artifacts
    kw_cache.cache_put("extract:seed1", {"d": 1})
    kw_cache.cache_put("plans:seed1", {"d": 2})
    kw_cache.cache_put("extract:seed2", {"d": 3})
    try:
        all_keys = {e["key"] for e in kw_cache.cache_list()}
        assert {"extract:seed1", "extract:seed2", "plans:seed1"} <= all_keys
        extract_keys = {e["key"] for e in kw_cache.cache_list("extract:")}
        assert "extract:seed1" in extract_keys and "plans:seed1" not in extract_keys
        assert kw_cache.cache_clear("extract:") == 2
        assert kw_cache.cache_get("extract:seed1") is None
        assert kw_cache.cache_get("plans:seed1") is not None
    finally:
        kw_cache.cache_clear()


# ── C4: extract_invoice caching (CSV, deterministic) ──────────────────────


def test_extract_invoice_csv_caches_and_reuses(tmp_path):
    csv = _write_csv(tmp_path, "Invoice number,Amount,Tax\nINV1,10.25,1.00\n")
    first = _call("extract_invoice", {"file_path": csv})
    assert first["success"] is True
    assert first["from_cache"] is False
    cols = first["columns"]
    assert "Invoice number" in cols and "Amount" in cols
    ck = first["cache_key"]

    # Repeat same call → cache hit (no re-extraction).
    second = _call("extract_invoice", {"file_path": csv})
    assert second["from_cache"] is True
    assert second["columns"] == cols

    # Different expected_columns → distinct artifact, not stale reuse.
    third = _call("extract_invoice", {"file_path": csv, "expected_columns": ["Tax"]})
    assert third["from_cache"] is False
    assert third["cache_key"] != ck


# ── C5: normalize_to_focus reuses seeded plans ────────────────────────────


def test_normalize_reuses_seeded_plans(tmp_path):
    csv = _write_csv(tmp_path, "Invoice number,Amount\nINV1,10.25\nINV2,20.5\n")
    file_bytes = (tmp_path / "inv.csv").read_bytes()

    variant = kw_cache.sha256_bytes(json.dumps({"provider": "unknown", "cols": []}).encode())[:8]
    config = {
        "converter_plan_name": "mcp_focus_merged",
        "focus_columns": [
            {
                "focus_column": "ProviderName",
                "steps": [{"plan_name": "p", "type": "General.set_static_value", "static_value": "ACME"}],
            },
            {
                "focus_column": "BilledCost",
                "steps": [{"plan_name": "b", "type": "General.rename_column", "source_column": "Amount"}],
            },
        ],
    }
    kw_cache.step_cache_put(
        "plans", file_bytes, variant, {"source": csv, "provider": "unknown", "config": config, "plan_count": 2}
    )
    try:
        out = _call("normalize_to_focus", {"file_path": csv, "use_cache": True, "validate": False})
        assert out["success"] is True
        assert out["plans_from_cache"] is True
        assert out["plan_count"] == 2
        assert "BilledCost" in (out.get("normalized_df_summary") or {}).get("columns", [])
        assert "ProviderName" in (out.get("normalized_df_summary") or {}).get("columns", [])
    finally:
        kw_cache.cache_clear()


# ── C6: cache_* MCP tools round-trip ──────────────────────────────────────


def test_cache_tools_roundtrip_via_mcp():
    put = _call("cache_put", {"key": "mytool:seed", "payload_json": json.dumps({"hello": "world"})})
    assert put["success"] is True
    got = _call("cache_get", {"key": "mytool:seed"})
    assert got["payload"] == {"hello": "world"}
    lst = _call("cache_list", {"prefix": "mytool:"})
    assert any(e["key"] == "mytool:seed" for e in lst["entries"])
    clear = _call("cache_clear", {"prefix": "mytool:"})
    assert clear["removed"] >= 1
    assert _call("cache_get", {"key": "mytool:seed"})["success"] is False

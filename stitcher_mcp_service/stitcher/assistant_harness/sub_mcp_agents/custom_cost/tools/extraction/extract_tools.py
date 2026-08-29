"""Extract-step tool + KW-cache management for the custom-cost sub-MCP.

Provides:
  * ``extract_invoice`` — run ONLY the extraction step (no plan-gen / normalize),
    with an ``expected_columns`` hint to influence WHICH columns the extractor
    pulls. The raw extraction output is saved to the KW cache keyed by a content
    hash of the source file (+ the columns hint), and is REUSED on a repeat call
    (zero LLM, near-instant).
  * ``cache_list`` / ``cache_get`` / ``cache_put`` / ``cache_clear`` — inspect and
    manage the server-side step-artifact cache so outputs can be added, updated,
    and reused across calls.

Design notes
------------
* ``expected_columns`` is threaded into ``InvoiceParserWorkflow.run(...,
  expected_columns=...)`` — the parser's lever for which fields to capture.
* The cache key for extraction is ``extract:<file_id>:<cols_variant>`` where
  ``file_id`` is the SHA-256 of the file bytes and ``cols_variant`` hashes the
  requested columns — so changing the column hint produces a distinct cached
  artifact (no stale-reuse across different requests).
* Cache artifacts live server-side (``~/.stitcher/kw-cache``); only these tools
  expose them.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import tempfile
import time
from typing import Any

import polars as pl
from fastmcp import Context, FastMCP

from .. import kw_cache
from ..focus.focus_normalization_tools import _serialize_df, _validate_pdf

logger = logging.getLogger(__name__)


def _read_input_bytes(
    pdf_b64: str | None, file_path: str | None, filename: str | None
) -> tuple[bytes | None, str | None, str | None]:
    """Resolve raw input bytes + human source label + temp path to clean up.

    Returns (bytes, source_label, temp_path_for_cleanup). For ``file_path`` the
    bytes are read directly (no temp). For ``pdf_b64`` the bytes are written to a
    temp file so the extractor can open it. Errors are returned as a dict string.
    """
    if pdf_b64 and file_path:
        return None, "ERR: pass only one of pdf_b64 or file_path, not both.", None
    if not pdf_b64 and not file_path:
        return None, "ERR: provide pdf_b64 (+filename) or file_path.", None
    if pdf_b64:
        try:
            raw = base64.b64decode(pdf_b64, validate=True)
        except (ValueError, TypeError) as e:
            return None, f"ERR: pdf_b64 not valid base64: {e}", None
        return raw, filename or "upload.pdf", filename or "upload.pdf"
    if not os.path.isfile(file_path):
        return None, f"ERR: no such file: {file_path}", None
    with open(file_path, "rb") as f:
        return f.read(), file_path, None


def register(mcp: FastMCP) -> None:
    @mcp.tool
    async def extract_invoice(
        ctx: Context,
        file_path: str | None = None,
        pdf_b64: str | None = None,
        filename: str | None = None,
        expected_columns: list[str] | None = None,
        use_cache: bool = True,
        max_sample_rows: int = 5,
    ) -> dict[str, Any]:
        """Run ONLY the extraction step on an invoice PDF/CSV → raw columns.

        Use this to inspect / influence the raw columns before normalization.
        ``expected_columns`` (optional) tells the extractor to capture specific
        fields it might otherwise skip, e.g. [\"Invoice number\", \"Tax\", \"Region\"].

        Pass ONE input: ``file_path`` (server path, preferred) or ``pdf_b64`` +
        ``filename``. The output is saved to the KW cache (keyed by file content +
        the columns hint) and is reused on a repeat call with the same input —
        no re-extraction, no LLM.

        Args:
            file_path: server path to a PDF/CSV. Preferred.
            pdf_b64: base64 bytes (+ filename). Only when the file isn't on disk.
            filename: logical filename (drives .pdf/.csv extension) when using pdf_b64.
            expected_columns: optional list of column names to influence extraction.
            use_cache: when true (default), reuse the cached extraction for this
              (file, columns) instead of re-running the LLM.
            max_sample_rows: sample rows to include in the output summary.
        """
        t0 = time.time()

        def _err(message: str) -> dict[str, Any]:
            return {"success": False, "error": message, "elapsed_seconds": round(time.time() - t0, 2)}

        if not expected_columns:
            expected_columns = []
        # Deterministic variant so different hints → different cache artifacts.
        cols_variant = kw_cache.sha256_bytes(json.dumps(sorted(expected_columns)).encode())[:8]

        raw, source, tmp_to_clean = _read_input_bytes(pdf_b64, file_path, filename)
        if raw is None:
            return _err(source)  # source holds the error string

        ext = (source or "").rsplit(".", 1)[-1].lower()

        # ── Cache read (zero LLM) ──────────────────────────────────────
        cached = None
        if use_cache:
            cached = kw_cache.step_cache_get("extract", raw, cols_variant)
            if cached and ext in ("pdf", "csv"):
                cached_data = cached.get("data", {})
                return {
                    "success": True,
                    "source": source,
                    "provider_detected": cached_data.get("provider_detected"),
                    "columns": cached_data.get("columns"),
                    "raw_df_summary": cached_data.get("raw_df_summary"),
                    "from_cache": True,
                    "cache_key": kw_cache.step_cache_key("extract", raw, cols_variant),
                    "elapsed_seconds": round(time.time() - t0, 2),
                }

        # ── Run extraction ─────────────────────────────────────────────
        temp_path: str | None = None
        try:
            if pdf_b64:
                with tempfile.NamedTemporaryFile(mode="wb", suffix=f".{ext}", delete=False) as tf:
                    tf.write(raw)
                    tf.flush()
                    temp_path = tf.name
                pdf_path = temp_path
            else:
                pdf_path = file_path  # type: ignore[arg-type]

            if ext == "csv":
                raw_df = await asyncio.to_thread(pl.read_csv, pdf_path)
                detected_provider = "unknown"
            else:
                invalid = _validate_pdf(pdf_path)
                if invalid:
                    return _err(invalid)
                await ctx.report_progress(1, 1, "Extracting rows from invoice (PDF OCR)...")
                from ..focus.focus_normalization_tools import _extract_raw_df

                raw_df, detected_provider = await _extract_raw_df(pdf_path, expected_columns=expected_columns)
                if not detected_provider:
                    detected_provider = "unknown"

            if raw_df.is_empty():
                return _err("extraction returned no rows.")

            summary = _serialize_df(raw_df, max_sample_rows)
            columns = raw_df.columns
            payload = {
                "source": source,
                "provider_detected": detected_provider,
                "columns": columns,
                "raw_df_summary": summary,
            }
            cache_key = kw_cache.step_cache_key("extract", raw, cols_variant)
            kw_cache.step_cache_put("extract", raw, cols_variant, payload)

            return {
                "success": True,
                "source": source,
                "provider_detected": detected_provider,
                "columns": columns,
                "raw_df_summary": summary,
                "from_cache": False,
                "cache_key": cache_key,
                "cache_summary": kw_cache.cache_metrics(),
                "elapsed_seconds": round(time.time() - t0, 2),
            }
        except Exception as e:  # noqa: BLE001
            logger.exception("extract_invoice failed")
            return _err(f"extraction failed: {type(e).__name__}: {str(e)[:500]}")
        finally:
            if temp_path and pdf_b64 and os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except OSError:
                    pass

    @mcp.tool
    def cache_list(prefix: str = "") -> dict[str, Any]:
        """List KW-cache entries (key, size, mtime) for a step prefix or all.

        E.g. ``cache_list("extract")`` lists all extracted artifacts; ``cache_list()``
        lists everything. Includes overall count + total bytes.
        """
        try:
            entries = kw_cache.cache_list(prefix)
        except Exception as e:  # noqa: BLE001
            return {"success": False, "error": str(e)}
        return {"success": True, "count": len(entries), "entries": entries}

    @mcp.tool
    def cache_get(key: str) -> dict[str, Any]:
        """Read a single KW-cache entry by its key (see ``cache_list``)."""
        payload = kw_cache.cache_get(key)
        if payload is None:
            return {"success": False, "error": f"no cached entry for key {key!r}"}
        return {"success": True, "key": key, "payload": payload}

    @mcp.tool
    def cache_put(key: str, payload_json: str) -> dict[str, Any]:
        """Add or update a KW-cache entry with a JSON payload.

        Useful to inject/adjust a step artifact for a later step to reuse.
        ``key`` must be a plain token like ``extract:<file_id>:<variant>``.
        """
        try:
            payload = json.loads(payload_json)
        except json.JSONDecodeError as e:
            return {"success": False, "error": f"payload_json is not valid JSON: {e}"}
        try:
            kw_cache.cache_put(key, payload)
        except Exception as e:  # noqa: BLE001
            return {"success": False, "error": str(e)}
        return {"success": True, "key": key, "cache_summary": kw_cache.cache_metrics()}

    @mcp.tool
    def cache_clear(prefix: str | None = None) -> dict[str, Any]:
        """Delete KW-cache entries, optionally only those whose key starts with ``prefix``.

        Pass ``prefix="extract"`` to clear all extraction artifacts, or omit to
        clear the entire cache. Returns the number removed.
        """
        removed = kw_cache.cache_clear(prefix)
        return {"success": True, "removed": removed, "cache_summary": kw_cache.cache_metrics()}

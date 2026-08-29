"""User-visible artifact persistence — one place for the "show the user a file" pattern.

Three writers share this contract today: parquet frames (``persist_parquet``),
validator conformance reports (``persist_json``), and saved normalize configs
(``save_focus_config`` writes YAML via its own verified writer). The common
rules they must all follow (and which this module centralizes):

  * output goes to a USER-VISIBLE directory — ``$VAR`` override with a
    ``<tmp>/<default>`` fallback, created on demand;
  * names are content-derived and timestamped so reruns NEVER overwrite an
    earlier artifact;
  * writers NEVER raise — an artifact is a deliverable, not the data path; a
    failure degrades to ``{"error": ...}`` that the caller reports explicitly;
  * the writer never alters the payload (parquet round-trip equality is tested).
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import time
from pathlib import Path
from typing import Any

import polars as pl

logger = logging.getLogger(__name__)


def user_output_dir(env_var: str, default_name: str) -> Path:
    """Resolve + create a user-visible output directory (env-overridable)."""
    default_root = Path(tempfile.gettempdir()) / "stitcher-artifacts"
    d = Path(os.environ.get(env_var, default_root / default_name))
    d.mkdir(parents=True, exist_ok=True)
    return d


def artifact_name(source: str, kind: str, ext: str) -> str:
    """``<source-stem>.<kind>.<YYYYmmdd-HHMMSS>.<ext>`` — rerun-safe by construction."""
    stem = Path(source or "dataset").stem.replace(" ", "_").replace(":", "-") or "dataset"
    return f"{stem[:60]}.{kind}.{time.strftime('%Y%m%d-%H%M%S')}.{ext.lstrip('.')}"


def persist_parquet(df: pl.DataFrame, kind: str, source: str, env_var: str, default_name: str) -> dict[str, Any]:
    """Write ``df`` as a user-visible parquet artifact. Never raises.

    Returns ``{"path", "bytes", "rows", "columns"}`` on success or ``{"error"}``
    on failure — callers attach it to their output verbatim.
    """
    try:
        path = user_output_dir(env_var, default_name) / artifact_name(source, kind, "parquet")
        df.write_parquet(path)
        return {"path": str(path), "bytes": path.stat().st_size, "rows": df.height, "columns": df.width}
    except Exception as e:  # noqa: BLE001 — artifact failure must not fail the pipeline
        logger.warning("persist_parquet(%s) failed: %s", kind, e)
        return {"error": f"failed to persist {kind} parquet: {type(e).__name__}: {str(e)[:300]}"}


def persist_json(obj: Any, kind: str, source: str, env_var: str, default_name: str) -> str | None:
    """Write ``obj`` as a user-visible JSON artifact; return its path (never raises)."""
    try:
        path = user_output_dir(env_var, default_name) / artifact_name(source, kind, "json")
        path.write_text(json.dumps(obj, default=str))
        return str(path)
    except Exception as e:  # noqa: BLE001
        logger.warning("persist_json(%s) failed: %s", kind, e)
        return None

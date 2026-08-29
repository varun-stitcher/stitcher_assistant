"""KW (knowledge-work) cache — a persistent key-value store for pipeline step outputs.

Each deterministic/fuzzy step of the custom-cost pipeline (extract, plan-gen,
normalize, validate) can save its serialized output here keyed by a stable
identity (source-file content hash + step + variant). Later calls can read,
reuse, update, or add to a step's output without re-running the expensive LLM
steps.

Design
------
* Persistence: one JSON file per key under ``CACHE_DIR`` (env
  ``STITCHER_STEP_CACHE_DIR``, default ``~/.stitcher/kw-cache``).
* Keys: ``<step>:<file_id>[:<variant>]`` where ``file_id`` is the SHA-256 of the
  source file bytes and ``variant`` is an optional short discriminator (e.g. a
  hash of ``expected_columns``). ``file_id`` is truncated for readable keys.
* Writes are atomic (tmp file + rename) so a crash can't corrupt an entry.
* The cache is server-side storage for reuse across calls — it is NOT exposed to
  the client as raw filesystem access, only via the cache_* tools.
"""

from __future__ import annotations

import functools
import hashlib
import json
import os
import pathlib
import time
from typing import Any


@functools.lru_cache(maxsize=1)
def _cache_dir_setting() -> str:
    """STITCHER_STEP_CACHE_DIR via StitcherAssistantConfig (once); '' when unset."""
    from ....common.config import StitcherAssistantConfig

    return StitcherAssistantConfig().step_cache_dir


def cache_dir() -> pathlib.Path:
    d = pathlib.Path(_cache_dir_setting() or pathlib.Path.home() / ".stitcher" / "kw-cache")
    d.mkdir(parents=True, exist_ok=True)
    return d


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def file_id(data: bytes, truncate: int = 16) -> str:
    """Stable content-based id for a file's bytes (truncated for readable keys)."""
    return sha256_bytes(data)[:truncate]


def _key_file(key: str) -> pathlib.Path:
    # Keys are already sanitized by `make_key`; defensive guard against traversal.
    if "\x00" in key or ".." in key or "/" in key or "\\" in key:
        raise ValueError(f"invalid cache key: {key!r}")
    return cache_dir() / f"{key}.json"


def make_key(step: str, *parts: str) -> str:
    """Build a cache key from a step name and sanitized identity parts."""
    safe = [step]
    for p in parts:
        p_ = p.strip()
        # Keep short readable tokens; hash anything long/hostile.
        if p_ and all(c.isalnum() or c in "-_." for c in p_) and len(p_) <= 64:
            safe.append(p_)
        else:
            safe.append(sha256_bytes(p_.encode("utf-8"))[:16])
    return ":".join(safe)


def cache_put(key: str, payload: Any) -> None:
    """Save a JSON-serializable payload (dict/list/str) under ``key`` (atomic)."""
    f = _key_file(key)
    tmp = f.with_suffix(f.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, default=str), encoding="utf-8")
    os.replace(tmp, f)


def cache_get(key: str) -> dict[str, Any] | None:
    """Read a cached payload, or ``None`` if absent. Refuses corrupt entries."""
    f = _key_file(key)
    if not f.is_file():
        return None
    try:
        return json.loads(f.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        # Corrupt / unreadable → treat as missing, never crash the caller.
        return None


def cache_list(prefix: str = "") -> list[dict[str, Any]]:
    """List cached entries with key, step, size, and mtime, newest first."""
    out: list[dict[str, Any]] = []
    for f in cache_dir().glob("*.json"):
        key = f.name[: -len(".json")]
        if prefix and not key.startswith(prefix):
            continue
        try:
            stat = f.stat()
            out.append({"key": key, "size_bytes": stat.st_size, "modified_epoch": int(stat.st_mtime)})
        except OSError:
            continue
    out.sort(key=lambda e: e["modified_epoch"], reverse=True)
    return out


def cache_clear(prefix: str | None = None) -> int:
    """Delete cached entries (optionally only those with a key prefix). Returns count."""
    removed = 0
    for f in cache_dir().glob("*.json"):
        key = f.name[: -len(".json")]
        if prefix and not key.startswith(prefix):
            continue
        try:
            f.unlink()
            removed += 1
        except OSError:
            continue
    return removed


def cache_metrics() -> dict[str, Any]:
    """Count + total size of all cached entries (for status output)."""
    entries = cache_list()
    return {"count": len(entries), "total_bytes": sum(e["size_bytes"] for e in entries)}


# ── Step-artifact helpers (convenience for pipeline tools) ────────────────

_EXPIRY_S = 86400 * 30  # 30 days default TTL for cached step artifacts


def step_cache_key(step: str, file_data: bytes, variant: str = "") -> str:
    return make_key(step, file_id(file_data), variant)


def step_cache_get(step: str, file_data: bytes, variant: str = "") -> dict[str, Any] | None:
    """Read a step artifact if present and not expired (else purge + None)."""
    key = step_cache_key(step, file_data, variant)
    entry = cache_get(key)
    if entry is None:
        return None
    ts = entry.get("_ts")
    if ts is not None and time.time() - ts > _EXPIRY_S:
        cache_clear(key)
        return None
    return entry


def step_cache_put(step: str, file_data: bytes, variant: str, payload: Any) -> str:
    key = step_cache_key(step, file_data, variant)
    cache_put(key, {"_ts": time.time(), "data": payload})
    return key

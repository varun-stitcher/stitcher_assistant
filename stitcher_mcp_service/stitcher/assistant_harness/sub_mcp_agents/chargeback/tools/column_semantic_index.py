"""File-backed semantic index for FOCUS ``x_*`` column → FinOps dimension matching.

A **local, file-based vector store** for the chargeback column-classification step
(``cost_reader.classify_org_cost_center``), built for INITIAL TESTING before
committing to pgvector. Zero infra: one JSON file on disk, stdlib + numpy only.

Why this shape
--------------
``classify_org_cost_center`` currently runs a full Qwen round-trip (up to 60s +
retries) to map a destination's ``x_*`` columns → ``organization`` /
``cost_center``. The column set is **stable per (environment, destination)**, yet
the call is neither memoized nor persisted — every query and every restart
re-pays the LLM cost for the same answer. The classification is really a
nearest-neighbor problem over a **small, stable, curated corpus** of dimension
synonyms, so embedding the destination columns once and cosine-matching against
the corpus is sub-ms, deterministic, and free.

Determinism / safety (mirrors the human-in-the-loop pattern in ``cost_reader``)
-----------------------------------------------------------------------------
* The store is the **deterministic first path**; the LLM classifier becomes the
  rare fallback for novel/ambiguous columns (below the confidence threshold) and
  its result is **persisted back** into the store, so it is paid once.
* A column can NEVER be both org and cost-center (the same rule as
  ``_normalize_org_cc``): the higher-scoring dimension wins outright.
* Provider-prefixed ``x_*`` vendor tags are filtered out before matching (same
  ``_is_provider_prefixed_x`` filter) — a vendor tag can never be an org/cc.
* Every match carries a confidence score; below the threshold the index returns
  ``None`` for that dimension (never guesses) — the caller falls back.
* The store file is pure JSON (human-readable, diff-able, no binary blobs); the
  backend is swappable without changing the on-disk format.

Backend interface
------------------
``EmbedBackend`` is a protocol: ``name``, ``dim``, ``embed(texts) -> ndarray``.
The default ``HashingNgramBackend`` is stdlib + numpy (char 3-gram TF, L2-normal,
hashed to a fixed width) — no model download, no network, deterministic. A
``GatewayEmbedBackend`` stub is provided for later: it hits the Stitcher gateway
``/v1/embeddings`` via the existing ``openai`` client when the deployment serves
one; until then it raises a clear ``NotImplementedError`` so a misconfiguration
fails loudly (never silently degrades).

On-disk format (``ColumnSemanticIndex.save``)
---------------------------------------------
::

    {
      "version": 1,
      "backend": "hashing-ngram-v1",
      "created": "<iso8601>",
      "threshold": 0.60,
      "corpus": [
        {"dimension": "organization", "synonyms": ["organization", ...],
         "vectors": [[...], ...]},
        ...
      ],
      "destinations": {
        "<env_id>::<dest_id>::<schema_hash>": {
          "columns": ["x_CostCenter", ...],
          "vectors": [[...], ...],
          "matched_at": "<iso8601>",
          "matches": {"organization": {"column": "x_Organization", "score": 0.83}, ...}
        }
      }
    }

Run the probe to build + test it:
    python probe_vector.py            # default file, auto-resolve destination
    python probe_vector.py --file /tmp/idx.json --compare-llm
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

import numpy as np

# ── curated corpus (mirrors the LLM prompt examples + the deterministic x_* defaults) ─────────
# Extensible: add a (dimension, [synonyms]) entry here and rebuild. Synonyms are
# the human-readable phrases a customer might have named a column after — the
# char n-gram backend makes ``x_CostCenter`` ≈ ``cost center`` ≈ ``costcentre``.
DEFAULT_CORPUS: dict[str, list[str]] = {
    "organization": [
        "organization", "org", "organization id", "business unit", "bu",
        "division", "company", "entity", "tenant", "account owner", "department",
        "cost owner",
    ],
    "business_unit": [
        "business unit", "business_unit", "businessunit", "division", "segment",
        "line of business", "lob", "market", "profit center",
    ],
    "cost_center": [
        "cost center", "cost centre", "costcenter", "cost center id", "cc",
        "team", "team id", "team name", "project", "project id", "project name",
        "department", "dept", "product", "product line", "portfolio", "squad",
        "workstream", "budget owner",
    ],
}

DEFAULT_THRESHOLD = 0.60

# ── ambiguity / confidence guards (the honest bit) ────────────────────────────
# A bare score>=threshold is NOT enough to trust the deterministic answer: the
# lexical matcher can be *confident but wrong* when (a) several columns saturate
# the cosine bound for the same dimension (a near-tie it cannot disambiguate) or
# (b) the winner is matched only by a trivial short token (e.g. ``bu``→``bu``) while
# a more descriptive column is the real semantic winner. So each dimension gets a
# verdict:
#   match     — confident: deterministic answer is authoritative (skip the LLM)
#   ambiguous — do NOT trust: fall back to the LLM classifier, persist its answer
#   refused   — confident there is NO such dimension (best far below threshold)
VERDICT_MATCH = "match"
VERDICT_AMBIGUOUS = "ambiguous"
VERDICT_REFUSED = "refused"
# Where a cached verdict came from: the deterministic index, or an LLM-resolved
# answer persisted back (which is authoritative and paid only once).
SOURCE_INDEX = "index"
SOURCE_LLM = "llm"
# Below this best-score we are confident the dimension does NOT exist (deterministic
# refusal, no LLM needed). Between this and DEFAULT_THRESHOLD is a marginal no-man's
# land the lexical matcher may under-score → AMBIGUOUS (let the LLM decide).
REFUSE_THRESHOLD = 0.35
# A best-match needs at least this gap over the runner-up to be trusted; else the
# columns are effectively tied for this dimension → AMBIGUOUS.
MARGIN_EPS = 0.15

# ── allocation pipeline (the "what do we group cost by?" decision) ────────────
# When a destination has no explicit cost center, the next-best allocation dimension
# to group/chargeback on is tried in priority order. ``resolve_allocation_dimension``
# walks this list: the first dimension that CLEARLY resolves (confident match) is the
# grouping key; if the top priority is only ambiguous, we hand the candidate shortlist
# to the LLM as a scoped multiple-choice pick (see cost_reader._llm_pick_allocation).
ALLOCATION_PIPELINE: tuple[str, ...] = ("cost_center", "business_unit", "organization")


def _is_trivial_synonym(syn: str) -> bool:
    """True for a single short token (no space, <=3 chars) like ``bu`` / ``cc`` / ``org``.

    Such synonyms trigger exact, degenerate lexical matches that tend to disagree
    with semantic judgment (``x_bu`` outscoring the descriptive ``x_org_name``).
    When the best match rides on one of these, treat the dimension as AMBIGUOUS.
    """
    s = syn.strip()
    return bool(s) and " " not in s and len(s) <= 3


# Canonical (exact-name) spellings per dimension — used as a deterministic tiebreak.
# When a destination carries an EXPLICIT canonical column (``x_CostCenter``) but ALSO a
# generic one that reads as the same dimension (``x_team``), the char-ngram matcher ties
# them at ~1.0 and would otherwise call the dimension AMBIGUOUS, wrongly skipping to a
# LOWER-priority dimension. The explicit canonical column is the unambiguous winner.
_DIM_CANON: dict[str, set[str]] = {
    "cost_center": {"costcenter", "costcentre"},
    "business_unit": {"businessunit"},
    "organization": {"organization", "org"},
}


def _col_canon(col: str) -> set[str]:
    """Normalized identifier of a column (strip ``x_``, split, rejoin) for exact-canon checks."""
    t = _tokens(col)
    return {"".join(t)} if t else set()

# Provider-prefixed x_* columns are NEVER org/cost-center candidates (parity with
# ``cost_reader._PROVIDER_X_PREFIXES`` — kept as a local copy so this module has
# no cross-module import for the pure-python backend path).
_PROVIDER_X_PREFIXES = (
    "aws", "gcp", "azure", "google", "anthropic", "openai", "twilio",
    "snowflake", "confluent", "datadog", "github", "stripe", "sentry",
)


def _is_provider_prefixed_x(col: str) -> bool:
    lower = col.lower()
    return any(lower.startswith(f"x_{p}") for p in _PROVIDER_X_PREFIXES)


def candidate_x_columns(schema: list[str]) -> list[str]:
    """Filter a schema to the org/cost-center candidate ``x_*`` columns.

    Drops provider-prefixed vendor tags (they can never be an org/cc dimension)
    — parity with ``cost_reader.classify_org_cost_center``.
    """
    return sorted(
        c for c in schema if c.startswith("x_") and not _is_provider_prefixed_x(c)
    )


def schema_hash(schema: list[str]) -> str:
    """Stable hash of a schema's column set → cache key (deterministic, order-independent)."""
    h = hashlib.sha256()
    for c in sorted(schema):
        h.update(c.lower().encode())
        h.update(b"\x00")
    return h.hexdigest()[:16]


# ── embedding backends ───────────────────────────────────────────────────────────────────────


@runtime_checkable
class EmbedBackend(Protocol):
    """Swappable embedding backend. All vectors are L2-normalized (unit length)."""

    name: str
    dim: int

    def embed(self, texts: list[str]) -> np.ndarray:  # (N, dim), float32, unit-norm
        ...


def _normalize(v: np.ndarray) -> np.ndarray:
    """L2-normalize rows; zero rows stay zero (avoid div-by-zero)."""
    n = np.linalg.norm(v, axis=1, keepdims=True)
    n = np.where(n == 0, 1.0, n)
    return (v / n).astype(np.float32)


def _tokens(text: str) -> list[str]:
    """Lowercase, strip a leading ``x_`` namespace, split on non-alphanumeric.

    So ``x_CostCenter`` → ``["costcenter"]`` and ``x_business_unit`` →
    ``["business","unit"]`` — both compare cleanly against ``cost center`` /
    ``business unit``.
    """
    t = text.lower().strip()
    if t.startswith("x_"):
        t = t[2:]
    return [w for w in re.split(r"[^a-z0-9]+", t) if w]


class HashingNgramBackend:
    """Char-3-gram TF embedding, hashed to a fixed width — stdlib + numpy only.

    No model download, no network, fully deterministic. Not SOTA semantic, but
    excellent for **short column-name** matching (handles camelCase, underscores,
    abbreviations, transpositions). The store format is backend-agnostic, so a
    real embedding model can replace this later without touching the file.
    """

    name = "hashing-ngram-v1"

    def __init__(self, dim: int = 2048) -> None:
        self.dim = dim

    def _vector(self, text: str) -> np.ndarray:
        v = np.zeros(self.dim, dtype=np.float32)
        toks = _tokens(text)
        joined = " ".join(toks)
        # char 3-grams over the joined token stream (spans token boundaries too,
        # so "cost center" shares grams with "costcenter")
        if len(joined) < 3:
            grams = [joined] if joined else []
        else:
            grams = [joined[i : i + 3] for i in range(len(joined) - 2)]
        for g in grams:
            h = int(hashlib.blake2b(g.encode(), digest_size=4).hexdigest(), 16) % self.dim
            v[h] += 1.0
        # also weight full tokens (exact word overlap boost: "team" vs "team_id")
        for tok in toks:
            h = int(hashlib.blake2b(b"#" + tok.encode(), digest_size=4).hexdigest(), 16) % self.dim
            v[h] += 2.0
        return v

    def embed(self, texts: list[str]) -> np.ndarray:
        if not texts:
            return np.zeros((0, self.dim), dtype=np.float32)
        mat = np.stack([self._vector(t) for t in texts])
        return _normalize(mat)


class GatewayEmbedBackend:
    """Stub: hit the Stitcher gateway ``/v1/embeddings`` via the openai client.

    NOT wired by default — the deployed Qwen gateway does not currently serve an
    embeddings endpoint, so this raises ``NotImplementedError`` loudly rather
    than silently degrading. When the deployment adds an embedding model, fill in
    ``_client`` + ``_model`` and the store format stays unchanged.
    """

    name = "stitcher-gateway-embed-v1"
    dim = 0  # set at runtime once the deployed model's dimension is known

    def __init__(self, model: str = "", base_url: str = "", api_key: str = "") -> None:
        self._model = model
        self._base_url = base_url
        self._api_key = api_key
        self._client = None  # built lazily when a real endpoint is configured

    def embed(self, texts: list[str]) -> np.ndarray:
        raise NotImplementedError(
            "GatewayEmbedBackend is not wired — the Stitcher Qwen gateway does not "
            "currently serve /v1/embeddings. Use HashingNgramBackend (default) for "
            "initial testing, or configure a real embedding model here."
        )


def default_backend() -> HashingNgramBackend:
    return HashingNgramBackend()


# ── the index ────────────────────────────────────────────────────────────────────────────────


@dataclass
class Match:
    """A per-dimension verdict from the semantic index (NOT just a picked column).

    ``verdict`` is one of:
    - ``match``  → confident: use ``column`` (deterministic, skip the LLM)
    - ``ambiguous`` → do NOT trust ``column`` as final: clear signal the lexical
      matcher cannot disambiguate (near-tie, trivial-token winner, or marginal
      near-threshold score) → caller should fall back to the LLM.
    - ``refused`` → confident there is NO such dimension (``column`` is None).

    ``confident`` is True for ``match`` and ``refused`` (both deterministic),
    False for ``ambiguous``.
    """

    dimension: str
    column: str | None
    score: float
    best_synonym: str
    runner_up: str | None = None
    margin: float = 0.0
    verdict: str = VERDICT_REFUSED
    source: str = SOURCE_INDEX

    @property
    def confident(self) -> bool:
        """True when the verdict is trustworthy without the LLM."""
        return self.verdict in (VERDICT_MATCH, VERDICT_REFUSED)


@dataclass
class MatchResult:
    organization: Match | None = None
    cost_center: Match | None = None
    candidate_columns: list[str] = field(default_factory=list)
    all_scores: dict[str, dict[str, float]] = field(default_factory=dict)


class ColumnSemanticIndex:
    """File-backed semantic index: curated corpus + per-destination cached matches.

    Build once (``build``), persist (``save``), and reload (``load``) across
    runs. ``match`` is the hot path: embed the destination's ``x_*`` columns (or
    reuse cached vectors), cosine-match against the corpus, enforce the
    "a column can't be both org+cc" rule, and return typed matches.
    """

    VERSION = 1

    def __init__(
        self,
        backend: EmbedBackend,
        corpus: dict[str, list[str]] | None = None,
        threshold: float = DEFAULT_THRESHOLD,
        refuse_threshold: float = REFUSE_THRESHOLD,
        margin_eps: float = MARGIN_EPS,
    ) -> None:
        self.backend = backend
        self.corpus = corpus or {k: list(v) for k, v in DEFAULT_CORPUS.items()}
        self.threshold = threshold
        self.refuse_threshold = refuse_threshold
        self.margin_eps = margin_eps
        # corpus vectors: {dimension: ndarray (n_syn, dim)}
        self._corpus_vectors: dict[str, np.ndarray] = {}
        # per-destination cache: key -> dict (the on-disk "destinations" entry)
        self._dest_cache: dict[str, dict[str, Any]] = {}
        self._built = False

    # ── build / persistence ─────────────────────────────────────────────────

    def build(self) -> ColumnSemanticIndex:
        """Embed the curated corpus (run once after construction or corpus edits)."""
        for dim, syns in self.corpus.items():
            self._corpus_vectors[dim] = self.backend.embed(syns)
        self._built = True
        return self

    def save(self, path: str | os.PathLike) -> None:
        """Persist corpus vectors + per-destination cache to a JSON file."""
        out: dict[str, Any] = {
            "version": self.VERSION,
            "backend": self.backend.name,
            "created": datetime.now(UTC).isoformat(timespec="seconds"),
            "threshold": self.threshold,
            "refuse_threshold": self.refuse_threshold,
            "margin_eps": self.margin_eps,
            "corpus": [],
            "destinations": self._dest_cache,
        }
        for dim, syns in self.corpus.items():
            vecs = self._corpus_vectors.get(dim)
            out["corpus"].append(
                {
                    "dimension": dim,
                    "synonyms": syns,
                    "vectors": vecs.tolist() if vecs is not None else [],
                }
            )
        Path(path).write_text(json.dumps(out, indent=2), encoding="utf-8")

    @classmethod
    def load(cls, path: str | os.PathLike, backend: EmbedBackend | None = None) -> ColumnSemanticIndex:
        """Load an index from a JSON file. Backend defaults to HashingNgramBackend.

        The on-disk ``backend`` name is checked against the supplied backend; a
        mismatch raises so a silent format/semantics drift never corrupts matches.
        """
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        if data.get("version") != cls.VERSION:
            raise ValueError(f"unsupported store version: {data.get('version')}")
        be = backend or default_backend()
        if data.get("backend") != be.name:
            raise ValueError(
                f"store backend mismatch: file is {data.get('backend')!r}, "
                f"backend is {be.name!r}. Use the matching backend or rebuild."
            )
        idx = cls(be, threshold=float(data.get("threshold", DEFAULT_THRESHOLD)))
        idx.refuse_threshold = float(data.get("refuse_threshold", REFUSE_THRESHOLD))
        idx.margin_eps = float(data.get("margin_eps", MARGIN_EPS))
        idx._dest_cache = dict(data.get("destinations", {}) or {})
        for entry in data.get("corpus", []):
            dim = entry["dimension"]
            idx.corpus[dim] = list(entry["synonyms"])
            vecs = np.asarray(entry.get("vectors", []), dtype=np.float32)
            idx._corpus_vectors[dim] = _normalize(vecs) if vecs.size else np.zeros((0, be.dim), dtype=np.float32)
        idx._built = True
        return idx

    # ── matching ────────────────────────────────────────────────────────────

    def _cosine_to_corpus(self, col_vec: np.ndarray) -> dict[str, dict[str, float]]:
        """For one column vector, best cosine score per dimension (+ best synonym)."""
        scores: dict[str, dict[str, float]] = {}
        for dim, syn_vecs in self._corpus_vectors.items():
            if syn_vecs.shape[0] == 0:
                scores[dim] = {"score": 0.0, "best_synonym": ""}
                continue
            sims = syn_vecs @ col_vec  # both unit-norm → dot = cosine
            best = int(np.argmax(sims))
            scores[dim] = {"score": float(sims[best]), "best_synonym": self.corpus[dim][best]}
        return scores

    def _dim_match(self, dim: str, entries: list[tuple[str, float, str]]) -> Match:
        """Determine one dimension's verdict from (col, score, best_syn) per column.

        Verdict rules (see the module constants — the honest ambiguity guard):
        - an EXPLICIT canonical column (``x_CostCenter``) present at score ≥ threshold is
          the unambiguous winner, even if generic columns tie with it (canonical tiebreak);
        - best below ``refuse_threshold``  → confident ``refused`` (no such dim);
        - best in [refuse_threshold, threshold) → ``ambiguous`` (marginal; the lexical
          matcher may under-score a real dimension → LLM decides);
        - best >= threshold but the winning synonym is a trivial short token
          (``bu``/``cc``/``org``) OR the margin over the runner-up < ``margin_eps``
          (columns effectively tied) → ``ambiguous`` (confident-but-wrong risk);
        - otherwise → confident ``match``.
        """
        if not entries:
            return Match(dim, None, 0.0, "", verdict=VERDICT_REFUSED)

        # canonical-exact tiebreak: an explicit canonical column wins deterministically
        ca = self._canonical_winner(dim, entries)
        if ca is not None:
            col, score, syn = ca
            others = [e for e in entries if e[0] != col]
            runner = max(others, key=lambda e: e[1]) if others else None
            runner_score = runner[1] if runner else 0.0
            runner_col = runner[0] if runner else None
            margin = score - runner_score if others else score
            return Match(dim, col, score, syn, runner_col, margin, VERDICT_MATCH)

        col, score, syn = max(entries, key=lambda e: e[1])
        others = [e for e in entries if e[0] != col]
        runner = max(others, key=lambda e: e[1]) if others else None
        runner_score = runner[1] if runner else 0.0
        runner_col = runner[0] if runner else None
        margin = score - runner_score if others else score

        if score < self.refuse_threshold:
            return Match(dim, None, score, syn, runner_col, margin, VERDICT_REFUSED)
        if score < self.threshold:
            return Match(dim, col, score, syn, runner_col, margin, VERDICT_AMBIGUOUS)
        if _is_trivial_synonym(syn) or margin < self.margin_eps:
            return Match(dim, col, score, syn, runner_col, margin, VERDICT_AMBIGUOUS)
        return Match(dim, col, score, syn, runner_col, margin, VERDICT_MATCH)

    def _canonical_winner(self, dim: str, entries: list[tuple[str, float, str]]) -> tuple[str, float, str] | None:
        """Best explicit-canonical column (score ≥ threshold), else None."""
        canon = _DIM_CANON.get(dim)
        if not canon:
            return None
        best = None
        for col, score, syn in entries:
            if score < self.threshold:
                continue
            if _col_canon(col) & canon:
                if best is None or score > best[1]:
                    best = (col, score, syn)
        return best

    @staticmethod
    def _as_ambiguous(m: Match) -> Match:
        """Downgrade a confident match to ambiguous (used by the can't-be-both rule)."""
        return Match(m.dimension, m.column, m.score, m.best_synonym, m.runner_up, m.margin,
                     VERDICT_AMBIGUOUS, m.source)

    def match(
        self,
        columns: list[str],
        env_id: str = "",
        dest_id: str = "",
        cache_key: str | None = None,
        persist_result: bool = False,
        path: str | os.PathLike | None = None,
    ) -> MatchResult:
        """Match a destination's ``x_*`` columns to org / cost-center dimensions.

        Each dimension gets a verdict (``match``/``ambiguous``/``refused``) via
        :meth:`_dim_match`; a dimension is only ``confident`` for ``match`` or
        ``refused``. Callers MUST NOT use an ``ambiguous`` dimension's column as
        the final answer — that is the deterministic-matcher-degrades case and
        must fall back to the LLM (which is why the verdict is surfaced here).

        Args:
            columns: the destination's ``x_*`` columns (ALREADY filtered to
                candidates — pass ``candidate_x_columns(schema)``).
            env_id / dest_id: scope for the on-disk cache key.
            cache_key: explicit override (else ``env::dest::schema_hash``).
            persist_result: write the match back to the store file (needs ``path``).
            path: store file path (required when ``persist_result``).
        """
        if not self._built:
            raise RuntimeError("index not built — call .build() or .load() first")
        if not columns:
            return MatchResult(candidate_columns=[])

        key = cache_key or f"{env_id}::{dest_id}::{schema_hash(columns)}"
        cached = self._dest_cache.get(key)
        # cache-hit is order-independent (schema_hash is too) — exact-list-order
        # equality rejected a persisted LLM entry whose column order differed from
        # the caller's sorted candidates, forcing a needless recompute.
        if cached and cached.get("columns") and set(cached["columns"]) == set(columns):
            return self._result_from_cache(cached)

        col_vecs = self.backend.embed(columns)
        all_scores: dict[str, dict[str, float]] = {}
        dim_entries: dict[str, list[tuple[str, float, str]]] = {}
        for i, col in enumerate(columns):
            scores = self._cosine_to_corpus(col_vecs[i])
            all_scores[col] = scores
            for dim, s in scores.items():
                dim_entries.setdefault(dim, []).append((col, s["score"], s["best_synonym"]))

        org_m = self._dim_match("organization", dim_entries.get("organization", []))
        cc_m = self._dim_match("cost_center", dim_entries.get("cost_center", []))

        # enforce "a column can't be both org and cost-center" — if EACH is a
        # confident match and they name the same column, keep the higher-margin
        # one and downgrade the other to ambiguous (the LLM decides the loser).
        if (
            org_m.verdict == VERDICT_MATCH
            and cc_m.verdict == VERDICT_MATCH
            and org_m.column is not None
            and org_m.column == cc_m.column
        ):
            if org_m.margin >= cc_m.margin:
                cc_m = self._as_ambiguous(cc_m)
            else:
                org_m = self._as_ambiguous(org_m)

        result = MatchResult(
            organization=org_m if org_m.verdict != VERDICT_REFUSED else None,
            cost_center=cc_m if cc_m.verdict != VERDICT_REFUSED else None,
            candidate_columns=list(columns),
            all_scores=all_scores,
        )

        if persist_result:
            if path is None:
                raise ValueError("persist_result=True requires a path")
            self._dest_cache[key] = self._cache_entry(columns, col_vecs, result)
            self.save(path)
        return result

    def allocation_candidates(self, columns: list[str], pipeline=None) -> list[dict[str, Any]]:
        """Rank candidate columns across an allocation-dimension pipeline (deterministic).

        Embeds the candidate ``x_*`` columns ONCE and returns a per-dimension verdict
        (same guard as :meth:`_dim_match`) for each pipeline dimension, in pipeline
        order. Each entry: ``{dimension, column, score, margin, verdict, confident}``
        where ``column`` is None when refused/absent for that dimension.
        """
        if not self._built:
            raise RuntimeError("index not built — call .build() or .load() first")
        pipeline = tuple(pipeline) if pipeline else ALLOCATION_PIPELINE
        if not columns:
            return []
        col_vecs = self.backend.embed(columns)
        dim_entries: dict[str, list[tuple[str, float, str]]] = {}
        for i, col in enumerate(columns):
            for dim, s in self._cosine_to_corpus(col_vecs[i]).items():
                dim_entries.setdefault(dim, []).append((col, s["score"], s["best_synonym"]))
        out: list[dict[str, Any]] = []
        for dim in pipeline:
            entries = dim_entries.get(dim, [])
            m = self._dim_match(dim, entries)
            out.append(
                {
                    "dimension": dim,
                    "column": m.column,
                    "score": m.score,
                    "margin": m.margin,
                    "verdict": m.verdict,
                    "confident": m.confident,
                    "best_synonym": m.best_synonym,
                }
            )
        return out

    def pick_allocation(self, columns: list[str], pipeline=None) -> dict[str, Any]:
        """Resolve the grouping dimension deterministically via the allocation pipeline.

        Walks the pipeline in priority order and stops at the first dimension that is NOT
        confidently refused:
          - if it resolved confidently (``match``) → that is the answer;
          - if it is only ``ambiguous`` → do NOT skip to a lower-priority dimension (that
            would wrongly pick organization over an explicit-but-tied cost center); return
            that decision point as ``confident=False`` so the caller hands its candidates
            to the LLM for a scoped multiple-choice pick.
        Returns ``{confident, dimension, column, score, candidates}`` where ``candidates`` is
        the full per-dimension readout (for the LLM shortlist and diagnostics).
        """
        cands = self.allocation_candidates(columns, pipeline)
        for c in cands:
            if c["verdict"] == VERDICT_MATCH:
                return {
                    "confident": True,
                    "dimension": c["dimension"],
                    "column": c["column"],
                    "score": c["score"],
                    "candidates": cands,
                }
            if c["verdict"] == VERDICT_AMBIGUOUS and c["column"]:
                # first (highest-priority) UNRESOLVED dimension — defer, never skip
                # down to a lower-priority match over an explicit-but-tied cost center.
                return {
                    "confident": False,
                    "dimension": c["dimension"],
                    "column": c["column"],
                    "score": c["score"],
                    "candidates": cands,
                }
            # VERDICT_REFUSED (or no column) → try the next pipeline dimension
        return {"confident": False, "dimension": None, "column": None,
                "score": 0.0, "candidates": cands}

    def _result_from_cache(self, cached: dict[str, Any]) -> MatchResult:
        ms = cached.get("matches", {})

        def _m(dim: str, entry: dict[str, Any] | None) -> Match | None:
            if not entry or entry.get("verdict") == VERDICT_REFUSED:
                return None
            return Match(
                dim,
                entry.get("column"),
                float(entry.get("score", 0.0)),
                entry.get("best_synonym", ""),
                entry.get("runner_up"),
                float(entry.get("margin", 0.0)),
                entry.get("verdict", VERDICT_AMBIGUOUS),
                entry.get("source", SOURCE_INDEX),
            )

        return MatchResult(
            organization=_m("organization", ms.get("organization")),
            cost_center=_m("cost_center", ms.get("cost_center")),
            candidate_columns=list(cached.get("columns", [])),
        )

    def _cache_entry(self, columns: list[str], vecs: np.ndarray, result: MatchResult) -> dict[str, Any]:
        def _m(m: Match | None) -> dict[str, Any]:
            if m is None:
                return {"verdict": VERDICT_REFUSED, "column": None, "score": 0.0,
                        "best_synonym": "", "runner_up": None, "margin": 0.0,
                        "source": SOURCE_INDEX}
            return {
                "verdict": m.verdict,
                "column": m.column,
                "score": round(m.score, 4),
                "best_synonym": m.best_synonym,
                "runner_up": m.runner_up,
                "margin": round(m.margin, 4),
                "source": m.source,
            }

        return {
            "columns": list(columns),
            "vectors": vecs.tolist(),
            "matched_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "matches": {"organization": _m(result.organization), "cost_center": _m(result.cost_center)},
        }

    def persist_llm_result(
        self,
        columns: list[str],
        organization: str | None,
        cost_center: str | None,
        env_id: str = "",
        dest_id: str = "",
        path: str | os.PathLike | None = None,
    ) -> None:
        """Persist a ground-truth LLM classification back into the store.

        The LLM is the escape hatch for genuinely ambiguous columns; once it has
        decided, that answer is authoritative for this (env, dest, schema) and is
        paid only once. Written as a confident verdict with no column vectors
        (a cache hit needs only the matches — no re-embedding, no re-classifying).
        """
        if path is None:
            raise ValueError("persist_llm_result requires a path")

        def _m(dim: str, col: str | None) -> dict[str, Any]:
            if col is None:
                return {"verdict": VERDICT_REFUSED, "column": None, "score": 0.0,
                        "best_synonym": "", "runner_up": None, "margin": 0.0,
                        "source": SOURCE_LLM}
            return {"verdict": VERDICT_MATCH, "column": col, "score": 1.0,
                    "best_synonym": "", "runner_up": None, "margin": 0.0,
                    "source": SOURCE_LLM}

        key = f"{env_id}::{dest_id}::{schema_hash(columns)}"
        self._dest_cache[key] = {
            "columns": list(columns),
            "vectors": [],
            "matched_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "matches": {"organization": _m("organization", organization),
                         "cost_center": _m("cost_center", cost_center)},
        }
        self.save(path)


__all__ = [
    "ColumnSemanticIndex",
    "HashingNgramBackend",
    "GatewayEmbedBackend",
    "EmbedBackend",
    "Match",
    "MatchResult",
    "DEFAULT_CORPUS",
    "DEFAULT_THRESHOLD",
    "REFUSE_THRESHOLD",
    "MARGIN_EPS",
    "ALLOCATION_PIPELINE",
    "VERDICT_MATCH",
    "VERDICT_AMBIGUOUS",
    "VERDICT_REFUSED",
    "SOURCE_INDEX",
    "SOURCE_LLM",
    "candidate_x_columns",
    "schema_hash",
    "default_backend",
]

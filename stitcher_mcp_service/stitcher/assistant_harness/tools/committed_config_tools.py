"""Common committed-config tools for the top-level MCP.

Grounds on the REAL prior checked-in git config for this environment + pipeline by exercising SOE
``get_vsc_commit_dir`` **as-is** (SWS pipeline lookup + GitHub App auth + per-stage loader), then
reads the returned committed stage config *objects*:

  - ``get_committed_config(branch, stage)`` — a compact per-stage summary of what the committed
    configs ALREADY do (existing Lookups' join keys / imports / provider scope, filters, …) so the
    agent EXTENDS the pipeline instead of duplicating a Lookup it already has. Never raw YAML.
  - ``derived_columns(contains)`` — the ``x_*`` columns the committed configs CREATE (bridge join
    keys), each with the config + operator that writes it.

``get_vsc_commit_dir`` is ``async`` and network-gated (SWS + GitHub App + Vault); with real env
creds it fetches the live branch head. When unscoped / unreachable it degrades to a clear note.
"""
from __future__ import annotations

from typing import Any

from fastmcp import FastMCP

# Which committed stage config keys to summarize, and whether they are enhance (have operations).
_ENHANCE_STAGE_KEYS = ("enhance_prepare_config", "enhance_enrich_config")


def _as_str(v: Any) -> str:
    return str(v).split(".")[0].strip() if v is not None else ""


def _scope_text(op: dict) -> str:
    """Compact provider/data_source scope for an operation dict (from model_dump)."""
    scope = op.get("scope") or {}
    inputs = scope.get("scope_inputs") or []
    parts = []
    for si in inputs:
        if not isinstance(si, dict):
            continue
        provider = _as_str(si.get("provider"))
        ds = si.get("data_sources") or []
        ds_txt = ", ".join(_as_str(d) for d in ds) if ds else "all"
        parts.append(f"{provider or '?'}:{ds_txt}")
    return "; ".join(parts) if parts else "all"


def _imports_text(op: dict) -> str:
    out = []
    for ic in op.get("import_columns") or []:
        if not isinstance(ic, dict):
            continue
        nm = _as_str(ic.get("name"))
        rn = _as_str(ic.get("rename_to"))
        out.append(f"{nm}" + (f"->{rn}" if rn and rn != nm else ""))
    joined = ", ".join(c for c in out if c)
    return f" imports=[{joined}]" if joined else ""


def _describe_operation(op: dict, stage: str) -> str:
    """One committed operation -> a compact line: `type name: scope + specifics`."""
    ot = _as_str(op.get("operation_type")) or "op"
    name = _as_str(op.get("name"))
    line = f"- [{ot}]"
    if name:
        line += f" {name}"
    scope = _scope_text(op)
    line += f"  (scope: {scope})"
    if ot == "Lookup":
        bn = _as_str(op.get("business_dataset_name"))
        join_cols = []
        for jc in op.get("join_columns") or []:
            if not isinstance(jc, dict):
                continue
            cj = jc.get("cost_dataset_join_column") or {}
            bj = jc.get("business_dataset_join_column") or {}
            ck = _as_str(cj.get("input")) if isinstance(cj, dict) else _as_str(cj)
            bk = _as_str(bj.get("input")) if isinstance(bj, dict) else _as_str(bj)
            join_cols.append(f"{ck}={bk}")
        line += f"  dataset={bn}"
        if join_cols:
            line += f"  join=[{', '.join(join_cols)}]"
        line += _imports_text(op)
    elif ot in ("Filter rows",):
        cond = op.get("condition") or op.get("conditions")
        line += f"  condition={_as_str(cond) or cond}"
    elif ot in ("Mapping", "Compute column", "AI assisted mapping"):
        tgt = _as_str(op.get("column_name") or op.get("target_column"))
        if tgt:
            line += f"  writes={tgt}"
    return line


def _stage_summary(key: str, config_obj: Any, stage_label: str) -> str:
    """Summarize one committed enhance config object into a few lines."""
    if config_obj is None:
        return f"## {stage_label}\n(no committed config)"
    ops = getattr(config_obj, "enhance_operations", None) or []
    ops = list(ops)
    if not ops:
        return f"## {stage_label}\n(committed config with no operations)"
    lines = [f"## {stage_label}  ({len(ops)} operation(s))"]
    for op in ops:
        try:
            d = op.model_dump(mode="json") if hasattr(op, "model_dump") else op
        except Exception:
            d = {}
        lines.append(_describe_operation(d, key))
    return "\n".join(lines)


# ── derived columns (what the committed configs CREATE) ──────────────────────
def _created_by_op(op: dict) -> list[str]:
    """Columns this one operation CREATES (bridge join keys, enrich outputs)."""
    cols: list[str] = []
    for k in ("column_name", "target_column"):  # Mapping / Compute / AI assisted mapping
        c = _as_str(op.get(k))
        if c:
            cols.append(c)
    for ic in op.get("import_columns") or []:  # Lookup
        if isinstance(ic, dict):
            nm = _as_str(ic.get("rename_to") or ic.get("name"))
            if nm:
                cols.append(nm)
    for r in op.get("key_selection_rules") or []:  # Unpack object / JSON value
        if isinstance(r, dict):
            tgt = _as_str(r.get("target_column_name"))
            if tgt:
                cols.append(tgt)
    return list(dict.fromkeys(c for c in cols if c))


def _build_derived(pipeline_configs: dict) -> dict[str, list[dict]]:
    """Map column -> [ {config, operator, name} ] for every column the committed configs create."""
    index: dict[str, list[dict]] = {}
    provenance = {
        "enhance_prepare_config": ("enhance/prepare", "prepare"),
        "enhance_enrich_config": ("enhance/enrich", "enrich"),
    }
    for key, (stage_path, _stage) in provenance.items():
        cfg = pipeline_configs.get(key)
        if cfg is None:
            continue
        ops = getattr(cfg, "enhance_operations", None) or []
        for op in ops:
            try:
                d = op.model_dump(mode="json") if hasattr(op, "model_dump") else op
            except Exception:
                continue
            for col in _created_by_op(d):
                index.setdefault(col, []).append(
                    {
                        "column": col,
                        "stage": stage_path,
                        "operator": _as_str(d.get("operation_type")) or "op",
                        "name": _as_str(d.get("name")),
                    }
                )
    return index


def _derived_text(pipeline_configs: dict, contains: str) -> str:
    index = _build_derived(pipeline_configs)
    if not index:
        return "(no derived columns found in the committed enhance configs this environment/pipeline)"
    terms = [t.strip().lower() for t in (contains or "").split(",") if t.strip()]
    names = sorted(index)
    if terms:
        names = [n for n in names if any(t in n.lower() for t in terms)]
    if not names:
        return f"(no derived columns matching {contains!r})"
    lines = [f"# derived columns ({len(names)})"]
    for col in names:
        srcs = index[col]
        prov = "; ".join(f"{s['stage']}/{s['operator']}" + (f"({s['name']})" if s["name"] else "") for s in srcs)
        lines.append(f"- {col}  — {prov}")
    return "\n".join(lines)


def _committed(pipeline_configs: dict, stage: str) -> str:
    stage = (stage or "").strip().lower()
    if stage not in ("", "prepare", "enrich", "enhance", "all"):
        return f"ERR: stage must be '' | prepare | enrich | enhance | all (got {stage!r})"
    wanted = {
        "prepare": ["enhance_prepare_config"],
        "enrich": ["enhance_enrich_config"],
        "enhance": list(_ENHANCE_STAGE_KEYS),
        "all": list(_ENHANCE_STAGE_KEYS),
        "": list(_ENHANCE_STAGE_KEYS),
    }[stage]
    labels = {
        "enhance_prepare_config": "Enhance · Prepare",
        "enhance_enrich_config": "Enhance · Enrich",
    }
    blocks = [_stage_summary(k, pipeline_configs.get(k), labels[k]) for k in wanted]
    return "\n\n".join(b for b in blocks if b)


def register(mcp: FastMCP, client, soe) -> None:
    @mcp.tool
    async def get_committed_config(branch: str = "", stage: str = "enrich") -> str:
        """Fetch the LATEST COMMITTED pipeline config from the git branch for this environment's
        pipeline via the SOE git integration, and return a compact summary of what the enhance
        configs ALREADY do (each operation's type, name, provider scope, Lookup join keys + imported
        columns, filters). Call BEFORE authoring so you extend the pipeline rather than duplicate a
        Lookup it already has. stage = '' | prepare | enrich | enhance | all. Never returns raw YAML."""
        pipeline_configs, err = await soe.fetch_committed_configs(branch)
        if err:
            return err
        return _committed(pipeline_configs or {}, stage)

    @mcp.tool
    async def derived_columns(contains: str = "") -> str:
        """The DERIVED columns (e.g. x_team) the COMMITTED enhance configs create — built by an
        earlier committed config (a Lookup import rename_to, a Mapping/Compute output, an
        AI-assisted-mapping target), each with the stage + operator that writes it. Use this to find
        a bridge join key instead of asking the user. contains = comma-separated substrings; empty = all."""
        pipeline_configs, err = await soe.fetch_committed_configs("")
        if err:
            return err
        return _derived_text(pipeline_configs or {}, contains)

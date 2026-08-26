"""Authoring / validation / save tools for the config_generation sub-MCP (full scope).

All four are deterministic and **validate-by-construction** through the REAL SPC enhance pydantic
models (`EnhancePrepareConfigModelV1` / `EnhanceEnrichConfigModelV1`), so a bad judgment cannot
produce an invalid config:

  - ``generate_lookup(...)`` — build an enrich/prepare Lookup operation deterministically; refuse
    an empty join, a duplicated/shadowing import rename, or (when metadata is supplied) an import
    column that is not in the business dataset.
  - ``generate_filter(...)`` — build a Filter-rows operation with the CORRECT polarity by
    construction (keep_or_drop states the intent; the tool emits Include/Exclude so the polarity
    can't be inverted the way hand-written YAML was).
  - ``validate_config(stage, yaml_text)`` — parse + validate a config against the stage model,
    returning PASS or precise per-operation errors.
  - ``save_config(stage, name, yaml_text)`` — validate then write to an output dir (the only persist).
"""
from __future__ import annotations

import pathlib
from typing import Any, Optional

import yaml
from fastmcp import FastMCP

_STAGE_MODELS = {
    "prepare": "EnhancePrepareConfigModelV1",
    "enrich": "EnhanceEnrichConfigModelV1",
}
_MODEL_MOD = "stitcher.pipeline.common.pipeline_config_models.versions.v1_alpha.enhance.enhance_config"


def _stage_model(stage: str):
    from importlib import import_module

    mod = import_module(_MODEL_MOD)
    return getattr(mod, _STAGE_MODELS[stage])


def _validate_stage_text(stage: str, yaml_text: str) -> tuple[Optional[dict], list[str]]:
    """Parse YAML and validate against the stage model. Returns (config_dict, error_list)."""
    try:
        doc = yaml.safe_load(yaml_text)
    except Exception as e:  # noqa: BLE001
        return None, [f"YAML parse error: {e}"]
    if not isinstance(doc, dict):
        return None, ["config must be a YAML mapping with an enhance_operations list"]
    queries = doc.get("enhance_operations")
    if not isinstance(queries, list) or not queries:
        return None, ["enhance_operations must be a non-empty list"]
    try:
        _stage_model(stage).model_validate(doc)
        return doc, []
    except Exception as e:  # noqa: BLE001
        errs = []
        for er in getattr(e, "errors", lambda: [])():
            loc = ".".join(str(p) for p in er.get("loc", ()) if "[" not in str(p))
            errs.append(f"[pydantic] {loc}: {er.get('msg', 'invalid')}")
        return doc, errs or [f"[pydantic] {str(e).splitlines()[0]}"]


def _scope_dict(providers: list) -> Optional[dict]:
    """Build the operation's provider scope by constructing SPC's ``ScopeObject`` /
    ``BasicScopeInput`` directly (validate-by-construction) — a malformed scope is refused by
    pydantic at construction, not discovered at the end of authoring. Returns ``None`` (no scope)
    when no providers are given."""
    from stitcher.pipeline.common.pipeline_config_models.versions.v1_alpha.common.scope_input_model import (
        BasicScopeInput,
        ScopeObject,
        ScopeType,
    )

    providers = [str(p).strip() for p in (providers or []) if p and str(p).strip()]
    if not providers:
        return None
    scope = ScopeObject(
        type=ScopeType.INCLUDE,
        scope_inputs=[BasicScopeInput(provider=p) for p in providers],
    )
    return scope.model_dump(mode="json")


def _fmt_errs(prefix: str, errs: list[str]) -> str:
    return prefix + "\n" + "\n".join(f"  - {e}" for e in errs)


def lookup_text(
    stage: str,
    business_dataset: str,
    cost_join_column: str,
    business_join_column: str,
    imports: list,
    providers: list | None = None,
    available_columns: str = "",
    name: str = "",
) -> str:
    """Pure, testable core of generate_lookup (the tool delegates to this). Returns YAML or an ERR string."""
    if stage not in _STAGE_MODELS:
        return f"ERR: stage must be one of {list(_STAGE_MODELS)} (got {stage!r})"
    if not business_dataset or not cost_join_column or not business_join_column:
        return "ERR: business_dataset, cost_join_column, and business_join_column are all required."
    imports = [i for i in (imports or []) if isinstance(i, dict)]
    if not imports:
        return "ERR: at least one import column ({'name': ..., 'rename_to': ...}) is required."
    avail = [c.strip() for c in (available_columns or "").split(",") if c.strip()]
    known = set(c.lower() for c in avail) if avail else set()
    if known:
        for i in imports:
            nm = str(i.get("name") or "").strip()
            if nm and nm.lower() not in known:
                return f"ERR: import column {nm!r} is not in available_columns (known: {', '.join(avail)})"
    seen = set()
    for i in imports:
        rn = str(i.get("rename_to") or i.get("name") or "").strip()
        if not rn:
            return "ERR: every import needs a name (and rename_to)."
        if rn in seen:
            return f"ERR: import rename_to {rn!r} is used twice in one Lookup (would shadow)."
        seen.add(rn)

    op = {
        "operation_type": "Lookup",
        "name": name or f"{business_dataset}-enrich",
        "business_dataset_name": business_dataset,
        "join_columns": [
            {
                "cost_dataset_join_column": {"input": cost_join_column, "type": "Column"},
                "business_dataset_join_column": {"input": business_join_column, "type": "Column"},
            }
        ],
        "import_columns": [
            {"name": str(i.get("name")), "type": "Custom column", "rename_to": str(i.get("rename_to") or i.get("name"))}
            for i in imports
        ],
    }
    sc = _scope_dict(providers)
    if sc:
        op["scope"] = sc
    doc = {"config_type": stage, "enhance_operations": [op]}
    _, errs = _validate_stage_text(stage, yaml.safe_dump(doc, sort_keys=False))
    if errs:
        return _fmt_errs("FAIL — config invalid:", errs)
    return yaml.safe_dump(doc, sort_keys=False)


def _filter_text(
    stage: str,
    column: str,
    operator: str,
    keep_or_drop: str,
    value: Any = None,
    providers: list | None = None,
    name: str = "",
    value_type: str = "",
) -> str:
    """Pure, testable core of generate_filter. Returns YAML (with polarity note) or an ERR string."""
    if stage not in _STAGE_MODELS:
        return f"ERR: stage must be one of {list(_STAGE_MODELS)} (got {stage!r})"
    if not column:
        return "ERR: column is required."
    op = (operator or "").strip().lower()
    valid_ops = {"=", "!=", ">", "<", ">=", "<=", "is null", "is not null"}
    if op not in valid_ops:
        return f"ERR: operator must be one of {sorted(valid_ops)} (got {operator!r})"
    drop = str(keep_or_drop or "").strip().lower() in ("drop", "exclude", "remove", "omit")
    if op in ("is null", "is not null"):
        condition = {"column_name": column, "operator": op}
    else:
        if value is None:
            return f"ERR: operator {op} requires a value."
        vt = (str(value_type) or "").strip()
        cond = {"column_name": column, "operator": op}
        cond["value"] = {"input": value, "type": vt if vt in ("Text", "Numerical", "Boolean", "Column") else "Text"}
        condition = cond
    op_dict = {
        "operation_type": "Filter rows",
        "name": name or f"filter-{'-'.join(column.lower().split())}",
        "filter_method": "Exclude" if drop else "Include",
        "conditions": [condition],
    }
    sc = _scope_dict(providers)
    if sc:
        op_dict["scope"] = sc
    doc = {"config_type": stage, "enhance_operations": [op_dict]}
    _, errs = _validate_stage_text(stage, yaml.safe_dump(doc, sort_keys=False))
    if errs:
        return _fmt_errs("FAIL — config invalid:", errs)
    verb = "drop" if drop else "keep"
    note = f"# filter_method {op_dict['filter_method']} — keeps {'only rows MATCHING' if not drop else 'ALL rows EXCEPT those matching'}\n"
    return note + yaml.safe_dump(doc, sort_keys=False)


def register(mcp: FastMCP, client, soe) -> None:
    @mcp.tool
    def generate_lookup(
        stage: str = "enrich",
        business_dataset: str = "",
        cost_join_column: str = "",
        business_join_column: str = "",
        imports: list = None,
        providers: list = None,
        available_columns: str = "",
        name: str = "",
    ) -> str:
        """Author an enhance Lookup operation DETERMINISTICALLY (no hand-written YAML).

        Joins ``business_dataset`` onto the cost rows on ``cost_join_column`` =
        ``business_join_column`` and imports the listed columns. imports = [{"name","rename_to"}].
        Validated against the real SPC enhance model before returning. Refuses an empty join, a
        duplicate/renaming-shadow import, and (when available_columns is supplied) an import whose
        ``name`` is not among them. stage = prepare | enrich. Returns the config YAML."""
        if stage not in _STAGE_MODELS:
            return f"ERR: stage must be one of {list(_STAGE_MODELS)} (got {stage!r})"
        if not business_dataset or not cost_join_column or not business_join_column:
            return "ERR: business_dataset, cost_join_column, and business_join_column are all required."
        imports = [i for i in (imports or []) if isinstance(i, dict)]
        if not imports:
            return "ERR: at least one import column ({'name': ..., 'rename_to': ...}) is required."
        avail = [c.strip() for c in (available_columns or "").split(",") if c.strip()]
        known = set(c.lower() for c in avail) if avail else set()
        # refuse unknown imports when the business-dataset columns are supplied
        if known:
            for i in imports:
                nm = str(i.get("name") or "").strip()
                if nm and nm.lower() not in known:
                    return f"ERR: import column {nm!r} is not in available_columns (known: {', '.join(avail)})"
        # duplicate / shadowing rename
        seen = set()
        for i in imports:
            rn = str(i.get("rename_to") or i.get("name") or "").strip()
            if not rn:
                return "ERR: every import needs a name (and rename_to)."
            if rn in seen:
                return f"ERR: import rename_to {rn!r} is used twice in one Lookup (would shadow)."
            seen.add(rn)

        return lookup_text(
            stage,
            business_dataset,
            cost_join_column,
            business_join_column,
            imports,
            providers,
            available_columns,
            name,
        )

    @mcp.tool
    def generate_filter(
        stage: str = "enrich",
        column: str = "",
        operator: str = "=",
        keep_or_drop: str = "keep",
        value: Any = None,
        providers: list = None,
        name: str = "",
        value_type: str = "",
    ) -> str:
        """Author a Filter-rows operation DETERMINISTICALLY with correct polarity by construction.

        State the INTENT, not the polarity: keep_or_drop='drop' REMOVES the rows matching the
        condition (emits Exclude); keep retains only those rows (Include). So the polarity can't be
        inverted the way hand-written YAML was. operator = = | != | > | < | >= | <= | 'is null' |
        'is not null'. Validated against the real SPC enhance model. stage = prepare | enrich."""
        if stage not in _STAGE_MODELS:
            return f"ERR: stage must be one of {list(_STAGE_MODELS)} (got {stage!r})"
        return _filter_text(
            stage,
            column,
            operator,
            keep_or_drop,
            value,
            providers,
            name,
            value_type,
        )

    @mcp.tool
    def validate_config(stage: str, yaml_text: str) -> str:
        """Validate a config (YAML text) against the real SPC enhance model for the stage.
        Returns PASS or precise per-operation errors. stage = prepare | enrich."""
        if stage not in _STAGE_MODELS:
            return f"ERR: stage must be one of {list(_STAGE_MODELS)} (got {stage!r})"
        doc, errs = _validate_stage_text(stage, yaml_text)
        if errs:
            return _fmt_errs("FAIL:", errs)
        n = len((doc or {}).get("enhance_operations") or [])
        model_name = _STAGE_MODELS[stage]
        return f"PASS ✓  {n} operation(s) valid against {model_name}"

    @mcp.tool
    def save_config(stage: str, name: str, yaml_text: str) -> str:
        """Validate then persist a config to the output/enhance/ dir (the only way to write).
        stage = prepare | enrich. Returns 'saved: <path>' on success, FAIL + errors otherwise."""
        if stage not in _STAGE_MODELS:
            return f"ERR: stage must be one of {list(_STAGE_MODELS)} (got {stage!r})"
        doc, errs = _validate_stage_text(stage, yaml_text)
        if errs:
            return _fmt_errs("FAIL — not saved:", errs)
        out_dir = pathlib.Path(soe.output_dir) / "enhance" / stage
        out_dir.mkdir(parents=True, exist_ok=True)
        safe_name = (name or "generated").replace("/", "_").replace("\\", "_") + ".yaml"
        path = out_dir / safe_name
        path.write_text(yaml.safe_dump(doc, sort_keys=False))
        return f"saved: {path}"

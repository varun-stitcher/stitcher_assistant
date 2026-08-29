"""Operator + environment-context tools for the config_generation sub-MCP.

  - ``list_operators(stage)`` — enumerate the enhance operators (prepare|enrich) each with a
    short purpose, grounded on SPC's ``EnhanceOperationType`` + per-operation field specs.
  - ``describe_operator(stage, operation_type)`` — one operator's full field list (title, type,
    required/default, description) + a REAL example pulled from the SPC example configs.
  - ``environment_context()`` — the SOE scope this sub-MCP is bound to (env/pipeline/branch/auth).

All grounding is deterministic (SPC models + example configs, no LLM). pi turns the listed
operator vocabulary into a choice the user picks from, and copies described examples' shapes when
authoring.
"""
from __future__ import annotations

from typing import Optional

from fastmcp import FastMCP

from stitcher.pipeline.common.pipeline_config_models.versions.v1_alpha.enhance.sub_models.common_fields import (
    EnhanceOperationType,
)
from stitcher.pipeline.common.pipeline_config_models.versions.v1_alpha.help_text import enhance as ht

_VALID_STAGES = ("prepare", "enrich")

# Canonical operator vocabulary — single source of truth is SPC's EnhanceOperationType enum, NOT a
# hand-maintained copy (keeps the LLM from inventing/guessing an operation_type).
_KNOWN_OPERATORS = tuple(op.value for op in EnhanceOperationType)

# operation_type display value -> (module suffix, class name) in the SPC enhance sub_models package.
# Matches the TRANSFORM_UNION_TYPE members on EnhancePrepare/EnrichConfigModelV1. SPC exposes no
# display->class map, so this small local table is unavoidable (used only to locate the model).
_MODULE = "stitcher.pipeline.common.pipeline_config_models.versions.v1_alpha.enhance.sub_models"
_OPERATOR_MODELS = {
    "Lookup": ("enhance_lookup_rule", "EnhanceLookupRule"),
    "Mapping": ("enhance_mapping_rule", "EnhanceMappingRule"),
    "Unpack object value": ("enhance_complex_field_unpack_rule", "EnhanceComplexFieldUnpackRule"),
    "Unpack JSON value": ("enhance_complex_field_unpack_rule", "EnhanceJsonFieldUnpackRule"),
    "Compute column": ("enhance_compute_column", "EnhanceComputeRule"),
    "Filter rows": ("enhance_filter_rule", "EnhanceFilterRowsRule"),
    "Remove columns": ("enhance_remove_columns_rule", "EnhanceRemoveColumnsRule"),
    "Simulate periodic cost": ("enhance_cost_simulation_rule", "EnhanceCostSimulatorRule"),
    "AI assisted mapping": ("enhance_ai_assisted_mapping_rule", "EnhanceAiAssistedMappingRule"),
    "Amortize cost": ("enhance_cost_amortization_rule", "EnhanceCostAmortizationRule"),
}


# operation_type -> one-line purpose, sourced from SPC's PER-OPERATOR help text (the *_MODEL_SHORT
# constants on each enhance rule's model) — the "operator description from our SPC base object". No
# hand-written duplicate. Whitespace is normalized (the source strings are parenthesized literals).
def _clean_purpose(constant: str) -> str:
    return " ".join(constant.split())


_OP_PURPOSE = {
    EnhanceOperationType.LOOKUP.value: _clean_purpose(ht.ENHANCE_LOOKUP_MODEL_SHORT),
    EnhanceOperationType.MAPPING.value: _clean_purpose(ht.ENHANCE_MAPPING_MODEL_SHORT),
    EnhanceOperationType.COMPLEX_FIELD_UNPACK.value: _clean_purpose(ht.ENHANCE_FIELD_UNPACK_MODEL_SHORT),
    EnhanceOperationType.JSON_FIELD_UNPACK.value: _clean_purpose(ht.ENHANCE_JSON_FIELD_UNPACK_MODEL_SHORT),
    EnhanceOperationType.COMPUTE_COLUMN.value: _clean_purpose(ht.ENHANCE_COMPUTE_COLUMN_MODEL_SHORT),
    EnhanceOperationType.FILTER_ROWS.value: _clean_purpose(ht.ENHANCE_FILTER_ROWS_MODEL_SHORT),
    EnhanceOperationType.REMOVE_COLUMNS.value: _clean_purpose(ht.ENHANCE_REMOVE_COLUMNS_MODEL_SHORT),
    EnhanceOperationType.SIMULATE_PERIODIC_COST.value: _clean_purpose(ht.ENHANCE_COST_SIMULATION_MODEL_SHORT),
    EnhanceOperationType.AMORTIZE_COST.value: _clean_purpose(ht.ENHANCE_COST_AMORTIZATION_MODEL_SHORT),
    EnhanceOperationType.AI_ASSISTED_MAPPING.value: _clean_purpose(ht.ENHANCE_AI_ASSISTED_MAPPING_MODEL_SHORT),
}

# Refuse at import if the local tables drift from the SPC enum — the enum is the source of truth.
assert set(_OPERATOR_MODELS) == set(_KNOWN_OPERATORS), "operator_tools vocabulary diverged from EnhanceOperationType"
assert set(_OP_PURPOSE) == set(_KNOWN_OPERATORS), "operator_tools purposes diverged from EnhanceOperationType"


def _check_stage(stage: str) -> Optional[str]:
    st = (stage or "").strip().lower()
    if st not in _VALID_STAGES:
        return f"ERR: stage must be one of {_VALID_STAGES} (got {stage!r})"
    return None


def _load_model(operation_type: str):
    """Return the SPC pydantic sub-model class for an operation_type, or None."""
    entry = _OPERATOR_MODELS.get(operation_type)
    if entry is None:
        return None
    mod_name, cls_name = entry
    try:
        import importlib

        mod = importlib.import_module(f"{_MODULE}.{mod_name}")
        return getattr(mod, cls_name)
    except Exception:
        return None


def _example_for(stage: str, operation_type: str) -> Optional[str]:
    """Find a real example config for this stage+operation from the SPC example configs.
    Returns a YAML snippet of the single operation, or None."""
    try:
        from importlib.resources import files

        base = (
            files("stitcher.pipeline.common.pipeline_config_models.ai.config_generation_agent.example_configs")
            .joinpath("enhance")
            .joinpath(stage)
        )
        if not base.is_dir():
            return None
        for path in base.iterdir():
            if not path.name.endswith((".yaml", ".yml")):
                continue
            try:
                import yaml

                doc = yaml.safe_load(path.read_text())
                ops = (doc or {}).get("enhance_operations") or []
                for op in ops:
                    if isinstance(op, dict) and op.get("operation_type") == operation_type:
                        return yaml.safe_dump(op, sort_keys=False).strip()
            except Exception:
                continue
    except Exception:
        pass
    return None


def _describe_fields(model) -> str:
    """Serialize the pydantic model's fields: name, type, required/default, title, description."""
    lines = []
    for name, field in model.model_fields.items():
        ann = field.annotation
        ann_str = getattr(ann, "__name__", None) or getattr(ann, "_name", None) or str(ann)
        req = "required" if field.is_required() else f"default={_fmt_default(field.default)}"
        title = field.title or name
        desc = (field.description or "").strip()
        lines.append(f"- {name}  ({ann_str}, {req})")
        lines.append(f"    {title}" + (f" — {desc}" if desc else ""))
    return "\n".join(lines)


def _fmt_default(v) -> str:
    if v is None:
        return "None"
    if hasattr(v, "value"):
        return str(v.value)
    if isinstance(v, (list, dict)):
        return str(v)
    return str(v)


def register(mcp: FastMCP, client, soe) -> None:
    @mcp.tool
    def list_operators(stage: str = "enrich") -> str:
        """Enumerate the enhance operators for a stage (prepare | enrich) with a one-line purpose
        and the operation_type value to pass to describe_operator when authoring. Deterministic
        (grounded on the SPC enhance model vocabulary), no LLM."""
        err = _check_stage(stage)
        if err:
            return err
        lines = [f"# Enhance operators — stage={stage}", ""]
        for op in _KNOWN_OPERATORS:
            lines.append(f"- `{op}` — {_OP_PURPOSE.get(op, '')}".rstrip())
        lines.append("")
        lines.append("Call `describe_operator(stage, operation_type)` for a full field spec + a real example.")
        return "\n".join(lines)

    @mcp.tool
    def describe_operator(stage: str, operation_type: str) -> str:
        """One operator's full field spec (name / type / required-or-default / description) plus a
        REAL example from this pipeline's example configs. Copy the example's shape when authoring.
        stage = prepare | enrich."""
        err = _check_stage(stage)
        if err:
            return err
        op = (operation_type or "").strip()
        if op not in _KNOWN_OPERATORS:
            known = ", ".join(_KNOWN_OPERATORS)
            return f"ERR: unknown operation_type {op!r}. Known: {known}"
        model = _load_model(op)
        if model is None:
            return f"ERR: could not load the SPC model for operation_type {op!r}"
        lines = [f"# operator {op!r}  (stage={stage})", ""]
        lines.append(_OP_PURPOSE.get(op, "").strip())
        lines.append("")
        lines.append("## Fields")
        lines.append(_describe_fields(model))
        ex = _example_for(stage, op)
        if ex:
            lines.append("")
            lines.append("## Real example")
            lines.append("```yaml")
            lines.append(ex)
            lines.append("```")
        else:
            lines.append("")
            lines.append("(no example in the example configs for this stage+operation)")
        return "\n".join(lines)

    @mcp.tool
    def environment_context() -> str:
        """Report the SOE scope this config_generation agent is bound to: environment id, pipeline id,
        pipeline name, git branch, auth tenant, whether SOE env files were loaded. No secrets."""
        return soe.summary()

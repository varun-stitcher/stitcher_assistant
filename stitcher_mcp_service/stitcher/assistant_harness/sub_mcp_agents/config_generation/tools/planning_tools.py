"""LLM-assisted operation planning for the config_generation sub-MCP.

The **fuzzy** step that maps a natural-language enhance requirement to the best enhance
operation(s) + the concrete fields each needs. Deliberately thin on the LLM side (ONE structured
call, reusing the Stitcher gateway via ``LLMAgentProxy`` — same pattern as ``custom_cost``), with
all safety held server-side:

  1. **Grounding (deterministic)** — the stage's operator vocabulary (operation_type + purpose)
     and, when the caller passes them, the real data-source columns + business datasets gathered by
     ``get_data_source_metadata`` / ``get_committed_config``.
  2. **The one fuzzy call** — ask the LLM for a structured ``EnhanceOperationPlan`` (list of
     operation drafts, each with operation_type / name / rationale / fields).
  3. **Guard (deterministic, refuse-by-construction)** — drop/flag any operation whose
     ``operation_type`` is not in the stage's known vocabulary, or that references a column /
     business dataset not present in the provided metadata. Every drop is reported, never merged.

No silent fallback: if the LLM gateway is unreachable the tool returns a clear error (never a
fabricated "plan").
"""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

from fastmcp import Context, FastMCP
from pydantic import BaseModel, Field

from stitcher.pipeline.common.pipeline_config_models.versions.v1_alpha.enhance.sub_models.common_fields import (
    EnhanceOperationType,
)

from .operator_tools import _KNOWN_OPERATORS, _OP_PURPOSE  # single source of truth (sourced from SPC)

logger = logging.getLogger(__name__)


def _op_type_value(raw) -> str:
    """Normalize an operation_type (enum member or raw string) to its display string."""
    return raw.value if isinstance(raw, EnhanceOperationType) else str(raw or "").strip()


class EnhanceOperationDraft(BaseModel):
    """A single operation the requirement maps to. ``fields`` holds the operator-specific inputs
    (e.g. for a Lookup: business_dataset_name, join_columns, import_columns).

    ``operation_type`` is typed as SPC's ``EnhanceOperationType`` enum so the LLM's structured
    output is constrained to the real vocabulary (no guessing) — an out-of-union value is refused
    by construction at the pydantic boundary."""

    operation_type: EnhanceOperationType = Field(
        description="One of the stage's known enhance operation types (from EnhanceOperationType)."
    )
    name: str = Field(description="A short slug for the operation (e.g. 'aws-service-to-team').")
    rationale: str = Field(description="One sentence explaining why this operation fits the requirement.")
    fields: Dict[str, Any] = Field(
        default_factory=dict,
        description="Concrete inputs this operation needs (join keys, imports, target column, condition, …).",
    )


class EnhanceOperationPlan(BaseModel):
    """The deterministic-guarded output of the planning step."""

    operations: List[EnhanceOperationDraft] = Field(
        default_factory=list, description="The ordered operations to apply to satisfy the requirement."
    )
    summary: str = Field(default="", description="A one-line plain-language summary of the plan.")


def _vocabulary(stage: str) -> str:
    return "\n".join(f"- {op}: {_OP_PURPOSE.get(op, '')}" for op in _KNOWN_OPERATORS)


def _normalize_cols(raw: str) -> List[str]:
    return [c.strip() for c in (raw or "").split(",") if c.strip()]


def _guard(
    plan: EnhanceOperationPlan,
    stage: str,
    available_columns: List[str],
    business_datasets: List[str],
    business_dataset_columns: Optional[List[str]] = None,
) -> List[dict]:
    """Deterministic refusals. Returns a list of {op, reason} for every operation that must NOT
    be shipped (unknown operation_type, or a referenced column/dataset not in the provided
    metadata).

    Columns are validated **per side**, because a Lookup's imports live on the BUSINESS dataset
    while its join key lives on the COST dataset — validating imports against the cost columns is
    what over-refused valid Lookups before:
      * `available_columns` (cost-dataset columns) → cost-side refs: cost_dataset_join_column,
        column_name / source_column / target_column for Mapping/Compute/AI-assisted.
      * `business_dataset_columns` (the business/reference dataset's columns) → business-side refs:
        business_dataset_join_column + import_columns[].name.
    A side's refs are only validated when that side's column list was provided (so passing only the
    cost columns no longer nukes valid imports). Output columns the op CREATES (rename_to, and the
    Mapping/Compute/AI target) are not forced into any input allowlist."""
    dropped: List[dict] = []
    known_set = set(_KNOWN_OPERATORS)
    cost_set = {c.lower() for c in available_columns}
    ds_set = {d.lower() for d in business_datasets}
    biz_col_set = {c.lower() for c in (business_dataset_columns or [])}
    for op in plan.operations:
        ot = _op_type_value(op.operation_type)
        if ot not in known_set:
            dropped.append(
                {"operation_type": ot, "name": op.name, "reason": f"unknown operation type for stage {stage}"}
            )
            continue  # one refusal per op; skip the rest of its checks
        f = op.fields or {}
        # Lookup: business_dataset_name + join columns + import columns must be grounded
        bn = str(f.get("business_dataset_name") or "").strip()
        if bn and ds_set and bn.lower() not in ds_set:
            dropped.append(
                {
                    "operation_type": ot,
                    "name": op.name,
                    "reason": f"business_dataset_name {bn!r} not in the provided datasets",
                }
            )
            continue
        # split every reference by side
        cost_refs: List[str] = []
        biz_refs: List[str] = []
        for jc in f.get("join_columns") or []:
            if isinstance(jc, dict):
                # cost side: the left/join key into the cost dataset
                cost_refs.append(str(jc.get("cost_dataset_join_column") or ""))
                # business side: the key + imports live on the business dataset
                biz_refs.append(str(jc.get("business_dataset_join_column") or ""))
            else:
                cost_refs.append(str(jc))
        for ic in f.get("import_columns") or []:
            if isinstance(ic, dict):
                biz_refs.append(str(ic.get("name") or ""))  # imports come FROM the business dataset
            else:
                biz_refs.append(str(ic))
        # Mapping/Compute/AI-assisted operate on the cost dimension (column_name / source_column)
        cost_refs.append(str(f.get("column_name") or ""))
        cost_refs.append(str(f.get("source_column") or ""))
        # Filter rows condition is a free-form expression (may reference cost, business or derived
        # columns); don't force it into either input allowlist by exact-match (that over-refused).
        # NOTE: target_column is an OUTPUT the op creates — not validated against inputs.
        bad_cost = [c for c in cost_refs if c and c.lower() not in cost_set]
        if bad_cost and cost_set:
            dropped.append(
                {
                    "operation_type": ot,
                    "name": op.name,
                    "reason": "references columns not in the COST dataset metadata: "
                    + ", ".join(dict.fromkeys(bad_cost)),
                }
            )
            continue
        bad_biz = [c for c in biz_refs if c and c.lower() not in biz_col_set]
        if bad_biz and biz_col_set:
            dropped.append(
                {
                    "operation_type": ot,
                    "name": op.name,
                    "reason": "references columns not in the BUSINESS dataset metadata: "
                    + ", ".join(dict.fromkeys(bad_biz)),
                }
            )
    return dropped


def register(mcp: FastMCP, client, soe) -> None:
    @mcp.tool
    async def plan_enhance_operations(
        ctx: Context,
        stage: str,
        requirement: str,
        available_columns: str = "",
        business_datasets: str = "",
        business_dataset_columns: str = "",
        model_name: str | None = None,
        timeout_seconds: float = 60.0,
    ) -> str:
        """Map a natural-language enhance requirement to the best operation(s) to apply.

        Makes ONE structured LLM call (Stitcher gateway) grounded on the stage's operator vocabulary
        + the real metadata you pass in, then deterministically guards the result: any operation with
        an unknown type, or referencing a column / business dataset NOT in the metadata you supplied,
        is dropped and reported — never shipped.

        Pass `available_columns` (the COST dataset's columns, from get_data_source_metadata) and
        `business_datasets` (from list_data_sources / get_committed_config) so the plan can name real
        join keys and datasets. Pass `business_dataset_columns` (the business/reference dataset's own
        columns, from get_data_source_metadata) so a Lookup's imports + business join keys are checked
        against the BUSINESS side — otherwise imports are skipped (passing only cost columns must NOT
        nuke valid Lookup imports). stage = prepare | enrich. Returns the plan as JSON plus the
        dropped/refused rows.

        To author the chosen operation(s), pass the resulting fields to generate_lookup /
        generate_filter, or hand-write YAML and validate_config it.
        """
        from stitcher.pipeline.common.invoice_parser.parser_settings import get_parser_settings
        from stitcher.pipeline.common.invoice_parser.utils.openai_utils import get_openai_client
        from stitcher.pipeline.common.pipeline_config_models.ai.common.ai_agent_proxy.base import LLMAgentProxy

        t0 = time.time()
        stage = (stage or "").strip().lower()
        if stage not in ("prepare", "enrich"):
            return f"ERR: stage must be prepare | enrich (got {stage!r})"
        req = (requirement or "").strip()
        if not req:
            return "ERR: provide a requirement to plan operations for."

        try:
            settings = get_parser_settings()
            effective_model = model_name or settings.plan_generation_model
            client = get_openai_client()
        except Exception as e:  # noqa: BLE001
            return f"ERR: LLM gateway not configured: {type(e).__name__}: {e}"

        cols = _normalize_cols(available_columns)
        dss = _normalize_cols(business_datasets)
        biz_cols = _normalize_cols(business_dataset_columns)
        prompt = (
            "You are a FinOps assistant authoring an enhance configuration.\n"
            f"Stage: {stage}\n"
            f"The user requirement: {req}\n\n"
            f"Known enhance operations (purpose):\n{_vocabulary(stage)}\n"
        )
        if cols:
            prompt += f"\nAvailable COST-dataset columns (use these EXACT names):\n{', '.join(cols)}\n"
        if biz_cols:
            prompt += (
                f"\nAvailable BUSINESS-dataset columns (use these EXACT names for Lookup imports "
                f"and business join keys):\n{', '.join(biz_cols)}\n"
            )
        if dss:
            prompt += f"\nAvailable business datasets (use these EXACT names for Lookup):\n{', '.join(dss)}\n"
        prompt += (
            "\nDecide the BEST operation(s) to satisfy the requirement and return an EnhanceOperationPlan.\n"
            "- operation_type MUST be one of the known enhance operations above.\n"
            "- name: a short slug.\n"
            "- Put the concrete inputs each operation needs in `fields` (e.g. for Lookup: "
            "business_dataset_name, join_columns=[{cost_dataset_join_column, business_dataset_join_column}], "
            "import_columns=[{name, rename_to}]; for Mapping/Compute: column_name; for Filter rows: "
            "condition).\n"
            "- Only reference columns from the provided lists; do NOT invent columns or datasets.\n"
            "- If the requirement is ambiguous, pick the most sensible operation(s) and note it in summary."
        )

        try:
            proxy = LLMAgentProxy(
                model=effective_model,
                client=client,
                sai_product="coordination_workflow",
                sai_product_step="config_generation_llm",
            )
            program = proxy.generate_llamaindex_pydantic_program(
                base_model=EnhanceOperationPlan,
                prompt_template_str=prompt,
                model_name=effective_model,
                attributes={"purpose": "enhance_operation_planning", "step": "single_pass"},
                seed=42,
            )
            await ctx.report_progress(1, 2, "Planning the enhance operations (LLM)...")
            import asyncio

            plan: EnhanceOperationPlan = await asyncio.wait_for(program.acall(), timeout=timeout_seconds)
        except Exception as e:  # noqa: BLE001
            logger.exception("plan_enhance_operations: LLM call failed")
            return f"ERR: enhance planning LLM call failed: {type(e).__name__}: {str(e)[:500]}"

        # Deterministic guard
        await ctx.report_progress(2, 2, "Guarding the plan against the real metadata...")
        dropped = _guard(plan, stage, cols, dss, biz_cols)
        dropped_sig = {(d.get("operation_type"), d.get("name")) for d in dropped}
        known_set = set(_KNOWN_OPERATORS)
        kept = []
        for op in plan.operations:
            ot = _op_type_value(op.operation_type)
            if ot in known_set and (ot, op.name) not in dropped_sig:
                kept.append(op)
        import json as _json

        return _json.dumps(
            {
                "success": True,
                "stage": stage,
                "summary": plan.summary,
                "operations": [op.model_dump(mode="json") for op in kept],
                "operations_dropped": dropped,
                "guard": {
                    "known_operation_types": list(_KNOWN_OPERATORS),
                    "available_columns": cols,
                    "business_datasets": dss,
                    "business_dataset_columns": biz_cols,
                },
                "elapsed_seconds": round(time.time() - t0, 2),
            },
            indent=2,
        )

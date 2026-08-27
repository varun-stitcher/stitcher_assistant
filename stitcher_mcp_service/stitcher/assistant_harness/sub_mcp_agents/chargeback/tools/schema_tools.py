"""Cost schema discovery + business-column classification for the chargeback tools.

Reads a FOCUS data-lake **destination**'s schema (``cost_reader.read_cost_schema`` — no data
scan) and classifies its columns into business-context buckets (organization / cost_center /
project / environment / allocation / provider / period / cost) by heuristic name patterns.
Destinations are FOCUS-normalized, so the defaults (``x_*`` custom columns +
``BilledCost``/``ChargePeriodStart``) almost always apply.
"""

from __future__ import annotations

from fastmcp import FastMCP

from . import common as cm
from . import cost_reader as cr

# Heuristic name patterns for classifying columns into business-context buckets (matched
# case-insensitively as substrings against the column name, in iteration order).
_BUSINESS_COLUMN_PATTERNS: dict[str, list[str]] = {
    "organization": [
        "organization",
        "org",
        "company",
        "business_unit",
        "businessunit",
        "department",
        "team",
        "division",
        "owner",
        "project.name",
    ],
    "cost_center": [
        "cost_center",
        "costcenter",
        "cost_centre",
        "costcentre",
        "project.id",
    ],
    "project": ["project", "product"],
    "environment": ["environment"],
    "allocation": ["allocation"],
    "provider": ["provider", "service_name", "service.description"],
    "period": ["period", "usage_start", "usage_end", "charge_period", "billing_period", "invoice"],
    "cost": ["cost", "amount", "rate", "price"],
}


def classify_columns(schema: dict[str, str]) -> dict[str, list[str]]:
    """Classify all schema columns into business-context categories (case-insensitive substring
    match; a column lands in the first matching category in iteration order)."""
    classified: dict[str, list[str]] = {cat: [] for cat in _BUSINESS_COLUMN_PATTERNS}
    classified["other"] = []
    for col in schema:
        lower = col.lower()
        matched = False
        for cat, patterns in _BUSINESS_COLUMN_PATTERNS.items():
            if any(p in lower for p in patterns):
                classified[cat].append(col)
                matched = True
                break
        if not matched:
            classified["other"].append(col)
    return classified


def pick_column(classified: dict[str, list[str]], category: str, override: str | None = None) -> str | None:
    """Pick the best column for a business category. Precedence: explicit override → the single
    discovered candidate → ``None`` if zero or multiple (caller should ask the user)."""
    if override:
        return override
    candidates = classified.get(category, [])
    if len(candidates) == 1:
        return candidates[0]
    return None


def register(mcp: FastMCP, client, soe) -> None:
    @mcp.tool
    async def list_chargeback_destinations(
        environment_id: str | None = None,
    ) -> str:
        """List the environment's FOCUS data-lake **destinations** (what Stitcher has written).

        Chargeback only ever reads **destinations** — the FOCUS-normalized tables Stitcher has
        *written* (BigQuery / Snowflake DB-export destinations). This lists the queryable ones so
        the agent can ground before running a report. Omit ``data_source`` on the report/invoice
        tools to auto-resolve the single FOCUS lake, or pass one of the names returned here.

        Args:
            environment_id: Scope.
        """
        try:
            env_id = cm.resolve_env_id(soe, environment_id)
        except RuntimeError as exc:
            return str(exc)
        try:
            dests = cr.list_chargeback_destinations(soe)
        except Exception as e:  # noqa: BLE001
            return f"ERR (list_chargeback_destinations): {str(e)[:300]}"
        if not dests:
            return (
                f"# Chargeback destinations — env {env_id}\n\n"
                "No queryable FOCUS data-lake destination found for this environment. "
                "Chargeback only reads destinations (what Stitcher has written). Configure a "
                "BigQuery or Snowflake export destination and try again."
            )
        lines = [f"# Chargeback destinations — env {env_id}", ""]
        lines.append("| Name | Engine | Dataset | Table ref |")
        lines.append("|---|---|---|---|")
        from stitcher.operation_executor.workflows.assistant_workflow.activities.focus_query import (
            _build_table_ref as _tref,
        )
        from stitcher.operation_executor.workflows.assistant_workflow.activities.focus_query import _conn_engine as _eng

        for c in dests:
            eng = _eng(c) or "?"
            tbl = _tref(c, eng) if eng != "?" else ""
            lines.append(
                f"| {getattr(c, 'name', '')} | {eng} | " f"{getattr(c, 'dataset_name', '') or ''} | {tbl or ''} |"
            )
        lines.append("")
        lines.append(
            "Pass any ``Name`` above as ``data_source`` to the chargeback tools, or omit "
            "``data_source`` to auto-resolve the first (canonical) FOCUS lake."
        )
        return "\n".join(lines)

    @mcp.tool
    async def discover_cost_schema(
        data_source: str = "",
        environment_id: str | None = None,
    ) -> str:
        """Discover a FOCUS data-lake **destination**'s schema and classify its columns by
                business context.

                Reads the schema via the SOE metadata operator (no data scan) and classifies every
        column
                by likely business context (cost, period, organization, cost_center, project, provider,
                environment, allocation). Returns a suggested column mapping for the chargeback tools.

                **Call this before chargeback_by_cost_center / query_focus_cost /
        generate_chargeback_invoices**
                to confirm which columns map to which business dimensions in your destination. If the
                classification is ambiguous (multiple candidates), pass the correct column explicitly on
                the report tools.

                Args:
                    data_source: Name or id of the **destination** (a FOCUS data-lake export connection).
                        Omit to auto-resolve the environment's single FOCUS data lake.
                    environment_id: Scope.
        """
        try:
            dc, schema, _cost_col = cm.prep_read(
                soe, "discover_cost_schema", data_source, environment_id, require_cost=False
            )
            env_id = cm.resolve_env_id(soe, environment_id)
        except cm.ToolRefusal as exc:
            return str(exc)

        classified = classify_columns(schema)
        cost_col = cr.resolve_cost_column(schema)
        period_col = cr.resolve_period_column(schema)
        org_col = pick_column(classified, "organization")
        cc_col = pick_column(classified, "cost_center")
        alloc_src, alloc_dst = cr.resolve_allocation_columns(schema)

        lines = [
            f"# Cost schema — {getattr(dc, 'name', data_source)}  (env {env_id})",
            "",
            f"**columns ({len(schema)}):** " + ", ".join(f"`{c}`" for c in sorted(schema)),
            "",
            "## Business classification",
        ]
        for cat in (
            "cost",
            "period",
            "organization",
            "cost_center",
            "project",
            "environment",
            "allocation",
            "provider",
        ):  # noqa: E501
            cols = classified.get(cat) or []
            if cols:
                lines.append(f"- **{cat}**: " + ", ".join(f"`{c}`" for c in cols))
        lines.append("")
        lines.append("## Suggested mapping")
        lines.append(f"- **cost_column**: {cost_col or '(none — pass cost_column)'}")
        lines.append(f"- **period_column**: {period_col or '(none — pass period_column)'}")
        lines.append(f"- **organization**: {org_col or '(none / ambiguous)'}")
        lines.append(f"- **cost_center**: {cc_col or '(none / ambiguous)'}")
        lines.append(f"- **allocation**: {alloc_src or '(none)'} → {alloc_dst or '(none)'}")

        confirmations: list[str] = []
        if not org_col and len(classified["organization"]) > 1:
            confirmations.append(
                f"organization has multiple candidates {classified['organization']} — pass org_column."
            )
        if not cc_col and len(classified["cost_center"]) > 1:
            confirmations.append(
                f"cost_center has multiple candidates {classified['cost_center']} — pass cost_center_column."
            )
        if not org_col and not classified["organization"]:
            confirmations.append("no org-attribution column found — pass org_column.")
        if not cc_col and not classified["cost_center"]:
            confirmations.append("no cost-center column found — pass cost_center_column.")
        if not cost_col:
            confirmations.append("no cost column found — pass cost_column.")
        if confirmations:
            lines.append("")
            lines.append("⚠ **Needs confirmation:** " + " ".join(confirmations))
            lines.append("Then pass the confirmed column names to the chargeback tools.")
        else:
            lines.append("")
            lines.append("Mapping looks usable — proceed with query_focus_cost or chargeback_by_cost_center.")
        return "\n".join(lines)

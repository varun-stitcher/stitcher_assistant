"""Stitcher API tools — thin MCP wrappers over StitcherClient (which forwards to
the existing stitcher_web_service_client). Register on the mcp server."""

from __future__ import annotations

from fastmcp import FastMCP

from ..common.client import StitcherClient
from ..common.config import StitcherAssistantConfig

# Capabilities catalog for the always-active `stitcher_capabilities` tool. Each
# sub-MCP bundle hosts tools that are INACTIVE until activated via
# `activate_sub_mcp(name)` (pi Dynamic Tool Loading). Keeping this on the
# top-level (always-active) coordinator means an agent that "checks what tools
# are available" — e.g. via stitcher_context — immediately learns a bundle
# exists and the exact one-line way to turn its tools on, instead of hunting in
# the shell for capabilities that only exist as MCP tools.
_SUB_MCP_CATALOG = {
    "custom_cost": {
        "purpose": "FOCUS cost normalization + validation — bring any invoice PDF/CSV and normalize it to the FOCUS v1.2 column shape.",
        "activate": "activate_sub_mcp(name=\"custom_cost\")",
        "tools": {
            "normalize_to_focus(file_path=...)": "Full pipeline: extract → LLM plan-gen → normalize to FOCUS (optional validation). THE entry point for a single invoice.",
            "extract_invoice(file_path=...)": "Run ONLY the extraction step (influence which columns are pulled).",
            "generate_focus_plans(...)": "Map source cols → FOCUS cols (returns an InlineNormalizeDatasourceDto).",
            "validate_and_repair_focus(...)": "Validate a FOCUS frame + repair deterministic gaps (e.g. currency).",
            "list_focus_providers / detect_provider / apply_conversion_plans / simulate_normalize_config": "Provider-aware FOCUS conversion plumbing.",
            "cache_list / cache_get / cache_put / cache_clear": "Manage the step-artifact (KW) cache so repeat normalize calls are zero-LLM.",
        },
    },
    "config_generation": {
        "purpose": "Enhance/enrich config generation grounded on the REAL environment by exercising SOE functions as-is.",
        "activate": "activate_sub_mcp(name=\"config_generation\")",
        "tools": {
            "list_operators / describe_operator": "The enhance operator vocabulary (Lookup, Mapping, Compute, Filter, ...) + full field specs + real examples.",
            "list_data_sources / get_data_source_metadata / scan_data": "Live datasource catalog + columns/dtypes + real $ splits via the SOE metadata/extract operators.",
            "get_committed_config / derived_columns": "Prior checked-in git configs for this env + pipeline (compact op summary + x_* bridge columns).",
            "plan_enhance_operations": "LLM-maps a requirement to the best operation(s), then a deterministic guard.",
            "generate_lookup / generate_filter / validate_config / save_config": "Deterministic, validate-by-construction authoring against the real SPC enhance models.",
        },
    },
}


def register(mcp: FastMCP, client: StitcherClient, settings: StitcherAssistantConfig) -> None:
    @mcp.tool
    def stitcher_capabilities() -> str:
        """Discover the heavy sub-MCP tool bundles and how to activate them.

        The active tool list stays deliberately small; domain bundles (custom_cost
        FOCUS normalization, config_generation enhance config) live in sub-MCP
        servers and are INACTIVE until you call ``activate_sub_mcp(<name>)``. Call
        this tool whenever you need a capability that isn't in your active tools
        (e.g. normalize an invoice to FOCUS): it lists each bundle, the tools it
        hosts, and the exact activation call.
        """
        registry = settings.sub_mcp_registry
        if not registry:
            return (
                "No sub-MCP bundles are configured (STITCHER_SUB_MCP_URLS is empty). "
                "Only the top-level coordinator tools are available."
            )
        lines = [
            "Sub-MCP bundles are INACTIVE until activated. To use one, call its activation: ",
            "",
        ]
        for name, url in registry.items():
            entry = _SUB_MCP_CATALOG.get(name)
            lines.append(f"### {name}  ({url})")
            if entry:
                lines.append(f"Purpose: {entry['purpose']}")
                lines.append(f"Activate: {entry['activate']}")
                lines.append("Hosted tools:")
                for tool, desc in entry["tools"].items():
                    lines.append(f"  - {tool}: {desc}")
            else:
                lines.append("Purpose: (no catalog entry). Use list_sub_mcp_servers to see its tools.")
            lines.append("")
        lines.append(
            "Activation is purely additive and persists for the rest of the session. "
            "Then call the tools directly (they forward to the sub-MCP server)."
        )
        return "\n".join(lines)

    @mcp.tool
    def stitcher_context() -> str:
        """Report the Stitcher scope this agent is bound to (API URL, environment id, pipeline name). No secrets."""
        return client.context()
    @mcp.tool
    def stitcher_context() -> str:
        """Report the Stitcher scope this agent is bound to (API URL, environment id, pipeline name). No secrets."""
        return client.context()

    @mcp.tool
    def list_connections(
        scope: str = "datasources",
        environment_id: str | None = None,
    ) -> str:
        """List Stitcher data connections via the stitcher_web_service_client.

        scope: 'datasources' (cost/business data sources, default) | 'destinations' (write targets).
        Environment from the arg or STITCHER_ENVIRONMENT_ID. Returns each connection's
        name, provider, connector, status, and id.
        """
        return client.list_connections(scope=scope, environment_id=environment_id)

    @mcp.tool
    def get_connection(
        scope: str,
        name_or_id: str,
        environment_id: str | None = None,
    ) -> str:
        """Fetch one Stitcher data connection by name or id via the client.

        scope: 'datasources' | 'destinations'.
        """
        return client.get_connection(scope=scope, name_or_id=name_or_id, environment_id=environment_id)

    @mcp.tool
    def get_pipeline(
        pipeline_name: str | None = None,
        environment_id: str | None = None,
    ) -> str:
        """Get a Stitcher pipeline via the client (name from arg, else STITCHER_PIPELINE_NAME).

        Returns name, id, organization, repository, and status.
        """
        return client.get_pipeline(pipeline_name=pipeline_name, environment_id=environment_id)

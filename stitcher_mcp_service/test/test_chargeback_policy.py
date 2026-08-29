"""T0.2 acceptance — the chargeback policy is the single source of truth (plans/chargeback-agent-service.md).

The catalog entry in ``stitcher_tools._SUB_MCP_CATALOG`` must DERIVE from
``chargeback/policy.py`` (no drift), and ``SYSTEM_POLICY`` must carry the
destination-only prime directive with the grounding/report flow.
"""

from __future__ import annotations

from stitcher.assistant_harness.sub_mcp_agents.chargeback.policy import (
    DESTINATION_ONLY_SUMMARY,
    SYSTEM_POLICY,
)
from stitcher.assistant_harness.tools.stitcher_tools import _SUB_MCP_CATALOG


def test_system_policy_carries_the_prime_directive():
    """The three load-bearing phrases the plan's acceptance criteria require."""
    assert "destinations" in SYSTEM_POLICY
    assert "list_chargeback_destinations" in SYSTEM_POLICY
    assert "NEVER scan the source datasources" in SYSTEM_POLICY


def test_system_policy_names_the_tool_flow():
    """The policy must teach the exact flow the live session proved (no invented names)."""
    for needle in (
        "discover_cost_schema",
        "chargeback_by_cost_center(period=",
        'group_by="cost_center,service"',
        "chargeback_provider_lineage",
        "generate_chargeback_invoices",
    ):
        assert needle in SYSTEM_POLICY, f"SYSTEM_POLICY missing {needle!r}"


def test_catalog_chargeback_entry_is_derived_not_duplicated():
    """No drift: the catalog purpose IS the policy's summary constant."""
    assert _SUB_MCP_CATALOG["chargeback"]["purpose"] is DESTINATION_ONLY_SUMMARY
    # and the summary itself still states the destination-only rule
    assert "NEVER scans the source datasources" in DESTINATION_ONLY_SUMMARY
    assert "FOCUS" in DESTINATION_ONLY_SUMMARY

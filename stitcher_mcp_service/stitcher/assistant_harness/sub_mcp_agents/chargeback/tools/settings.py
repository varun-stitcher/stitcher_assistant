"""Deployment-tunable settings + business-policy defaults for the chargeback tools.

Two layers, mirroring SPC:

1. **``ChargebackSettings``** — env-tunable knobs (``CHARGEBACK_*`` prefix), read once per
   process via :func:`get_chargeback_settings`. Deployment overrides, e.g.::

       CHARGEBACK_MATERIALITY_THRESHOLD_USD=25.0
       CHARGEBACK_BQ_COST_PER_TIB=8.0

2. **``CHARGEBACK_POLICY``** — the environment's chargeback *business policy*: the cost-center
   registry, allocation rules, monthly schedule, and ERP target. These are defaults (parity with
   SPC); the FOCUS cost **data** is always resolved at runtime from the environment's real
   destination connection, never hardcoded.

Only the FOCUS cost data is environment-resolved; these constants describe the chargeback
contract this environment operates under.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

_BQ_COST_PER_TIB_DEFAULT = 6.25
_TIB = 1024**4


class ChargebackSettings(BaseSettings):
    """Env-tunable chargeback knobs (aliases are the ``CHARGEBACK_*`` env vars operators set)."""

    model_config = SettingsConfigDict(
        env_prefix="CHARGEBACK_",
        env_file=[".env", ".env.local.dev", ".env.local"],
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    materiality_threshold_usd: float = Field(
        default=10.0,
        alias="CHARGEBACK_MATERIALITY_THRESHOLD_USD",
        description=(
            "Invoices below this dollar amount are filtered into a combined 'below materiality' "
            "entry rather than emitted as individual invoices. Set to 0 to disable filtering."
        ),
    )
    bq_cost_per_tib: float = Field(
        default=_BQ_COST_PER_TIB_DEFAULT,
        alias="CHARGEBACK_BQ_COST_PER_TIB",
        description="USD cost estimate applied to BigQuery bytes-scanned ($ per TiB).",
    )


@lru_cache
def get_chargeback_settings() -> ChargebackSettings:
    """Cached settings instance — read once per process."""
    return ChargebackSettings()


# ── Business-policy defaults (parity with SPC tools/chargeback.py) ──────────
# The FOCUS cost *data* is always resolved at runtime from the environment's destination data
# connection; these are the environment's business policy (cost-center registry, ERP target,
# schedule), not "demo" data.

CHARGEBACK_POLICY: dict = {
    "cost_centers": [
        {
            "id": "cc-120",
            "name": "Platform Engineering",
            "organization": "R&D",
            "owner_email": "platform-lead@stitcher.ai",
            "monthly_budget_usd": 600,
        },
        {
            "id": "cc-123",
            "name": "Data Engineering",
            "organization": "R&D",
            "owner_email": "data-lead@stitcher.ai",
            "monthly_budget_usd": 500,
        },
        {
            "id": "cc-141",
            "name": "ML Platform",
            "organization": "R&D",
            "owner_email": "ml-lead@stitcher.ai",
            "monthly_budget_usd": 300,
        },
        {
            "id": "cc-143",
            "name": "Frontend",
            "organization": "R&D",
            "owner_email": "fe-lead@stitcher.ai",
            "monthly_budget_usd": 150,
        },
        {
            "id": "cc-131",
            "name": "Backend Services",
            "organization": "R&D",
            "owner_email": "be-lead@stitcher.ai",
            "monthly_budget_usd": 150,
        },
        {
            "id": "cc-153",
            "name": "Infrastructure",
            "organization": "R&D",
            "owner_email": "infra-lead@stitcher.ai",
            "monthly_budget_usd": 150,
        },
        {
            "id": "cc-154",
            "name": "Security Engineering",
            "organization": "R&D",
            "owner_email": "sec-lead@stitcher.ai",
            "monthly_budget_usd": 100,
        },
        {
            "id": "cc-200",
            "name": "Finance Ops",
            "organization": "Finance",
            "owner_email": "finance-ops@stitcher.ai",
            "monthly_budget_usd": 100,
        },
        {
            "id": "cc-311",
            "name": "Customer Onboarding",
            "organization": "Customer Success",
            "owner_email": "cs-lead@stitcher.ai",
            "monthly_budget_usd": 200,
        },
        {
            "id": "Operations",
            "name": "Internal IT Operations",
            "organization": "internal-it",
            "owner_email": "it-ops@stitcher.ai",
            "monthly_budget_usd": 4000,
            "type": "shared_cost_pool",
        },
    ],
    "allocation_rules": [
        {
            "name": "shared-infra-fanout",
            "source": "internal-it / Operations",
            "destination": "all R&D cost centers",
            "method": "proportional to direct R&D spend",
            "active": True,
        },
        {
            "name": "untagged-fallback",
            "source": "untagged AWS / Azure resources",
            "destination": "(unallocated) bucket",
            "method": "no allocation (orphan spend)",
            "active": True,
            "current_orphan_pct": 39.6,
        },
    ],
    "chargeback_schedule": {
        "frequency": "monthly",
        "cutoff_day_of_month": 1,
        "post_to_erp_by_day_of_month": 5,
        "approver": "finance-ops@stitcher.ai",
    },
    "erp_integration": {
        "system": "Zoho Books",
        "organization_id": "922370566",
        "default_journal_account": "5800 - Cloud Infrastructure (Allocated)",
        "cost_center_to_dimension_mapping": "1:1 — cost_center.id → Zoho Books Cost Center",
    },
}

# Cost-center id → display-name lookup (from the policy registry).
CC_NAMES = {cc["id"]: cc["name"] for cc in CHARGEBACK_POLICY["cost_centers"]}

SUPPORTED_ERPS = [
    "QuickBooks Online",
    "NetSuite",
    "Xero",
    "Sage Intacct",
    "Microsoft Dynamics 365 Finance",
    "Workday Financial Management",
    "Zoho Books",
]

# Hints for the assistant when matching its connected MCP servers against ERPs. Claude Code /
# Claude Desktop tool names tend to look like "zoho-books:create_invoice" / "qbo-mcp:...".
ERP_SERVER_HINTS = {
    "Zoho Books": ["zoho", "zoho-books", "zohobooks"],
    "QuickBooks Online": ["qbo", "quickbooks"],
    "NetSuite": ["netsuite", "ns-mcp"],
    "Xero": ["xero"],
    "Sage Intacct": ["sage", "intacct"],
    "Microsoft Dynamics 365 Finance": ["dynamics", "d365"],
    "Workday Financial Management": ["workday"],
}

# Deterministic seed so re-running submit returns the same confirmation IDs within a session.
ERP_DOC_BASE = 14201

__all__ = [
    "CC_NAMES",
    "CHARGEBACK_POLICY",
    "ChargebackSettings",
    "ERP_DOC_BASE",
    "ERP_SERVER_HINTS",
    "SUPPORTED_ERPS",
    "_BQ_COST_PER_TIB_DEFAULT",
    "_TIB",
    "get_chargeback_settings",
]

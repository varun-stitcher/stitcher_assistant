"""Deployment-tunable settings + business-policy defaults for the chargeback tools.

Two layers, mirroring SPC:

1. **``ChargebackSettings``** — env-tunable knobs (``CHARGEBACK_*`` prefix), read once per
   process via :func:`get_chargeback_settings`. Deployment override, e.g.::

       CHARGEBACK_MATERIALITY_THRESHOLD_USD=25.0

2. **``CC_NAMES`` / ``CHARGEBACK_POLICY``** — the environment's chargeback *business policy*:
   the cost-center display-name registry and the ERP target. These are defaults (parity with
   SPC); the FOCUS cost **data** is always resolved at runtime from the environment's real
   destination connection, never hardcoded.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


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


@lru_cache
def get_chargeback_settings() -> ChargebackSettings:
    """Cached settings instance — read once per process."""
    return ChargebackSettings()


# ── Business-policy defaults (parity with SPC tools/chargeback.py) ──────────
# The FOCUS cost *data* is always resolved at runtime from the environment's destination data
# connection; these are the environment's business policy, not "demo" data. (The cost-center
# registry, allocation rules, and schedule that SPC carried were never read by any tool here —
# only the ERP target + the id → display-name lookup are consumed.)

CHARGEBACK_POLICY: dict = {
    "erp_integration": {
        "system": "Zoho Books",
        "organization_id": "922370566",
        "default_journal_account": "5800 - Cloud Infrastructure (Allocated)",
        "cost_center_to_dimension_mapping": "1:1 — cost_center.id → Zoho Books Cost Center",
    },
}

# Cost-center id → display-name lookup (registry trimmed to what the report renders).
CC_NAMES: dict = {
    "cc-120": "Platform Engineering",
    "cc-123": "Data Engineering",
    "cc-141": "ML Platform",
    "cc-143": "Frontend",
    "cc-131": "Backend Services",
    "cc-153": "Infrastructure",
    "cc-154": "Security Engineering",
    "cc-200": "Finance Ops",
    "cc-311": "Customer Onboarding",
    "Operations": "Internal IT Operations",
}

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
    "get_chargeback_settings",
]

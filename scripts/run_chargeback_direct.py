#!/usr/bin/env python3
"""Direct-Python chargeback runner — NO MCP / NO pi / NO SWS gateway.

Runs the chargeback cost-center report end-to-end by calling the SOE functions and the
deterministic ``cost_reader`` logic DIRECTLY, in one process. This is the fast, reliable path
(the BQ query returns in ~3s) and the escape hatch if the pi/MCP orchestration layer ever
times out.

Usage (run from ``pi_coding_agent/`` where ``.env.local`` resolves, like the harness):

    ../stitcher_mcp_service/.venv/bin/python ../scripts/run_chargeback_direct.py [PERIOD]

    PERIOD: "last_month" (default) or "YYYY-MM". "last month" = July 2026 when today is
            2026-08-27.

Env (set in shell or via run.local.sh): STITCHER_ENVIRONMENT_ID, STITCHER_AUTH_TENANT,
STITCHER_API_URL, STITCHER_MODEL_API_KEY, STITCHER_SSL_CA_CERTIFICATE_PATH, USE_STITCHER_MODEL.
"""

from __future__ import annotations

import asyncio
import pathlib
import sys

# The SOE / assistant_harness packages are imported from the stitcher_mcp_service tree.
_MCP = "/Users/vmittal/Code/stitcher-worktrees/fix-pi-agent/stitcher_assistant/stitcher_mcp_service"
if _MCP not in sys.path:
    sys.path.insert(0, _MCP)

from stitcher.assistant_harness.common.client import StitcherClient
from stitcher.assistant_harness.common.config import (
    StitcherAssistantConfig,
)
from stitcher.assistant_harness.common.oidc_auth import OIDCAuth
from stitcher.assistant_harness.common.soe_context import (
    build_soe_context,
)
from stitcher.assistant_harness.sub_mcp_agents.chargeback.tools import (
    common as cm,
)
from stitcher.assistant_harness.sub_mcp_agents.chargeback.tools import (
    cost_reader as cr,
)
from stitcher.assistant_harness.sub_mcp_agents.chargeback.tools.period import (
    resolve_period,
)


def _money(v: float | None) -> str:
    v = float(v or 0.0)
    if abs(v) < 0.005:
        return "—"
    return f"(${abs(v):,.2f})" if v < 0 else f"${v:,.2f}"


def _period_window(period: str):
    start, end, label = resolve_period(period, since_days=30)
    return start, end, label


async def main() -> int:
    period = (
        sys.argv[1] if len(sys.argv) > 1 else "last_month"
    ).strip() or "last_month"

    settings = StitcherAssistantConfig()
    settings.require_scope()  # STITCHER_API_URL + STITCHER_MODEL_API_KEY
    settings.export_llm_env()
    # Reuse the assistant_harness OIDC token state (same dir the chargeback build_server uses),
    # so we don't trigger a fresh device-login here.
    state_dir = pathlib.Path(_MCP) / "stitcher" / "assistant_harness"
    auth = OIDCAuth(settings, state_dir)
    client = StitcherClient(settings, auth)
    soe = build_soe_context(settings, auth, client)

    print(
        f"env={soe.environment_id} tenant={soe.auth_tenant} pipeline={soe.pipeline_name or soe.pipeline_id}",
        flush=True,
    )

    # Resolve the Stitcher-ALLOCATED destination (BigQuery FOCUS export).
    dc = cr.resolve_destination(soe, "")
    schema = cr.read_cost_schema(soe, dc)
    print(f"destination={getattr(dc, 'name', '?')} columns={len(schema)}", flush=True)

    cost_col = cr.resolve_cost_column(schema)
    period_col = cr.resolve_period_column(schema)
    classification = await cr.classify_org_cost_center(schema)
    cc_col = classification.get("cost_center") or "x_CostCenter"
    org_col = classification.get("organization")
    provider_col = cr.resolve_provider_column(schema)
    if not cost_col or not cc_col:
        print(
            f"ERR: no cost column ({cost_col!r}) / cost-center column ({cc_col!r}) found in schema."
        )
        return 1

    start, end, label = _period_window(period)
    group_by_cols = [cc_col]
    for c in (org_col, provider_col):
        if c and c in schema and c not in group_by_cols:
            group_by_cols.append(c)

    # Aggregate IN SQL (GROUP BY dimensions, SUM the cost column) so we pull ~cost-center rows,
    # not the whole month.
    df = cr.read_aggregated_cost(
        soe,
        dc,
        group_by_cols,
        cost_col,
        period_col,
        start,
        end,
        None,
        schema.get(period_col),
        top_n=200,
    )
    if df.is_empty():
        print(f"No rows in the destination for {label}.")
        return 0
    print(f"read {df.height:,} aggregated row(s) in window for {label}", flush=True)

    agg, total, records = cm.cost_summary(df, [cc_col], cost_col, top_n=200)
    total = round(total, 2)

    print(f"\n# Chargeback by cost center — {label}")
    print(
        f"source: `{getattr(dc, 'name', '?')}`  ·  {records:,} charge records  ·  TOTAL {_money(total)}\n"
    )
    print("| Cost Center | Cost | Rows | % Share |")
    print("|---|---|---:|---:|")
    for r in agg.iter_rows(named=True):
        cc = r[cc_col]
        cc_s = "--unallocated--" if cc is None else str(cc)
        cost = float(r.get("cost") or 0.0)
        share = (cost / total * 100.0) if total else 0.0
        print(f"| {cc_s} | {_money(cost)} | {r.get('row_count', 0):,} | {share:.1f}% |")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

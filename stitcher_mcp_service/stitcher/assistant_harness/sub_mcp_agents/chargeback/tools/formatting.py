"""Shared formatting helpers for the chargeback report / invoice / query tools.

Keeps money / table rendering in one place so every tool renders the same way (money cells,
negative-as-parentheses). Ported from SPC's ``tools/chargeback.py`` (``_fmt_money`` /
``_provider_notes`` / ``_build_posted_summary_markdown``).
"""

from __future__ import annotations


def fmt_money(v: float | None) -> str:
    """Render a money cell. Zero becomes ``—`` (clearly nothing, not a coincidental round
    number); negatives get parenthesized so credits stand out from charges."""
    if v is None or abs(v) < 0.005:
        return "—"
    if v < 0:
        return f"(${abs(v):,.2f})"
    return f"${v:,.2f}"


def provider_notes(providers: list[dict]) -> str:
    """Per-cost-center notes string: top direct providers + allocation-in/out lines, so the
    FinOps reader sees *where the cost came from* next to the totals."""
    direct = sorted([p for p in providers if p["direct_cost"] > 0], key=lambda p: -p["direct_cost"])[:3]
    alloc_in = sorted([p for p in providers if p["allocation_in"] > 0], key=lambda p: -p["allocation_in"])[:2]
    alloc_out = sorted([p for p in providers if p["allocation_out"] < 0], key=lambda p: p["allocation_out"])[:2]
    parts: list[str] = []
    if direct:
        parts.append("Direct: " + ", ".join(f"{p['provider']} ${p['direct_cost']:,.2f}" for p in direct))
    if alloc_in:
        parts.append("Allocated in: " + ", ".join(f"{p['provider']} ${p['allocation_in']:,.2f}" for p in alloc_in))
    if alloc_out:
        parts.append("Credits: " + ", ".join(f"{p['provider']} (${abs(p['allocation_out']):,.2f})" for p in alloc_out))
    return " · ".join(parts) if parts else "—"


def build_posted_summary_markdown(invoices: list[dict], materiality: dict) -> str:
    """Render the end-of-run posting view: one row per (cost_center, provider), including
    internal allocations like 'Shared Infrastructure'."""
    header = "| Cost Center | Provider | Items | Amount |\n" "|---|---|---:|---:|"
    body_lines: list[str] = []
    for inv in invoices:
        for item in inv["line_items"]:
            body_lines.append(
                "| "
                + " | ".join(
                    [inv["cost_center"], item["provider"], item["description"], fmt_money(item["billed_cost"])]
                )
                + " |"
            )
    total_billed = round(sum(inv["total_billed"] for inv in invoices), 2)
    body_lines.append("| **TOTAL** |  |  | " f"**{fmt_money(total_billed)}** |")
    if materiality.get("filtered_count"):
        body_lines.append(
            f"| _Below materiality (combined, threshold ${materiality['threshold']:.2f})_ |  | "
            f"{materiality['filtered_count']} skipped | {fmt_money(materiality['filtered_total'])} |"
        )
    return header + "\n" + "\n".join(body_lines)


__all__ = [
    "build_posted_summary_markdown",
    "fmt_money",
    "provider_notes",
]

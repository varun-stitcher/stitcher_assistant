<!-- golden capture · period 2026-07 · env aac8c9ec-a865-4e3b-c1f8-00155dcea122 · capture_chargeback_golden.py -->

# Chargeback by cost center — July 2026
source: `BigQuery Export`  ·  3,883,510 charge records in window

| Cost Center | Org | Direct | Allocation in | Allocation out | Net Chargeback | Notes |
|---|---|---:|---:|---:|---:|---|
| (unallocated) | (unallocated) | $11,768.53 | $3,078.33 | ($2,715.72) | $12,131.14 | Direct: Google Cloud $11,720.85, Anthropic $42.78, Twilio $3.46 · Allocated in: Google Cloud $2,715.72, StitcherAI IT $362.61 · Credits: Google Cloud ($2,715.72) |
| CC-2 | P&E | — | $2,339.09 | — | $2,339.09 | Allocated in: StitcherAI IT $2,339.09 |
| Miscellaneous (below materiality) | (various) | $4.92 | — | — | $4.92 | 2 cost centers combined (threshold $10.00) |
| **TOTAL** |  | **$11,773.45** | **$5,417.42** | **($2,715.72)** | **$14,475.15** |  |

Render the table VERBATIM — do NOT collapse the lineage columns. Negative numbers are credits (in parentheses). Summarize the TOTAL row in 1–2 sentences.

Typical next steps: chargeback_provider_lineage(period=…) to drill into one cost center, then generate_chargeback_invoices(period=…).
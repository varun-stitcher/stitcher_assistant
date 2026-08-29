"""Tool modules for the chargeback sub-MCP.

Each module exposes ``register(mcp, client, soe)`` (tools that need Stitcher state take the
``client``/``soe`` instance from the server's ``register`` signature; env-scoped tools use
``soe`` — the shared ``SoeContext``). Heavy determinism is reused from SOE/SPC **as-is**; pi
stays a thin caller.

Modules:
  - common        — shared tool prelude (``prep_read`` / refusals / window clamp) + the
                    cost-summary / share-table renderers.
  - settings      — ``ChargebackSettings`` (env-tunable) + the chargeback business-policy defaults.
  - period        — ``resolve_period`` (YYYY-MM / last_month / rolling window).
  - cost_reader   — SOE focus-query SQL cost reader (destination resolution, schema probe,
                    SQL ``GROUP BY``/``SUM`` aggregation, column discovery).
  - schema_tools  — ``discover_cost_schema`` + ``list_chargeback_destinations`` + column classification.
  - formatting    — money / markdown-table rendering shared by the report/invoice/query tools.
  - report_tools  — ``chargeback_by_billing_account`` / ``chargeback_by_cost_center`` /
                    ``chargeback_provider_lineage``.
  - invoice_tools — ``generate_chargeback_invoices`` / ``discover_erp_integrations`` /
                    ``submit_invoices_to_erp``.
  - query_tools   — ``query_focus_cost`` (sync general-purpose cost query).
"""

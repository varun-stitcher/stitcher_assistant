"""Tool modules for the chargeback sub-MCP.

Each module exposes ``register(mcp, client, soe)`` (tools that need Stitcher state take the
``client``/``soe`` instance from the server's ``register`` signature; env-scoped tools use
``soe`` — the shared ``SoeContext``). Heavy determinism is reused from SOE/SPC **as-is**; pi
stays a thin caller.

Modules:
  - settings      — ``ChargebackSettings`` (env-tunable) + the chargeback business-policy defaults.
  - period        — ``resolve_period`` (YYYY-MM / last_month / rolling window).
  - cost_reader   — SOE extract-based cost reader (``read_cost_schema`` / ``read_cost_dataframe`` /
                    column discovery) — the same machinery ``scan_data`` uses, in polars.
  - schema_tools  — ``discover_cost_schema`` + column classification / pick.
  - formatting    — money / bytes / markdown-table rendering shared by the report/invoice/query tools.
  - report_tools  — ``chargeback_by_billing_account`` / ``chargeback_by_cost_center`` /
                    ``chargeback_provider_lineage``.
  - invoice_tools — ``generate_chargeback_invoices`` / ``discover_erp_integrations`` /
                    ``submit_invoices_to_erp``.
  - query_tools   — ``query_focus_cost`` (sync general-purpose cost query).
"""

"""chargeback sub-MCP — FOCUS cost-query + chargeback report/invoice tools (pi-router port).

A **sub-MCP** (mirrors ``custom_cost`` / ``config_generation``): a standalone FastMCP server
hosting the chargeback / FOCUS cost-query tool bundle. The pi extension discovers this server's
tools and registers them as **inactive**; the agent activates them on demand via the top-level
``activate_sub_mcp("chargeback")`` loader tool (pi's native Dynamic Tool Loading).

Environment-scoped (like ``config_generation``): chargeback always operates on a customer
environment's REAL cost datasource. ``build_server()`` instantiates
``StitcherAssistantConfig`` / ``OIDCAuth`` / ``StitcherClient`` + ``build_soe_context`` and
refuses to start without ``STITCHER_ENVIRONMENT_ID`` / ``STITCHER_PIPELINE_NAME``. Cost data is
read via the SOE **extract tools** **as-is** (``ExtractRefDataSubOperator`` +
``MetadataConsolidateOperator`` — the same machinery ``scan_data`` uses) and aggregated in
polars — NOT the SWS gateway (whose standalone ``focus_query`` is a stub). Launching from
``pi_coding_agent/`` (where ``.env.local`` / ``.env.local.dev`` are symlinked) makes
``ExecutorConfig()`` resolve at import, exactly as the config-gen sub-MCP does.

See ``chargeback_mcp_server.py`` for the server entrypoint and ``tools/`` for the tool modules.
"""

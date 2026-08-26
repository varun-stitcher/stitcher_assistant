"""stitcher-mcp-service — the Python MCP tool server for the stitcher-pi agent.

The server lives in the ``stitcher.assistant_harness`` package. Run from the
``stitcher_mcp_service`` directory:

    python -m stitcher.assistant_harness.mcp_server --http 8791   # Streamable HTTP
    python -m stitcher.assistant_harness.mcp_server               # stdio

Package layout (under ``stitcher/assistant_harness/``):

    mcp_server  — thin coordinator (builds FastMCP, registers tools, runs)
    common/     — shared infrastructure: StitcherSettings (config), OIDCAuth,
                  StitcherClient
    tools/      — the MCP tools: file_tools, stitcher_tools, auth_tools
"""

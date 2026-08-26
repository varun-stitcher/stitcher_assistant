// Minimal MCP client for the stitcher-pi extension: connect to the FastMCP server
// over Streamable HTTP, list tools, call a tool, and close. Close-per-call so a
// lingering SSE connection never keeps the Node process (or pi) alive at exit.
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";

export const MCP_URL = process.env.STITCHER_MCP_URL || "http://127.0.0.1:8791/mcp/";

async function withClient(fn) {
  const transport = new StreamableHTTPClientTransport(new URL(MCP_URL));
  const client = new Client({ name: "stitcher-pi-client", version: "0.1.0" });
  await client.connect(transport);
  try {
    return await fn(client);
  } finally {
    try { await client.close(); } catch {}
    try { await transport.close(); } catch {}
  }
}

export async function listTools() {
  return withClient(async (c) => {
    const { tools } = await c.listTools();
    return tools.map((t) => ({
      name: t.name,
      description: t.description || "",
      inputSchema: t.inputSchema || {},
    }));
  });
}

export async function callTool(name, args) {
  return withClient(async (c) => {
    const res = await c.callTool({ name, arguments: args || {} });
    const text = (res.content || []).map((b) => b.text ?? "").filter(Boolean).join("\n");
    return text || JSON.stringify(res).slice(0, 2000);
  });
}

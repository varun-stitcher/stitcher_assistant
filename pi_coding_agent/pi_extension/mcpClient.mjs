// Minimal MCP client for the stitcher-pi extension: connect to a FastMCP server
// over Streamable HTTP, list tools, call a tool, and close. Close-per-call so a
// lingering SSE connection never keeps the Node process (or pi) alive at exit.
//
// The top-level coordinator server URL comes from STITCHER_MCP_URL. Sub-MCP
// server URLs are passed explicitly by the extension (from STITCHER_SUB_MCP_URLS)
// so one client module serves both the top-level server and every sub-MCP.
//
// Streaming progress: when the caller supplies an `onProgress` callback, the
// tool request includes a `_meta.progressToken` so the server emits
// `notifications/progress` (FastMCP `ctx.report_progress`), which we forward to
// the callback as `{ progress, total, message }`. This surfaces "what's
// happening" while a long-running tool (e.g. normalize_to_focus) runs.
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';
import { ProgressNotificationSchema } from '@modelcontextprotocol/sdk/types.js';

export const MCP_URL = process.env.STITCHER_MCP_URL || 'http://127.0.0.1:8791/mcp/';

async function withClient(url, fn) {
  const transport = new StreamableHTTPClientTransport(new URL(url));
  const client = new Client({ name: 'stitcher-pi-client', version: '0.1.0' });
  await client.connect(transport);
  try {
    return await fn(client);
  } finally {
    try {
      await client.close();
    } catch {
      /* best-effort close; nothing to report here */
    }
    try {
      await transport.close();
    } catch {
      /* best-effort close; nothing to report here */
    }
  }
}

export async function listTools(url = MCP_URL) {
  return withClient(url, async (c) => {
    const { tools } = await c.listTools();
    return tools.map((t) => ({
      name: t.name,
      description: t.description || '',
      inputSchema: t.inputSchema || {},
    }));
  });
}

/**
 * Call a tool. When `onProgress` is provided, requests server progress and
 * forwards each `notifications/progress` as `{ progress, total, message }`.
 */
export async function callTool(url, name, args, onProgress) {
  // Sub-MCP tools are forwarded to their owning server URL; the top-level tools
  // use the default when called without a URL (kept for backwards compat).
  const target = url || MCP_URL;
  return withClient(target, async (c) => {
    let progressToken;
    if (typeof onProgress === 'function') {
      progressToken = `stitcher-${Date.now()}-${Math.random().toString(36).slice(2)}`;
      c.setNotificationHandler(ProgressNotificationSchema, (notification) => {
        onProgress({
          progress: notification.params?.progress,
          total: notification.params?.total,
          message: notification.params?.message,
        });
      });
    }
    const params = { name, arguments: args || {} };
    if (progressToken) params._meta = { progressToken };
    const res = await c.callTool(params);
    const text = (res.content || [])
      .map((b) => b.text ?? '')
      .filter(Boolean)
      .join('\n');
    return text || JSON.stringify(res).slice(0, 2000);
  });
}

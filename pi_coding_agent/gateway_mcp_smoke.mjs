// Smoke test — connect to the agent gateway's higher-order MCP (:8792), list the orchestrator
// tools, and (optionally) drive one real turn. Used by the gateway smoke-test / docs.
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';

const ENDPOINT = process.env.GW_MCP_URL || 'http://127.0.0.1:8792/mcp/';
const ENV_ID = process.env.GW_ENV_ID || 'd7dad3dc-d02a-48f8-bfc3-a874111c0013';
const PIPELINE = process.env.GW_PIPELINE || 'finops-main';

async function withClient(fn) {
  const transport = new StreamableHTTPClientTransport(new URL(ENDPOINT));
  const client = new Client({ name: 'gateway-smoke', version: '0.1.0' });
  await client.connect(transport);
  try {
    return await fn(client);
  } finally {
    try { await client.close(); } catch {}
    try { await transport.close(); } catch {}
  }
}

const action = process.argv[2] || 'list';
await withClient(async (c) => {
  const { tools } = await c.listTools();
  console.log('TOOLS:', tools.map((t) => t.name).join(', '));
  if (action === 'list') return;

  // Drive one read-only real turn through the agent (per-call scoped tool MCP + headless pi).
  console.log(`\n>>> exploring environment ${ENV_ID} / ${PIPELINE} (real turn; may take a while)...`);
  const res = await c.callTool({
    name: action,
    arguments:
      action === 'explore_environment'
        ? { environment_id: ENV_ID, pipeline_name: PIPELINE, auth_tenant: 'stitcherai-wsmo5' }
        : { environment_id: ENV_ID, pipeline_name: PIPELINE },
  });
  const text = (res.content || []).map((b) => b.text ?? '').join('\n');
  console.log('\n=== RESULT ===');
  console.log(text.slice(0, 4000));
});

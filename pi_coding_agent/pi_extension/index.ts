/**
 * stitcher-pi-extension — the "extremely thin" wrapper that wires pi to the
 * Stitcher LLM endpoint and a FastMCP tool server. It does exactly two things:
 *
 *  1. Register the Stitcher LiteLLM / OpenAI-compatible provider from env vars:
 *       STITCHER_MODEL_BASE_URL   (default https://app.dev.stitcher.ai/llm/v1)
 *       STITCHER_MODEL_API_KEY    (pi interpolates "$STITCHER_MODEL_API_KEY")
 *       STITCHER_MODEL_NAME       (default qwen3.6-27b-mtp)
 *  2. Discover EACH tool the FastMCP server exposes (STITCHER_MCP_URL) and
 *     register it as a pi tool that proxies over MCP — so adding a tool on the
 *     server side is the only change ever needed. No per-tool pi code.
 *
 * Load:  pi --model stitcher/<model> -e ./pi_extension/index.ts   (see run.sh)
 */
import type { ExtensionAPI } from '@earendil-works/pi-coding-agent';
import { Type, type TSchema } from 'typebox';
import { listTools, callTool } from './mcpClient.mjs';

const BASE_URL = process.env.STITCHER_MODEL_BASE_URL || 'https://app.dev.stitcher.ai/llm/v1';
const MODEL = process.env.STITCHER_MODEL_NAME || 'qwen3.6-27b-mtp';

// Minimal JSON-Schema properties -> TypeBox (string/number/integer/boolean/array).
// Good enough for the thin wrapper; the server re-validates anyway.
type JsonSchemaProperty = { type?: string };
type JsonSchema = { properties?: Record<string, JsonSchemaProperty> };

function toTypebox(schema: JsonSchema): TSchema {
  const props: Record<string, JsonSchemaProperty> = (schema && schema.properties) || {};
  const out: Record<string, TSchema> = {};
  for (const [k, v] of Object.entries(props)) {
    const t = v && v.type;
    out[k] =
      t === 'number'
        ? Type.Number()
        : t === 'integer'
          ? Type.Integer()
          : t === 'boolean'
            ? Type.Boolean()
            : t === 'array'
              ? Type.Array(Type.Any())
              : Type.String();
  }
  return Type.Object(out);
}

export default async function (pi: ExtensionAPI) {
  // The Stitcher-hosted qwen gateway (LiteLLM). Creds come from the env, never
  // hardcoded. Gateway keeps the system-first 400 trap: keep exactly one system
  // message. The x-litellm-tags header satisfies the sai_tag_guard.
  pi.registerProvider('stitcher', {
    name: 'Stitcher LiteLLM gateway',
    baseUrl: BASE_URL,
    apiKey: '$STITCHER_MODEL_API_KEY',
    api: 'openai-completions',
    headers: {
      'x-litellm-tags':
        'sai_team:sai,sai_product:coordination_workflow,sai_product_step:config_generation_llm,sai_effort:medium',
    },
    models: [
      {
        id: MODEL,
        name: MODEL,
        reasoning: false,
        input: ['text'],
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
        contextWindow: 262144,
        maxTokens: 16384,
      },
    ],
  });

  // Proxy every FastMCP tool into pi.
  const tools = await listTools();
  for (const t of tools) {
    pi.registerTool({
      name: t.name,
      label: t.name,
      description: t.description,
      parameters: toTypebox(t.inputSchema),
      async execute(_toolCallId, params) {
        try {
          const text = await callTool(t.name, params);
          return { content: [{ type: 'text', text }], details: { error: false } };
        } catch (e) {
          const why = e instanceof Error ? e.message : String(e);
          return {
            content: [{ type: 'text', text: `[stitcher-pi] error calling ${t.name}: ${why}` }],
            details: { error: true },
          };
        }
      },
    });
  }
  console.error(`[stitcher-pi] registered ${tools.length} tool(s) over MCP; model=${MODEL}`);
}

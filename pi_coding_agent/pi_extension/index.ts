/**
 * stitcher-pi-extension — the "extremely thin" wrapper that wires pi to the
 * Stitcher LLM endpoint and a FastMCP tool server. It does three things:
 *
 *  1. Register the Stitcher LiteLLM / OpenAI-compatible provider from env vars:
 *       STITCHER_MODEL_BASE_URL   (default https://app.dev.stitcher.ai/llm/v1)
 *       STITCHER_MODEL_API_KEY    (pi interpolates "$STITCHER_MODEL_API_KEY")
 *       STITCHER_MODEL_NAME       (default qwen3.6-27b-mtp)
 *  2. Discover EACH tool the **top-level** FastMCP server exposes
 *     (STITCHER_MCP_URL) and register it as a pi tool that proxies over MCP —
 *     active by default. These are the lightweight coordinator tools.
 *  3. Discover every **sub-MCP** server listed in STITCHER_SUB_MCP_URLS (a JSON
 *     object of `name -> url`) and register each of their tools as **inactive**
 *     pi tools (registered but held out of the active set). The agent activates
 *     a sub-MCP's tools on demand with the `activate_sub_mcp` loader tool, which
 *     calls `pi.setActiveTools([...active, ...sub])` — pi's native Dynamic Tool
 *     Loading. This keeps the agent's initial tool list pristine: only the
 *     top-level tools + `list_sub_mcp_servers` + `activate_sub_mcp` are visible
 *     until the agent explicitly "switches into" a sub-MCP.
 *
 * Adding a tool on either server side is the only change ever needed — no
 * per-tool pi code.
 *
 * Load:  pi --model stitcher/<model> -e ./pi_extension/index.ts   (see run.sh)
 */
import type { ExtensionAPI } from '@earendil-works/pi-coding-agent';
import { Type, type TSchema } from 'typebox';
import { listTools, callTool, MCP_URL } from './mcpClient.mjs';

const BASE_URL = process.env.STITCHER_MODEL_BASE_URL || 'https://app.dev.stitcher.ai/llm/v1';
const MODEL = process.env.STITCHER_MODEL_NAME || 'qwen3.6-27b-mtp';

// Sub-MCP registry: name -> MCP endpoint URL. Parsed from STITCHER_SUB_MCP_URLS
// (JSON object). Missing/empty ⇒ no sub-MCPs, top-level tools only.
type SubMcpRegistry = Record<string, string>;
function parseSubMcps(): SubMcpRegistry {
  const raw = process.env.STITCHER_SUB_MCP_URLS;
  if (!raw) return {};
  try {
    const parsed = JSON.parse(raw);
    if (parsed && typeof parsed === 'object') return parsed as SubMcpRegistry;
  } catch (e) {
    console.error(`[stitcher-pi] STITCHER_SUB_MCP_URLS is not valid JSON: ${e}`);
  }
  return {};
}

// Minimal JSON-Schema properties -> TypeBox (string/number/integer/boolean/array).
// Honors the schema's `required` list so OPTIONAL tool params stay optional — the
// thin wrapper previously made every param required, which forced the model to
// send junk for params like `expected_columns` (optional) and got pydantic 422s.
type McpToolInfo = { name: string; description: string; inputSchema: JsonSchema };

type JsonSchemaProperty = { type?: string | string[] };
type JsonSchema = { properties?: Record<string, JsonSchemaProperty>; required?: string[] };

function pickType(t: string | string[] | undefined): string | undefined {
  if (Array.isArray(t)) return t.find((x) => x !== 'null'); // e.g. ["array","null"] -> "array"
  return t;
}

function toTypebox(schema: JsonSchema): TSchema {
  const props: Record<string, JsonSchemaProperty> = (schema && schema.properties) || {};
  const required: Set<string> = new Set((schema && schema.required) || []);
  const out: Record<string, TSchema> = {};
  for (const [k, v] of Object.entries(props)) {
    const t = pickType(v && v.type);
    let tb: TSchema =
      t === 'number'
        ? Type.Number()
        : t === 'integer'
          ? Type.Integer()
          : t === 'boolean'
            ? Type.Boolean()
            : t === 'array'
              ? Type.Array(Type.Any())
              : Type.String();
    if (!required.has(k)) tb = Type.Optional(tb);
    out[k] = tb;
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

  // toolName -> owning MCP endpoint URL, so each proxied tool forwards to the
  // server that actually hosts it (top-level or a specific sub-MCP).
  const toolOwnerUrl = new Map<string, string>();

  // Sub-MCP tool names, grouped by server name — used by `activate_sub_mcp` and
  // by the session_start filter that keeps them inactive initially.
  const subMcpToolNames = new Map<string, string[]>(); // serverName -> [toolName]
  const subMcpByUrl = new Map<string, string>(); // url -> serverName (for listing)

  // Heavy tools: calling these inline floods the orchestrator's context with huge
  // payloads (multi-page extraction plans, full FOCUS tables, report markdown). The
  // proxied description gets a delegation nudge so the steering sits exactly where the
  // model is looking when it decides to call the tool — model-agnostic, not
  // dependent on prompt guidelines being heeded.
  const HEAVY_TOOLS = new Set([
    "extract_invoice",
    "normalize_to_focus",
    "validate_and_repair_focus",
    "chargeback_by_cost_center",
    "chargeback_by_billing_account",
    "chargeback_provider_lineage",
    "query_focus_cost",
    "generate_chargeback_invoices",
  ]);
  const heavy = (name: string) =>
    HEAVY_TOOLS.has(name)
      ? " CONTEXT-HEAVY: prefer running this via the subagent tool (agent: \"tool-runner\") instead of inline, so the large result does not consume this conversation's context."
      : "";

  const registerProxiedTool =
    (url: string, serverLabel: string) => (t: { name: string; description: string; inputSchema: JsonSchema }) => {
      if (toolOwnerUrl.has(t.name)) {
        console.error(
          `[stitcher-pi] tool name collision: '${t.name}' from ${serverLabel} already registered by ` +
            `${toolOwnerUrl.get(t.name)} — skipping the duplicate`
        );
        return;
      }
      toolOwnerUrl.set(t.name, url);
      pi.registerTool({
        name: t.name,
        label: t.name,
        description: t.description + heavy(t.name),
        parameters: toTypebox(t.inputSchema),
        async execute(_toolCallId, params, _signal, onUpdate) {
          try {
            // Stream server progress (ctx.report_progress) to the TUI as tool
            // execution updates, so users see what's happening while a long
            // tool (e.g. normalize_to_focus) runs.
            const onProgress = onUpdate
              ? (p: { progress?: number; total?: number; message?: string }) => {
                  const msg =
                    (p.message && p.message.trim() ? p.message.trim() : `${t.name}: ${p.progress ?? ''}`) +
                    (p.total && p.total > 0 ? `  [${p.progress}/${p.total}]` : '');
                  onUpdate({
                    content: [{ type: 'text', text: msg }],
                    details: { phase: 'progress', ...p },
                  });
                }
              : undefined;
            const text = await callTool(url, t.name, params, onProgress);
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
    };

  // ── 1. Top-level coordinator tools (active by default) ────────────────
  const topLevelTools = (await listTools()) as McpToolInfo[];
  topLevelTools.forEach(registerProxiedTool(MCP_URL, 'top-level'));

  // ── 2. Sub-MCP tools (registered but inactive; activated on demand) ───
  const subMcps = parseSubMcps();
  for (const [serverName, url] of Object.entries(subMcps)) {
    try {
      const tools = (await listTools(url)) as McpToolInfo[];
      subMcpByUrl.set(url, serverName);
      const names: string[] = tools.map((t) => t.name);
      tools.forEach(registerProxiedTool(url, `sub-mcp:${serverName}`));
      subMcpToolNames.set(serverName, names);
      console.error(`[stitcher-pi] sub-MCP '${serverName}': ${names.length} tool(s) registered (inactive) @ ${url}`);
    } catch (e) {
      console.error(
        `[stitcher-pi] sub-MCP '${serverName}' @ ${url} unreachable: ${e instanceof Error ? e.message : e}`
      );
    }
  }

  // ── 3. The two loader/meta tools (active by default) ──────────────────
  const serverList = () =>
    Object.entries(subMcps).map(([name, url]) => ({ name, url, tool_count: subMcpToolNames.get(name)?.length ?? 0 }));

  pi.registerTool({
    name: 'list_sub_mcp_servers',
    label: 'list_sub_mcp_servers',
    description:
      'List the sub-MCP servers available to this agent and the tools each one hosts. ' +
      'Sub-MCP tools are NOT active by default — call `activate_sub_mcp(name)` to switch into a sub-MCP and expose its tools.',
    parameters: Type.Object({}),
    async execute() {
      const rows = serverList();
      const text =
        rows.length === 0
          ? 'No sub-MCP servers configured.'
          : rows.map((r) => `- ${r.name}  (${r.tool_count} tools)  ${r.url}`).join('\n');
      return { content: [{ type: 'text', text }], details: { servers: rows } };
    },
  });

  pi.registerTool({
    name: 'activate_sub_mcp',
    label: 'activate_sub_mcp',
    description:
      'Switch INTO a sub-MCP: activate its tools for the rest of the session (purely additive — current tools stay). ' +
      'Call `list_sub_mcp_servers` first to see the available servers. Returns the tool names that just became active. ' +
      'Idempotent: re-activating an already-active sub-MCP is a no-op.',
    promptSnippet:
      'Activate sub-MCP tool bundles on demand with activate_sub_mcp(name) when a task needs a custom-cost / FOCUS / config-generation capability that is not in the active tool list.',
    promptGuidelines: [
      'When a task needs a capability not in the active tools, call list_sub_mcp_servers (or stitcher_capabilities) then activate_sub_mcp(name) to load the relevant sub-MCP bundle.',
      'To normalize an invoice to FOCUS: call activate_sub_mcp(name="custom_cost"), then normalize_to_focus(file_path=...). normalize_to_focus is an MCP tool, not a shell command — never search for it with which/command -v.',
      'To generate an enhance/enrich config: call activate_sub_mcp(name="config_generation"), then use the operator_tools / data_source_tools / authoring_tools.',
    ],
    parameters: Type.Object({
      name: Type.String({ description: 'Sub-MCP server name (from list_sub_mcp_servers), e.g. "custom_cost"' }),
    }),
    async execute(_toolCallId, params) {
      const serverName = String(params.name ?? '').trim();
      const activeNow = () => pi.getActiveTools();
      if (!serverName) {
        return {
          content: [{ type: 'text', text: 'ERR: pass a sub-MCP name (see list_sub_mcp_servers).' }],
          details: { error: true, server: '', added: [] as string[], active_now: activeNow() },
        };
      }
      if (!subMcpToolNames.has(serverName)) {
        const known = Object.keys(subMcps).join(', ') || '(none configured)';
        return {
          content: [{ type: 'text', text: `ERR: unknown sub-MCP '${serverName}'. Known: ${known}` }],
          details: { error: true, server: '', added: [] as string[], active_now: activeNow() },
        };
      }
      const wanted = subMcpToolNames.get(serverName) ?? [];
      const active = activeNow();
      const added = wanted.filter((n) => !active.includes(n));
      if (added.length > 0) {
        // Purely additive — preserves the cached prompt prefix on models with
        // native deferred loading (Anthropic/OpenAI tool_search).
        pi.setActiveTools([...new Set([...active, ...added])]);
      }
      const text =
        added.length > 0
          ? `Activated sub-MCP '${serverName}': ${added.length} tool(s) now active → ${added.join(', ')}`
          : `Sub-MCP '${serverName}' already active (tools: ${wanted.join(', ')}).`;
      return {
        content: [{ type: 'text', text }],
        details: { error: false, server: serverName, added, active_now: activeNow() },
      };
    },
  });

  // ── 4. Keep sub-MCP tools inactive at startup ─────────────────────────
  // registerTool() makes tools active by default. The sub-MCP tools were
  // registered above, so they're in pi.getAllTools() but we must hold them OUT
  // of the active set until the agent explicitly switches in. Filter at
  // session_start (before the first model request) — mirrors pi's documented
  // Dynamic Tool Loading pattern.
  const subMcpToolNameSet = new Set<string>();
  for (const names of subMcpToolNames.values()) for (const n of names) subMcpToolNameSet.add(n);

  pi.on('session_start', () => {
    const initial = pi.getActiveTools().filter((n) => !subMcpToolNameSet.has(n));
    // Ensure the two loader tools are active even if something filtered them.
    const ensured = [...new Set([...initial, 'list_sub_mcp_servers', 'activate_sub_mcp'])];
    pi.setActiveTools(ensured);
  });

  const topCount = topLevelTools.length;
  const subCount = [...subMcpToolNames.values()].reduce((a, n) => a + n.length, 0);
  console.error(
    `[stitcher-pi] registered ${topCount} top-level tool(s) active + ${subCount} sub-MCP tool(s) inactive ` +
      `(${subMcpToolNames.size} server(s)); model=${MODEL}`
  );
}

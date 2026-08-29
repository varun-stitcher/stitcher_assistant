/**
 * Conversation Logger Extension
 *
 * REPO-SCOPED: lives in THIS repo only under pi_coding_agent/.pi/extensions/.
 * Because it is project-local, pi only auto-discovers it when the process cwd is
 * this directory (or a child) — i.e. exactly when `pi_coding_agent/run.sh` execs
 * `pi`. It does NOT load in other repos or at the superrepo root, and it is
 * never installed to ~/.pi/agent/extensions (global scope).
 *
 * Records every pi conversation to a JSONL file for later analysis:
 *   - user messages (text + image metadata)
 *   - assistant messages: text blocks, internal THINKING blocks, tool calls,
 *     usage, stop reason, model/provider
 *   - tool executions: name, args, content, isError, timing, nested usage
 *   - turn / agent lifecycle boundaries
 *   - model changes
 *
 * Output: one JSONL file per session under `<cwd>/.pi/conversation-logs/`
 * (override with PI_CONVERSATION_LOG_DIR). Disable with PI_CONVERSATION_LOG=0.
 *
 * Each line is a self-contained record:
 *   { ts, seq, sessionId, sessionFile, type, ...payload }
 *
 * Commands:
 *   /convo-log            Show the active log path + record counts
 *   /convo-log off|on     Pause / resume recording for this session
 *
 * The logger is passive: it never blocks, mutates, or injects anything into
 * the conversation. It only observes.
 */

import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { appendFile, mkdir } from "node:fs/promises";
import { join, resolve } from "node:path";
import { homedir } from "node:os";

// ── Config ─────────────────────────────────────────────────────────────────

const MAX_CONTENT_BYTES = 256 * 1024; // cap any single content blob written to disk

function isDisabled(): boolean {
	return process.env.PI_CONVERSATION_LOG === "0" || process.env.PI_CONVERSATION_LOG === "false";
}

function logDir(cwd: string): string {
	const override = process.env.PI_CONVERSATION_LOG_DIR;
	if (override && override.trim().length > 0) return resolve(override);
	return join(cwd, ".pi", "conversation-logs");
}

// ── Record types ───────────────────────────────────────────────────────────

type AnyRecord = Record<string, unknown>;

interface LoggerState {
	dir: string;
	path: string | undefined;
	sessionId: string | undefined;
	sessionFile: string | undefined;
	seq: number;
	paused: boolean;
	counts: Record<string, number>;
	toolStart: Map<string, number>; // toolCallId -> start ts
}

// ── Helpers ─────────────────────────────────────────────────────────────────

function ts(): string {
	return new Date().toISOString();
}

function truncateForLog(value: unknown): { value: unknown; truncated: boolean } {
	if (typeof value !== "string") {
		try {
			value = JSON.stringify(value);
		} catch {
			return { value: "<unserializable>", truncated: false };
		}
	}
	const str = value as string;
	if (str.length <= MAX_CONTENT_BYTES) return { value: str, truncated: false };
	return {
		value: str.slice(0, MAX_CONTENT_BYTES) + `\n…[truncated ${str.length - MAX_CONTENT_BYTES} chars]`,
		truncated: true,
	};
}

/**
 * Convert a message `content` field (string or content-block array) into a
 * structured, serializable form that preserves thinking blocks verbatim.
 */
function serializeContent(content: unknown): unknown {
	if (typeof content === "string") return content;
	if (!Array.isArray(content)) return content;

	const out: AnyRecord[] = [];
	for (const block of content) {
		if (!block || typeof block !== "object") continue;
		const b = block as AnyRecord;
		switch (b.type) {
			case "text":
				out.push({ type: "text", text: truncateForLog(b.text).value });
				break;
			case "thinking":
				// Internal reasoning — keep verbatim (this is the key ask).
				out.push({ type: "thinking", thinking: truncateForLog(b.thinking).value });
				break;
			case "toolCall":
				out.push({
					type: "toolCall",
					id: b.id,
					name: b.name,
					arguments: b.arguments,
				});
				break;
			case "toolResult":
				out.push({
					type: "toolResult",
					toolCallId: b.toolCallId,
					toolName: b.toolName,
					content: serializeContent(b.content),
				});
				break;
			case "image":
				out.push({ type: "image", mediaType: b.mimeType ?? b.mediaType, bytes: b.data?.length ?? 0 });
				break;
			default:
				out.push(b);
		}
	}
	return out;
}

function summarizeDetails(details: unknown): unknown {
	if (details == null) return details;
	const t = truncateForLog(details);
	return t.truncated ? { truncated: true, preview: t.value } : details;
}

// ── Logger ──────────────────────────────────────────────────────────────────

function createLogger(cwd: string): LoggerState {
	return {
		dir: logDir(cwd),
		path: undefined,
		sessionId: undefined,
		sessionFile: undefined,
		seq: 0,
		paused: false,
		counts: {},
		toolStart: new Map(),
	};
}

async function writeRecord(state: LoggerState, type: string, payload: AnyRecord): Promise<void> {
	if (state.paused || state.path === undefined) return;
	state.seq += 1;
	state.counts[type] = (state.counts[type] ?? 0) + 1;
	const record: AnyRecord = {
		ts: ts(),
		seq: state.seq,
		sessionId: state.sessionId,
		sessionFile: state.sessionFile,
		type,
		...payload,
	};
	let line: string;
	try {
		line = JSON.stringify(record);
	} catch {
		line = JSON.stringify({ ...record, payload: "<unserializable payload>" });
	}
	await appendFile(state.path, line + "\n");
}

async function ensureLog(state: LoggerState, ctx: ExtensionContext): Promise<void> {
	if (state.path !== undefined) return;
	try {
		state.sessionId = ctx.sessionManager.getSessionId?.();
	} catch {
		/* noop */
	}
	try {
		state.sessionFile = ctx.sessionManager.getSessionFile() ?? undefined;
	} catch {
		/* noop */
	}
	const stamp = new Date().toISOString().replace(/[:.]/g, "-");
	const sid = state.sessionId ?? "nosession";
	state.path = join(state.dir, `${sid}-${stamp}.jsonl`);
	await mkdir(state.dir, { recursive: true });
	// Write a header record so the file exists immediately.
	await writeRecord(state, "log_open", { logPath: state.path, cwd: ctx.cwd, homedir: homedir() });
}

// ── Extension ───────────────────────────────────────────────────────────────

export default function (pi: ExtensionAPI) {
	if (isDisabled()) return;

	const state = createLogger(process.cwd());

	const safeWrite = async (type: string, payload: AnyRecord, ctx?: ExtensionContext) => {
		try {
			if (ctx) await ensureLog(state, ctx);
			await writeRecord(state, type, payload);
		} catch (err) {
			// Never let logging break the agent.
			console.error("[conversation-logger] write failed:", err);
		}
	};

	// ── Session lifecycle ──────────────────────────────────────────────────
	pi.on("session_start", async (event, ctx) => {
		await ensureLog(state, ctx);
		await safeWrite("session_start", { reason: event.reason, previousSessionFile: event.previousSessionFile }, ctx);
		const model = ctx.model;
		await safeWrite(
			"model_active",
			{ model: model ? `${model.provider}/${model.id}` : null, thinkingLevel: ctx.thinkingLevel },
			ctx,
		);
		if (ctx.hasUI) ctx.ui.setStatus("convo-log", state.paused ? "recording paused" : "recording conversation");
	});

	pi.on("session_shutdown", async (event, ctx) => {
		await safeWrite("session_shutdown", { reason: event.reason, targetSessionFile: event.targetSessionFile, totalRecords: state.seq }, ctx);
	});

	// ── Agent / turn lifecycle ──────────────────────────────────────────────
	pi.on("before_agent_start", async (event, ctx) => {
		await safeWrite("user_prompt", { prompt: truncateForLog(event.prompt).value, images: (event.images ?? []).length }, ctx);
	});

	pi.on("agent_start", async (_event, ctx) => safeWrite("agent_start", {}, ctx));
	pi.on("agent_end", async (event, ctx) => safeWrite("agent_end", { messageCount: event.messages?.length ?? 0 }, ctx));

	pi.on("turn_start", async (event, ctx) => safeWrite("turn_start", { turnIndex: event.turnIndex }, ctx));
	pi.on("turn_end", async (event, ctx) => safeWrite("turn_end", { turnIndex: event.turnIndex, toolResults: event.toolResults?.length ?? 0 }, ctx));

	// ── Messages (the meat) ─────────────────────────────────────────────────
	pi.on("message_end", async (event, ctx) => {
		const msg = event.message as AnyRecord;
		const role = msg?.role;
		const base: AnyRecord = { role, timestamp: msg?.timestamp };
		if (role === "assistant") {
			await safeWrite(
				"assistant_message",
				{
					...base,
					provider: msg?.provider,
					model: msg?.model,
					api: msg?.api,
					stopReason: msg?.stopReason,
					errorMessage: msg?.errorMessage,
					content: serializeContent(msg?.content),
					usage: msg?.usage,
				},
				ctx,
			);
		} else if (role === "user") {
			await safeWrite("user_message", { ...base, content: serializeContent(msg?.content) }, ctx);
		} else if (role === "toolResult") {
			await safeWrite(
				"tool_result_message",
				{
					...base,
					toolCallId: msg?.toolCallId,
					toolName: msg?.toolName,
					isError: msg?.isError,
					content: serializeContent(msg?.content),
					details: summarizeDetails(msg?.details),
					usage: msg?.usage,
				},
				ctx,
			);
		} else if (role === "bashExecution") {
			await safeWrite(
				"bash_execution",
				{
					...base,
					command: truncateForLog(msg?.command).value,
					exitCode: msg?.exitCode,
					cancelled: msg?.cancelled,
					truncated: msg?.truncated,
					excludeFromContext: msg?.excludeFromContext,
					outputPreview: truncateForLog(msg?.output).value,
				},
				ctx,
			);
		} else if (role === "custom") {
			await safeWrite("custom_message", { ...base, customType: msg?.customType, content: serializeContent(msg?.content) }, ctx);
		} else {
			await safeWrite("message_other", { ...base, content: serializeContent(msg?.content) }, ctx);
		}
	});

	// ── Tool execution lifecycle (timing + args) ────────────────────────────
	pi.on("tool_execution_start", async (event, ctx) => {
		state.toolStart.set(event.toolCallId, Date.now());
		await safeWrite(
			"tool_call",
			{ toolCallId: event.toolCallId, toolName: event.toolName, args: event.args },
			ctx,
		);
	});

	pi.on("tool_execution_update", async (event, ctx) => {
		await safeWrite(
			"tool_update",
			{ toolCallId: event.toolCallId, toolName: event.toolName, partialResult: summarizeDetails(event.partialResult) },
			ctx,
		);
	});

	pi.on("tool_execution_end", async (event, ctx) => {
		const started = state.toolStart.get(event.toolCallId);
		state.toolStart.delete(event.toolCallId);
		await safeWrite(
			"tool_execution_end",
			{
				toolCallId: event.toolCallId,
				toolName: event.toolName,
				isError: event.isError,
				durationMs: started != null ? Date.now() - started : null,
				result: summarizeDetails(event.result),
			},
			ctx,
		);
	});

	// ── Model changes ──────────────────────────────────────────────────────
	pi.on("model_select", async (event, ctx) => {
		await safeWrite(
			"model_select",
			{
				model: `${event.model.provider}/${event.model.id}`,
				previousModel: event.previousModel ? `${event.previousModel.provider}/${event.previousModel.id}` : null,
				source: event.source,
			},
			ctx,
		);
	});

	pi.on("thinking_level_select", async (event, ctx) => {
		await safeWrite("thinking_level_select", { level: event.level, previousLevel: event.previousLevel }, ctx);
	});

	// ── Command: inspect / toggle the logger ──────────────────────────────
	pi.registerCommand("convo-log", {
		description: "Show conversation log path/counts, or toggle recording (off|on)",
		handler: async (args, ctx) => {
			const arg = args.trim().toLowerCase();
			if (arg === "off") {
				state.paused = true;
				ctx.ui.setStatus("convo-log", "recording paused");
				ctx.ui.notify("Conversation logging paused", "info");
				return;
			}
			if (arg === "on") {
				state.paused = false;
				ctx.ui.setStatus("convo-log", "recording conversation");
				ctx.ui.notify("Conversation logging resumed", "info");
				return;
			}
			await ensureLog(state, ctx);
			const lines = [
				`log: ${state.path ?? "(not opened yet)"}`,
				`dir: ${state.dir}`,
				`session: ${state.sessionId ?? "?"}`,
				`records: ${state.seq}`,
				`state: ${state.paused ? "paused" : "recording"}`,
			];
			const breakdown = Object.entries(state.counts)
				.sort((a, b) => b[1] - a[1])
				.map(([k, v]) => `  ${k}: ${v}`)
				.join("\n");
			if (breakdown) lines.push("by type:", breakdown);
			if (ctx.mode === "tui") {
				await ctx.ui.custom((_tui, theme, _kb, done) => {
					// eslint-disable-next-line @typescript-eslint/no-var-requires
					const { Container, Text, matchesKey } = require("@earendil-works/pi-tui") as typeof import("@earendil-works/pi-tui");
					const container = new Container();
					container.addChild(new Text(theme.bold("Conversation Logger"), 1, 0));
					for (const l of lines) container.addChild(new Text(l, 0, 0));
					container.addChild(new Text(theme.fg("dim", "Press Enter or Esc to close"), 1, 0));
					return {
						render: (width: number) => container.render(width),
						invalidate: () => container.invalidate(),
						handleInput: (data: string) => {
							if (matchesKey(data, "enter") || matchesKey(data, "escape")) done(undefined);
						},
					};
				});
			} else {
				ctx.ui.notify(lines.join("\n"), "info");
			}
		},
	});
}

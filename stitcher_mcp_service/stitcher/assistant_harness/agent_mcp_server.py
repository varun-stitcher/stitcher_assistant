"""Higher-order MCP server — the stitcher pi agent as an orchestrator (Claude Code / Desktop).

This is the "agent is the tool" surface: Claude Code / Claude Desktop register this server as an
MCP client and invoke a **task-typed** tool. Each tool drives one headless pi turn (via
`AgentRunner`) scoped to the caller's `environment_id` / `pipeline_name`, the agent orchestrates
(ground → activate sub-MCP → author/normalize → validate → save) and calls `submit_result`, and
the tool returns the **structured output** to the caller.

Task-typed tools (small set, each with a typed return schema):
  - ``generate_enhance_config``  — author + validate + save an enhance prepare/enrich config.
  - ``normalize_invoice_to_focus`` — normalize any invoice to the FOCUS v1.2 column shape.
  - ``explore_environment``     — read-only scout (data sources / committed configs / derived cols).

Env scope is **per-call** (every tool takes ``environment_id`` + ``pipeline_name``); config
generation is environment-scoped and refuses without them. ``normalize_invoice_to_focus`` is
env-agnostic (the custom_cost sub-MCP) so its scope args are optional but accepted for consistency.

Run (served by the gateway, not standalone): ``python -m stitcher.assistant_harness.gateway``.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from fastmcp import Context, FastMCP

from .agent_runner import AgentResult, AgentRunner, harvest_saved_config

logger = logging.getLogger(__name__)

# One shared runner per gateway process; run() is concurrency-safe (per-call ephemeral port + dir).
_runner: AgentRunner | None = None


def _get_runner() -> AgentRunner:
    global _runner
    if _runner is None:
        _runner = AgentRunner()
    return _runner


async def _run_with_progress(ctx: Context, prompt: str, **scope) -> AgentResult:
    """Run an orchestrator turn, forwarding best-effort progress to the MCP client via ctx."""
    runner = _get_runner()
    queue: asyncio.Queue = asyncio.Queue()
    loop = asyncio.get_running_loop()

    def _on_event(ev: dict) -> None:
        loop.call_soon_threadsafe(queue.put_nowait, ev)

    runner_task = asyncio.create_task(asyncio.to_thread(runner.run, prompt, on_event=_on_event, **scope))

    # Drain progress events while the turn runs.
    while not runner_task.done():
        try:
            ev = await asyncio.wait_for(queue.get(), timeout=0.25)
            await ctx.report_progress(
                progress=ev.get("turn", 0),
                total=0,
                message=f"{ev.get('stage', 'orchestrating')}: {ev.get('message', '')}",
            )
        except TimeoutError:
            pass
    return await runner_task


def build_server() -> FastMCP:
    """Build the higher-order orchestrator MCP server (the task-typed tool surface)."""
    mcp = FastMCP(name="stitcher-pi-agent")

    @mcp.tool
    async def generate_enhance_config(
        ctx: Context,
        environment_id: str,
        pipeline_name: str,
        requirement: str,
        stage: str = "enrich",
        auth_tenant: str = "",
    ) -> dict[str, Any]:
        """Author + validate + save an enhance config for the given requirement, scoped to an
        environment. The agent grounds on the real environment (lists data sources, inspects
        metadata, reads committed configs), activates the config_generation sub-MCP, authors the
        operation(s) deterministically, validates against the real SPC enhance model, saves the
        config, and returns a structured result.

        Args:
            environment_id: the Stitcher environment UUID to operate on (required).
            pipeline_name: the pipeline the agent is bound to (required).
            requirement: plain-English description of the enhance operation to author
              (e.g. "enrich my AI spend with the owning team from the app metadata table" or
              "drop rows where BilledCost = 0").
            stage: "prepare" or "enrich" (default "enrich").
            auth_tenant: Keycloak realm / org id for SOE reads (required for scan/metadata/git;
              the tool still runs without it but those grounding reads will fail).

        Returns:
            ``{task, status, stage, config_type, config_yaml, saved_path, validation,
            data_sources_used, summary}`` on success; ``{task, status:"needs_input", question}``
            if the agent needs clarification; ``{task, status:"failed", error}`` on failure.
        """
        if stage not in ("prepare", "enrich"):
            return {
                "task": "generate_enhance_config",
                "status": "failed",
                "error": "stage must be 'prepare' or 'enrich'.",
            }
        if not requirement.strip():
            return {"task": "generate_enhance_config", "status": "failed", "error": "requirement is required."}
        prompt = (
            f"Generate an enhance {stage} config for environment {environment_id} / pipeline {pipeline_name}.\n"
            f"Requirement: {requirement}\n"
            "Ground on the real environment: list_data_sources, get_data_source_metadata, scan_data, "
            "get_committed_config, derived_columns as needed. Then activate_sub_mcp('config_generation'), "
            "author the operation(s) with generate_lookup / generate_filter, validate_config, save_config, "
            "and submit_result with the generate_enhance_config schema."
        )
        res = await _run_with_progress(
            ctx, prompt, environment_id=environment_id, pipeline_name=pipeline_name, auth_tenant=auth_tenant
        )
        out: dict[str, Any] = {
            "task": "generate_enhance_config",
            "status": res.status,
            "stage": stage,
            "summary": res.text,
            "turns": res.turns,
            "elapsed_seconds": res.elapsed,
            "run_dir": res.run_dir,
        }
        if res.status == "ok" and res.result:
            out.update(res.result)
            # Authoritative cross-check: save_config is the only persist — harvest the newest YAML.
            saved = harvest_saved_config(res.run_dir, stage)
            if saved:
                out["saved_path"] = saved["saved_path"]
                out["config_yaml"] = saved["config_yaml"]
                out["validation"] = "PASS"
        else:
            out["error"] = res.error
            # Surface the agent's own structured failure (e.g. needs_input) when it submitted one.
            if res.result:
                out.update(res.result)
        return out

    @mcp.tool
    async def normalize_invoice_to_focus(
        ctx: Context,
        file_path: str = "",
        pdf_b64: str = "",
        filename: str = "",
        provider_name: str = "auto",
        expected_columns: list[str] | None = None,
        validate: bool = True,
        environment_id: str = "",
        pipeline_name: str = "",
        auth_tenant: str = "",
    ) -> dict[str, Any]:
        """Normalize any invoice (PDF/CSV) to the FOCUS v1.2 column shape. The agent activates the
        custom_cost sub-MCP, runs normalize_to_focus, and returns the normalization result
        (extraction summary, plans, FOCUS-normalized summary, validation report).

        Pass ``file_path`` to a file already on the server (preferred), OR ``pdf_b64`` + ``filename``
        when the bytes aren't local. custom_cost is environment-agnostic, so the scope args are
        optional (accepted for consistency with the other tools).

        Returns:
            ``{task, status, focus:{success, extraction_summary, plans, focus_summary,
            validation_report, elapsed_seconds}}`` on success; ``{task, status:"failed", error}``
            on failure.
        """
        if not file_path and not pdf_b64:
            return {
                "task": "normalize_invoice_to_focus",
                "status": "failed",
                "error": "Provide either file_path or pdf_b64 (+ filename).",
            }
        source = f"file_path={file_path}" if file_path else f"pdf_b64=<{len(pdf_b64)} bytes>, filename={filename}"
        cols = f", expected_columns={expected_columns}" if expected_columns else ""
        prompt = (
            f"Normalize an invoice to FOCUS. Source: {source}, provider_name={provider_name}, "
            f"validate={validate}{cols}.\n"
            "activate_sub_mcp('custom_cost'), call normalize_to_focus with those arguments, and "
            "submit_result with the normalize_invoice_to_focus schema (the focus dict verbatim)."
        )
        scope: dict[str, Any] = {
            "environment_id": environment_id or "env-agnostic",
            "pipeline_name": pipeline_name or "n/a",
        }
        if auth_tenant:
            scope["auth_tenant"] = auth_tenant
        res = await _run_with_progress(ctx, prompt, **scope)
        out: dict[str, Any] = {
            "task": "normalize_invoice_to_focus",
            "status": res.status,
            "summary": res.text,
            "turns": res.turns,
            "elapsed_seconds": res.elapsed,
            "run_dir": res.run_dir,
        }
        if res.status == "ok" and res.result:
            out["focus"] = res.result.get("focus", res.result)
        else:
            out["error"] = res.error
            if res.result:
                out["focus"] = res.result.get("focus", res.result)
        return out

    @mcp.tool
    async def explore_environment(
        ctx: Context,
        environment_id: str,
        pipeline_name: str,
        auth_tenant: str = "",
    ) -> dict[str, Any]:
        """Read-only scout of an environment: list the data sources, the committed pipeline
        configs, and the derived columns. No authoring. Use this to scope a follow-up
        ``generate_enhance_config`` call.

        Args:
            environment_id: the Stitcher environment UUID (required).
            pipeline_name: the pipeline (required).
            auth_tenant: Keycloak realm / org id for SOE reads (needed for committed-config fetch).

        Returns:
            ``{task, status, data_sources:[...], committed_configs:{...}, derived_columns:[...],
            summary}`` on success; ``{task, status:"failed", error}`` on failure.
        """
        prompt = (
            f"Explore environment {environment_id} / pipeline {pipeline_name} (read-only). "
            "Call list_data_sources, get_committed_config, derived_columns as available, and "
            "submit_result with the explore_environment schema."
        )
        res = await _run_with_progress(
            ctx, prompt, environment_id=environment_id, pipeline_name=pipeline_name, auth_tenant=auth_tenant
        )
        out: dict[str, Any] = {
            "task": "explore_environment",
            "status": res.status,
            "summary": res.text,
            "turns": res.turns,
            "elapsed_seconds": res.elapsed,
            "run_dir": res.run_dir,
        }
        if res.status == "ok" and res.result:
            out.update(res.result)
        else:
            out["error"] = res.error
            if res.result:
                out.update(res.result)
        return out

    return mcp

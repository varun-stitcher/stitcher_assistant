"""SOE-as-is context shared by the ``assistant_harness`` sub-MCP agents (common scaffolding).

Configured-generation and other sub-MCP flows ground on the REAL environment by exercising SOE
functions directly (``ExtractRefDataSubOperator`` schema/reads, ``get_vsc_commit_dir``) — not
vendored copies. Those functions take a ``WorkflowContext`` + rely on ``ExecutorConfig()`` /
``WebserviceCommonSettings()``, both of which are pydantic-settings ``BaseSettings`` that load
``.env.local`` / ``.env.local.dev`` from the **current working directory** — exactly the way SOE
itself runs. The launcher (``run.sh``) therefore starts this sub-MCP from the SOE dir so those env
files are resolved as-is; there is no manual env-file parsing here. This module builds and caches a
hand-built ``WorkflowContext`` (a pydantic ``BaseModel``, constructible outside Temporal — verified
by the Step 1 spike: no ``workflow.info()`` / ``activity.`` calls in the target functions).

SOE env tuple (environment_id, pipeline_id, branch, auth_tenant) is read from ``StitcherAssistantConfig``
(``SAI_ENV_CONTEXT`` JSON — mirroring ``pi_agent_coding_harness/server/env_context.py`` — with per-field
fallback to ``STITCHER_ENVIRONMENT_ID`` / ``STITCHER_PIPELINE_ID`` / ``STITCHER_GIT_BRANCH`` /
``STITCHER_AUTH_TENANT``). ``pipeline_id`` is resolved lazily from the pipeline name via ``StitcherClient``
when missing.
"""

from __future__ import annotations

import logging
import pathlib
import uuid

logger = logging.getLogger(__name__)

# Where the save_config tool writes authored configs (a local, gitignored dir): anchored to the
# pi_coding_agent dir via this module's location (parents[4] = stitcher_assistant), independent of
# the server's CWD. Overridable via STITCHER_OUTPUT_DIR.
_OUTPUT_DIR = pathlib.Path(__file__).resolve().parents[4] / "pi_coding_agent" / ".output"

# ── SOE env tuple (environment_id, pipeline_id, branch, auth_tenant) ──────────


class SoeContext:
    """Holds the SOE scope + lazily-built ``WorkflowContext`` for the sub-MCP tools.

    Constructed once in ``build_server()`` from the top-level ``StitcherAssistantConfig`` /
    ``OIDCAuth`` / ``StitcherClient`` (env-scoped, like the top-level coordinator) and passed
    to each tool module's ``register(mcp, client, soe)``.
    """

    def __init__(self, settings, auth, client) -> None:
        self.s = settings
        self.auth = auth
        self.client = client
        ctx = settings.env_context  # SAI_ENV_CONTEXT JSON, parsed by StitcherAssistantConfig
        self.environment_id: str = str(ctx.get("environment_id") or settings.environment_id or "")
        self.pipeline_id: str | None = str(ctx.get("pipeline_id") or settings.pipeline_id or "")
        self.pipeline_name: str = str(ctx.get("pipeline_name") or settings.pipeline_name or "")
        self.branch: str = str(ctx.get("branch") or settings.git_branch or "main")
        self.auth_tenant: str = str(ctx.get("auth_tenant") or settings.auth_tenant or "")
        self.output_dir: str = str(pathlib.Path(settings.output_dir or _OUTPUT_DIR))
        self._workflow_context = None
        self._pipeline_id_resolved = False
        self.pipeline_resolve_error: str = ""

    # ── scope checks ──────────────────────────────────────────────────────────

    @property
    def is_scoped(self) -> bool:
        """True when an environment_id is present (enough for data-source / metadata / scan)."""
        return bool(self.environment_id)

    @property
    def has_pipeline(self) -> bool:
        """True when a pipeline_id is present (additionally needed for git-config fetch)."""
        return bool(self.pipeline_id)

    @property
    def has_tenant(self) -> bool:
        """True when an auth_tenant (Keycloak realm / org id) is present. Without it the SOE
        DataConnectionUtil / get_vsc_commit_dir paths authenticate at Keycloak with a bogus realm
        and fail with 'Realm does not exist'."""
        return bool(self.auth_tenant)

    def scope_error(self) -> str:
        """A usable 'unscoped' message, or '' when scoped."""
        if not self.environment_id:
            return "ERR: no STITCHER_ENVIRONMENT_ID (or SAI_ENV_CONTEXT.environment_id) — config generation is environment-scoped."
        return ""

    def tenant_error(self) -> str:
        """A precise, actionable message when an SOE function needs a real Keycloak realm but
        ``auth_tenant`` is missing — instead of the cryptic 'Realm does not exist' JWT failure the
        agent would otherwise hit mid-flow. Returns '' when a tenant is present."""
        if self.auth_tenant:
            return ""
        return (
            "ERR: STITCHER_AUTH_TENANT is not set — SOE data-source/metadata/scan/git-config reads "
            "authenticate at Keycloak as org_id=auth_tenant and will fail with 'Realm does not exist'. "
            "Set STITCHER_AUTH_TENANT (or SAI_ENV_CONTEXT.auth_tenant) to this environment's org/tenant "
            "realm (e.g. the value from environment_context / the sai-plugin-e2e skill), then retry."
        )

    # ── lazy pipeline_id resolution ────────────────────────────────────────────

    def resolve_pipeline_id(self) -> str | None:
        """Resolve the pipeline UUID from the pipeline name via ``StitcherClient`` when missing.
        Cached after the first success. Best-effort (returns None on failure); records the reason in
        ``self.pipeline_resolve_error`` so the caller can surface a diagnostic instead of a bare None."""
        if self.pipeline_id:
            self._pipeline_id_resolved = True
            self.pipeline_resolve_error = ""
            return self.pipeline_id
        if self._pipeline_id_resolved:
            return self.pipeline_id
        if not self.pipeline_name:
            self.pipeline_resolve_error = "no pipeline_name set (SAI_ENV_CONTEXT.pipeline_name or settings.pipeline_name) to resolve a pipeline_id from"
            self._pipeline_id_resolved = True
            return None
        try:
            # StitcherClient.get_pipeline returns a multi-line string that includes `id=…`;
            # parse the id line rather than reach into the generated client here.
            blob = self.client.get_pipeline(pipeline_name=self.pipeline_name)
            for line in str(blob).splitlines():
                if line.strip().startswith("id="):
                    self.pipeline_id = line.split("=", 1)[1].strip()
                    self.pipeline_resolve_error = ""
                    break
            if not self.pipeline_id:
                self.pipeline_resolve_error = (
                    f"SWS get_pipeline(pipeline_name={self.pipeline_name!r}) returned no `id=` line "
                    f"(pipelines first {len(str(blob).splitlines())} lines: {str(blob).splitlines()[:3]!r}) — "
                    "check the pipeline name / SWS auth, or set STITCHER_PIPELINE_ID directly."
                )
            self._pipeline_id_resolved = True
        except Exception as e:  # noqa: BLE001
            logger.debug("resolve_pipeline_id failed: %s", e)
            self.pipeline_resolve_error = (
                f"SWS get_pipeline(pipeline_name={self.pipeline_name!r}) raised: {str(e)[:250]}"
            )
            self._pipeline_id_resolved = True
        return self.pipeline_id or None

    # ── WorkflowContext (hand-built, cached) ──────────────────────────────────

    def get_workflow_context(self):
        """A hand-built SOE ``WorkflowContext`` for the SOE functions we call (metadata operator,
        extract reference-data reads, get_vsc_commit_dir). pydantic BaseModel — constructible
        outside Temporal (Step 1 spike). Refuses to build when unscoped."""
        if self._workflow_context is not None:
            return self._workflow_context
        if not self.is_scoped:
            raise RuntimeError(self.scope_error())
        from datetime import date as _date

        from stitcher.operation_executor.models.workflow_context import WorkflowContext
        from stitcher.pipeline.common.schema.date_input import SimpleDateRange
        from stitcher.pipeline.common.schema.stitcher_workflow_progress_tracker import (
            StitcherWorkflowProgressTracker,
        )
        from stitcher.pipeline.common.schema.user_defined_configs import (
            OrgExternalConfigSchema,
            OrgInternalConfigSchema,
            WorkflowConfigSchema,
        )

        today = _date.today()
        env_uuid = uuid.UUID(self.environment_id)
        run_uuid = uuid.uuid4()
        wc = WorkflowContext(
            auth_tenant=self.auth_tenant or "config-gen",
            environment_id=env_uuid,
            pipeline_name=self.pipeline_name or "finops",
            pipeline_run_id=run_uuid,
            environment_fq_name=f"env-{env_uuid}",
            org_ext_configs=OrgExternalConfigSchema(),
            org_int_configs=OrgInternalConfigSchema(),
            date_range=SimpleDateRange(start_date=today.replace(day=1), end_date=today),
            month=f"{today.year}{today.month:02d}",
            workflow_args=WorkflowConfigSchema(),
            progress_tracker=StitcherWorkflowProgressTracker(),
            request_id=f"config-gen-{run_uuid.hex[:8]}",
        )
        self._workflow_context = wc
        return wc

    # ── committed-config git fetch (shared by any agent needing the committed state) ──────────

    def _pipelines_in_env_hint(self) -> str:
        """Best-effort list of the pipelines that DO exist in this environment, for a 404 hint.
        Uses the SAME SOE service-account token path as the fetch itself (the harness browser-
        OIDC token may not be present, and the diagnostic must not depend on it). Never raises —
        a failed lookup just means no hint — deterministic, no fabrication."""
        try:
            import uuid as _uuid

            from stitcher.operation_executor.common.vcs_repo import WebserviceIntegration

            token = WebserviceIntegration.get_token(
                auth_tenant=self.auth_tenant, environment_id=_uuid.UUID(self.environment_id)
            )
            from stitcher.assistant_harness.common.config import StitcherAssistantConfig
            from stitcher.webservice.client import ApiClient, Configuration
            from stitcher.webservice.client.api.pipeline_api import PipelineApi

            host = StitcherAssistantConfig().api_url
            conf = Configuration(host=host, access_token=token)
            if verify := self.auth._http_verify():
                if verify is not True:
                    conf.ssl_ca_cert = str(verify)
            else:
                conf.verify_ssl = False
                conf.assert_hostname = False
            with ApiClient(conf) as client:
                api = PipelineApi(client)
                resp = api.list_pipelines(environment=self.environment_id)
            objs = getattr(resp, "objects", None) or []
            rows = [f"  - {getattr(p, 'name', '?')}: {getattr(p, 'id', '?')}" for p in objs if getattr(p, "id", None)]
            if rows:
                return "Pipelines that exist in this environment:\n" + "\n".join(rows[:10])
        except Exception:  # noqa: BLE001 — hint only, never a new failure
            pass
        return ""

    def _diagnose_vcs_error(self, e: Exception, pipeline_id: str) -> str:
        """Turn a raw committed-config-fetch exception into an actionable message for the
        OPERATOR (never a traceback to the agent). Classifies the known credential/infra
        failures at each layer of the chain: Keycloak → SWS pipeline lookup → Vault
        installation id → GitHub App key → GitHub token exchange → repo clone."""
        type_name = type(e).__name__
        msg = str(e)

        # ── GitHub App private key missing (FileNotFoundError from open(...pem)) ─
        if type_name == "FileNotFoundError" or ("No such file or directory" in msg and ".pem" in msg):
            return (
                "ERR: GitHub App private key not found — the committed-config read cannot "
                "authenticate to the pipeline's config repository.\n"
                f"  raw error: {msg[:200]}\n"
                "  Fix: place the stitcherai-dev GitHub App private key at the path configured by "
                "GITHUB_PRIVATE_KEY_PATH in pi_coding_agent/.env.local.dev "
                "(default ../local/github/gh_app_key.pem), then retry."
            )

        # ── SWS pipeline not found (404) — include which pipelines DO exist ─────
        if type_name == "NotFoundException" or ("404" in msg and "pipeline" in msg.lower()):
            hint = self._pipelines_in_env_hint()
            return (
                f"ERR: SWS returned 404 — no pipeline with id {pipeline_id} exists in environment "
                f"{self.environment_id} (note: SWS itself IS reachable — this is a scope/config "
                "mismatch, not an outage).\n"
                + (hint + "\n" if hint else "")
                + "  Fix: set STITCHER_PIPELINE_ID (or STITCHER_PIPELINE_NAME) to one of the "
                "pipelines above in run.local.sh / the gateway call scope."
            )

        # ── SWS / Keycloak auth failures (401/403) ─────────────────────────────
        if type_name in ("UnauthorizedException", "ForbiddenException") or "401" in msg or "403" in msg:
            return (
                f"ERR: authentication failed for the committed-config read ({type_name}: {msg[:150]}).\n"
                f"  Fix: check STITCHER_AUTH_TENANT (currently: {self.auth_tenant or 'UNSET'}) — the "
                "service account must exist in that Keycloak realm with access to this environment. "
                "Also verify STITCHER_API_TOKEN / STITCHER_MODEL_API_KEY are current."
            )

        # ── Vault: installation id lookup failed ────────────────────────────────
        if "installation" in msg.lower() or "vault" in msg.lower():
            return (
                f"ERR: could not resolve the GitHub App installation for pipeline {pipeline_id} "
                f"({msg[:200]}).\n"
                "  Fix: confirm the stitcherai-dev GitHub App is installed on the pipeline's config "
                "repository and the installation id is stored in Vault for this environment."
            )

        # ── Unreachable service (connection refused / DNS / TLS) ────────────────
        if any(
            k in msg.lower()
            for k in ("connection refused", "name or service not known", "name resolution", "timed out", "ssl")
        ):
            return (
                f"ERR: a service needed for the committed-config read is unreachable ({msg[:200]}).\n"
                "  Fix: check that the local stack is up (docker/start_services.sh) and that "
                "SWS_URL / Keycloak URLs in .env.local are reachable from this machine."
            )

        # ── fallback: keep the raw message (never a fabricated success) ─────────
        return f"ERR fetching committed config from git: {msg[:250]}"

    async def fetch_committed_configs(self, branch: str = "") -> tuple[dict | None, str]:
        """Fetch the LATEST COMMITTED pipeline configs from the git branch for this environment's
        pipeline via the SOE git integration (``get_vsc_commit_dir``). Enforces the SOE read
        preconditions (scoped, tenant, pipeline_id) and returns ``(pipeline_configs, error_msg)`` —
        ``error_msg`` is non-empty on refusal/failure, never a fabricated result. Reusable by any
        sub-MCP agent that needs the committed state."""
        from stitcher.operation_executor.common.vcs_repo import get_vsc_commit_dir

        if not self.is_scoped:
            return None, "ERR: no STITCHER_ENVIRONMENT_ID — committed-config fetch is environment-scoped."
        if self.scope_error():
            return None, self.scope_error()
        if ten := self.tenant_error():
            return None, ten
        pipeline_id = self.resolve_pipeline_id()
        if not pipeline_id:
            why = self.pipeline_resolve_error or "no pipeline_id resolved"
            return None, (
                "ERR: could not resolve pipeline_id from the pipeline name — "
                f"{why}. Set STITCHER_PIPELINE_ID (or SAI_ENV_CONTEXT.pipeline_id) to bypass resolution."
            )
        wc = self.get_workflow_context()
        try:
            result = await get_vsc_commit_dir(
                workflow_context=wc,
                environment_id=uuid.UUID(self.environment_id),
                pipeline_id=uuid.UUID(pipeline_id),
                git_branch=branch or self.branch or "main",
            )
        except Exception as e:  # noqa: BLE001
            return None, self._diagnose_vcs_error(e, pipeline_id)
        return result.get("pipeline_configs"), ""

    # ── summary for the environment_context tool ──────────────────────────────

    def summary(self) -> str:
        lines = [
            "# config_generation environment context",
            f"environment_id: {self.environment_id or '(unset — unscoped)'}",
            f"pipeline_id: {self.pipeline_id or '(unset — resolve on demand)'}",
            f"pipeline_name: {self.pipeline_name or '(unset)'}",
            f"branch: {self.branch}",
            "auth_tenant: "
            + (
                self.auth_tenant or "(UNSET — SOE reads/metadata/scan/git-config will FAIL with 'Realm does not exist')"
            ),
            f"scoped: {'yes' if self.is_scoped else 'no'}",
        ]
        if self.is_scoped and not self.has_pipeline:
            lines.append(
                "(environment_id set — data-source/metadata/scan available; pipeline_id needed for git-config fetch)"
            )
        if self.auth_tenant:
            lines.append("(auth_tenant set — SOE data/metadata/scan/git reads can authenticate at Keycloak)")
        return "\n".join(lines)


def build_soe_context(settings, auth, client) -> SoeContext:
    """Construct the SoeContext (SOE env files are resolved by BaseSettings from the server CWD)."""
    return SoeContext(settings, auth, client)

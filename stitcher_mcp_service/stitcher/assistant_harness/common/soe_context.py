"""SOE-as-is context shared by the ``assistant_harness`` sub-MCP agents (common scaffolding).

Configured-generation and other sub-MCP flows ground on the REAL environment by exercising SOE
functions directly (``ExtractRefDataSubOperator`` schema/reads, ``get_vsc_commit_dir``) — not
vendored copies. Those functions take a ``WorkflowContext`` + rely on ``ExecutorConfig()`` /
``WebserviceCommonSettings()``, both of which read SOE ``.env.local`` / ``.env.local.dev``. This
module loads those env files into ``os.environ`` (a broadened superset of the harness's
``_NEEDED_VARS`` — see spike Step 1) BEFORE the first ``ExecutorConfig()`` /
``WebserviceCommonSettings()`` construction, then builds and caches a hand-built ``WorkflowContext``
(a pydantic ``BaseModel``, constructible outside Temporal — verified by the Step 1 spike: no
``workflow.info()`` / ``activity.`` calls in the target functions).

Env-file resolution order (first existing wins):
  1. ``STITCHER_SOE_ENV_DIR`` (explicit; point at a copied set for portability)
  2. ``<repo_root>/stitcher_assistant/pi_coding_agent/.soe-env`` (the plan's local copy, gitignored)
  3. ``<repo_root>/stitcher_operation_executor`` (the SOE dir itself — "as is")

SOE env tuple (environment_id, pipeline_id, branch, auth_tenant) is read from ``SAI_ENV_CONTEXT``
(a JSON blob, mirroring ``pi_agent_coding_harness/server/env_context.py``) with per-field fallback to
``STITCHER_ENVIRONMENT_ID`` / ``STITCHER_PIPELINE_ID`` / ``STITCHER_GIT_BRANCH`` / ``STITCHER_AUTH_TENANT``.
``pipeline_id`` is resolved lazily from the pipeline name via ``StitcherClient`` when missing.
"""

from __future__ import annotations

import json
import logging
import os
import pathlib
import uuid
from datetime import date
from functools import lru_cache
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ── env-file loader ────────────────────────────────────────────────────────────
# ExecutorConfig (StitcherBaseConfigClass) + WebserviceCommonSettings both read SOE .env files.
# We load a whitelisted superset of the scalar vars both need, skipping the multi-line K8S JSON blob
# (line-by-line parsing chokes on it — same reason the harness restricts to a whitelist). The set
# below is the harness's ExecutorConfig vars PLUS the WebserviceCommonSettings / vault vars the
# DataConnectionUtil + get_vsc_commit_dir paths additionally require (Step 1 spike finding).
_SOE_ENV_VARS: set[str] = {
    # ExecutorConfig (DataProcessingBasePathsConfig / StitcherServiceURLConfig / pairing accounts)
    "BASE_PATH",
    "BASE_TEMP_PATH",
    "LONG_TERM_STORAGE_BASE_PATH",
    "GCS_EXTRACTED_DATA_CACHE",
    "GCS_TEMP_PROXY_FOR_GCP_OPERATIONS",
    "SWS_URL",
    "SWS_ADMIN_URL",
    "APP_BASE_URL",
    "SSL_CA_CERTIFICATE_PATH",
    "LOG_LEVEL",
    "EXECUTOR",
    "NAMESPACE_SUFFIX",
    "AWS_PAIRING_ACCOUNT_ID",
    "GCP_SERVICE_ACCOUNT_PROJECT_ID",
    "GCP_GKE_SERVICE_ACCOUNT_PROJECT_ID",
    # WebserviceCommonSettings (DataConnectionUtil JWT + get_vsc_commit_dir auth)
    "VAULT_ROLE_TEMPLATE_CLASS",
    "OPENID_SA_CLIENT_ID",
    "OPENID_SA_CLIENT_SECRET",
    "KEYCLOAK_URL",
    "VAULT_URL",
    "AWS_ROLE_NAME_SUFFIX",
}

# Path-valued vars whose RELATIVE values are relative to the SOE dir (the `.env.local` files use
# e.g. `SSL_CA_CERTIFICATE_PATH=../local/certs/ca.crt`, intended relative to `stitcher_operation_executor/`,
# NOT to CWD). When the sub-MCP runs from a different CWD we absolutize them against the SOE dir so
# ExecutorConfig's `FilePath` validation passes regardless of where run.sh launched from.
_PATH_VARS: set[str] = {
    "SSL_CA_CERTIFICATE_PATH",
    "BASE_PATH",
    "BASE_TEMP_PATH",
    "LONG_TERM_STORAGE_BASE_PATH",
    "GCS_EXTRACTED_DATA_CACHE",
    "GCS_TEMP_PROXY_FOR_GCP_OPERATIONS",
}


def _find_repo_root() -> pathlib.Path:
    """Walk up from this file until a dir containing both ``stitcher_operation_executor`` and
    ``stitcher_pipeline_common`` is found (the superrepo root). Falls back to the package's
    parents[7] (this module's depth from the root) if the marker walk fails."""
    here = pathlib.Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "stitcher_operation_executor").is_dir() and (parent / "stitcher_pipeline_common").is_dir():
            return parent
    return here.parents[7]


_REPO_ROOT = _find_repo_root()
_SOE_DIR = _REPO_ROOT / "stitcher_operation_executor"
_LOCAL_SOE_ENV = _REPO_ROOT / "stitcher_assistant" / "pi_coding_agent" / ".soe-env"


def _soe_env_dir() -> pathlib.Path:
    """Where to read SOE ``.env.local`` / ``.env.local.dev`` from (first existing wins)."""
    explicit = os.environ.get("STITCHER_SOE_ENV_DIR")
    if explicit:
        p = pathlib.Path(explicit).expanduser()
        if p.is_dir():
            return p
    if _LOCAL_SOE_ENV.is_dir():
        return _LOCAL_SOE_ENV
    return _SOE_DIR


def _load_soe_env_files() -> list[str]:
    """Load the whitelisted scalar SOE env vars into ``os.environ`` (idempotent — never overwrites
    an already-set var so an explicit caller override wins). Returns the list of keys loaded."""
    env_dir = _soe_env_dir()
    loaded: list[str] = []
    for env_name in (".env.local", ".env.local.dev"):
        env_path = env_dir / env_name
        if not env_path.exists():
            logger.debug("SOE env file not found: %s", env_path)
            continue
        try:
            text = env_path.read_text()
        except OSError as e:
            logger.warning("could not read %s: %s", env_path, e)
            continue
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            if k not in _SOE_ENV_VARS:
                continue
            v = v.strip().strip('"').strip("'")
            # absolutize relative path vars against the SOE dir (where the .env.local's relative
            # paths are anchored, e.g. '../local/certs/ca.crt') so FilePath validation passes from any CWD
            if k in _PATH_VARS and v and not os.path.isabs(v):
                v = str((_SOE_DIR / v).resolve())
            # skip obviously-multiline JSON blobs (a value starting with '{' across many lines)
            if v and k not in os.environ:
                os.environ[k] = v
                loaded.append(k)
    return loaded


_loaded_once = False


def ensure_soe_env_loaded() -> list[str]:
    """Load SOE env files once (before the first ExecutorConfig/WebserviceCommonSettings use).
    Safe to call repeatedly; only loads the first time."""
    global _loaded_once
    if _loaded_once:
        return []
    loaded = _load_soe_env_files()
    _loaded_once = True
    if loaded:
        logger.info("SOE env loaded from %s: %d var(s)", _soe_env_dir(), len(loaded))
    return loaded


@lru_cache(maxsize=1)
def get_executor_config():
    """Cached ``ExecutorConfig()``. MUST be called after ``ensure_soe_env_loaded()``."""
    from stitcher.operation_executor.config.executor_config import ExecutorConfig

    return ExecutorConfig()


# ── SOE env tuple (environment_id, pipeline_id, branch, auth_tenant) ──────────


def _read_env_context() -> dict[str, Any]:
    """Read the SOE env tuple from ``SAI_ENV_CONTEXT`` (JSON) if present, else {}."""
    raw = os.environ.get("SAI_ENV_CONTEXT", "")
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, TypeError):
        logger.warning("SAI_ENV_CONTEXT is not valid JSON; ignoring")
        return {}


class SoeContext:
    """Holds the SOE scope + lazily-built ``WorkflowContext`` for the config-gen tools.

    Constructed once in ``build_server()`` from the top-level ``StitcherSettings`` /
    ``OIDCAuth`` / ``StitcherClient`` (env-scoped, like the top-level coordinator) and passed
    to each tool module's ``register(mcp, client, soe)``.
    """

    def __init__(self, settings, auth, client) -> None:
        self.s = settings
        self.auth = auth
        self.client = client
        ctx = _read_env_context()
        self.environment_id: str = str(ctx.get("environment_id") or settings.environment_id or "")
        self.pipeline_id: Optional[str] = str(ctx.get("pipeline_id") or os.environ.get("STITCHER_PIPELINE_ID") or "")
        self.pipeline_name: str = str(ctx.get("pipeline_name") or settings.pipeline_name or "")
        self.branch: str = str(ctx.get("branch") or os.environ.get("STITCHER_GIT_BRANCH") or "main")
        self.auth_tenant: str = str(ctx.get("auth_tenant") or os.environ.get("STITCHER_AUTH_TENANT") or "")
        # Where authored configs are written by the save_config tool (a local, gitignored dir).
        self.output_dir: str = str(
            pathlib.Path(
                os.environ.get("STITCHER_OUTPUT_DIR")
                or (_REPO_ROOT / "stitcher_assistant" / "pi_coding_agent" / ".output")
            )
        )
        self._workflow_context = None
        self._pipeline_id_resolved = False
        self.pipeline_resolve_error: str = ""  # why pipeline_id could NOT be resolved (for diagnostics)

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

    def resolve_pipeline_id(self) -> Optional[str]:
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

    async def fetch_committed_configs(self, branch: str = "") -> tuple[Optional[dict], str]:
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
            return None, f"ERR fetching committed config from git: {str(e)[:250]}"
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
            f"soe_env_dir: {_soe_env_dir()}",
        ]
        if self.is_scoped and not self.has_pipeline:
            lines.append(
                "(environment_id set — data-source/metadata/scan available; pipeline_id needed for git-config fetch)"
            )
        if self.auth_tenant:
            lines.append("(auth_tenant set — SOE data/metadata/scan/git reads can authenticate at Keycloak)")
        return "\n".join(lines)


def build_soe_context(settings, auth, client) -> SoeContext:
    """Construct the singleton SoeContext, loading SOE env files first."""
    ensure_soe_env_loaded()
    return SoeContext(settings, auth, client)

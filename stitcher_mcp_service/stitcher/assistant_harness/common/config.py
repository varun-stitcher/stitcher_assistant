"""Stitcher harness runtime config — bound from env via pydantic-settings.

Single source of truth for every environment read across the assistant_harness.
Code uses ``StitcherAssistantConfig`` instead of reading ``os.environ`` directly.
Scope + auth + SOE tuple + infra live here:

* ``STITCHER_*`` — scope, auth, pipeline/git/tenant, output/cache dirs, sub-MCP
  registry (via ``env_prefix``).
* ``SAI_ENV_CONTEXT`` — optional JSON blob (mirrors
  ``pi_agent_coding_harness/server/env_context.py``) carrying an SOE env tuple
  (environment_id / pipeline_id / branch / auth_tenant).
* ``USE_STITCHER_MODEL`` — gate for routing tool-side LLM calls to the Stitcher
  gateway (SPC consumes the env var; we read the alias here).

Scope fields default empty so the class is safe to construct in any process
(env-agnostic ``custom_cost``, leaf modules). The **env-scoped** server entry
points (top-level ``mcp_server`` + ``config_generation``) call
``require_scope()`` to refuse to start without an environment.
"""

from __future__ import annotations

import json

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class StitcherAssistantConfig(BaseSettings):
    """Stitcher assistant config, bound from the environment at launch."""

    # ── Stitcher scope + auth (STITCHER_* prefix) ───────────────────────────
    api_url: str = ""  # STITCHER_API_URL — e.g. https://app.dev.stitcher.ai/v1
    environment_id: str = ""  # STITCHER_ENVIRONMENT_ID (environment UUID)
    pipeline_name: str = ""  # STITCHER_PIPELINE_NAME
    model_api_key: str = ""  # STITCHER_MODEL_API_KEY (gateway key; token fallback)
    api_token: str | None = None  # STITCHER_API_TOKEN (optional; static bearer — overrides for SWS auth)
    # OIDC (local-port auth-code + PKCE). Defaults match the stitcher-harness-login public client.
    auth_url: str = ""  # STITCHER_AUTH_URL (optional Keycloak base; default = api_url origin)
    oidc_realm: str = "stitcher"  # STITCHER_OIDC_REALM
    oidc_client_id: str = "stitcher-harness-login"  # STITCHER_OIDC_CLIENT_ID (public client, PKCE, no secret)
    oauth_callback_port: int = 8086  # STITCHER_OAUTH_CALLBACK_PORT
    ssl_ca_certificate_path: str = ""  # STITCHER_SSL_CA_CERTIFICATE_PATH (CA bundle for local/dev self-signed Keycloak)

    # ── SOE env tuple (STITCHER_* prefix; see SAI_ENV_CONTEXT override below) ─
    pipeline_id: str = ""  # STITCHER_PIPELINE_ID (optional; resolved from the pipeline name lazily)
    git_branch: str = ""  # STITCHER_GIT_BRANCH (default "main" where consumed)
    auth_tenant: str = ""  # STITCHER_AUTH_TENANT (Keycloak realm / org id for SOE auth)
    output_dir: str = ""  # STITCHER_OUTPUT_DIR (where save_config writes authored configs)
    step_cache_dir: str = ""  # STITCHER_STEP_CACHE_DIR (KW step-artifact cache, default ~/.stitcher/kw-cache)
    sub_mcp_urls: str = ""  # STITCHER_SUB_MCP_URLS (JSON: sub-MCP name -> MCP endpoint URL)

    # ── infra: non-STITCHER_ env names (must use an explicit alias) ──────────
    sai_env_context: str = Field(default="", validation_alias="SAI_ENV_CONTEXT")
    use_stitcher_model: bool = Field(default=True, validation_alias="USE_STITCHER_MODEL")

    model_config = SettingsConfigDict(
        env_prefix="STITCHER_",
        env_file=(".env.local", ".env.local.dev"),
        extra="ignore",
    )

    # ── derived helpers ─────────────────────────────────────────────────────

    @property
    def token(self) -> str:
        """Static bearer: STITCHER_API_TOKEN if set, else STITCHER_MODEL_API_KEY."""
        return self.api_token or self.model_api_key

    @property
    def sub_mcp_registry(self) -> dict[str, str]:
        """Parsed ``STITCHER_SUB_MCP_URLS`` (name -> MCP endpoint URL), or {}."""
        try:
            data = json.loads(self.sub_mcp_urls)
            return data if isinstance(data, dict) else {}
        except (json.JSONDecodeError, TypeError):
            return {}

    @property
    def env_context(self) -> dict:
        """Parsed ``SAI_ENV_CONTEXT`` JSON blob (SOE env tuple), or {}."""
        raw = self.sai_env_context
        if not raw:
            return {}
        try:
            data = json.loads(raw)
            return data if isinstance(data, dict) else {}
        except (json.JSONDecodeError, TypeError):
            return {}

    def require_scope(self) -> None:
        """Raise a clear error if the environment scope needed to operate on a
        Stitcher environment is missing. Called by the env-scoped server entry
        points so the process refuses to start misconfigured (no silent default)."""
        missing = [
            name
            for name, value in (
                ("STITCHER_API_URL", self.api_url),
                ("STITCHER_ENVIRONMENT_ID", self.environment_id),
                ("STITCHER_PIPELINE_NAME", self.pipeline_name),
                ("STITCHER_MODEL_API_KEY", self.model_api_key),
            )
            if not value
        ]
        if missing:
            raise RuntimeError(
                "refusing to start without Stitcher scope — set "
                + ", ".join(missing)
                + " (env or .env.local) before launching"
            )

    def export_llm_env(self) -> None:
        """Route tool-side LLM calls to the Stitcher gateway by writing
        ``USE_STITCHER_MODEL=true`` into the environment for **external**
        consumers (SPC's LLM config reads env, not this config). Only a write to
        os.environ for that boundary — harness code reads this config. An explicit
        ``USE_STITCHER_MODEL=false`` (or no gateway key) is honored and left unset.
        """
        if self.use_stitcher_model and self.model_api_key:
            import os

            os.environ["USE_STITCHER_MODEL"] = "true"

"""Stitcher runtime scope, bound from ``STITCHER_*`` env vars via pydantic."""
from __future__ import annotations

from pydantic_settings import BaseSettings


class StitcherSettings(BaseSettings):
    """Stitcher scope + auth, bound FROM STITCHER_* env vars at launch.

    All scope vars are REQUIRED (no defaults) — the server refuses to start
    without knowing its API URL, environment, and pipeline. Only the auth token
    may fall back (STITCHER_API_TOKEN -> STITCHER_MODEL_API_KEY).
    """

    api_url: str  # STITCHER_API_URL — e.g. https://app.dev.stitcher.ai/v1
    environment_id: str  # STITCHER_ENVIRONMENT_ID
    pipeline_name: str  # STITCHER_PIPELINE_NAME
    model_api_key: str  # STITCHER_MODEL_API_KEY (gateway key; token fallback)
    api_token: str | None = None  # STITCHER_API_TOKEN (optional; static bearer — overrides for SWS auth)
    # OIDC (local-port auth-code + PKCE). Defaults match the stitcher-harness-login public client.
    auth_url: str = ""  # STITCHER_AUTH_URL (optional Keycloak base; default = api_url origin)
    oidc_realm: str = "stitcher"  # STITCHER_OIDC_REALM
    oidc_client_id: str = "stitcher-harness-login"  # STITCHER_OIDC_CLIENT_ID (public client, PKCE, no secret)
    oauth_callback_port: int = 8086  # STITCHER_OAUTH_CALLBACK_PORT
    ssl_ca_certificate_path: str = ""  # STITCHER_SSL_CA_CERTIFICATE_PATH (CA bundle for local/dev self-signed Keycloak)

    model_config = {
        "env_prefix": "STITCHER_",
        "env_file": (".env.local", ".env.local.dev"),
        "extra": "ignore",
    }

    @property
    def token(self) -> str:
        """Static bearer: STITCHER_API_TOKEN if set, else STITCHER_MODEL_API_KEY."""
        return self.api_token or self.model_api_key

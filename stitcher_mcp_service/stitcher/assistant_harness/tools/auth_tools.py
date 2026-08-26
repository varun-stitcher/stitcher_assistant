"""Auth tools — expose the local-port OIDC flow as MCP tools."""

from __future__ import annotations

import base64
import datetime
import json

from fastmcp import FastMCP

from ..common.oidc_auth import OIDCAuth


def _decode_json_payload(token: str) -> dict:
    """Decode the JWT payload into a dict (best-effort)."""
    payload = token.split(".")[1]
    payload += "=" * (-len(payload) % 4)
    return json.loads(base64.urlsafe_b64decode(payload))


def register(mcp: FastMCP, auth: OIDCAuth) -> None:
    @mcp.tool
    def auth_get_url() -> str:
        """Start a local :8086 OIDC login, open the browser, and return the Keycloak URL.

        After you sign in, Keycloak redirects back to this agent's local callback
        which exchanges the code for an access token and stores it for all
        subsequent Stitcher tool calls.
        """
        try:
            self = auth.get_login_url()
            return self
        except OSError as e:  # noqa: BLE001
            return f"ERR: cannot bind callback port {auth.s.oauth_callback_port} ({e}) — is another auth server running? Free the port and retry."
        except Exception as e:  # noqa: BLE001
            return f"ERR: OIDC discovery failed ({e})"

    @mcp.tool
    def auth_environments() -> str:
        """List the environments the CURRENT access token can access (from its `available-environments` claim)."""
        token = auth.obtain_token()
        if not token:
            return "No token. Run auth_get_url and sign in first."
        try:
            claims = _decode_json_payload(token)
        except Exception as e:  # noqa: BLE001
            return f"ERR: could not decode token ({e})"
        envs = claims.get("available-environments") or claims.get("available_environments") or []
        if isinstance(envs, str):
            envs = [envs]
        head = "Live OIDC token" if auth.has_live_token else "Static STITCHER token"
        if not envs:
            return (
                f"{head}: NO available-environments claim. The account hasn't been granted any environment"
                f" (or the keycloak client doesn't map the claim). Grant access / add the user to the env group, then re-auth."
            )
        lines = [f"{head}: {len(envs)} environment(s) accessible"]
        for env in sorted(envs):
            lines.append(f"  - {env}  {'<-- STITCHER_ENVIRONMENT_ID' if env == auth.s.environment_id else ''}")
        return "\n".join(lines)

    @mcp.tool
    def auth_status() -> str:
        """Report whether a Stitcher access token is available for API calls (and roughly when it expires)."""
        token = auth.obtain_token()
        if not token:
            return "No token. Run auth_get_url and sign in (or auth_set_token <token>)."
        exp = None
        try:
            exp = _decode_json_payload(token).get("exp")
        except Exception:  # noqa: BLE001
            pass
        head = "Live OIDC token" if auth.has_live_token else "Static STITCHER token"
        if exp:
            remaining = datetime.datetime.fromtimestamp(exp, datetime.UTC) - datetime.datetime.now(datetime.UTC)
            return f"{head} — expires in ~{max(0, int(remaining.total_seconds() // 60))} min"
        return f"{head} — expiry unknown"

    @mcp.tool
    def auth_set_token(token: str) -> str:
        """Accept a Stitcher/Keycloak access token directly (paste it) and use it for all subsequent API calls."""
        if not token or not token.strip():
            return "ERR: token is empty"
        auth.set_token(token)
        return auth_status()

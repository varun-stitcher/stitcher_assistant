"""OIDCAuth — process-scoped login state for the local-port Keycloak flow.

Owns everything about authenticating against the Stitcher broker realm:

* the access/refresh tokens (persisted across restarts),
* the in-flight authorization-code + PKCE login (state/verifier, persisted to a
  shared file so a callback landing in a different process still resolves),
* the local ``:8086`` callback HTTP server that receives Keycloak's redirect,
* transparent refresh of an expiring access token.

State is intentionally a long-lived object shared by every MCP tool-call handler
and the background callback thread (a login started in one tool call is consumed
by an unrelated HTTP request later) — NOT a ``contextmanager`` scope.
"""

from __future__ import annotations

import base64
import datetime
import hashlib
import json
import pathlib
import secrets
import threading
import urllib.parse
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx

from .config import StitcherSettings


class _CallbackServer(ThreadingHTTPServer):
    """HTTP server that carries a reference back to the owning OIDCAuth."""

    auth: OIDCAuth


class _OIDCCallback(BaseHTTPRequestHandler):
    """Handles GET /callback?code=...&state=... from the Keycloak redirect."""

    @property
    def auth(self) -> OIDCAuth:
        return self.server.auth  # type: ignore[attr-defined]

    def do_GET(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        if parsed.path != "/callback":
            self._reply(404, "not found")
            return
        code_list = params.get("code")
        state_list = params.get("state")
        code: str | None = code_list[0] if code_list else None
        state: str | None = state_list[0] if state_list else None
        # Resolve the code_verifier for this authorization code. PKCE is the real
        # secret (the code is useless without it); `state` is belt-and-suspenders.
        # 1) exact state in memory -> 2) exact state in the shared pending file ->
        # 3) the single outstanding login. Step 3 makes the login immune to a
        # fragment landing in a fresh/different process while staying PKCE-bound.
        verifier = self.auth._pending_states.get(state) if state else None
        if verifier is None and state:
            self.auth._load_persisted_pending()
            verifier = self.auth._pending_states.get(state)
        if verifier is None and self.auth._pending_states:
            verifier = next(iter(self.auth._pending_states.values()))
        if not code or not verifier:
            self._reply(
                400,
                "no in-progress login matches this callback — run auth_get_url and complete the NEWEST login (or free port 8086 if a stale one is serving)",
            )
            return
        try:
            tokens = httpx.post(
                self.auth._discovery()["token_endpoint"],
                data={
                    "grant_type": "authorization_code",
                    "code": code,
                    "redirect_uri": self.auth._redirect_uri(),
                    "client_id": self.auth.s.oidc_client_id,
                    "code_verifier": verifier,
                },
                timeout=30,
                verify=self.auth._http_verify(),
            )
            tokens.raise_for_status()
            data = tokens.json()
            self.auth._access_token = data["access_token"]
            self.auth._refresh_token = data.get("refresh_token")
            self.auth._clear_pending(state)
            self.auth._persist_token(data["access_token"], data.get("refresh_token"))
            self._reply(200, "Authenticated! You can close this tab and return to the agent.")
        except Exception as e:  # noqa: BLE001
            self._reply(500, f"token exchange failed: {e}")

    def _reply(self, status: int, body: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(body.encode())

    def log_message(self, *a) -> None:  # silence request logging
        pass


class OIDCAuth:
    """Holds all auth state + behavior in one place (no module globals)."""

    def __init__(self, settings: StitcherSettings, state_dir: pathlib.Path) -> None:
        self.s = settings
        self._token_file = state_dir / "stitcher_token.json"
        self._pending_file = state_dir / "stitcher_auth_pending.json"
        self._server: _CallbackServer | None = None

        self._access_token: str | None = None
        self._refresh_token: str | None = None
        self._state: str | None = None
        self._verifier: str | None = None
        self._pending_states: dict[str, str] = {}

        self._load_persisted()

    # ── config-derived helpers ──────────────────────────────────────────────

    @property
    def _origin(self) -> str:
        """Keycloak base for OIDC discovery. STITCHER_AUTH_URL, else api_url minus any /v1."""
        base = (self.s.auth_url or self.s.api_url).rstrip("/")
        return base[:-3] if base.endswith("/v1") else base

    def _redirect_uri(self) -> str:
        return f"http://127.0.0.1:{self.s.oauth_callback_port}/callback"

    def _http_verify(self):
        """Local/dev Keycloak uses a self-signed CA; use the bundle if present else skip verify."""
        path = self.s.ssl_ca_certificate_path or "../local/certs/ca.crt"
        p = pathlib.Path(path)
        return str(p) if p.exists() else False

    def _discovery(self) -> dict:
        url = f"{self._origin}/realms/{self.s.oidc_realm}/.well-known/openid-configuration"
        return httpx.get(url, timeout=30, verify=self._http_verify()).json()

    # ── token lifecycle ─────────────────────────────────────────────────────

    def obtain_token(self) -> str:
        """Live (auto-refreshed) OIDC access token, else the static STITCHER token."""
        tok = self._access_token
        if not tok:
            return self.s.token
        if self._is_expired(tok):
            self._refresh_access_token()
            tok = self._access_token
        return tok or self.s.token

    def set_token(self, token: str) -> None:
        """Accept a token directly and make it the live one for the session."""
        self._access_token = token.strip()
        self._persist_token(self._access_token)

    @property
    def has_live_token(self) -> bool:
        return bool(self._access_token)

    def _jwt_exp(self, token: str) -> int | None:
        try:
            payload = token.split(".")[1]
            payload += "=" * (-len(payload) % 4)
            return json.loads(base64.urlsafe_b64decode(payload)).get("exp")
        except Exception:  # noqa: BLE001
            return None

    def _is_expired(self, token: str) -> bool:
        exp = self._jwt_exp(token)
        return exp is not None and (exp - 60) <= datetime.datetime.now(datetime.UTC).timestamp()

    def _refresh_access_token(self) -> None:
        if not self._refresh_token:
            return
        try:
            r = httpx.post(
                self._discovery()["token_endpoint"],
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": self._refresh_token,
                    "client_id": self.s.oidc_client_id,
                },
                timeout=30,
                verify=self._http_verify(),
            )
            r.raise_for_status()
            d = r.json()
            self._access_token = d["access_token"]
            self._refresh_token = d.get("refresh_token") or self._refresh_token
            self._persist_token(self._access_token, self._refresh_token)
        except Exception:  # noqa: BLE001
            pass  # keep whatever we already have

    # ── persistence ─────────────────────────────────────────────────────────

    def _persist_pending(self, state: str, verifier: str) -> None:
        try:
            self._pending_file.write_text(json.dumps({"state": state, "verifier": verifier}))
        except Exception:  # noqa: BLE001
            pass

    def _persist_token(self, token: str, refresh: str | None = None) -> None:
        try:
            self._token_file.write_text(json.dumps({"access_token": token, "refresh_token": refresh}))
        except Exception:  # noqa: BLE001
            pass

    def _load_persisted(self) -> None:
        try:
            d = json.loads(self._token_file.read_text())
            self._access_token = d.get("access_token")
            self._refresh_token = d.get("refresh_token")
        except Exception:  # noqa: BLE001
            pass
        self._load_persisted_pending()

    def _load_persisted_pending(self) -> None:
        try:
            d = json.loads(self._pending_file.read_text())
            if d.get("state") and d.get("verifier"):
                self._state, self._verifier = d["state"], d["verifier"]
                self._pending_states[d["state"]] = d["verifier"]
        except Exception:  # noqa: BLE001
            pass

    def _clear_pending(self, state: str | None = None) -> None:
        if state is not None:
            self._pending_states.pop(state, None)
        self._state = self._verifier = None
        try:
            self._pending_file.unlink(missing_ok=True)
        except Exception:  # noqa: BLE001
            pass

    # ── callback server ─────────────────────────────────────────────────────

    def _start_callback_server(self) -> _CallbackServer:
        """Start (once) the local callback server that Keycloak redirects to."""
        if self._server is not None:
            return self._server
        server = _CallbackServer(("127.0.0.1", self.s.oauth_callback_port), _OIDCCallback)
        server.auth = self
        self._server = server
        threading.Thread(target=server.serve_forever, daemon=True).start()
        return self._server

    def close(self) -> None:
        """Shut down the callback server (best-effort; daemon thread exits with the process)."""
        if self._server is not None:
            try:
                self._server.shutdown()
            except Exception:  # noqa: BLE001
                pass

    # ── the login entry point (used by the auth_get_url tool) ───────────────

    def get_login_url(self) -> str:
        """Build + persist the PKCE authorize URL and start the callback server. Returns the URL."""
        if self._state and self._verifier:
            state, verifier = self._state, self._verifier  # reuse in-flight login
        else:
            state = secrets.token_urlsafe(16)
            verifier = secrets.token_urlsafe(32)
            self._state, self._verifier = state, verifier
        self._pending_states[state] = verifier
        self._persist_pending(state, verifier)
        self._start_callback_server()
        challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
        params = {
            "response_type": "code",
            "client_id": self.s.oidc_client_id,
            "redirect_uri": self._redirect_uri(),
            "scope": "openid profile email offline_access",
            "state": state,
            "code_challenge": challenge,
            "code_challenge_method": "S256",
        }
        url = self._discovery().get("authorization_endpoint", "") + "?" + urllib.parse.urlencode(params)
        try:
            webbrowser.open(url)
            opened = True
        except Exception:  # noqa: BLE001
            opened = False
        lines = [
            "Opening your browser to sign in…",
            f"  {url}",
            "",
            f"Agent callback listening on {self._redirect_uri()} — after you log in, Keycloak ",
            "redirects here and the agent captures the token automatically.",
        ]
        if not opened:
            lines.insert(1, "(could not auto-open the browser — open the URL above manually)")
        try:
            disc = self._discovery()
            if disc.get("device_authorization_endpoint"):
                lines += [
                    "",
                    f"(Device flow also available: POST {disc['device_authorization_endpoint']} "
                    f"with client_id={self.s.oidc_client_id} — use auth_set_token to accept the token.)",
                ]
        except Exception:  # noqa: BLE001
            pass
        return "\n".join(lines)

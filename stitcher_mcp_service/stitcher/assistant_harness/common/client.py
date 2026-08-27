"""StitcherClient — a thin wrapper over the existing stitcher_web_service_client.

Holds no query logic of its own; it just builds an authenticated ApiClient from
the runtime scope (settings) + current token (OIDCAuth) and forwards to the
generated client's methods. All determinism stays in the generated client.
"""

from __future__ import annotations

from stitcher.webservice.client import ApiClient, Configuration
from stitcher.webservice.client.api import (
    ConnectionDatasourcesDestinationsApi,
    PipelineApi,
)
from stitcher.webservice.client.models import DataConnType

from .config import StitcherAssistantConfig
from .oidc_auth import OIDCAuth


class StitcherClient:
    def __init__(self, settings: StitcherAssistantConfig, auth: OIDCAuth) -> None:
        self.s = settings
        self.auth = auth
        self._connections_api = ConnectionDatasourcesDestinationsApi
        self._pipeline_api = PipelineApi

    # ── plumbing ────────────────────────────────────────────────────────────

    def _configuration(self) -> Configuration:
        conf = Configuration(host=self.s.api_url or "/v1", access_token=self.auth.obtain_token() or None)
        verify = self.auth._http_verify()
        if verify is False:
            # Local/dev SWS uses a self-signed cert with no bundled CA — skip verify.
            conf.verify_ssl = False
            conf.assert_hostname = False
        else:
            conf.ssl_ca_cert = str(verify)
        return conf

    def _connections(self) -> ConnectionDatasourcesDestinationsApi:
        return self._connections_api(ApiClient(self._configuration()))

    def _env(self, environment_id: str | None) -> tuple[str, str]:
        """Resolve the environment to use, or a usable error marker."""
        env = environment_id or self.s.environment_id
        return env, ("" if env else "ERR: set STITCHER_ENVIRONMENT_ID at launch (or pass environment_id)")

    # ── context / queries ───────────────────────────────────────────────────

    def context(self) -> str:
        return (
            f"api_url={self.s.api_url or '(unset)'}\n"
            f"environment_id={self.s.environment_id or '(unset)'}\n"
            f"pipeline_name={self.s.pipeline_name or '(unset)'}"
        )

    def list_connections(self, scope: str = "datasources", environment_id: str | None = None) -> str:
        env, err = self._env(environment_id)
        if err:
            return err
        if scope not in ("datasources", "destinations"):
            return f"ERR: scope must be 'datasources' | 'destinations' (got {scope!r})"
        try:
            with ApiClient(self._configuration()) as client:
                resp = self._connections_api(client).list_data_connections(environment=env, type=DataConnType(scope))
        except Exception as e:  # noqa: BLE001
            return f"ERR: {e}"
        items = resp.objects or []
        if not items:
            return f"No {scope} connections for environment {env}."
        lines = [f"== {scope} ({len(items)}) =="]
        for ds in items:
            lines.append(
                f"  - {ds.name}  provider={ds.provider_name or '?'}  "
                f"connector={ds.connector_template_display_name}  "
                f"status={self._status(ds.status)}  id={ds.id}"
            )
        return "\n".join(lines)

    def get_connection(self, scope: str, name_or_id: str, environment_id: str | None = None) -> str:
        env, err = self._env(environment_id)
        if err:
            return err
        if scope not in ("datasources", "destinations"):
            return f"ERR: scope must be 'datasources' | 'destinations' (got {scope!r})"
        try:
            with ApiClient(self._configuration()) as client:
                ds = self._connections_api(client).get_data_connection(
                    environment=env, type=DataConnType(scope), name_or_id=name_or_id
                )
        except Exception as e:  # noqa: BLE001
            return f"ERR: {e}"
        return (
            f"name={ds.name}\n"
            f"id={ds.id}\n"
            f"provider={ds.provider_name or '?'}\n"
            f"dataset={ds.dataset_name or ds.dataset_display_name or '?'}\n"
            f"connector={ds.connector_template_display_name} ({ds.connector_template_name})\n"
            f"status={self._status(ds.status)}"
        )

    def get_pipeline(self, pipeline_name: str | None = None, environment_id: str | None = None) -> str:
        name = pipeline_name or self.s.pipeline_name
        if not name:
            return "ERR: pass pipeline_name or set STITCHER_PIPELINE_NAME at launch"
        env, err = self._env(environment_id)
        if err:
            return err
        try:
            with ApiClient(self._configuration()) as client:
                p = self._pipeline_api(client).get_pipeline(environment=env, name_or_id=name)
        except Exception as e:  # noqa: BLE001
            return f"ERR: {e}"
        return (
            f"name={p.name}\n"
            f"id={p.id}\n"
            f"organization={p.organization_name}\n"
            f"repository={p.repository or '?'}\n"
            f"status={self._status(p.status)}"
        )

    @staticmethod
    def _status(status) -> str:
        return status.value if hasattr(status, "value") else str(status)

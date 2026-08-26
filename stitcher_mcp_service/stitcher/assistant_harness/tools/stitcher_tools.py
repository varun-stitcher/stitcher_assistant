"""Stitcher API tools — thin MCP wrappers over StitcherClient (which forwards to
the existing stitcher_web_service_client). Register on the mcp server."""

from __future__ import annotations

from fastmcp import FastMCP

from ..common.client import StitcherClient


def register(mcp: FastMCP, client: StitcherClient) -> None:
    @mcp.tool
    def stitcher_context() -> str:
        """Report the Stitcher scope this agent is bound to (API URL, environment id, pipeline name). No secrets."""
        return client.context()

    @mcp.tool
    def list_connections(
        scope: str = "datasources",
        environment_id: str | None = None,
    ) -> str:
        """List Stitcher data connections via the stitcher_web_service_client.

        scope: 'datasources' (cost/business data sources, default) | 'destinations' (write targets).
        Environment from the arg or STITCHER_ENVIRONMENT_ID. Returns each connection's
        name, provider, connector, status, and id.
        """
        return client.list_connections(scope=scope, environment_id=environment_id)

    @mcp.tool
    def get_connection(
        scope: str,
        name_or_id: str,
        environment_id: str | None = None,
    ) -> str:
        """Fetch one Stitcher data connection by name or id via the client.

        scope: 'datasources' | 'destinations'.
        """
        return client.get_connection(scope=scope, name_or_id=name_or_id, environment_id=environment_id)

    @mcp.tool
    def get_pipeline(
        pipeline_name: str | None = None,
        environment_id: str | None = None,
    ) -> str:
        """Get a Stitcher pipeline via the client (name from arg, else STITCHER_PIPELINE_NAME).

        Returns name, id, organization, repository, and status.
        """
        return client.get_pipeline(pipeline_name=pipeline_name, environment_id=environment_id)

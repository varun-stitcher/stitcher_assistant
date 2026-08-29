"""Adversarial tests for the committed-config error diagnostics (soe_context).

The contract: a credential/infra failure at ANY layer of the
Keycloak → SWS-pipeline → Vault → GitHub-App-key chain must STOP the fetch
(no fabricated success) and give the OPERATOR an actionable message that
names the layer and the fix — never a raw traceback, never an ambiguous
"404" that invites the 'SWS is down' misdiagnosis (the exact mistake a live
agent turn made on 2026-08-29 when a stale STITCHER_PIPELINE_ID 404'd).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from stitcher.assistant_harness.common.soe_context import SoeContext

ENV = "11111111-1111-1111-1111-111111111111"
PIPE = "22222222-2222-2222-2222-222222222222"


def _soe() -> SoeContext:
    """A SoeContext with a mocked client + auth (no network)."""
    s = MagicMock()
    s.environment_id = ENV
    s.pipeline_name = "finops"
    s.pipeline_id = PIPE
    s.auth_tenant = "test-tenant"
    soe = SoeContext.__new__(SoeContext)
    soe.s = s
    soe.environment_id = ENV
    soe.pipeline_name = "finops"
    soe.pipeline_id = PIPE
    soe.pipeline_resolve_error = ""
    soe._pipeline_id_resolved = True
    soe.auth_tenant = "test-tenant"
    soe.branch = "main"
    soe.client = MagicMock()
    return soe


class TestDiagnoseVcsError:
    def test_missing_pem_file_names_the_fix(self):
        soe = _soe()
        e = FileNotFoundError(2, "No such file or directory", "../local/github/gh_app_key.pem")
        msg = soe._diagnose_vcs_error(e, PIPE)
        assert "GitHub App private key not found" in msg
        assert "GITHUB_PRIVATE_KEY_PATH" in msg and "gh_app_key.pem" in msg
        assert "Traceback" not in msg

    def test_pipeline_404_names_what_actually_exists(self, monkeypatch):
        soe = _soe()
        monkeypatch.setattr(
            SoeContext,
            "_pipelines_in_env_hint",
            lambda self: "Pipelines that exist in this environment:\n  - finops: 5d7b2f8b-0782-4e43-8a0c-52acc9b5e3a5",
        )
        e = type("NotFoundException", (Exception,), {})("(404) Reason: Not Found")
        msg = soe._diagnose_vcs_error(e, PIPE)
        assert "404" in msg and PIPE in msg
        # the hint must name the pipeline that DOES exist and tell the operator to switch
        assert "5d7b2f8b-0782-4e43-8a0c-52acc9b5e3a5" in msg and "finops" in msg
        assert "STITCHER_PIPELINE_ID" in msg
        # and it must say SWS is reachable — kill the 'SWS is down' misdiagnosis
        assert "IS reachable" in msg
        assert "Traceback" not in msg

    def test_pipeline_404_without_hint_still_actionable(self, monkeypatch):
        soe = _soe()
        monkeypatch.setattr(SoeContext, "_pipelines_in_env_hint", lambda self: "")  # lookup failed / empty
        e = type("NotFoundException", (Exception,), {})("(404)")
        msg = soe._diagnose_vcs_error(e, PIPE)
        assert "404" in msg and "STITCHER_PIPELINE_ID" in msg

    def test_auth_401_names_tenant_and_vars(self):
        soe = _soe()
        e = type("UnauthorizedException", (Exception,), {})("(401) unauthorized")
        msg = soe._diagnose_vcs_error(e, PIPE)
        assert "authentication failed" in msg
        assert "STITCHER_AUTH_TENANT" in msg and "test-tenant" in msg
        assert "Traceback" not in msg

    def test_installation_or_vault_failure_is_actionable(self):
        soe = _soe()
        e = RuntimeError("Vault: installation id lookup failed for pipeline")
        msg = soe._diagnose_vcs_error(e, PIPE)
        assert "installation" in msg and PIPE in msg
        assert "GitHub App is installed" in msg

    def test_unreachable_service_is_distinct_from_not_found(self):
        soe = _soe()
        e = ConnectionError("[Errno 61] Connection refused")
        msg = soe._diagnose_vcs_error(e, PIPE)
        assert "unreachable" in msg
        assert "docker/start_services.sh" in msg

    def test_unknown_error_falls_back_to_raw_message_not_a_crash(self):
        soe = _soe()
        e = RuntimeError("something completely unexpected happened")
        msg = soe._diagnose_vcs_error(e, PIPE)
        assert msg.startswith("ERR fetching committed config from git:")
        assert "something completely unexpected" in msg


class TestFetchCommittedConfigsStops:
    @pytest.mark.asyncio
    async def test_fetch_returns_clear_error_never_fabricates(self, monkeypatch):
        soe = _soe()
        soe.get_workflow_context = lambda: MagicMock()  # network-gated; mocked
        monkeypatch.setattr(SoeContext, "resolve_pipeline_id", lambda self: PIPE)

        async def boom(**kwargs):
            raise FileNotFoundError(2, "No such file or directory", "../local/github/gh_app_key.pem")

        import stitcher.operation_executor.common.vcs_repo as vcs

        monkeypatch.setattr(vcs, "get_vsc_commit_dir", boom)
        cfg, err = await soe.fetch_committed_configs("")
        assert cfg is None, "a failed fetch must NOT return a fabricated config"
        assert "GitHub App private key not found" in err

    @pytest.mark.asyncio
    async def test_hint_failure_does_not_mask_the_diagnosis(self, monkeypatch):
        soe = _soe()
        soe.get_workflow_context = lambda: MagicMock()
        soe.client.list_pipelines.side_effect = RuntimeError("hint lookup broke")

        async def boom(*a, **k):
            raise type("NotFoundException", (Exception,), {})("(404)")

        import stitcher.operation_executor.common.vcs_repo as vcs

        monkeypatch.setattr(vcs, "get_vsc_commit_dir", boom)
        cfg, err = await soe.fetch_committed_configs("")
        assert cfg is None
        assert "404" in err and "STITCHER_PIPELINE_ID" in err  # hint failed silently, message survives

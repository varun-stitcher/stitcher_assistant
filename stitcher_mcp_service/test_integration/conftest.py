"""Fixtures + gating for the stitcher full-pipeline integration suite.

These tests drive the REAL pipeline end-to-end: a natural-language prompt is run
through a headless pi agent turn (AgentRunner), which spawns the real combined
MCP server and calls the real tools. Every turn is a live LLM call — so the
suite is:

  * OPT-IN: skipped unless STITCHER_INTEGRATION=1 AND the STITCHER_* scope env
    is present (missing vars are named, never guessed — no silent fallback);
  * SLOW: each test may run several minutes; run them explicitly, not in `make check`;
  * SIGNAL-LEVEL: see signals.py — deterministic evidence (transcript, artifacts)
    is asserted exactly; LLM prose only via any-match regex signals.

Run:
    set -a; source <(grep '^export ' ../pi_coding_agent/run.local.sh); set +a
    STITCHER_INTEGRATION=1 .venv/bin/python -m pytest test_integration/ -v
"""

from __future__ import annotations

import os
import pathlib
import shutil

import pytest

from stitcher.assistant_harness.agent_gateway.agent_runner import AgentRunner

HERE = pathlib.Path(__file__).parent
FIXTURES = HERE / "fixtures"

REQUIRED_SCOPE_VARS = (
    "STITCHER_MODEL_BASE_URL",
    "STITCHER_MODEL_API_KEY",
    "STITCHER_MODEL_NAME",
    "STITCHER_API_URL",
    "STITCHER_ENVIRONMENT_ID",
    "STITCHER_PIPELINE_NAME",
    "STITCHER_AUTH_TENANT",
)


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "integration: live full-pipeline turn (real LLM + MCP; opt-in)")


def _missing_scope_vars() -> list[str]:
    return [v for v in REQUIRED_SCOPE_VARS if not os.environ.get(v)]


def _gate(reason_unmet: str) -> bool:
    """Global opt-in gate for the live tests. Skips honestly with the reason."""
    if os.environ.get("STITCHER_INTEGRATION") != "1":
        pytest.skip("STITCHER_INTEGRATION != 1 — live LLM tests are opt-in (see test_integration/README.md)")
    missing = _missing_scope_vars()
    if missing:
        pytest.skip(f"STITCHER_INTEGRATION=1 but missing scope env: {', '.join(missing)}")
    if shutil.which("pi") is None:
        pytest.skip(reason_unmet)
    return True


@pytest.fixture(scope="session")
def runner() -> AgentRunner:
    _gate("`pi` CLI not on PATH")
    return AgentRunner()


@pytest.fixture(scope="session")
def agent_model() -> str:
    """Model override for integration turns (STITCHER_INTEGRATION_MODEL, verbatim if it
    contains '/'). Empty → the AgentRunner/STITCHER_MODEL_NAME default. The agent model is
    interchangeable here — the pipeline's LLM calls go through the Stitcher gateway either way."""
    return os.environ.get("STITCHER_INTEGRATION_MODEL", "")


@pytest.fixture(scope="session")
def scope() -> dict[str, str]:
    _gate("unreachable")  # same gate; keeps skip reason consistent for both fixtures
    return {
        "environment_id": os.environ["STITCHER_ENVIRONMENT_ID"],
        "pipeline_name": os.environ["STITCHER_PIPELINE_NAME"],
        "auth_tenant": os.environ.get("STITCHER_AUTH_TENANT", ""),
    }


@pytest.fixture()
def artifact_dir(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> pathlib.Path:
    """Redirect ALL artifact writers to a per-test temp dir, so tests assert on
    artifacts THIS test produced (no cross-run pollution of user dirs)."""
    d = tmp_path / "artifacts"
    d.mkdir()
    monkeypatch.setenv("FOCUS_PARQUET_OUTPUT_DIR", str(d / "parquet"))
    monkeypatch.setenv("FOCUS_CONFIG_OUTPUT_DIR", str(d / "configs"))
    return d


@pytest.fixture()
def focus_csv(tmp_path: pathlib.Path) -> pathlib.Path:
    """A small FOCUS v1.2-shaped CSV (committed fixture — known-good header)."""
    src = FIXTURES / "focus_sample.csv"
    dst = tmp_path / "focus_sample.csv"
    shutil.copy(src, dst)
    return dst


@pytest.fixture()
def focus_csv_missing_currency(focus_csv: pathlib.Path) -> pathlib.Path:
    """The same data with BillingCurrency REMOVED — guarantees a non-compliant
    result so the repair/next-steps path is exercised (adversarial fixture)."""
    import polars as pl

    dst = focus_csv.with_name("focus_missing_currency.csv")
    df = pl.read_csv(focus_csv)
    df.drop("BillingCurrency").write_csv(dst)
    return dst

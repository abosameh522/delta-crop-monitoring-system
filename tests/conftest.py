"""pytest configuration and fixtures."""

import os

os.environ.setdefault("PREFECT_LOGGING_TO_API_WHEN_MISSING_FLOW", "ignore")

import pytest  # noqa: F401


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: long-running regression checks")


@pytest.fixture(autouse=True)
def _prefect_no_api():
    """Suppress Prefect API logging warning in tests that import Prefect."""
    os.environ["PREFECT_LOGGING_TO_API_WHEN_MISSING_FLOW"] = "ignore"
    yield
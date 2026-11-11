"""Phase 6b: deployment registration, healthcheck and packaging invariants.

No Docker daemon is needed: the deployment options are pure, the healthcheck
takes an injected HTTP opener, and the Dockerfile/compose checks read the files
as text.
"""

from __future__ import annotations

import urllib.error
from datetime import timedelta
from pathlib import Path

from delta_crops.orchestration import healthcheck, serve as serve_module
from delta_crops.orchestration.flow import scheduled_flow

ROOT = Path(__file__).resolve().parents[1]


def _dockerfile() -> str:
    return (ROOT / "Dockerfile").read_text(encoding="utf-8")


def _dockerignore() -> list[str]:
    return [
        line.strip() for line in (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


def test_deployment_options_use_five_day_interval() -> None:
    options = serve_module.deployment_options("/srv/data", "config/aoi.yaml")
    assert options["interval"] == timedelta(days=5)
    assert options["name"] == "delta-crops-scheduled"


def test_deployment_options_allow_only_one_concurrent_run() -> None:
    options = serve_module.deployment_options("/srv/data", None)
    assert options["global_limit"] == 1
    # Paused while the API is down, resumed on the next start.
    assert options["pause_on_shutdown"] is True
    assert options["parameters"]["config_path"] is None


def test_deployment_parameters_default_to_the_data_dir_env(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("DELTA_CROPS_DATA_DIR", str(tmp_path / "lake"))
    options = serve_module.deployment_options()
    assert options["parameters"]["data_dir"] == str(tmp_path / "lake")


def test_serve_passes_the_options_to_flow_serve(monkeypatch, tmp_path) -> None:
    captured: dict = {}

    def fake_serve(**kwargs) -> None:
        captured.update(kwargs)

    monkeypatch.setattr(scheduled_flow, "serve", fake_serve)
    serve_module.serve(tmp_path, "config/aoi.yaml")

    assert captured["global_limit"] == 1
    assert captured["interval"] == timedelta(days=5)
    assert captured["parameters"] == {"data_dir": str(tmp_path), "config_path": "config/aoi.yaml"}


class _Response:
    def __init__(self, status: int, body: bytes) -> None:
        self.status = status
        self._body = body

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, *exc_info) -> bool:
        return False


def test_healthcheck_is_healthy_on_http_200(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(healthcheck, "_open", lambda url, timeout: _Response(200, b"true"))
    healthy, detail = healthcheck.check(home=str(tmp_path))
    assert healthy is True
    assert "/health" in detail


def test_healthcheck_fails_when_the_api_is_unreachable(monkeypatch, tmp_path) -> None:
    def boom(url, timeout):
        raise urllib.error.URLError("connection refused")

    monkeypatch.setattr(healthcheck, "_open", boom)
    healthy, detail = healthcheck.check(home=str(tmp_path))
    assert healthy is False
    assert "unreachable" in detail


def test_healthcheck_fails_when_prefect_home_is_missing(monkeypatch, tmp_path) -> None:
    def boom(url, timeout):  # must never be reached
        raise AssertionError("HTTP must not be attempted")

    monkeypatch.setattr(healthcheck, "_open", boom)
    healthy, detail = healthcheck.check(home=str(tmp_path / "missing"))
    assert healthy is False
    assert "PREFECT_HOME" in detail


def test_dockerfile_has_a_nonroot_runtime_and_a_dev_test_stage() -> None:
    text = _dockerfile()
    assert "FROM python:3.12-slim" in text
    assert "uv sync --frozen --no-dev" in text
    assert "FROM runtime AS test" in text
    assert "uv sync --frozen --extra dev" in text
    assert "USER app" in text
    # The default (runtime) stage must be the last stage built by `docker build`,
    # so `test` must not be the final target when no --target is given.
    stages = [line for line in text.splitlines() if line.startswith("FROM ")]
    assert stages[-1].endswith("AS test")


def test_dockerignore_excludes_data_venv_git_and_prefect() -> None:
    patterns = _dockerignore()
    for required in ("data/", ".venv/", ".git/", ".prefect/", ".env"):
        assert required in patterns, required


def test_compose_mounts_data_and_keeps_prefect_home_outside_it() -> None:
    text = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    assert "./data:/app/data" in text
    assert "./config:/app/config:ro" in text
    assert "./.prefect:/var/lib/prefect" in text
    assert "PREFECT_HOME: /var/lib/prefect" in text
    assert "DELTA_CROPS_DATA_DIR: /app/data" in text
    assert "DELTA_CROPS_CONFIG: /app/config/aoi.yaml" in text
    assert "restart: unless-stopped" in text
    # The read-only data mount is what keeps tests away from the real lake.
    assert "./data:/app/data:ro" in text

"""Phase 6b: deployment registration, healthcheck and packaging invariants.

No Docker daemon is needed: the deployment options are pure, the healthcheck
takes an injected HTTP opener, and the compose checks parse the YAML so a
service that cannot start fails here instead of at `docker compose up`.
"""

from __future__ import annotations

import json
import urllib.error
from datetime import timedelta
from pathlib import Path

import pytest
import yaml

from delta_crops.orchestration import healthcheck
from delta_crops.orchestration import serve as serve_module
from delta_crops.orchestration.flow import scheduled_flow

ROOT = Path(__file__).resolve().parents[1]


def _compose() -> dict:
    return yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))


def _dockerfile() -> str:
    return (ROOT / "Dockerfile").read_text(encoding="utf-8")


def _dockerignore() -> list[str]:
    return [
        line.strip() for line in (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


def _service_healthcheck_api_url(service: dict) -> str | None:
    test = service.get("healthcheck", {}).get("test", [])
    if "--api-url" not in test:
        return None
    return test[test.index("--api-url") + 1]


# --------------------------------------------------------------- serve ----

def test_deployment_options_use_five_day_interval() -> None:
    options = serve_module.deployment_options("/srv/data", "config/aoi.yaml")
    assert options["interval"] == timedelta(days=5)
    assert options["name"] == "delta-crops-scheduled"


def test_deployment_options_allow_only_one_concurrent_run() -> None:
    options = serve_module.deployment_options("/srv/data", None)
    assert options["global_limit"] == 1
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

    monkeypatch.setenv("PREFECT_API_URL", "http://server:4200/api")
    monkeypatch.setattr(scheduled_flow, "serve", fake_serve)
    serve_module.serve(tmp_path, "config/aoi.yaml")

    assert captured["global_limit"] == 1
    assert captured["interval"] == timedelta(days=5)
    assert captured["parameters"] == {"data_dir": str(tmp_path), "config_path": "config/aoi.yaml"}


def test_serve_refuses_to_start_without_an_api_url(monkeypatch, tmp_path) -> None:
    """An ephemeral API has no scheduler, so a served schedule would never fire."""
    monkeypatch.delenv("PREFECT_API_URL", raising=False)
    monkeypatch.setattr(scheduled_flow, "serve", lambda **kwargs: None)
    with pytest.raises(SystemExit, match="PREFECT_API_URL"):
        serve_module.serve(tmp_path, None)


# ---------------------------------------------------------- healthcheck ----

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


def _fake_api(monkeypatch, routes: dict[str, tuple[int, bytes]]) -> list[str]:
    seen: list[str] = []

    def fake_open(url, timeout):
        seen.append(url)
        status, body = routes[url]
        return _Response(status, body)

    monkeypatch.setattr(healthcheck, "_open", fake_open)
    return seen


def test_healthcheck_is_healthy_on_http_200(monkeypatch, tmp_path) -> None:
    _fake_api(monkeypatch, {"http://api.test/health": (200, b"true")})
    healthy, detail = healthcheck.check("http://api.test", home=str(tmp_path))
    assert healthy is True
    assert "/health" in detail


def test_healthcheck_fails_when_the_api_is_unreachable(monkeypatch, tmp_path) -> None:
    def boom(url, timeout):
        raise urllib.error.URLError("connection refused")

    monkeypatch.setattr(healthcheck, "_open", boom)
    healthy, detail = healthcheck.check("http://api.test", home=str(tmp_path))
    assert healthy is False
    assert "unreachable" in detail


def test_healthcheck_fails_when_prefect_home_is_missing(monkeypatch, tmp_path) -> None:
    def boom(url, timeout):  # must never be reached
        raise AssertionError("HTTP must not be attempted")

    monkeypatch.setattr(healthcheck, "_open", boom)
    healthy, detail = healthcheck.check(home=str(tmp_path / "missing"))
    assert healthy is False
    assert "PREFECT_HOME" in detail


def test_healthcheck_requires_the_deployment_to_be_registered(monkeypatch, tmp_path) -> None:
    _fake_api(monkeypatch, {
        "http://api.test/health": (200, b"true"),
        "http://api.test/deployments/name/flow-a/dep-b": (404, b'{"detail":"not found"}'),
    })
    healthy, detail = healthcheck.check("http://api.test", home=str(tmp_path),
                                        deployment="flow-a/dep-b")
    assert healthy is False
    assert "not registered" in detail


def test_healthcheck_requires_the_deployment_to_be_unpaused(monkeypatch, tmp_path) -> None:
    _fake_api(monkeypatch, {
        "http://api.test/health": (200, b"true"),
        "http://api.test/deployments/name/flow-a/dep-b": (200, json.dumps({"paused": True}).encode()),
    })
    healthy, detail = healthcheck.check("http://api.test", home=str(tmp_path),
                                        deployment="flow-a/dep-b")
    assert healthy is False
    assert "paused" in detail


def test_healthcheck_accepts_an_active_deployment(monkeypatch, tmp_path) -> None:
    _fake_api(monkeypatch, {
        "http://api.test/health": (200, b"true"),
        "http://api.test/deployments/name/flow-a/dep-b": (200, json.dumps({"paused": False}).encode()),
    })
    healthy, detail = healthcheck.check("http://api.test", home=str(tmp_path),
                                        deployment="flow-a/dep-b")
    assert healthy is True
    assert "active" in detail


# ------------------------------------------------------------ packaging ----

def test_dockerfile_has_a_nonroot_runtime_and_a_dev_test_stage() -> None:
    text = _dockerfile()
    assert "FROM python:3.12-slim" in text
    assert "uv sync --frozen --no-dev" in text
    assert "FROM runtime AS test" in text
    assert "uv sync --frozen --extra dev" in text
    assert "USER app" in text
    assert 'ARG UID=1000' in text and 'ARG GID=1000' in text
    stages = [line for line in text.splitlines() if line.startswith("FROM ")]
    assert stages[-1].endswith("AS test")


def test_dockerignore_excludes_data_venv_git_and_prefect() -> None:
    patterns = _dockerignore()
    for required in ("data/", ".venv/", ".git/", ".prefect/", ".env"):
        assert required in patterns, required


def test_compose_runs_a_real_prefect_server_for_the_scheduler() -> None:
    """An ephemeral API skips the scheduler, so served schedules never fire."""
    services = _compose()["services"]
    assert "server" in services, "compose must run `prefect server start`"
    command = [str(token) for token in services["server"]["command"]]
    assert command[:3] == ["prefect", "server", "start"]
    assert "--no-ui" in command
    assert services["orchestrator"]["depends_on"]["server"]["condition"] == "service_healthy"


def test_compose_healthcheck_probes_the_api_each_service_uses() -> None:
    services = _compose()["services"]
    for name, service in services.items():
        api_url = (service.get("environment") or {}).get("PREFECT_API_URL")
        if not api_url:
            continue
        assert _service_healthcheck_api_url(service) == api_url, (
            f"{name} sets PREFECT_API_URL={api_url} but its healthcheck probes "
            f"{_service_healthcheck_api_url(service)!r}"
        )


def test_compose_runner_does_not_use_a_loopback_api_url() -> None:
    """127.0.0.1 inside the runner container is not the server container."""
    env = _compose()["services"]["orchestrator"]["environment"]
    assert not env["PREFECT_API_URL"].startswith("http://127.0.0.1")
    assert _compose()["services"]["orchestrator"]["healthcheck"]["test"].count("--deployment") == 1

"""Phase 9 documentation contract: attributions, links, results, demo, compose.

These tests exist because the README is part of the deliverable. They fail when
a hand-edited number drifts from the artifacts, when a required credit
disappears, when a relative link points at nothing, or when the compose file
loses the read-only dashboard contract.
"""

from __future__ import annotations

import importlib.util
import json
import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
README = ROOT / "README.md"
LIMITATIONS = ROOT / "docs" / "limitations.md"
DEMO = ROOT / "docs" / "demo-2min.md"
COMPOSE = ROOT / "docker-compose.yml"
DOCKERFILE = ROOT / "Dockerfile"
ARTIFACTS = ROOT / "artifacts" / "phase7"


def _results_module():
    spec = importlib.util.spec_from_file_location(
        "readme_results", ROOT / "scripts" / "readme_results.py"
    )
    module = importlib.util.module_from_spec(spec)
    import sys

    sys.modules[spec.name] = module  # dataclasses needs the module registered
    spec.loader.exec_module(module)
    return module


def _markdown_targets(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8")
    return re.findall(r"\]\(([^)\s]+)\)", text)


def test_required_data_attributions_are_present() -> None:
    text = README.read_text(encoding="utf-8")
    required = [
        "ESA WorldCover 10 m 2021 v200",
        "© ESA WorldCover project 2021",
        "Contains modified Copernicus Sentinel data (2021)",
        "CC BY 4.0",
        "Weather data by Open-Meteo.com",
        "https://creativecommons.org/licenses/by/4.0/",
        "https://esa-worldcover.org/en/data-access",
        "https://open-meteo.com/",
    ]
    missing = [phrase for phrase in required if phrase not in text]
    assert not missing, f"README is missing required attribution text: {missing}"


@pytest.mark.skipif(not (ARTIFACTS / "xgb_temporal.json").exists(),
                    reason="Phase 7 artifacts are gitignored (fresh clone)")
def test_readme_results_block_matches_the_artifacts() -> None:
    results = _results_module()
    committed = results.extract_block(README.read_text(encoding="utf-8"))
    assert committed == results.render_block(ARTIFACTS)

    temporal = json.loads((ARTIFACTS / "xgb_temporal.json").read_text())
    loso = json.loads((ARTIFACTS / "xgb_loso.json").read_text())
    spatial = json.loads((ARTIFACTS / "xgb_spatial.json").read_text())
    table = results.load_results(ARTIFACTS)

    by_key = {(row.campaign.split()[0], row.variant): row for row in table["rows"]}
    for variant in ("full", "ndvi_only"):
        s3 = temporal["variants"][variant]["groups"]["cropland/pooled"]["models"]["M"]
        s4 = loso["summary"]["verdict"]["variants"][variant]
        s5 = spatial["summary"]["variants"][variant]
        assert by_key[("S3", variant)].metrics["pr_auc"] == s3["pr_auc"]
        assert by_key[("S4", variant)].metrics["pr_auc"] == s4["pr_auc"]
        assert by_key[("S5", variant)].metrics["pr_auc"] == s5["point"]["pr_auc"]
        assert by_key[("S5", variant)].bars["pr_auc"] == s5["pr_auc_bar"]

    assert by_key[("S3", "full")].metrics["pr_auc"] == pytest.approx(0.8213704513, abs=5e-10)

    # The headline claim in prose must match the artifact: full fails PR-AUC in all three.
    assert not temporal["variants"]["full"]["groups"]["cropland/pooled"]["models"]["M"]["pr_auc"] \
        >= by_key[("S3", "full")].bars["pr_auc"]
    assert loso["summary"]["verdict"]["variants"]["full"]["pr_auc_passed"] is False
    assert spatial["summary"]["variants"]["full"]["pr_auc_passed"] is False


def test_relative_markdown_links_resolve() -> None:
    broken: list[str] = []
    for document in (README, LIMITATIONS, DEMO):
        for target in _markdown_targets(document):
            if target.startswith(("http://", "https://", "mailto:", "#")):
                continue
            candidate = (document.parent / target.split("#", 1)[0]).resolve()
            if not candidate.exists():
                broken.append(f"{document.relative_to(ROOT)} -> {target}")
    assert not broken, f"broken relative links: {broken}"


def test_every_named_limitation_is_documented() -> None:
    text = LIMITATIONS.read_text(encoding="utf-8")
    named = {
        "36.70 % DN = 0 on 2021-07-10": ["36.70", "2021-07-10", "not established"],
        "scheduled flow never ran on real data": ["scheduled flow never ran end-to-end on real data"],
        "container concurrency unproven": ["concurrency is unproven", "global_limit=1"],
        "gold_regression after 2024": ["gold_regression.py", "15/15 table-years OK", "48.14 s"],
        "S5 reliability gap": ["S5 calibration is unmeasured", "27 of 28"],
    }
    missing = {
        name: [phrase for phrase in phrases if phrase not in text]
        for name, phrases in named.items()
    }
    assert not any(missing.values()), f"limitations page is incomplete: {missing}"
    assert "docs/limitations.md" in README.read_text(encoding="utf-8")


def test_demo_script_and_commands_are_documented() -> None:
    script = ROOT / "scripts" / "demo.sh"
    assert script.is_file()
    assert script.stat().st_mode & 0o111, "scripts/demo.sh must be executable"

    text = README.read_text(encoding="utf-8")
    for command in ("delta-crops-dashboard",
                    "python -m delta_crops.dashboard.health",
                    "python -m delta_crops.models.predictions"):
        assert command in text, f"README does not document {command!r}"

    demo_text = DEMO.read_text(encoding="utf-8")
    assert "no visible heatwave response" in demo_text
    assert "not proof that heat has no effect" in demo_text


def test_compose_serves_the_dashboard_read_only_behind_a_profile() -> None:
    compose = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    services = compose["services"]
    dashboard = services["dashboard"]
    publish = services["health-publish"]

    assert dashboard["profiles"] == ["dashboard"]
    assert publish["profiles"] == ["dashboard"]
    assert dashboard["ports"] == ["127.0.0.1:8501:8501"]
    assert "./data:/app/data:ro" in dashboard["volumes"]
    assert "./artifacts:/app/artifacts:ro" in dashboard["volumes"]
    assert publish["volumes"] == ["./data:/app/data"]
    assert dashboard["depends_on"]["health-publish"]["condition"] == "service_completed_successfully"
    assert publish["command"] == ["python", "-m", "delta_crops.dashboard.health"]
    assert services["server"]["image"] == "delta-crops:local"
    assert services["tests"]["image"] == "delta-crops:test"

    dockerfile = DOCKERFILE.read_text(encoding="utf-8")
    assert "FROM runtime AS dashboard" in dockerfile
    assert "--extra dashboard" in dockerfile
    runtime = dockerfile.split("FROM runtime AS dashboard")[0]
    assert "--extra dashboard" not in runtime, "the pipeline image must stay streamlit-free"


def test_licence_is_declared_and_linked() -> None:
    assert (ROOT / "LICENSE").is_file()
    licence = (ROOT / "LICENSE").read_text(encoding="utf-8")
    assert licence.startswith("MIT License")
    assert "MIT Licence" in README.read_text(encoding="utf-8")

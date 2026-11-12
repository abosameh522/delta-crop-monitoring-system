"""Tests for the Phase 8 pipeline-health publisher (Option A).

The publisher is a projection, not a check: these tests pin that it copies the
quality verdict and the newest reports unchanged, that it is deterministic, and
that it refuses to invent a health report when no quality report exists.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from delta_crops.dashboard import health


def _quality(run_id: str, status: str = "passed", checks=None) -> dict:
    return {
        "run_id": run_id,
        "status": status,
        "total_rows": 84_234_000,
        "expected_ledger_dates": 352,
        "schema_valid_files": 352,
        "kept_cells": 239_304,
        "pandera_aggregate_validation": "passed",
        "gap_definition": "a gap is a run of AOI days without acquisition",
        "checks": checks or [
            {"check": "missing_partition", "severity": "critical", "count": 0,
             "status": "pass", "message": "Expected outputs are enumerated from ledger dates"},
            {"check": "duplicate_key", "severity": "critical", "count": 0,
             "status": "pass", "message": "no duplicate (cell, date)"},
        ],
        "seasons": [
            {"start": "2024-05-01", "end": "2024-09-30", "acquisition_dates": 59,
             "aoi_no_acquisition_days": 94, "aoi_longest_gap_days": 4,
             "aoi_gaps": [{"start": "2024-05-10", "days": 4}], "gap_alert_days": 10},
        ],
        "dates": [
            {"date": "2024-05-06", "rows": 239_304, "null_rows": 0},
            {"date": "2024-06-12", "rows": 239_304, "null_rows": 1_200},
        ],
        "config": {"gap_alert_days": 10},
    }


def _gate(run_id: str) -> dict:
    return {
        "run_id": run_id,
        "status": "passed",
        "gate_max_null_pct": 9.0,
        "min_observations": 2,
        "max_masked_pct": 90.0,
        "weeks": 20,
        "kept_cells": 239_304,
        "cropland_cells": 194_778,
        "failing": [],
        "seasons": [{"year": 2024, "weeks": 20, "structural_weeks": 1}],
        "timings": {"seconds": 1.0},
    }


def _write_lake(root: Path, quality: dict, gate: dict | None) -> None:
    quality_dir = root / "silver" / "_reports" / "quality"
    quality_dir.mkdir(parents=True, exist_ok=True)
    name = f"quality_{quality['run_id']}.json"
    (quality_dir / name).write_text(json.dumps(quality), encoding="utf-8")
    if gate is not None:
        gate_dir = root / "gold" / "_reports"
        gate_dir.mkdir(parents=True, exist_ok=True)
        (gate_dir / f"weekly_gate_{gate['run_id']}.json").write_text(
            json.dumps(gate), encoding="utf-8"
        )


def test_latest_report_picks_the_newest_run_id(tmp_path: Path) -> None:
    directory = tmp_path / "quality"
    directory.mkdir()
    older = directory / "quality_20260101T000000Z_aaaaaaaa.json"
    newer = directory / "quality_20260202T000000Z_bbbbbbbb.json"
    older.write_text("{}", encoding="utf-8")
    newer.write_text("{}", encoding="utf-8")

    assert health.latest_report(directory, "quality_*.json") == newer
    assert health.latest_report(tmp_path / "absent", "quality_*.json") is None


def test_build_health_summarises_the_newest_reports(tmp_path: Path) -> None:
    _write_lake(tmp_path, _quality("20260101T000000Z_aaaaaaaa"), _gate("20260101T000000Z_aaaaaaaa"))
    _write_lake(tmp_path, _quality("20260202T000000Z_bbbbbbbb", status="failed"),
                _gate("20260202T000000Z_bbbbbbbb"))

    published = health.build_health(tmp_path)

    assert published["schema_version"] == health.SCHEMA_VERSION
    assert published["sources"] == {
        "quality": "quality_20260202T000000Z_bbbbbbbb.json",
        "gate": "weekly_gate_20260202T000000Z_bbbbbbbb.json",
    }
    assert published["quality"]["status"] == "failed"
    assert [check["status"] for check in published["quality"]["checks"]] == ["pass", "pass"]
    assert published["quality"]["dates"] == {
        "n": 2, "with_nulls": 1, "max_null_rows": 1_200,
        "max_null_date": "2024-06-12", "rows_min": 239_304, "rows_max": 239_304,
    }
    assert published["quality"]["seasons"][0]["gap_count"] == 1
    assert isinstance(published["quality"]["dates"], dict)
    assert published["gate"]["seasons"][0]["year"] == 2024
    assert "timings" not in published["gate"]


def test_publish_is_idempotent_and_writes_under_gold(tmp_path: Path) -> None:
    _write_lake(tmp_path, _quality("20260202T000000Z_bbbbbbbb"), _gate("20260202T000000Z_bbbbbbbb"))

    first = health.publish(tmp_path)
    first_bytes = first.read_bytes()
    second = health.publish(tmp_path)

    assert first == second == tmp_path / "gold" / "_reports" / "pipeline_health.json"
    assert second.read_bytes() == first_bytes
    assert not list(second.parent.glob("*.tmp"))


def test_publish_refuses_without_a_quality_report(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="no quality report"):
        health.publish(tmp_path)


def test_publish_tolerates_a_missing_gate(tmp_path: Path) -> None:
    _write_lake(tmp_path, _quality("20260202T000000Z_bbbbbbbb"), None)

    published = json.loads(health.publish(tmp_path).read_text(encoding="utf-8"))

    assert published["gate"] is None
    assert published["sources"]["gate"] is None

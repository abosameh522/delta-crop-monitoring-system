"""Publish the latest pipeline health summary into the Gold lake.

Option A of the Phase 8 plan: the dashboard must read only Gold tables and
model outputs, but the Phase 4 quality report lives under
``data/silver/_reports/quality/``. This module reads the newest quality and
Gold-gate reports and writes one compact, deterministic summary to
``data/gold/_reports/pipeline_health.json``.

It never recomputes a check and never touches the quality or gate code: it is a
projection of files those stages already wrote. Same inputs give the same bytes,
so rerunning the publish cannot create drift.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1
HEALTH_NAME = "pipeline_health.json"
QUALITY_PATTERN = "quality_*.json"
GATE_PATTERN = "weekly_gate_*.json"


def latest_report(directory: Path, pattern: str) -> Path | None:
    """Newest report by run id, which starts with a sortable UTC timestamp."""
    if not directory.is_dir():
        return None
    candidates = sorted(directory.glob(pattern))
    return candidates[-1] if candidates else None


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def summarise_quality(report: dict[str, Any]) -> dict[str, Any]:
    """Keep the verdict, the checks and the per-season gaps; drop the 352 dates."""
    dates = report.get("dates", [])
    null_dates = [entry for entry in dates if entry.get("null_rows")]
    return {
        "run_id": report.get("run_id"),
        "status": report.get("status"),
        "total_rows": report.get("total_rows"),
        "expected_ledger_dates": report.get("expected_ledger_dates"),
        "schema_valid_files": report.get("schema_valid_files"),
        "kept_cells": report.get("kept_cells"),
        "pandera_aggregate_validation": report.get("pandera_aggregate_validation"),
        "gap_definition": report.get("gap_definition"),
        "checks": [
            {
                "check": check.get("check"),
                "severity": check.get("severity"),
                "count": check.get("count"),
                "status": check.get("status"),
                "message": check.get("message"),
            }
            for check in report.get("checks", [])
        ],
        "seasons": [
            {
                "start": season.get("start"),
                "end": season.get("end"),
                "acquisition_dates": season.get("acquisition_dates"),
                "aoi_no_acquisition_days": season.get("aoi_no_acquisition_days"),
                "aoi_longest_gap_days": season.get("aoi_longest_gap_days"),
                "gap_count": len(season.get("aoi_gaps", [])),
                "gap_alert_days": season.get("gap_alert_days"),
            }
            for season in report.get("seasons", [])
        ],
        "dates": {
            "n": len(dates),
            "with_nulls": len(null_dates),
            "max_null_rows": max((entry.get("null_rows", 0) for entry in dates), default=0),
            "max_null_date": max(
                null_dates, key=lambda entry: entry.get("null_rows", 0), default={}
            ).get("date"),
            "rows_min": min((entry.get("rows", 0) for entry in dates), default=0),
            "rows_max": max((entry.get("rows", 0) for entry in dates), default=0),
        },
        "config": report.get("config", {}),
    }


def summarise_gate(report: dict[str, Any]) -> dict[str, Any]:
    """The gate report is already compact; keep it whole minus timings."""
    return {
        "run_id": report.get("run_id"),
        "status": report.get("status"),
        "gate_max_null_pct": report.get("gate_max_null_pct"),
        "min_observations": report.get("min_observations"),
        "max_masked_pct": report.get("max_masked_pct"),
        "weeks": report.get("weeks"),
        "kept_cells": report.get("kept_cells"),
        "cropland_cells": report.get("cropland_cells"),
        "failing": report.get("failing", []),
        "seasons": report.get("seasons", []),
    }


def build_health(root: Path) -> dict[str, Any]:
    """Summarise the newest quality and gate reports under ``root``."""
    quality_path = latest_report(root / "silver" / "_reports" / "quality", QUALITY_PATTERN)
    gate_path = latest_report(root / "gold" / "_reports", GATE_PATTERN)
    if quality_path is None:
        raise FileNotFoundError(
            f"no quality report under {root / 'silver' / '_reports' / 'quality'}"
        )
    quality = _read(quality_path)
    gate = _read(gate_path) if gate_path is not None else None
    return {
        "schema_version": SCHEMA_VERSION,
        "sources": {
            "quality": quality_path.name,
            "gate": gate_path.name if gate_path else None,
        },
        "quality": summarise_quality(quality),
        "gate": summarise_gate(gate) if gate else None,
    }


def publish(root: Path) -> Path:
    """Write the health summary atomically; returns the written path."""
    health = build_health(root)
    target = root / "gold" / "_reports" / HEALTH_NAME
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(".json.tmp")
    temporary.write_text(
        json.dumps(health, indent=2, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, target)
    return target


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default="data", help="lake root (default: data)")
    args = parser.parse_args(argv)
    target = publish(Path(args.root))
    health = _read(target)
    print(
        f"wrote {target}: quality {health['quality']['status']} "
        f"({health['sources']['quality']}), gate "
        f"{(health['gate'] or {}).get('status', 'missing')} "
        f"({health['sources']['gate']})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

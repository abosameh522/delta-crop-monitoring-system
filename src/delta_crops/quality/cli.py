"""Run Silver quality checks: python -m delta_crops.quality.cli."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from delta_crops.config import data_dir, load_config
from delta_crops.quality.checks import run_quality
from delta_crops.quality.report import QualityFailure


def _summary(report: dict) -> dict:
    """Compact, flow-friendly view of one quality report."""
    return {
        "status": report.get("status", "unknown"),
        "run_id": report.get("run_id"),
        "expected_ledger_dates": report.get("expected_ledger_dates"),
        "total_rows": report.get("total_rows", 0),
        "alerts": sum(
            c["count"] > 0 and c["severity"] == "alert" for c in report.get("checks", [])
        ),
        "failed_checks": [
            c["check"]
            for c in report.get("checks", [])
            if c["severity"] == "critical" and c["count"]
        ],
        "report": report.get("report_path"),
    }


def _write_summary(path: Path | None, payload: dict) -> None:
    """Write a machine-readable quality summary atomically; the flow reads it."""
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    tmp.replace(path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=None)
    parser.add_argument("--config", type=Path, default=None)
    parser.add_argument("--summary-json", type=Path, default=None)
    args = parser.parse_args(argv)
    summary_path = args.summary_json
    try:
        path = run_quality(args.data_dir or data_dir(), load_config(args.config))
    except QualityFailure as error:
        print(str(error))
        report_path = getattr(error, "report_path", None)
        payload = {"status": "failed"}
        if report_path and Path(report_path).is_file():
            payload = _summary(json.loads(Path(report_path).read_text()))
        _write_summary(summary_path, payload)
        return 10
    except ValueError as error:
        print(str(error))
        _write_summary(summary_path, {"status": "failed", "error": str(error)})
        return 1
    report = json.loads(path.read_text())
    alerts = sum(c["count"] > 0 and c["severity"] == "alert" for c in report["checks"])
    print(f"Quality PASSED: {report['expected_ledger_dates']} ledger dates, "
          f"{report.get('total_rows', 0):,} rows; {alerts} alert checks")
    print(f"Seconds: {report['timings']['total_seconds']:.3f}; report: {path}")
    _write_summary(summary_path, {**_summary(report), "report": str(path)})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

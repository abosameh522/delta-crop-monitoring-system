"""Run Silver quality checks: python -m delta_crops.quality.cli."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from delta_crops.config import data_dir, load_config
from delta_crops.quality.checks import run_quality
from delta_crops.quality.report import QualityFailure


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=None)
    parser.add_argument("--config", type=Path, default=None)
    args = parser.parse_args()
    try:
        path = run_quality(args.data_dir or data_dir(), load_config(args.config))
    except (QualityFailure, ValueError) as error:
        print(str(error))
        return 1
    report = json.loads(path.read_text())
    alerts = sum(c["count"] > 0 and c["severity"] == "alert" for c in report["checks"])
    print(f"Quality PASSED: {report['expected_ledger_dates']} ledger dates, "
          f"{report.get('total_rows', 0):,} rows; {alerts} alert checks")
    print(f"Seconds: {report['timings']['total_seconds']:.3f}; report: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

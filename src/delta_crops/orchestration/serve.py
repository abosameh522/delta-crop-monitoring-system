"""Serve the pipeline as one scheduled Prefect deployment.

A served deployment needs a **real** Prefect server: on an ephemeral API Prefect
skips the scheduler service, so interval schedules never fire
(``prefect/runner/runner.py:383-386`` warns about exactly this). ``serve``
therefore refuses to start without ``PREFECT_API_URL``; the compose ``server``
service provides it, with SQLite state in ``PREFECT_HOME``.

Re-registering on every container start is idempotent (Prefect upserts the
deployment by name). The interval is re-anchored at registration, so the next
run is at most five days after the last start; see ``docs/phase6b-docker.md``.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any

from delta_crops.config import data_dir
from delta_crops.orchestration.flow import scheduled_flow

DEPLOYMENT_NAME = "delta-crops-scheduled"
SCHEDULE_DAYS = 5
GLOBAL_LIMIT = 1


def require_api_url() -> str:
    """Return PREFECT_API_URL or fail fast with the reason it is required."""
    api = os.environ.get("PREFECT_API_URL", "").strip()
    if not api:
        raise SystemExit(
            "PREFECT_API_URL must point at a running Prefect server. An ephemeral "
            "API does not run the scheduler, so a served deployment would never fire."
        )
    return api


def deployment_options(data_root: str | Path | None = None,
                       config_path: str | None = None) -> dict[str, Any]:
    """Keyword arguments for ``scheduled_flow.serve``; pure, so tests can assert."""
    from datetime import timedelta

    return {
        "name": DEPLOYMENT_NAME,
        "interval": timedelta(days=SCHEDULE_DAYS),
        "global_limit": GLOBAL_LIMIT,
        # Pause the schedule when the runner is stopped; resume on the next start.
        "pause_on_shutdown": True,
        "parameters": {
            "data_dir": str(data_root or data_dir()),
            "config_path": config_path,
        },
        "tags": ["scheduled"],
    }


def serve(data_root: str | Path | None = None, config_path: str | None = None,
          **overrides: Any) -> None:
    """Register the deployment and block, running the five-day schedule."""
    api_url = require_api_url()
    options = deployment_options(data_root, config_path)
    options.update(overrides)
    print(f"serving {options['name']} against {api_url}: every {SCHEDULE_DAYS} days, "
          f"global_limit={options['global_limit']}, data_dir={options['parameters']['data_dir']}",
          flush=True)
    scheduled_flow.serve(**options)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", default=None,
                        help="data lake root (default: $DELTA_CROPS_DATA_DIR or ./data)")
    parser.add_argument("--config", default=None, help="AOI config path")
    args = parser.parse_args(argv)
    try:
        serve(args.data_dir, args.config)
    except KeyboardInterrupt:
        print("stopped", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

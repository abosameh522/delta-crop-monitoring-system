# Phase 6a orchestration

Phase 6a adds deterministic orchestration contracts without Docker packaging.
The scheduled flow processes only an explicitly configured in-progress year; with no in-progress year it records a logged no-op. Complete years require an explicit manual reprocess flag. Exit codes are 0 success, 1 unexpected, 2 argparse usage, 3 transient, 4 asset/date, 10 critical quality, and 11 Gold weekly-gate failure. Only code 3 retries with exponential backoff.

Gold uses a per-year `as_of`: season end for complete seasons, otherwise the minimum of latest ingested acquisition and latest weather date. Weeks whose week end is after `as_of` are `not_yet_observed`.

Regression: `.venv/bin/python /tmp/gold_regression.py` runs DuckDB EXCEPT in both directions and row counts against `data/_regression/gold_before`. Slow pytest: `.venv/bin/pytest -q -m slow`.

Docker packaging, Docker Compose, and container memory measurements remain Phase 6b.

"""Prefect 3 pipeline flow: ingest -> Silver -> quality -> Gold.

The flow calls each CLI ``main()`` in-process. In-process calls keep the exit
code contract (the CLI returns an int) without paying a Python interpreter
startup per task, and let tests monkeypatch the CLI mains directly.

Only exit code 3 (``TransientFailure``) is retryable. Codes 4, 10, 11, 1 and
anything unexpected become :class:`PipelineError` and are not retried.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from prefect import flow, task

from delta_crops.config import Config, load_config
from delta_crops.gold.as_of import season_states
from delta_crops.orchestration.steps import append_run_log

logger = logging.getLogger(__name__)

#: The only CLI exit code a Prefect task retries.
RETRYABLE_EXIT_CODES = frozenset({3})

#: Documented non-retryable codes, kept explicit for the run log and docs.
NON_RETRYABLE_EXIT_CODES = frozenset({1, 4, 10, 11})

DEFAULT_RETRIES = 3
DEFAULT_RETRY_DELAYS = [1.0, 2.0, 4.0]


class TransientError(RuntimeError):
    """Task-level transient failure: the only error Prefect retries."""


class PipelineError(RuntimeError):
    """Non-retryable task failure, carrying the CLI exit code."""

    def __init__(self, task_name: str, exit_code: int, message: str | None = None) -> None:
        self.task_name = task_name
        self.exit_code = exit_code
        super().__init__(message or f"{task_name} failed with exit code {exit_code}")


def retry_codes() -> set[int]:
    """CLI exit codes that make a task retry: exactly {3}."""
    return set(RETRYABLE_EXIT_CODES)


def raise_for_exit_code(task_name: str, code: int) -> None:
    """Translate a CLI exit code into the task retry contract."""
    if code == 0:
        return
    message = f"{task_name} exited with code {code}"
    if code in RETRYABLE_EXIT_CODES:
        raise TransientError(message)
    raise PipelineError(task_name, code, message)


def retry_on_transient(_task: Any, _task_run: Any, state: Any) -> bool:
    """Prefect ``retry_condition_fn``: retry only a :class:`TransientError`.

    Prefect 3.9.2 calls this with a ``Failed`` state whose ``data`` attribute is
    the raised exception (``task_engine.SyncTaskRunEngine.can_retry``).
    """
    error = getattr(state, "data", None)
    if isinstance(error, TransientError):
        return True
    return isinstance(getattr(error, "__cause__", None), TransientError)


def summary_path(root: Path, name: str) -> Path:
    """Per-run scratch path for a CLI summary; never read a previous attempt."""
    path = root / "_logs" / "task_summaries" / f"{name}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.unlink(missing_ok=True)
    return path


def read_summary(path: Path) -> dict[str, Any]:
    """Read a CLI summary; missing or corrupt files are tolerated (mocked mains)."""
    if not path.is_file():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_run_record(years: list[int], config: Config) -> dict[str, Any]:
    """Start one run-log record; the flow fills the task fields as it goes."""
    return {
        "run_id": uuid.uuid4().hex,
        "start_utc": _now(),
        "end_utc": None,
        "status": "running",
        "years": years,
        "as_of": {},
        "tasks": {},
        "ingest": {},
        "silver": {},
        "quality": {},
        "gold": {},
        "weekly_verify": "not_run",
        "duration_s": None,
        "config": config.name,
    }


def finish_run_record(record: dict[str, Any], status: str, started: float) -> dict[str, Any]:
    """Close a run record with end time, duration and final status."""
    record["end_utc"] = _now()
    record["status"] = status
    record["duration_s"] = round(time.monotonic() - started, 3)
    return record


@task(retries=0)
def scheduled_noop(years: list[int], in_progress_year: int | None) -> dict[str, Any]:
    """Explicit scheduled no-op when no configured season is in progress."""
    return {"status": "no_op", "years": years, "in_progress_year": in_progress_year}


def in_progress_years(root: Path, config: Config) -> list[int]:
    """Configured years whose season is not complete (today: none).

    A season is complete only when every configured weather day is present in
    Silver and the season end has passed (``gold.as_of``). A newly added year
    therefore becomes the scheduled in-progress year automatically.
    """
    states = season_states(root, config, list(config.seasons.years))
    return [year for year in config.seasons.years if not states[year].complete]


@flow(name="delta-crops-phase6a")
def scheduled_flow(
    data_dir: str,
    config_path: str | None = None,
    reprocess_year: int | None = None,
    retries: int = DEFAULT_RETRIES,
    retry_delays: list[float] | None = None,
) -> dict[str, Any]:
    """Scheduled entry point: process only incomplete years or an explicit year."""
    from delta_crops.orchestration.pipeline import run_pipeline

    return run_pipeline(
        data_dir=data_dir,
        config_path=config_path,
        reprocess_year=reprocess_year,
        retries=retries,
        retry_delays=retry_delays if retry_delays is not None else DEFAULT_RETRY_DELAYS,
        scheduled=True,
    )


@flow(name="delta-crops-weekly-verify")
def weekly_verify_flow(
    data_dir: str,
    config_path: str | None = None,
    years: list[int] | None = None,
) -> dict[str, Any]:
    """Weekly full-checksum Bronze verification (separate from scheduled ingest)."""
    from delta_crops.orchestration.pipeline import run_verify

    return run_verify(data_dir=data_dir, config_path=config_path, years=years)

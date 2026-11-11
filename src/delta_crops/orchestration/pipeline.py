"""Four-task Prefect pipeline and the scheduled / weekly entry logic.

This module is imported lazily from :mod:`delta_crops.orchestration.flow` so the
flow module stays importable without Prefect run context. The tasks call the
real CLI ``main()`` functions in-process; the CLI writes a small JSON summary
that becomes the run-log evidence.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

from prefect import task

from delta_crops.config import Config, load_config
from delta_crops.gold.as_of import season_states
from delta_crops.orchestration.flow import (
    DEFAULT_RETRIES,
    DEFAULT_RETRY_DELAYS,
    PipelineError,
    TransientError,
    finish_run_record,
    in_progress_years,
    new_run_record,
    raise_for_exit_code,
    read_summary,
    retry_on_transient,
    scheduled_noop,
    summary_path,
)
from delta_crops.orchestration.steps import append_run_log

logger = logging.getLogger(__name__)


def _cli_argv(data_dir: str, config_path: str | None, *rest: str) -> list[str]:
    """Build argv with the global options before the subcommand."""
    argv = ["--data-dir", data_dir]
    if config_path:
        argv += ["--config", config_path]
    return argv + list(rest)


def _make_tasks(retries: int, retry_delays: list[float]):
    """Build the four tasks with the configured retry budget.

    A factory (rather than module-level decorators) lets a run choose its retry
    delays: production uses 1/2/4 s, tests use 0 s.
    """
    retry_kwargs: dict[str, Any] = {
        "retries": retries,
        "retry_delay_seconds": retry_delays,
        "retry_condition_fn": retry_on_transient,
    }

    @task(name="ingest", **retry_kwargs)
    def ingest_task(
        data_dir: str,
        config_path: str | None,
        years: list[int],
        allow_partial_years: list[int],
    ) -> dict[str, Any]:
        """Download missing Bronze assets and weather for the selected years."""
        from delta_crops.ingestion import cli as ingest_cli

        result: dict[str, Any] = {
            "exit_code": 0,
            "new": 0,
            "skipped": 0,
            "bytes": 0,
            "failed": 0,
            "weather": {},
            "years": {},
        }
        for year in years:
            path = summary_path(Path(data_dir), f"ingest_{year}")
            argv = _cli_argv(
                data_dir, config_path,
                "ingest", "--year", str(year), "--no-checksum",
                "--summary-json", str(path),
            )
            if year in allow_partial_years:
                argv += ["--allow-partial-weather-year", str(year)]
            code = int(ingest_cli.main(argv))
            summary = read_summary(path)
            result["years"][str(year)] = {"exit_code": code, **summary}
            for key in ("new", "skipped", "bytes", "failed"):
                result[key] += int(summary.get(key, 0) or 0)
            result["weather"].update(summary.get("weather", {}))
            if code != 0:
                result["exit_code"] = code
                raise_for_exit_code("ingest", code)
        return result

    @task(name="silver", **retry_kwargs)
    def silver_task(
        data_dir: str,
        config_path: str | None,
        years: list[int],
    ) -> dict[str, Any]:
        """Turn Bronze windows into Silver NDVI partitions."""
        from delta_crops.transform import cli as transform_cli

        result: dict[str, Any] = {
            "exit_code": 0,
            "built": 0,
            "skipped": 0,
            "rebuilt_due_to_input_change": 0,
            "unfingerprinted": 0,
            "failed": 0,
            "years": {},
        }
        for year in years:
            path = summary_path(Path(data_dir), f"silver_{year}")
            code = int(transform_cli.main(_cli_argv(
                data_dir, config_path,
                "year", "--year", str(year), "--no-verify-checksum",
                "--summary-json", str(path),
            )))
            summary = read_summary(path)
            result["years"][str(year)] = {"exit_code": code, **summary}
            for key in ("built", "skipped", "rebuilt_due_to_input_change", "unfingerprinted", "failed"):
                result[key] += int(summary.get(key, 0) or 0)
            if code != 0:
                result["exit_code"] = code
                raise_for_exit_code("silver", code)
        return result

    @task(name="quality", retries=0)
    def quality_task(data_dir: str, config_path: str | None) -> dict[str, Any]:
        """Run critical Silver quality checks. Never retried."""
        from delta_crops.quality import cli as quality_cli

        path = summary_path(Path(data_dir), "quality")
        code = int(quality_cli.main(_cli_argv(
            data_dir, config_path, "--summary-json", str(path),
        )))
        summary = read_summary(path)
        if code != 0:
            raise_for_exit_code("quality", code)
        return {"exit_code": code, **summary}

    @task(name="gold", **retry_kwargs)
    def gold_task(data_dir: str, config_path: str | None) -> dict[str, Any]:
        """Build Gold tables and run the weekly verification query."""
        from delta_crops.gold import cli as gold_cli

        build_path = summary_path(Path(data_dir), "gold_build")
        code = int(gold_cli.main(_cli_argv(
            data_dir, config_path, "build", "--summary-json", str(build_path),
        )))
        build = read_summary(build_path)
        if code != 0:
            raise_for_exit_code("gold", code)

        verify_path = summary_path(Path(data_dir), "gold_verify")
        verify_code = int(gold_cli.main(_cli_argv(
            data_dir, config_path, "verify", "--summary-json", str(verify_path),
        )))
        verify = read_summary(verify_path)
        if verify_code != 0:
            raise_for_exit_code("gold_verify", verify_code)
        return {
            "exit_code": 0,
            "build": build,
            "verify": verify,
            "weekly_verify": verify.get("status", "ok"),
        }

    return ingest_task, silver_task, quality_task, gold_task


def _record_as_of(root: Path, config: Config, years: list[int]) -> dict[str, str | None]:
    states = season_states(root, config, years)
    return {
        str(year): (states[year].as_of.isoformat() if states[year].as_of else None)
        for year in years
    }


def run_pipeline(
    data_dir: str,
    config_path: str | None = None,
    reprocess_year: int | None = None,
    retries: int = DEFAULT_RETRIES,
    retry_delays: list[float] | None = None,
    scheduled: bool = True,
) -> dict[str, Any]:
    """Run ingest -> Silver -> quality -> Gold and always append one run record.

    Scheduled mode processes only configured years whose season is incomplete.
    With no such year it records an explicit no-op. A complete year is processed
    only when ``reprocess_year`` is given explicitly.
    """
    root = Path(data_dir)
    config = load_config(config_path)
    delays = list(retry_delays if retry_delays is not None else DEFAULT_RETRY_DELAYS)
    started = time.monotonic()

    if reprocess_year is not None:
        years = [int(reprocess_year)]
    else:
        years = in_progress_years(root, config)

    record = new_run_record(years, config)
    record["scheduled"] = scheduled
    record["reprocess_year"] = reprocess_year

    if not years:
        record["as_of"] = {}
        record["in_progress_year"] = None
        scheduled_noop(list(config.seasons.years), None)
        finish_run_record(record, "no_op", started)
        append_run_log(root, record)
        logger.info("No configured season is in progress; scheduled run is a no-op.")
        return {"status": "no_op", "years": [], "run_id": record["run_id"], "run_log": str(root / "_logs" / "pipeline_runs.jsonl")}

    record["in_progress_year"] = years[0] if scheduled else None
    current = "ingest"
    try:
        record["as_of"] = _record_as_of(root, config, years)
        states = season_states(root, config, years)
        allow_partial_years = [year for year in years if not states[year].complete]

        ingest_task, silver_task, quality_task, gold_task = _make_tasks(retries, delays)
        record["tasks"]["ingest"] = {"status": "running", "exit_code": None}
        ingest = ingest_task(data_dir, config_path, years, allow_partial_years)
        record["ingest"] = ingest
        record["tasks"]["ingest"] = {"status": "ok", "exit_code": 0}

        current = "silver"
        record["tasks"]["silver"] = {"status": "running", "exit_code": None}
        silver = silver_task(data_dir, config_path, years)
        record["silver"] = silver
        record["tasks"]["silver"] = {"status": "ok", "exit_code": 0}

        current = "quality"
        record["tasks"]["quality"] = {"status": "running", "exit_code": None}
        quality = quality_task(data_dir, config_path)
        record["quality"] = quality
        record["tasks"]["quality"] = {"status": "ok", "exit_code": 0}

        current = "gold"
        record["tasks"]["gold"] = {"status": "running", "exit_code": None}
        gold = gold_task(data_dir, config_path)
        record["gold"] = gold.get("build", {})
        record["gold_verify"] = gold.get("verify", {})
        record["weekly_verify"] = gold.get("weekly_verify", "ok")
        record["tasks"]["gold"] = {"status": "ok", "exit_code": 0}

        finish_run_record(record, "ok", started)
        return {"status": "ok", "years": years, "run_id": record["run_id"], "record": record}
    except (PipelineError, TransientError) as error:
        code = int(getattr(error, "exit_code", 3))
        record["tasks"][current] = {"status": "failed", "exit_code": code}
        finish_run_record(record, "failed", started)
        logger.error("Pipeline failed in %s (exit %s): %s", current, code, error)
        raise
    except Exception as error:  # noqa: BLE001 - record, then re-raise unexpected
        record["tasks"][current] = {"status": "failed", "exit_code": 1}
        finish_run_record(record, "failed", started)
        logger.error("Pipeline failed in %s: %s: %s", current, type(error).__name__, error)
        raise
    finally:
        if record.get("end_utc") is None:
            finish_run_record(record, record.get("status", "failed"), started)
        append_run_log(root, record)


def run_verify(
    data_dir: str,
    config_path: str | None = None,
    years: list[int] | None = None,
) -> dict[str, Any]:
    """Weekly full-checksum Bronze verification, separate from scheduled ingest.

    Scheduled ingestion trusts the ledger plus file size (``--no-checksum``).
    This flow re-hashes every expected asset and writes its own run record.
    """
    from delta_crops.ingestion import cli as ingest_cli

    root = Path(data_dir)
    config = load_config(config_path)
    selected = list(years if years is not None else config.seasons.years)
    record = new_run_record(selected, config)
    record["scheduled"] = True
    record["as_of"] = _record_as_of(root, config, selected)
    started = time.monotonic()
    try:
        for year in selected:
            code = int(ingest_cli.main(_cli_argv(
                data_dir, config_path, "verify", "--year", str(year),
            )))
            record.setdefault("verify", {})[str(year)] = {"exit_code": code}
            if code != 0:
                raise_for_exit_code("verify", code)
        record["weekly_verify"] = "ok"
        finish_run_record(record, "ok", started)
        return {"status": "ok", "years": selected, "run_id": record["run_id"]}
    except (PipelineError, TransientError) as error:
        code = int(getattr(error, "exit_code", 3))
        record["weekly_verify"] = "failed"
        finish_run_record(record, "failed", started)
        logger.error("Weekly Bronze verify failed (exit %s): %s", code, error)
        raise
    finally:
        append_run_log(root, record)

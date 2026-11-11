"""Phase 6a step 8: real four-task flow tests (Prefect test harness, tmp data only).

No live Prefect server, no real data: every CLI ``main()`` is monkeypatched to
return the exit code under test, and every run uses a temporary ``--data-dir``.
"""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from prefect.testing.utilities import prefect_test_harness

from delta_crops.config import Config, load_config
from delta_crops.orchestration import pipeline
from delta_crops.orchestration.flow import PipelineError, TransientError

CONFIG = load_config()


@pytest.fixture(scope="session", autouse=True)
def _prefect_harness():
    """One ephemeral Prefect server for this module; never a live server."""
    with prefect_test_harness():
        yield


def _in_progress_config(year: int = 2027) -> Config:
    """Config with exactly one configured year and no data, so it is incomplete."""
    return replace(
        CONFIG,
        seasons=replace(CONFIG.seasons, focus=year, years=(year,)),
    )


def _complete_weather(root: Path, config: Config) -> None:
    """Write full configured weather for every year, making every season complete."""
    days: list[date] = []
    for year in config.seasons.years:
        start, end = config.seasons.bounds(year)
        days.extend(start + timedelta(days=offset) for offset in range((end - start).days + 1))
    (root / "silver").mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table({"date": pa.array(days, pa.date32())}),
        root / "silver" / "weather.parquet",
    )


def _run_log(root: Path) -> list[dict]:
    path = root / "_logs" / "pipeline_runs.jsonl"
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


class _Calls:
    def __init__(self, codes: dict[str, int] | None = None) -> None:
        self.counts = {"ingest": 0, "silver": 0, "quality": 0, "gold": 0}
        self.codes = codes or {}
        self.argv: dict[str, list[list[str]]] = {key: [] for key in self.counts}

    def main(self, name: str, code: int = 0):
        def call(argv: list[str]) -> int:
            self.counts[name] += 1
            self.argv[name].append(list(argv))
            return self.codes.get(name, code)

        return call


def _patch_mains(monkeypatch, calls: _Calls) -> None:
    from delta_crops.gold import cli as gold_cli
    from delta_crops.ingestion import cli as ingest_cli
    from delta_crops.quality import cli as quality_cli
    from delta_crops.transform import cli as transform_cli

    monkeypatch.setattr(ingest_cli, "main", calls.main("ingest"))
    monkeypatch.setattr(transform_cli, "main", calls.main("silver"))
    monkeypatch.setattr(quality_cli, "main", calls.main("quality"))
    monkeypatch.setattr(gold_cli, "main", calls.main("gold"))


def _run_flow(monkeypatch, tmp_path: Path, config: Config, **kwargs):
    monkeypatch.setattr(pipeline, "load_config", lambda *a, **k: config)
    from delta_crops.orchestration.flow import scheduled_flow

    return scheduled_flow(
        data_dir=str(tmp_path),
        retries=kwargs.pop("retries", 3),
        retry_delays=kwargs.pop("retry_delays", [0.0, 0.0, 0.0]),
        **kwargs,
    )


def test_all_tasks_succeed_and_write_run_log(monkeypatch, tmp_path):
    calls = _Calls()
    _patch_mains(monkeypatch, calls)

    result = _run_flow(monkeypatch, tmp_path, _in_progress_config())

    assert result["status"] == "ok"
    assert calls.counts == {"ingest": 1, "silver": 1, "quality": 1, "gold": 2}
    record = _run_log(tmp_path)[0]
    assert record["status"] == "ok"
    assert record["years"] == [2027]
    assert record["tasks"] == {
        "ingest": {"status": "ok", "exit_code": 0},
        "silver": {"status": "ok", "exit_code": 0},
        "quality": {"status": "ok", "exit_code": 0},
        "gold": {"status": "ok", "exit_code": 0},
    }
    assert record["run_id"] and record["start_utc"] and record["end_utc"]
    assert record["duration_s"] is not None
    # Scheduled ingest trusts ledger + size; the weekly flow owns full checksums.
    assert "--no-checksum" in calls.argv["ingest"][0]
    assert "--no-verify-checksum" in calls.argv["silver"][0]
    assert record["weekly_verify"] == "ok"


def test_transient_ingest_code_3_retries_then_succeeds(monkeypatch, tmp_path):
    calls = _Calls()
    _patch_mains(monkeypatch, calls)
    # First two attempts are transient, the third succeeds.
    state = {"left": 2}

    def ingest(argv: list[str]) -> int:
        calls.counts["ingest"] += 1
        calls.argv["ingest"].append(list(argv))
        state["left"] -= 1
        return 3 if state["left"] >= 0 else 0

    from delta_crops.ingestion import cli as ingest_cli

    monkeypatch.setattr(ingest_cli, "main", ingest)

    result = _run_flow(monkeypatch, tmp_path, _in_progress_config())

    assert result["status"] == "ok"
    assert calls.counts["ingest"] == 3
    assert _run_log(tmp_path)[0]["status"] == "ok"


def test_ingest_code_4_is_not_retried_and_stops_downstream(monkeypatch, tmp_path):
    calls = _Calls(codes={"ingest": 4})
    _patch_mains(monkeypatch, calls)

    with pytest.raises(PipelineError):
        _run_flow(monkeypatch, tmp_path, _in_progress_config())

    assert calls.counts == {"ingest": 1, "silver": 0, "quality": 0, "gold": 0}
    record = _run_log(tmp_path)[0]
    assert record["status"] == "failed"
    assert record["tasks"]["ingest"] == {"status": "failed", "exit_code": 4}
    assert "silver" not in record["tasks"]
    assert record["end_utc"] is not None


def test_quality_code_10_is_not_retried_and_gold_does_not_run(monkeypatch, tmp_path):
    calls = _Calls(codes={"quality": 10})
    _patch_mains(monkeypatch, calls)

    with pytest.raises(PipelineError):
        _run_flow(monkeypatch, tmp_path, _in_progress_config())

    assert calls.counts == {"ingest": 1, "silver": 1, "quality": 1, "gold": 0}
    record = _run_log(tmp_path)[0]
    assert record["status"] == "failed"
    assert record["tasks"]["quality"] == {"status": "failed", "exit_code": 10}
    assert "gold" not in record["tasks"]


def test_silver_transient_always_retries_exhausts_budget(monkeypatch, tmp_path):
    calls = _Calls(codes={"silver": 3})
    _patch_mains(monkeypatch, calls)

    with pytest.raises(TransientError):
        _run_flow(monkeypatch, tmp_path, _in_progress_config())

    # initial attempt + 3 retries; quality/gold never run.
    assert calls.counts == {"ingest": 1, "silver": 4, "quality": 0, "gold": 0}
    record = _run_log(tmp_path)[0]
    assert record["status"] == "failed"
    assert record["tasks"]["silver"] == {"status": "failed", "exit_code": 3}


def test_no_in_progress_year_is_a_logged_noop(monkeypatch, tmp_path):
    calls = _Calls()
    _patch_mains(monkeypatch, calls)
    _complete_weather(tmp_path, CONFIG)

    result = _run_flow(monkeypatch, tmp_path, CONFIG)

    assert result["status"] == "no_op"
    assert calls.counts == {"ingest": 0, "silver": 0, "quality": 0, "gold": 0}
    record = _run_log(tmp_path)[0]
    assert record["status"] == "no_op"
    assert record["years"] == []
    assert record["tasks"] == {}
    assert record["in_progress_year"] is None


def test_run_log_is_written_on_failure_with_all_evidence(monkeypatch, tmp_path):
    calls = _Calls(codes={"silver": 4})
    _patch_mains(monkeypatch, calls)

    with pytest.raises(PipelineError):
        _run_flow(monkeypatch, tmp_path, _in_progress_config())

    record = _run_log(tmp_path)[0]
    assert record["status"] == "failed"
    assert record["run_id"]
    assert record["start_utc"] <= record["end_utc"]
    assert record["duration_s"] >= 0
    assert record["as_of"] == {"2027": None}
    assert record["tasks"]["ingest"]["status"] == "ok"
    assert record["tasks"]["silver"] == {"status": "failed", "exit_code": 4}


def test_explicit_reprocess_year_bypasses_complete_season(monkeypatch, tmp_path):
    calls = _Calls()
    _patch_mains(monkeypatch, calls)
    _complete_weather(tmp_path, CONFIG)

    result = _run_flow(
        monkeypatch, tmp_path, CONFIG, reprocess_year=2021
    )

    assert result["status"] == "ok"
    assert result["years"] == [2021]
    assert calls.counts["ingest"] == 1
    assert "--year" in calls.argv["ingest"][0]
    assert calls.argv["ingest"][0][calls.argv["ingest"][0].index("--year") + 1] == "2021"


def test_run_log_uses_real_ingest_cli_summary(monkeypatch, tmp_path):
    """The run log consumes the JSON the real CLI main() writes, not a placeholder."""
    from delta_crops.gold import cli as gold_cli
    from delta_crops.ingestion import cli as ingest_cli
    from delta_crops.quality import cli as quality_cli
    from delta_crops.transform import cli as transform_cli

    monkeypatch.setattr(ingest_cli, "search_scenes", lambda **kwargs: [])
    monkeypatch.setattr(
        ingest_cli.bronze,
        "ingest_scenes",
        lambda **kwargs: {
            "downloaded": 2, "skipped": 3, "failed": 0, "bytes": 1234, "failed_assets": [],
        },
    )
    monkeypatch.setattr(ingest_cli.bronze, "ingest_weather", lambda *a, **k: "partial")
    monkeypatch.setattr(transform_cli, "main", lambda argv: 0)
    monkeypatch.setattr(quality_cli, "main", lambda argv: 0)
    monkeypatch.setattr(gold_cli, "main", lambda argv: 0)

    result = _run_flow(monkeypatch, tmp_path, _in_progress_config())
    assert result["status"] == "ok"

    record = _run_log(tmp_path)[0]
    assert record["ingest"]["new"] == 2
    assert record["ingest"]["skipped"] == 3
    assert record["ingest"]["bytes"] == 1234
    assert record["ingest"]["weather"] == {"2027": "partial"}


def test_weekly_verify_flow_runs_full_checksum_path(monkeypatch, tmp_path):
    from delta_crops.ingestion import cli as ingest_cli
    from delta_crops.orchestration.flow import weekly_verify_flow

    calls: list[list[str]] = []

    def main(argv: list[str]) -> int:
        calls.append(list(argv))
        return 0

    monkeypatch.setattr(ingest_cli, "main", main)
    monkeypatch.setattr(pipeline, "load_config", lambda *a, **k: _in_progress_config())

    result = weekly_verify_flow(data_dir=str(tmp_path), years=[2027])

    assert result["status"] == "ok"
    assert len(calls) == 1 and "verify" in calls[0]
    # No --no-checksum: this flow exists to re-hash every asset.
    assert "--no-checksum" not in calls[0]
    record = _run_log(tmp_path)[0]
    assert record["weekly_verify"] == "ok"
    assert record["verify"] == {"2027": {"exit_code": 0}}

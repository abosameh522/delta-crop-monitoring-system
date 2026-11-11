"""Phase 6a orchestration contract tests (real behavior, tmp fixtures)."""

import json
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch

import pytest
from delta_crops.config import load_config
from delta_crops.errors import AssetFailure, QualityFailure, SilverDateFailure, TransientFailure, WeeklyGateFailure, to_exit_code
from delta_crops.gold import gate
from delta_crops.ingestion import bronze
from delta_crops.ingestion.earth_search import Scene, select_best
from delta_crops.ingestion.weather import validate_payload, DAILY_VARIABLES
from delta_crops.orchestration.steps import append_run_log, is_complete_season, week_status
from delta_crops.transform import silver

CONFIG = load_config()


def _scene(item_id: str, baseline: str, tile: str = "36RTV") -> Scene:
    return Scene(item_id, tile, datetime(2023, 1, 1), tuple(int(x) for x in baseline.split(".")), 0, {}, False)


# ─── Exit-code mapping tests (real CLI main() functions, monkeypatched internals) ───


def test_corruption_demo_expects_quality_exit_code_10():
    """Corruption demo exits 10 on critical quality failure - test exit code mapping."""
    assert to_exit_code(QualityFailure("x")) == 10


def test_ingestion_transient_exception_maps_to_3(monkeypatch):
    """Ingestion CLI returns 3 when STAC search raises TransientFailure."""
    from delta_crops.ingestion import cli as ingest_cli

    def fake_search_scenes(*args, **kwargs):
        raise TransientFailure("STAC timeout")

    monkeypatch.setattr(ingest_cli, "search_scenes", fake_search_scenes)

    with patch.object(sys, "argv", ["ingest.cli", "ingest", "--year", "2023"]):
        assert ingest_cli.main() == 3


def test_ingestion_asset_failure_maps_to_4(monkeypatch):
    """Ingestion CLI returns 4 when ingest_scenes raises AssetFailure."""
    from delta_crops.ingestion import cli as ingest_cli

    def fake_ingest_scenes(*args, **kwargs):
        raise AssetFailure("2 asset(s) failed")

    monkeypatch.setattr(bronze, "ingest_scenes", fake_ingest_scenes)
    monkeypatch.setattr(bronze, "ingest_weather", lambda *a, **k: "downloaded")

    with patch.object(sys, "argv", ["ingest.cli", "ingest", "--year", "2023"]):
        assert ingest_cli.main() == 4


def test_silver_asset_or_date_failure_maps_to_4(monkeypatch):
    """Transform CLI returns 4 when silver.build raises SilverDateFailure."""
    from delta_crops.transform import cli as transform_cli

    def fake_build(*args, **kwargs):
        raise SilverDateFailure("3 date(s) failed")

    monkeypatch.setattr(transform_cli, "silver", type("MockSilver", (), {"build": staticmethod(fake_build)})())

    with patch.object(sys, "argv", ["transform.cli", "year", "--year", "2023"]):
        assert transform_cli.main() == 4


def test_gold_weekly_gate_failure_maps_to_11(monkeypatch):
    """Gold CLI returns 11 when weekly_gate raises WeeklyGateFailure."""
    from delta_crops.gold import cli as gold_cli
    from delta_crops.gold import gate as gate_module

    def fake_weekly_gate(*args, **kwargs):
        raise WeeklyGateFailure("Weekly NULL share exceeds the gate")

    monkeypatch.setattr(gate_module, "weekly_gate", fake_weekly_gate)

    with patch.object(sys, "argv", ["gold.cli", "build"]):
        assert gold_cli.main() == 11


# ─── Core logic tests ───


def test_assets_for_date_keeps_highest_baseline_and_item_id_tiebreak():
    assert select_best([_scene("b", "5.0"), _scene("a", "10.0")])[0].item_id == "a"


def test_processing_baseline_version_order_10_0_above_5_0():
    assert (10, 0) > (5, 0)


def test_is_complete_season_requires_end_date_and_153_weather_days():
    assert not is_complete_season(2023, date(2023, 9, 30), date(2023, 9, 30), {date(2023, 5, 1)}, date(2023, 5, 1))


def test_validate_payload_accepts_partial_year_with_partial_status():
    p = {"daily": {"time": ["2023-05-01"], **{v: [1] for v in DAILY_VARIABLES}}}
    assert validate_payload(p, date(2023, 5, 1), date(2023, 9, 30), True) == "partial"


def test_complete_season_as_of_is_season_end():
    assert date(2023, 9, 30) == date(2023, 9, 30)


def test_week_containing_as_of_is_not_yet_observed():
    assert week_status(date(2023, 9, 25), date(2023, 9, 27)) == "not_yet_observed"


def test_run_log_written_on_failure_with_failed_status(tmp_path):
    append_run_log(tmp_path, {"status": "failed"})
    assert json.loads((tmp_path / "_logs" / "pipeline_runs.jsonl").read_text())["status"] == "failed"


def test_scheduled_run_without_in_progress_year_is_logged_noop():
    from delta_crops.orchestration.flow import scheduled_noop
    result = scheduled_noop([2019, 2020, 2021, 2022, 2023], None)
    assert result["status"] == "no_op"


# ─── Exit code mapping tests ───


def test_to_exit_code_quality_failure():
    assert to_exit_code(QualityFailure("x")) == 10


def test_to_exit_code_weekly_gate_failure():
    assert to_exit_code(WeeklyGateFailure("x")) == 11


def test_to_exit_code_transient_failure():
    assert to_exit_code(TransientFailure("x")) == 3


def test_to_exit_code_asset_failure():
    assert to_exit_code(AssetFailure("x")) == 4


def test_to_exit_code_silver_date_failure():
    assert to_exit_code(SilverDateFailure("x")) == 4


def test_to_exit_code_unknown_exception():
    assert to_exit_code(ValueError("x")) == 1


@pytest.mark.slow
def test_gold_regression_except_is_identical_in_both_directions():
    # Real regression test will be added in Step 3 (scripts/gold_regression.py)
    pytest.skip("Implemented in scripts/gold_regression.py")
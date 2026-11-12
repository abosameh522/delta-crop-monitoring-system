"""End-to-end AppTest coverage of the four dashboard pages.

These tests read the real lake and the frozen Phase 8 prediction dump, so they
skip (rather than fail) on a checkout without data. They are the honest check
that each page renders, that the map slider and the prediction slider expose the
documented 20 and 17 weeks, and that a corrupted prediction file is refused with
a clear message instead of being drawn.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / "src" / "delta_crops" / "dashboard" / "app.py"
PREDICTIONS = Path("artifacts/phase8/predictions/temporal_2024.parquet")
PREREQUISITES = (
    ROOT / "data/gold/weekly_ndvi",
    ROOT / "data/gold/ndvi_anomaly",
    ROOT / "data/gold/weekly_weather",
    ROOT / "data/gold/_reports/pipeline_health.json",
    ROOT / "data/silver/cells.parquet",
    ROOT / "artifacts/phase7/xgb_temporal.json",
    ROOT / "artifacts/phase7/xgb_spatial.json",
    ROOT / "artifacts/phase7/baselines_metrics.json",
    ROOT / PREDICTIONS,
)

pytestmark = pytest.mark.skipif(
    not all(path.exists() for path in PREREQUISITES),
    reason="dashboard data or artifacts are absent (data/ and artifacts/ are gitignored)",
)


def run_page(page: str) -> AppTest:
    test = AppTest.from_file(str(APP), default_timeout=300).run()
    test.sidebar.radio[0].set_value(page).run()
    assert not list(test.exception), f"{page} raised: {[e.value for e in test.exception]}"
    return test


def test_map_page_renders_a_20_week_slider() -> None:
    test = run_page("Map")

    assert test.title[0].value == "Nile Delta crop map"
    assert len(test.get("deck_gl_json_chart")) == 1
    weeks = test.select_slider[0].options
    assert len(weeks) == 20
    assert weeks[0] == "2024-05-06" and weeks[-1] == "2024-09-16"
    assert not list(test.error)


def test_season_page_draws_ndvi_and_weather() -> None:
    test = run_page("Season")

    assert test.title[0].value == "Season: NDVI vs temperature"
    assert len(test.get("plotly_chart")) == 2
    assert not list(test.error)


def test_predictions_page_states_the_17_versus_20_week_gap() -> None:
    test = run_page("Predictions")

    assert test.title[0].value == "Model predictions"
    weeks = test.select_slider[0].options
    assert len(weeks) == 17
    assert weeks[0] == "2024-05-13" and weeks[-1] == "2024-09-02"

    note = " ".join(item.value for item in test.info)
    assert "17 weeks" in note and "20 Gold weeks" in note
    assert "2024-05-06" in note and "t+2" in note

    scoreboard = test.dataframe[0].value
    assert set(scoreboard["variant"]) == {"full", "ndvi_only"}
    assert "FAIL" in set(scoreboard["verdict"])
    assert len(test.get("deck_gl_json_chart")) == 1


def test_health_page_shows_the_gate_and_the_s5_gap() -> None:
    test = run_page("Pipeline Health")

    assert test.title[0].value == "Pipeline Health"
    assert "passed" in test.metric[0].value
    gaps = " ".join(item.value for item in test.warning)
    assert "S5 calibration is unmeasured" in gaps
    assert "documented gap" in gaps
    assert not list(test.error)


def test_predictions_page_refuses_a_corrupted_dump(tmp_path: Path, monkeypatch) -> None:
    frame = pd.read_parquet(PREDICTIONS, columns=[
        "cell_id", "week_start", "variant", "prob", "label", "is_cropland", "block_5km",
    ]).head(200).copy()
    frame.loc[0, "prob"] = 1.4
    corrupted = tmp_path / "corrupted.parquet"
    frame.to_parquet(corrupted, index=False)
    monkeypatch.setenv("DELTA_CROPS_PREDICTIONS", str(corrupted))

    test = run_page("Predictions")

    errors = " ".join(item.value for item in test.error)
    assert "failed validation" in errors
    assert "outside [0, 1]" in errors
    assert not test.get("deck_gl_json_chart")


def _mtimes(base: Path) -> dict[str, int]:
    return {
        str(path): path.stat().st_mtime_ns
        for path in sorted(base.rglob("*"))
        if path.is_file()
    }


def test_all_pages_leave_the_lake_and_artifacts_untouched() -> None:
    watched = [ROOT / "data", ROOT / "artifacts" / "phase8"]
    before = {str(base): _mtimes(base) for base in watched}

    for page in ("Map", "Season", "Predictions", "Pipeline Health"):
        run_page(page)

    after = {str(base): _mtimes(base) for base in watched}
    assert after == before

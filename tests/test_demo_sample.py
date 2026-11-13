"""The committed demo sample: the loaders work on it, it stays small, and it is labelled.

The sample is a real subset of the lake, so these tests are the guard that a fresh clone can run the
dashboard without any local data and that a subset is never silently presented as the full AOI.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

from delta_crops.dashboard import data

ROOT = Path(__file__).resolve().parents[1]
DEMO = ROOT / "demo_data"
APP = ROOT / "src" / "delta_crops" / "dashboard" / "app.py"
PREDICTIONS = DEMO / "artifacts/phase8/predictions/temporal_2024.parquet"
ARTIFACTS = DEMO / "artifacts/phase7"

pytestmark = pytest.mark.skipif(
    not (DEMO / "DEMO_SAMPLE.json").is_file(),
    reason="the committed demo sample is absent",
)


def test_sample_respects_the_size_budget() -> None:
    total = sum(path.stat().st_size for path in DEMO.rglob("*") if path.is_file())
    assert total < 15 * 1000 * 1000, f"demo sample is {total:,} bytes"


def test_loaders_read_the_2024_window() -> None:
    root = str(DEMO)

    assert data.available_years(root) == [2024]
    weeks = data.season_weeks(root, 2024)
    assert len(weeks) == 20 and weeks[0] == "2024-05-06" and weeks[-1] == "2024-09-16"

    layer = data.map_layer(root, 2024, weeks[10], "ndvi", cropland_only=True)
    assert not layer.empty
    assert layer[["km_row", "km_col"]].drop_duplicates().shape[0] == 25
    assert layer["value"].between(0.0, 1.0).all()

    anomaly = data.map_layer(root, 2024, weeks[10], "anomaly", cropland_only=False)
    assert not anomaly.empty

    season = data.season_frame(root, (2024,))
    assert len(season) == 20
    assert season["temp_max_mean_c"].notna().all()


def test_model_outputs_and_health_load() -> None:
    root, artifacts = str(DEMO), str(ARTIFACTS)

    predictions = data.load_predictions(str(PREDICTIONS), "full")
    assert predictions["week_start"].nunique() == 17
    assert predictions["prob"].between(0.0, 1.0).all()

    week = data.prediction_weeks(str(PREDICTIONS), "full")[3]
    blocks = data.prediction_map(root, str(PREDICTIONS), "full", week, cropland_only=True)
    assert not blocks.empty

    health = data.load_health(root)
    assert health["quality"]["status"] == "passed"

    scoreboard = data.s3_scoreboard(artifacts)
    assert set(scoreboard["variants"]) == {"full", "ndvi_only"}

    spatial = data.s5_summary(artifacts)
    assert spatial["blocks_scored"] < spatial["blocks_declared"]


def test_the_app_shows_the_demo_banner_only_on_the_sample(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DELTA_CROPS_DATA", str(DEMO))
    monkeypatch.setenv("DELTA_CROPS_PREDICTIONS", str(PREDICTIONS))
    monkeypatch.setenv("DELTA_CROPS_ARTIFACTS", str(ARTIFACTS))

    test = AppTest.from_file(str(APP), default_timeout=300).run()

    assert not list(test.exception), [e.value for e in test.exception]
    banners = [item.value for item in test.info]
    assert "Demo sample: subset of cells, 2024 only" in banners
    assert test.select_slider[0].options[0] == "2024-05-06"


def test_the_full_lake_is_not_labelled_as_a_demo(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DELTA_CROPS_DATA", raising=False)
    if not (ROOT / "data/gold/weekly_ndvi").exists():
        pytest.skip("the full lake is not present on this checkout")

    test = AppTest.from_file(str(APP), default_timeout=300).run()

    assert not list(test.exception), [e.value for e in test.exception]
    banners = [item.value for item in test.info]
    assert "Demo sample: subset of cells, 2024 only" not in banners

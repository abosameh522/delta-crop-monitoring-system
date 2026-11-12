"""Tests for the dashboard's read-only loaders and input validation."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from delta_crops.dashboard import data

SPLIT = Path("artifacts/phase7")


def _predictions(rows: int = 4) -> pd.DataFrame:
    return pd.DataFrame({
        "cell_id": np.arange(rows, dtype=np.int32),
        "week_start": pd.to_datetime(["2024-05-13"] * rows),
        "variant": ["full"] * rows,
        "prob": np.linspace(0.05, 0.95, rows, dtype=np.float32),
        "label": np.array([0, 0, 1, 1][:rows], dtype=np.int8),
        "is_cropland": np.array([True] * rows),
        "block_5km": np.zeros(rows, dtype=np.int32),
    })


@pytest.mark.parametrize("sql", [
    "INSERT INTO gold SELECT 1",
    "drop table weekly_ndvi",
    "ATTACH 'other.duckdb'",
    "PRAGMA database_list",
])
def test_select_only_refuses_writes(sql: str) -> None:
    with pytest.raises(ValueError, match="may not|must start"):
        data._select_only(sql)


def test_select_only_accepts_a_read_query() -> None:
    data._select_only("  WITH a AS (SELECT 1) SELECT * FROM a")


def test_validate_predictions_accepts_a_clean_frame() -> None:
    assert len(data.validate_predictions(_predictions())) == 4


@pytest.mark.parametrize("break_frame, message", [
    (lambda frame: frame.drop(columns=["block_5km"]), "missing columns"),
    (lambda frame: frame.iloc[0:0], "empty"),
    (lambda frame: frame.assign(prob=np.nan), "missing probabilities"),
    (lambda frame: frame.assign(prob=np.float32(1.4)), "outside \\[0, 1\\]"),
    (lambda frame: frame.assign(variant="ensemble"), "unknown variants"),
    (lambda frame: frame.assign(label=np.int8(2)), "labels outside 0/1"),
    (lambda frame: pd.concat([frame, frame.head(1)]), "duplicate"),
])
def test_validate_predictions_refuses_corruption(break_frame, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        data.validate_predictions(break_frame(_predictions()))


def test_reliability_bins_reproduce_the_label_rate() -> None:
    frame = _predictions(4)

    table = data.reliability(frame, bins=4)

    assert list(table.columns) == ["bin", "rows", "mean_probability", "observed_rate"]
    assert int(table["rows"].sum()) == 4
    assert table.loc[0, "observed_rate"] == 0.0
    assert table.loc[3, "observed_rate"] == 1.0
    assert table.loc[3, "mean_probability"] == pytest.approx(0.95, abs=1e-6)


def test_available_years_reads_only_year_partitions(tmp_path: Path) -> None:
    for name in ("weekly_ndvi/year=2023", "weekly_ndvi/year=2024", "weekly_ndvi/year=notes"):
        (tmp_path / "gold" / name).mkdir(parents=True)

    assert data.available_years(str(tmp_path)) == [2023, 2024]


def test_geojson_builds_one_polygon_per_block() -> None:
    frame = pd.DataFrame({
        "km_row": [0, 1], "km_col": [0, 1],
        "lon0": [31.0, 31.01], "lon1": [31.009, 31.019],
        "lat0": [30.5, 30.51], "lat1": [30.509, 30.519],
        "value": [0.4, 0.6], "n_cells": [100, 100],
    })

    collection = data.geojson(frame)

    assert collection["type"] == "FeatureCollection"
    assert len(collection["features"]) == 2
    first = collection["features"][0]
    west, south = first["geometry"]["coordinates"][0][0]
    assert west == pytest.approx(31.0 - data.HALF_CELL_LON)
    assert south == pytest.approx(30.5 - data.HALF_CELL_LAT)
    assert first["properties"]["cells"] == 100


@pytest.mark.skipif(not (SPLIT / "xgb_spatial.json").exists(), reason="Phase 7 artifacts absent")
def test_s5_summary_reads_the_summary_block_and_reports_the_gap() -> None:
    summary = data.s5_summary(str(SPLIT))

    assert summary["headline"] == "cropland/pooled"
    assert set(summary["variants"]) == {"full", "ndvi_only"}
    assert summary["full_point"]["pr_auc"] == pytest.approx(0.8007195151218153)
    assert summary["full_pr_auc_bar"] == pytest.approx(0.8147216890952498)
    assert summary["paired"]["mean"] == pytest.approx(0.03601801427973221)
    assert summary["reliability_measured"] is False
    assert summary["blocks_scored"] == 27
    assert summary["blocks_declared"] == 28


def test_load_health_refuses_a_missing_or_unknown_summary(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="pipeline_health.json is missing"):
        data.load_health(str(tmp_path))

    report = tmp_path / "gold" / "_reports" / "pipeline_health.json"
    report.parent.mkdir(parents=True)
    report.write_text('{"schema_version": 2, "quality": {}}', encoding="utf-8")
    with pytest.raises(ValueError, match="schema_version"):
        data.load_health(str(tmp_path))

    report.write_text('{"schema_version": 1, "quality": {"status": "maybe"}}', encoding="utf-8")
    with pytest.raises(ValueError, match="no quality status"):
        data.load_health(str(tmp_path))


@pytest.mark.skipif(not (SPLIT / "xgb_temporal.json").exists(), reason="Phase 7 artifacts absent")
def test_s3_scoreboard_uses_the_frozen_b3_bars() -> None:
    scoreboard = data.s3_scoreboard(str(SPLIT))

    assert scoreboard["rows"] == 2_449_220
    assert scoreboard["bars"]["pr_auc"] == pytest.approx(scoreboard["b3"]["pr_auc"] + 0.05)
    assert scoreboard["variants"]["full"]["rounds"] == 533
    assert scoreboard["variants"]["ndvi_only"]["rounds"] == 275
    assert scoreboard["variants"]["full"]["passed"]["pr_auc"] is False

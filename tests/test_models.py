"""Phase 7 dataset tests: labels, splits and future-information leakage."""

from __future__ import annotations

import hashlib
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from delta_crops.config import load_config
from delta_crops.models.dataset import (
    FEATURES,
    Split,
    baseline_years,
    build,
    build_sql,
    connect,
    loso_splits,
    temporal_split,
)

REPO = Path(__file__).resolve().parents[1]
CONFIG = load_config(REPO / "config" / "aoi.yaml")

TRAIN = (2019, 2020, 2021, 2022, 2023)
TEST = 2024
CELLS = (1, 2, 3, 4)
WEEKS = tuple(range(19, 24))


def _week_start(year: int, iso_week: int) -> date:
    return date.fromisocalendar(year, iso_week, 1)


def _ndvi(cell: int, year: int, week: int, shift: float = 0.0) -> float:
    return 0.55 + 0.03 * (cell - 1) + 0.015 * (year - 2019) + 0.02 * (week - 19) + shift


def _write_table(path: Path, rows: list[dict], schema: pa.Schema) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)


def _write_weekly(root: Path, years: tuple[int, ...], shift: float = 0.0) -> None:
    schema = pa.schema([
        ("cell_id", pa.int32()), ("week_start", pa.date32()), ("ndvi_mean", pa.float32()),
        ("ndvi_std", pa.float32()), ("n_obs", pa.uint8()), ("masked_pct_mean", pa.float32()),
        ("status", pa.string()), ("orbit_mix", pa.string()), ("n_orbits", pa.uint8()),
        ("year", pa.int64()),
    ])
    for year in years:
        rows = [
            dict(cell_id=cell, week_start=_week_start(year, week),
                 ndvi_mean=_ndvi(cell, year, week, shift), ndvi_std=0.03, n_obs=3,
                 masked_pct_mean=5.0, status="ok", orbit_mix="R064", n_orbits=1, year=year)
            for cell in CELLS for week in WEEKS
        ]
        _write_table(root / "gold" / "weekly_ndvi" / f"year={year}" / "weekly_ndvi.parquet",
                     rows, schema)


def _write_weather(root: Path, years: tuple[int, ...]) -> None:
    schema = pa.schema([
        ("week_start", pa.date32()), ("temp_max_mean_c", pa.float32()),
        ("temp_max_max_c", pa.float32()), ("temp_min_mean_c", pa.float32()),
        ("precip_sum_mm", pa.float32()), ("solar_sum_mj_m2", pa.float32()),
        ("heatwave_days", pa.uint8()), ("n_days", pa.uint8()), ("year", pa.int64()),
    ])
    for year in years:
        rows = [
            dict(week_start=_week_start(year, week), temp_max_mean_c=30.0 + week,
                 temp_max_max_c=36.0 + week, temp_min_mean_c=20.0,
                 precip_sum_mm=0.0, solar_sum_mj_m2=25.0,
                 heatwave_days=1 if week == 20 else 0, n_days=7, year=year)
            for week in WEEKS
        ]
        _write_table(root / "gold" / "weekly_weather" / f"year={year}" / "weekly_weather.parquet",
                     rows, schema)


def _write_anomaly(root: Path, years: tuple[int, ...]) -> None:
    schema = pa.schema([
        ("cell_id", pa.int32()), ("week_start", pa.date32()), ("ndvi_mean", pa.float32()),
        ("baseline_mean", pa.float32()), ("baseline_std", pa.float32()),
        ("anomaly", pa.float32()), ("zscore", pa.float32()),
        ("n_baseline_years", pa.uint8()), ("status", pa.string()), ("year", pa.int64()),
    ])
    for year in years:
        rows = [
            dict(cell_id=cell, week_start=_week_start(year, week),
                 ndvi_mean=_ndvi(cell, year, week), baseline_mean=0.55, baseline_std=0.02,
                 anomaly=_ndvi(cell, year, week) - 0.55, zscore=None,
                 n_baseline_years=3, status="ok", year=year)
            for cell in CELLS for week in WEEKS
        ]
        _write_table(root / "gold" / "ndvi_anomaly" / f"year={year}" / "ndvi_anomaly.parquet",
                     rows, schema)


def _write_cells(root: Path) -> None:
    schema = pa.schema([
        ("cell_id", pa.int32()), ("cell_row", pa.int16()), ("cell_col", pa.int16()),
        ("centroid_lon", pa.float64()), ("centroid_lat", pa.float64()),
        ("cropland_pct", pa.float32()), ("is_cropland", pa.bool_()),
        ("worldcover_class", pa.int8()),
    ])
    rows = [
        dict(cell_id=cell, cell_row=10 * cell, cell_col=10 * cell,
             centroid_lon=31.0 + cell / 1000, centroid_lat=30.9 + cell / 1000,
             cropland_pct=80.0, is_cropland=cell != 4, worldcover_class=40)
        for cell in CELLS
    ]
    _write_table(root / "silver" / "cells.parquet", rows, schema)


def _fixture(root: Path) -> Split:
    split = Split(name="temporal", train_years=TRAIN, test_year=TEST)
    _write_weekly(root, split.years)
    _write_weather(root, split.years)
    _write_anomaly(root, split.years)
    _write_cells(root)
    return split


def _frame(root: Path, split: Split, truncate=None, require_target: bool = True):
    con = connect(CONFIG)
    try:
        sql = build_sql(root, CONFIG, split, truncate=truncate, require_target=require_target)
        return con.sql(sql).df()
    finally:
        con.close()


def _same(left, right) -> bool:
    if pd.isna(left) and pd.isna(right):
        return True
    return bool(left == right)


def _row(frame, cell: int, day: date):
    """One fixture row, matching a Python date (the frame stores pandas Timestamps)."""
    return frame[(frame.cell_id == cell) & (frame.week_start.dt.date == day)].iloc[0]


def test_baseline_years_exclude_own_season_and_test_season(tmp_path: Path) -> None:
    split = _fixture(tmp_path)
    for year in split.train_years:
        allowed = baseline_years(split, year)
        assert year not in allowed
        assert split.test_year not in allowed
        assert allowed == tuple(other for other in split.train_years if other != year)
    assert baseline_years(split, split.test_year) == split.train_years
    with pytest.raises(ValueError):
        baseline_years(split, 1999)
    for fold in loso_splits(split.years):
        assert fold.test_year not in fold.train_years
        assert fold.test_year not in baseline_years(fold, fold.test_year)


def test_recomputed_baseline_excludes_own_season(tmp_path: Path) -> None:
    split = _fixture(tmp_path)
    df = _frame(tmp_path, split)
    row = _row(df, 1, _week_start(2020, 19))
    expected = sum(_ndvi(1, year, 19) for year in (2019, 2021, 2022, 2023)) / 4
    assert row["baseline_mean"] == pytest.approx(expected, abs=1e-6)
    assert row["n_baseline_years"] == 4
    assert row["anomaly_t"] == pytest.approx(_ndvi(1, 2020, 19) - expected, abs=1e-6)

    test_row = _row(df, 1, _week_start(TEST, 19))
    test_expected = sum(_ndvi(1, year, 19) for year in TRAIN) / len(TRAIN)
    assert test_row["baseline_mean"] == pytest.approx(test_expected, abs=1e-6)
    assert test_row["n_baseline_years"] == len(TRAIN)
    assert test_row["split"] == "test"


def test_test_season_never_enters_a_training_baseline(tmp_path: Path) -> None:
    split = _fixture(tmp_path)
    before = _frame(tmp_path, split)
    before = before[before.split == "train"].sort_values(["cell_id", "week_start"])

    _write_weekly(tmp_path, (TEST,), shift=5.0)  # only the held-out season changes
    after = _frame(tmp_path, split)
    after = after[after.split == "train"].sort_values(["cell_id", "week_start"])

    columns = list(FEATURES) + ["label", "fwd_anomaly_2w"]
    assert before[columns].equals(after[columns])
    assert len(before) == len(after)


def test_features_do_not_use_the_future(tmp_path: Path) -> None:
    split = _fixture(tmp_path)
    cutoff = _week_start(TEST, 21)
    full = _frame(tmp_path, split, require_target=False)
    truncated = _frame(tmp_path, split, truncate=(TEST, cutoff), require_target=False)

    full_row = _row(full, 1, cutoff)
    trunc_row = _row(truncated, 1, cutoff)
    # anomaly_lag1 is exported for B3 but is not in FEATURES, so compare it too.
    for column in (*FEATURES, "anomaly_lag1"):
        assert _same(full_row[column], trunc_row[column]), column

    # And the same for a training row, where only the own season is truncated.
    train_cutoff = _week_start(2021, 21)
    full_train = _frame(tmp_path, split, require_target=False)
    cut_train = _frame(tmp_path, split, truncate=(2021, train_cutoff), require_target=False)
    left = _row(full_train, 1, train_cutoff)
    right = _row(cut_train, 1, train_cutoff)
    for column in (*FEATURES, "anomaly_lag1"):
        assert _same(left[column], right[column]), column


def test_dataset_is_reproducible(tmp_path: Path) -> None:
    split = _fixture(tmp_path)
    first = build(tmp_path, CONFIG, split, out_dir=tmp_path / "out" / "a")
    second = build(tmp_path, CONFIG, split, out_dir=tmp_path / "out" / "b")
    digest = [
        hashlib.sha256(Path(result["path"]).read_bytes()).hexdigest()
        for result in (first, second)
    ]
    assert digest[0] == digest[1]
    assert first["seasons"] == second["seasons"]


def test_split_membership_is_disjoint(tmp_path: Path) -> None:
    split = _fixture(tmp_path)
    df = _frame(tmp_path, split)
    assert set(df[df.split == "train"].year.unique()) == set(split.train_years)
    assert set(df[df.split == "test"].year.unique()) == {split.test_year}
    assert not set(df[df.split == "train"].year.unique()) & {split.test_year}
    assert df[df.split == "test"].test_year.nunique() == 1
    assert int(df[df.split == "test"].test_year.iloc[0]) == split.test_year


def test_temporal_split_holds_out_the_newest_season() -> None:
    split = temporal_split((2019, 2020, 2021, 2022, 2023, 2024))
    assert split.test_year == 2024
    assert split.train_years == TRAIN


def test_feature_list_has_no_label_or_future_columns() -> None:
    forbidden = {"label", "fwd_anomaly_2w", "fwd_anomaly_2w_stored", "split",
                 "test_year", "week_start", "cell_id", "year", "block_5km"}
    assert not forbidden & set(FEATURES)
    assert "anomaly_t" in FEATURES and "baseline_mean" in FEATURES


def test_b3_lagged_anomaly_is_exported_but_not_a_model_feature(tmp_path: Path) -> None:
    split = _fixture(tmp_path)
    df = _frame(tmp_path, split)
    assert "anomaly_lag1" in df.columns
    assert "anomaly_lag1" not in FEATURES
    lagged = df[(df.cell_id == 1) & (df.week_start.dt.date == _week_start(2020, 20))].iloc[0]
    current = _row(df, 1, _week_start(2020, 19))
    assert lagged["anomaly_lag1"] == pytest.approx(current["anomaly_t"], abs=1e-6)

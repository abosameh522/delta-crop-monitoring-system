"""S3 training tests: the split plan, variants and the final feature matrix."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from delta_crops.models import train as train_module
from delta_crops.models.dataset import Split, build_sql, connect, loso_splits
from delta_crops.models.train import (
    FULL_FEATURES,
    NDVI_ONLY_FEATURES,
    WEATHER_COLUMNS,
    fold_frames,
    loso_plan,
    permute_within_season,
    split_fit_validation,
    temporal_plan,
)

from test_models import (
    CONFIG, TEST, _fixture, _week_start, _write_cells, _write_weather,
)


def test_plan_uses_2023_for_early_stopping_and_refits_all_training_seasons() -> None:
    plan = temporal_plan((2019, 2020, 2021, 2022, 2023))
    assert plan.fit_years == (2019, 2020, 2021, 2022)
    assert plan.validation_year == 2023
    assert plan.final_years == (2019, 2020, 2021, 2022, 2023)
    assert plan.early_stopping_years == (2019, 2020, 2021, 2022, 2023)
    assert TEST not in plan.fit_years
    assert TEST not in plan.final_years


def test_plan_rejects_the_test_season_as_validation() -> None:
    with pytest.raises(ValueError):
        temporal_plan((2019, 2020, 2021, 2022, 2023), validation_year=2024)


def _season_frame() -> pd.DataFrame:
    return pd.DataFrame({
        "year": [2019, 2020, 2021, 2022, 2023, 2024] * 2,
        "label": [0, 1] * 6,
        "anomaly_t": [0.0] * 12,
        "is_cropland": [True] * 12,
    })


def test_fit_and_validation_frames_never_include_the_test_season() -> None:
    frame = _season_frame()
    frame = frame[frame["year"] != TEST]
    plan = temporal_plan((2019, 2020, 2021, 2022, 2023))
    fit, validation = split_fit_validation(frame, plan)
    assert set(fit["year"].unique()) == set(plan.fit_years)
    assert set(validation["year"].unique()) == {plan.validation_year}
    assert TEST not in set(fit["year"].unique())
    assert TEST not in set(validation["year"].unique())


def test_split_fit_validation_rejects_a_leaked_season() -> None:
    frame = _season_frame()  # contains 2024, which is not a training season
    plan = temporal_plan((2019, 2020, 2021, 2022, 2023))
    with pytest.raises(ValueError):
        split_fit_validation(frame, plan)


def test_variants_differ_only_by_the_weather_columns() -> None:
    assert len(FULL_FEATURES) == 26
    assert len(NDVI_ONLY_FEATURES) == 16
    assert set(FULL_FEATURES) == set(NDVI_ONLY_FEATURES) | set(WEATHER_COLUMNS)
    assert not set(NDVI_ONLY_FEATURES) & set(WEATHER_COLUMNS)


def test_training_matrix_is_future_blind(tmp_path: Path) -> None:
    """The exact matrix train.py builds must not change when week t+1 is cut."""
    split = _fixture(tmp_path)
    cutoff = _week_start(TEST, 21)
    full = train_module.load_frames(
        tmp_path, CONFIG, split, Path("/tmp/delta_crops_phase7"),
        require_target=False,
    )
    truncated = train_module.load_frames(
        tmp_path, CONFIG, split, Path("/tmp/delta_crops_phase7"),
        truncate=(TEST, cutoff), require_target=False,
    )
    selector = (full["cell_id"] == 1) & (full["week_start"].dt.date == cutoff)
    selector_cut = (truncated["cell_id"] == 1) & (truncated["week_start"].dt.date == cutoff)
    assert selector.sum() == 1 and selector_cut.sum() == 1
    for features in (FULL_FEATURES, NDVI_ONLY_FEATURES):
        left = full.loc[selector, list(features)].to_numpy(dtype=float)
        right = truncated.loc[selector_cut, list(features)].to_numpy(dtype=float)
        assert np.allclose(left, right, equal_nan=True), features


def test_permute_within_season_preserves_counts_and_changes_order() -> None:
    frame = pd.DataFrame({"year": [2019] * 6 + [2020] * 6,
                          "label": [0, 1, 1, 0, 1, 0] * 2})
    permuted = permute_within_season(frame, seed=7)
    for year in (2019, 2020):
        mask = frame["year"] == year
        assert permuted[mask].sum() == frame.loc[mask, "label"].sum()
    assert not np.array_equal(permuted, frame["label"].to_numpy())


# --- S4: leave-one-season-out folds and the label-window audit -----------------

LABEL_WEEKS = tuple(range(19, 27))


def _write_window_frames(root: Path) -> None:
    """Every season flat at 0.60 except a dip in the held-out 2024 weeks 22-23."""
    weekly_schema = pa.schema([
        ("cell_id", pa.int32()), ("week_start", pa.date32()), ("ndvi_mean", pa.float32()),
        ("ndvi_std", pa.float32()), ("n_obs", pa.uint8()), ("masked_pct_mean", pa.float32()),
        ("status", pa.string()), ("orbit_mix", pa.string()), ("n_orbits", pa.uint8()),
        ("year", pa.int64()),
    ])
    anomaly_schema = pa.schema([
        ("cell_id", pa.int32()), ("week_start", pa.date32()), ("ndvi_mean", pa.float32()),
        ("baseline_mean", pa.float32()), ("baseline_std", pa.float32()),
        ("anomaly", pa.float32()), ("zscore", pa.float32()),
        ("n_baseline_years", pa.uint8()), ("status", pa.string()), ("year", pa.int64()),
    ])
    for year in (*range(2019, 2025),):
        weekly, anomaly = [], []
        for week in LABEL_WEEKS:
            value = 0.30 if (year == TEST and week in (22, 23)) else 0.60
            weekly.append(dict(
                cell_id=1, week_start=_week_start(year, week), ndvi_mean=value,
                ndvi_std=0.02, n_obs=3, masked_pct_mean=5.0, status="ok",
                orbit_mix="R064", n_orbits=1, year=year,
            ))
            anomaly.append(dict(
                cell_id=1, week_start=_week_start(year, week), ndvi_mean=value,
                baseline_mean=0.60, baseline_std=0.02, anomaly=value - 0.60,
                zscore=None, n_baseline_years=5, status="ok", year=year,
            ))
        _write_table(root / "gold" / "weekly_ndvi" / f"year={year}" / "weekly_ndvi.parquet",
                     weekly, weekly_schema)
        _write_table(root / "gold" / "ndvi_anomaly" / f"year={year}" / "ndvi_anomaly.parquet",
                     anomaly, anomaly_schema)
    _write_weather(root, tuple(range(2019, 2025)))
    _write_cells(root)


def _write_table(path: Path, rows: list[dict], schema: pa.Schema) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)


def test_label_uses_t_plus_1_and_t_plus_2_not_the_current_week(tmp_path: Path) -> None:
    _write_window_frames(tmp_path)
    split = Split(name="temporal", train_years=tuple(range(2019, 2024)), test_year=TEST)
    con = connect(CONFIG)
    try:
        frame = con.sql(build_sql(tmp_path, CONFIG, split)).df()
    finally:
        con.close()

    expected = (frame["fwd_anomaly_2w"] <= -0.08).astype(int)
    assert (frame["label"].astype(int) == expected).all()
    assert np.allclose(frame["fwd_anomaly_2w"], frame["fwd_anomaly_2w_stored"],
                       equal_nan=True)

    # Recompute the label window from the stored anomaly table itself.
    stored = pd.concat([
        pq.read_table(path).to_pandas()
        for path in sorted((tmp_path / "gold" / "ndvi_anomaly").glob("year=*/*.parquet"))
    ], ignore_index=True)
    stored = stored[stored["cell_id"] == 1].set_index("week_start")["anomaly"]
    for _, row in frame[frame["split"] == "test"].iterrows():
        later = [row["week_start"] + pd.Timedelta(days=7),
                 row["week_start"] + pd.Timedelta(days=14)]
        if all(day in stored.index for day in later):
            manual = float(np.mean([stored.loc[day] for day in later]))
            assert row["fwd_anomaly_2w"] == pytest.approx(manual, abs=1e-6)

    test_rows = frame[frame["split"] == "test"]
    normal_now = test_rows[test_rows["week_start"].dt.date == _week_start(TEST, 21)].iloc[0]
    stressed_now = test_rows[test_rows["week_start"].dt.date == _week_start(TEST, 23)].iloc[0]
    # Current week healthy, next two weeks stressed -> positive label.
    assert normal_now["anomaly_t"] > -0.08
    assert normal_now["label"] == 1
    # Current week stressed, next two weeks healthy -> negative label.
    assert stressed_now["anomaly_t"] <= -0.08
    assert stressed_now["label"] == 0
    # The two candidate definitions are not the same vector.
    current_week = (frame["anomaly_t"] <= -0.08).astype(int)
    assert (current_week != frame["label"].astype(int)).any()


def test_loso_folds_hold_out_each_season_once() -> None:
    folds = loso_splits(tuple(range(2019, 2025)))
    assert [fold.test_year for fold in folds] == list(range(2019, 2025))
    for fold in folds:
        assert fold.test_year not in fold.train_years
        assert set(fold.train_years) == set(range(2019, 2025)) - {fold.test_year}
        plan = loso_plan(fold.train_years)
        assert plan.validation_year == max(fold.train_years)
        assert plan.validation_year not in plan.fit_years
        assert plan.final_years == fold.train_years
        assert fold.test_year not in plan.early_stopping_years
        assert fold.test_year not in plan.final_years


def test_loso_2024_plan_matches_the_temporal_plan_and_never_validates_on_test() -> None:
    temporal = temporal_plan((2019, 2020, 2021, 2022, 2023))
    fold_2024 = loso_plan((2019, 2020, 2021, 2022, 2023))
    assert fold_2024 == temporal
    for fold in loso_splits((2019, 2020, 2021, 2022, 2023, 2024)):
        plan = loso_plan(fold.train_years)
        assert fold.test_year != plan.validation_year
        assert fold.test_year not in plan.fit_years


def test_fold_frames_refuse_a_held_out_season_on_the_training_side() -> None:
    split = Split(name="loso-2024", train_years=(2019, 2020, 2021, 2022, 2023),
                  test_year=2024)
    rows = pd.DataFrame({
        "year": [2019, 2020, 2021, 2022, 2023, 2024],
        "split": ["train"] * 6,
    })
    with pytest.raises(ValueError):
        fold_frames(rows, split)

    correct = pd.DataFrame({
        "year": [2019, 2020, 2021, 2022, 2023, 2024],
        "split": ["train"] * 5 + ["test"],
    })
    train, test = fold_frames(correct, split)
    assert set(train["year"]) == set(split.train_years)
    assert set(test["year"]) == {split.test_year}


def test_fold_frames_refuse_a_test_side_that_is_not_the_held_out_season() -> None:
    split = Split(name="loso-2024", train_years=(2019, 2020, 2021, 2022, 2023),
                  test_year=2024)
    rows = pd.DataFrame({
        "year": [2019, 2020, 2021, 2022, 2023, 2023],
        "split": ["train"] * 5 + ["test"],
    })
    with pytest.raises(ValueError):
        fold_frames(rows, split)

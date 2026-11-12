"""S3 training tests: the split plan, variants and the final feature matrix."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from delta_crops.models import train as train_module
from delta_crops.models.train import (
    FULL_FEATURES,
    NDVI_ONLY_FEATURES,
    WEATHER_COLUMNS,
    permute_within_season,
    split_fit_validation,
    temporal_plan,
)

from test_models import CONFIG, TEST, _fixture, _week_start


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

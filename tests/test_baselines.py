"""Phase 7 baseline tests: metrics, per-fold datasets and sanity checks."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from delta_crops.models.baselines import (
    ONSET_GROUPS,
    Group,
    _fit_b3,
    _score_b3,
    average_precision,
    brier,
    evaluate_group,
    fit_logistic,
    group_mask,
    precision_at_recall,
    recall_at_precision,
    roc_auc,
)
from delta_crops.models.dataset import TARGET_NDVI_ANOMALY, Split, build_sql, connect, loso_splits

from test_models import CONFIG, _fixture


def test_average_precision_of_a_constant_score_is_the_prevalence() -> None:
    labels = np.array([0, 1, 0, 1, 0, 1, 0, 0])
    scores = np.full(len(labels), 0.25)
    assert average_precision(labels, scores) == pytest.approx(labels.mean(), abs=1e-12)


def test_average_precision_perfect_and_inverted_ranking() -> None:
    labels = np.array([0, 0, 1, 1])
    assert average_precision(labels, np.array([0.1, 0.2, 0.8, 0.9])) == pytest.approx(1.0)
    inverted = average_precision(labels, np.array([0.1, 0.2, 0.8, 0.9])[::-1])
    assert inverted < 0.6  # positives ranked last are bad but AP is not zero


def test_precision_and_recall_at_operating_points() -> None:
    labels = np.array([0, 1, 0, 1])
    scores = np.array([0.1, 0.9, 0.8, 0.7])
    # ranked: positive (1.0 precision), negative, positive, negative
    assert precision_at_recall(labels, scores, 0.5) == pytest.approx(1.0)
    assert recall_at_precision(labels, scores, 0.5) == pytest.approx(1.0)
    assert precision_at_recall(labels, scores, 1.0) == pytest.approx(2.0 / 3.0)
    assert recall_at_precision(labels, scores, 0.9) == pytest.approx(0.5)


def test_brier_and_roc_auc() -> None:
    labels = np.array([0, 1, 0, 1])
    assert brier(labels, labels.astype(float)) == pytest.approx(0.0)
    prevalence = labels.mean()
    assert brier(labels, np.full(len(labels), prevalence)) == pytest.approx(
        prevalence * (1.0 - prevalence)
    )
    assert roc_auc(np.array([0, 0, 1, 1]), np.array([0.1, 0.2, 0.8, 0.9])) == pytest.approx(1.0)
    assert roc_auc(labels, np.full(len(labels), 0.5)) == pytest.approx(0.5)


def test_logistic_fit_learns_the_direction_of_the_signal() -> None:
    x = np.linspace(-2.0, 2.0, 200).reshape(-1, 1)
    labels = (x[:, 0] < 0).astype(float)
    beta = fit_logistic(x, labels)
    assert beta[0] < 0  # positive labels live at negative x, so the slope must be negative
    scores = 1.0 / (1.0 + np.exp(-np.clip(x @ beta, -30.0, 30.0)))
    assert roc_auc(labels.astype(int), scores) == pytest.approx(1.0)


def _synthetic_frame(rows: int, *, missing_lag: bool = False, seed: int = 7) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    anomaly = rng.normal(0.0, 0.2, rows)
    frame = pd.DataFrame({
        "label": (anomaly + rng.normal(0.0, 0.05, rows) <= -0.08).astype(np.int64),
        "anomaly_t": anomaly,
        "anomaly_lag1": anomaly - 0.01,
        "week_index": rng.integers(1, 21, rows),
        "is_cropland": rng.random(rows) < 0.8,
        "ndvi_lag1": 0.5, "ndvi_lag2": 0.5, "ndvi_lag3": 0.5,
        "temp_max_mean_c_lag1": 30.0, "temp_max_mean_c_lag2": 30.0,
        "heatwave_days_lag1": 0,
    })
    if missing_lag:
        frame.loc[frame.index[: rows // 4], "ndvi_lag2"] = np.nan
    return frame


def test_b0_pr_auc_equals_prevalence_and_shuffled_control_is_near_it() -> None:
    train = _synthetic_frame(4000)
    test = _synthetic_frame(2000, seed=11)
    group = Group("all/pooled", "all", "pooled")
    result = evaluate_group(train, test, group)
    prevalence = result["prevalence_test"]
    assert result["models"]["B0"]["pr_auc"] == pytest.approx(prevalence, abs=1e-9)
    control = result["shuffled_control"]
    assert control["pr_auc"] == pytest.approx(control["prevalence_test"], abs=0.02)
    # B1 is a score, so its Brier is deliberately absent.
    assert result["models"]["B1"]["brier"] is None
    assert result["models"]["B3"]["brier"] is not None


def test_strata_are_fitted_and_scored_within_the_stratum() -> None:
    train = _synthetic_frame(4000, missing_lag=True)
    test = _synthetic_frame(2000, missing_lag=True, seed=13)
    complete = evaluate_group(train, test, Group("all/complete", "all", "complete"))
    incomplete = evaluate_group(train, test, Group("all/incomplete", "all", "incomplete"))
    assert complete["rows_train"] + incomplete["rows_train"] == len(train)
    assert complete["rows_test"] + incomplete["rows_test"] == len(test)
    assert complete["rows_train"] > 0 and incomplete["rows_train"] > 0


def test_b3_has_an_intercept_and_is_calibrated() -> None:
    train = _synthetic_frame(4000)
    test = _synthetic_frame(2000, seed=11)
    labels = train["label"].to_numpy(dtype=np.int64)
    beta, mean, std, imputation = _fit_b3(train, labels)
    assert len(beta) == 4  # intercept + three features
    probability = _score_b3(train, test, beta, mean, std, imputation)
    # Without the intercept this sits near 0.5 regardless of the base rate.
    assert probability.mean() == pytest.approx(test["label"].mean(), abs=0.03)


def test_onset_mask_keeps_only_unstressed_rows() -> None:
    frame = _synthetic_frame(2000)
    onset = Group("all/onset", "all", "pooled", onset=True)
    mask = group_mask(frame, onset)
    assert 0 < mask.sum() < len(frame)
    assert (frame.loc[mask, "anomaly_t"] > TARGET_NDVI_ANOMALY).all()
    assert not np.array_equal(mask, group_mask(frame, Group("all/pooled", "all", "pooled")))
    assert [group.name for group in ONSET_GROUPS] == ["all/onset", "cropland/onset"]


def test_onset_b0_still_equals_the_subset_prevalence() -> None:
    train = _synthetic_frame(4000)
    test = _synthetic_frame(2000, seed=11)
    result = evaluate_group(train, test, Group("cropland/onset", "cropland", "pooled", onset=True))
    assert result["rows_test"] > 0
    assert result["models"]["B0"]["pr_auc"] == pytest.approx(result["prevalence_test"], abs=1e-9)
    assert result["models"]["B1"]["brier"] is None


def test_split_rejects_a_test_season_in_training() -> None:
    with pytest.raises(ValueError):
        Split(name="bad", train_years=(2019, 2020), test_year=2020)
    with pytest.raises(ValueError):
        Split(name="empty", train_years=(), test_year=2024)


def test_loso_fold_never_trains_on_the_held_out_season(tmp_path: Path) -> None:
    split = _fixture(tmp_path)
    con = connect(CONFIG)
    try:
        for fold in loso_splits(split.years):
            sql = build_sql(tmp_path, CONFIG, fold)
            membership = con.sql(
                f"SELECT split, year FROM ({sql}) t GROUP BY split, year ORDER BY split, year"
            ).fetchall()
            train_years = {int(year) for name, year in membership if name == "train"}
            test_years = {int(year) for name, year in membership if name == "test"}
            assert test_years == {fold.test_year}, fold.name
            assert train_years == set(fold.train_years), fold.name
            assert fold.test_year not in train_years, fold.name
    finally:
        con.close()

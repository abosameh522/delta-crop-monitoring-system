"""Tests for the Phase 8 S3 prediction dump and its reproduction gate."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest

from delta_crops.models.dataset import Split
from delta_crops.models.predictions import (
    FULL_FEATURES,
    NDVI_ONLY_FEATURES,
    PREDICTION_SCHEMA,
    FrozenVariant,
    format_gate,
    gate_rows,
    guard_split,
    load_freeze,
    prediction_frame,
    write_predictions,
)

SPLIT = Split(name="temporal", train_years=(2019, 2020, 2021, 2022, 2023), test_year=2024)


def _frame(test_year: int = 2024) -> pd.DataFrame:
    """A tiny frame shaped like the built dataset, with one duplicated key."""
    rows = [
        # season, cell, week, label, split
        (2019, 1, "2019-05-06", 0, "train"),
        (2020, 1, "2020-05-04", 0, "train"),
        (2021, 1, "2021-05-10", 0, "train"),
        (2022, 1, "2022-05-09", 1, "train"),
        (2023, 1, "2023-05-08", 1, "train"),
        (2024, 1, "2024-05-06", 0, "test"),
        (2024, 1, "2024-05-13", 1, "test"),
        (2024, 2, "2024-05-06", 0, "test"),
    ]
    return pd.DataFrame({
        "year": [row[0] for row in rows],
        "split": [row[4] for row in rows],
        "test_year": [test_year] * len(rows),
        "cell_id": np.array([row[1] for row in rows], dtype=np.int32),
        "week_start": pd.to_datetime([row[2] for row in rows]),
        "label": np.array([row[3] for row in rows], dtype=np.int8),
        "is_cropland": [True, True, True, True, True, True, False, True],
        "block_5km": np.array([0, 0, 0, 0, 0, 0, 0, 1], dtype=np.int32),
    })


def _metrics(variant: str, rounds: int, features, pr_auc: float = 0.5) -> dict:
    return {
        "features": list(features),
        "rounds": {"best_round": rounds},
        "groups": {
            "cropland/pooled": {
                "rows": 2_449_220,
                "positives": 614_324,
                "models": {"M": {
                    "pr_auc": pr_auc,
                    "precision_at_recall_0_5": 0.8,
                    "brier": 0.1,
                }},
            }
        },
    }


def _freeze_payload() -> dict:
    return {
        "split": {"name": "temporal", "train_years": [2019, 2020, 2021, 2022, 2023],
                  "test_year": 2024},
        "variants": {
            "full": _metrics("full", 533, FULL_FEATURES),
            "ndvi_only": _metrics("ndvi_only", 275, NDVI_ONLY_FEATURES),
        },
    }


def test_load_freeze_reads_rounds_and_reference(tmp_path: Path) -> None:
    path = tmp_path / "xgb_temporal.json"
    path.write_text(json.dumps(_freeze_payload()), encoding="utf-8")

    split, variants = load_freeze(path)

    assert split["test_year"] == 2024
    assert variants["full"].rounds == 533
    assert variants["ndvi_only"].rounds == 275
    assert variants["full"].reference["brier"] == 0.1
    assert variants["full"].rows == 2_449_220
    assert list(variants["full"].features) == list(FULL_FEATURES)


@pytest.mark.parametrize("mutation, message", [
    (lambda payload: payload["split"].update(name="loso-2024"), "temporal split"),
    (lambda payload: payload["variants"]["full"]["rounds"].update(best_round=0), "rounds"),
    (lambda payload: payload["variants"]["full"].update(features=["anomaly_t"]), "feature set"),
])
def test_load_freeze_refuses_an_unexpected_artifact(tmp_path: Path, mutation, message) -> None:
    payload = _freeze_payload()
    mutation(payload)
    path = tmp_path / "xgb_temporal.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        load_freeze(path)


def test_guard_split_accepts_a_clean_frame() -> None:
    train, test = guard_split(_frame(), SPLIT)
    assert set(train["year"]) == {2019, 2020, 2021, 2022, 2023}
    assert set(test["year"]) == {2024}


@pytest.mark.parametrize("break_frame, message", [
    (lambda frame: frame.assign(split="train"), "training rows"),
    (lambda frame: frame.assign(year=frame["year"].where(frame["split"] == "train", 2023)),
     "test rows"),
    (lambda frame: pd.concat([frame, frame[frame["split"] == "test"].head(1)]),
     "duplicate"),
    (lambda frame: frame.assign(label=np.int8(2)), "outside 0/1"),
    (lambda frame: frame.assign(label=np.float32(np.nan)), "missing"),
])
def test_guard_split_refuses_leaks_duplicates_and_bad_labels(break_frame, message) -> None:
    with pytest.raises(ValueError, match=message):
        guard_split(break_frame(_frame()), SPLIT)


def test_gate_rows_passes_on_exact_values_and_flags_a_delta() -> None:
    frozen = FrozenVariant(
        name="full", rounds=533, features=tuple(FULL_FEATURES),
        reference={"pr_auc": 0.8213705, "precision_at_recall_0_5": 0.8713148,
                   "brier": 0.0936884},
        rows=2_449_220, positives=614_324,
    )
    exact = {"pr_auc": 0.8213705, "precision_at_recall_0_5": 0.8713148, "brier": 0.0936884}
    checks = gate_rows(frozen, exact, 2_449_220, 614_324)
    assert all(check["passed"] for check in checks)
    assert "expected" in format_gate(checks)

    off = {**exact, "pr_auc": 0.8213705 + 1e-7}
    checks = gate_rows(frozen, off, 2_449_220, 614_324)
    failed = [check for check in checks if not check["passed"]]
    assert [check["metric"] for check in failed] == ["pr_auc"]
    assert failed[0]["delta"] == pytest.approx(1e-7, rel=1e-6)

    short = gate_rows(frozen, exact, 2_449_219, 614_324)
    assert [check["metric"] for check in short if not check["passed"]] == ["rows"]
    assert [check["metric"] for check in gate_rows(frozen, exact, 2_449_220, 614_323)
            if not check["passed"]] == ["positives"]


def test_prediction_frame_schema_roundtrip(tmp_path: Path) -> None:
    _, test = guard_split(_frame(), SPLIT)
    probability = np.linspace(0.0, 1.0, len(test), dtype=np.float32)
    frame = prediction_frame(test, probability, "full")

    out = tmp_path / "predictions.parquet"
    rows, size = write_predictions([frame], out)

    assert rows == len(test) and size > 0
    assert pq.read_schema(out) == PREDICTION_SCHEMA
    read = pd.read_parquet(out)
    assert list(read.columns) == [field.name for field in PREDICTION_SCHEMA]
    assert read["prob"].between(0.0, 1.0).all()
    assert read["variant"].unique().tolist() == ["full"]


def test_write_predictions_is_idempotent(tmp_path: Path) -> None:
    _, test = guard_split(_frame(), SPLIT)
    frame = prediction_frame(test, np.full(len(test), 0.5, dtype=np.float32), "full")
    out = tmp_path / "predictions.parquet"

    first, _ = write_predictions([frame], out)
    second, _ = write_predictions([frame], out)

    assert first == second == len(test)
    assert len(pd.read_parquet(out)) == len(test)

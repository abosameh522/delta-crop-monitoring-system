"""Phase 8: persist S3 2024 predictions for the dashboard, with a gate.

Refits the two frozen S3 variants with the round counts recorded in
``artifacts/phase7/xgb_temporal.json``, scores the 2024 test season once, and
verifies the stored metrics reproduce to ``1e-9`` before writing anything.

This is not a new training run: no early stopping, no tuning, no sanity
controls. The freeze, including the row-order rationale for using the Phase 7
loader instead of the sorted Parquet, is ``docs/phase8-predictions-freeze.md``.
"""

from __future__ import annotations

import argparse
import json
import os
import resource
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from delta_crops.config import load_config
from delta_crops.models.baselines import group_mask, score_metrics
from delta_crops.models.dataset import temporal_split
from delta_crops.models.train import (
    FULL_FEATURES,
    MAX_ROUNDS,
    NDVI_ONLY_FEATURES,
    REPORT_GROUPS,
    fold_frames,
    load_frames,
    predict,
    refit,
)

TOLERANCE = 1e-9
GATE_METRICS = ("pr_auc", "precision_at_recall_0_5", "brier")
HEADLINE_GROUP = REPORT_GROUPS[0]  # cropland/pooled, the pre-registered headline
PREDICTION_SCHEMA = pa.schema([
    ("cell_id", pa.int32()),
    ("week_start", pa.date32()),
    ("variant", pa.string()),
    ("prob", pa.float32()),
    ("label", pa.int8()),
    ("is_cropland", pa.bool_()),
    ("block_5km", pa.int32()),
])
METRIC_LABELS = {
    "pr_auc": "PR-AUC",
    "precision_at_recall_0_5": "P@R.5",
    "brier": "Brier",
}


class GateError(RuntimeError):
    """The refit does not reproduce the frozen Phase 7 metrics."""


@dataclass(frozen=True)
class FrozenVariant:
    """One variant exactly as Phase 7 scored it."""

    name: str
    rounds: int
    features: tuple[str, ...]
    reference: dict[str, float]
    rows: int
    positives: int


def load_freeze(metrics_path: Path) -> tuple[dict[str, Any], dict[str, FrozenVariant]]:
    """Read the frozen S3 split, rounds and reference metrics from the artifact."""
    payload = json.loads(metrics_path.read_text(encoding="utf-8"))
    split = payload["split"]
    if split["name"] != "temporal":
        raise ValueError(f"expected the temporal split, found {split['name']!r}")

    variants: dict[str, FrozenVariant] = {}
    for name, features in (("full", FULL_FEATURES), ("ndvi_only", NDVI_ONLY_FEATURES)):
        entry = payload["variants"][name]
        rounds = int(entry["rounds"]["best_round"])
        if not 1 <= rounds <= MAX_ROUNDS:
            raise ValueError(f"{name} rounds {rounds} outside 1..{MAX_ROUNDS}")
        if set(entry["features"]) != set(features):
            raise ValueError(f"{name} feature set differs from the frozen list")
        group = entry["groups"][HEADLINE_GROUP.name]
        variants[name] = FrozenVariant(
            name=name,
            rounds=rounds,
            features=tuple(features),
            reference={metric: float(group["models"]["M"][metric])
                       for metric in GATE_METRICS},
            rows=int(group["rows"]),
            positives=int(group["positives"]),
        )
    return split, variants


def guard_split(frame, split) -> tuple[pd.DataFrame, pd.DataFrame]:
    """The Phase 7 split guard, plus the checks the index needs.

    Refuses rather than warns: a season on both sides, a duplicated
    ``(cell_id, week_start)`` key or a label outside {0, 1} would silently
    corrupt either the gate's counts or the dashboard's map.
    """
    train, test = fold_frames(frame, split)

    # `load_frames` does not select the builder's `test_year` column, so the
    # equivalent invariant is checked directly: the test side is exactly the
    # frozen season, and no training season can equal it.
    test_years = {int(year) for year in test["year"].unique()}
    if test_years != {split.test_year}:
        raise ValueError(f"test rows are not exactly {split.test_year}: {sorted(test_years)}")
    if test_years & {int(year) for year in train["year"].unique()}:
        raise ValueError("a season appears on both sides of the split")

    duplicate = test.duplicated(subset=["cell_id", "week_start"])
    if bool(duplicate.any()):
        raise ValueError(
            f"{int(duplicate.sum())} duplicate (cell_id, week_start) pairs in the test frame"
        )

    labels = pd.unique(test["label"].dropna())
    if test["label"].isna().any():
        raise ValueError("test labels contain missing values")
    if set(int(value) for value in labels) - {0, 1}:
        raise ValueError(f"test labels outside 0/1: {sorted(labels)}")

    return train, test


def gate_rows(frozen: FrozenVariant, metrics: dict[str, Any],
              rows: int, positives: int) -> list[dict[str, Any]]:
    """Compare one variant's recomputed metrics with the stored ones."""
    checks: list[dict[str, Any]] = []
    for metric in GATE_METRICS:
        expected = frozen.reference[metric]
        actual = float(metrics[metric])
        delta = actual - expected
        checks.append({
            "variant": frozen.name,
            "metric": metric,
            "expected": expected,
            "actual": actual,
            "delta": delta,
            "passed": bool(abs(delta) < TOLERANCE),
        })
    checks.append({
        "variant": frozen.name,
        "metric": "rows",
        "expected": float(frozen.rows),
        "actual": float(rows),
        "delta": float(rows - frozen.rows),
        "passed": rows == frozen.rows,
    })
    checks.append({
        "variant": frozen.name,
        "metric": "positives",
        "expected": float(frozen.positives),
        "actual": float(positives),
        "delta": float(positives - frozen.positives),
        "passed": positives == frozen.positives,
    })
    return checks


def format_gate(checks: list[dict[str, Any]]) -> str:
    header = (f"{'variant':<10} {'metric':<10} {'expected':>18} {'actual':>18} "
              f"{'delta':>12}  status")
    lines = [header, "-" * len(header)]
    for check in checks:
        label = METRIC_LABELS.get(check["metric"], check["metric"])
        lines.append(
            f"{check['variant']:<10} {label:<10} {check['expected']:>18.10f} "
            f"{check['actual']:>18.10f} {check['delta']:>+12.3e}  "
            f"{'pass' if check['passed'] else 'FAIL'}"
        )
    return "\n".join(lines)


def prediction_frame(test: pd.DataFrame, prediction: np.ndarray,
                     variant: str) -> pd.DataFrame:
    """The seven dumped columns, in the frozen order."""
    return pd.DataFrame({
        "cell_id": test["cell_id"].to_numpy(dtype=np.int32),
        "week_start": pd.to_datetime(test["week_start"].to_numpy()).values,
        "variant": variant,
        "prob": prediction.astype(np.float32),
        "label": test["label"].to_numpy(dtype=np.int8),
        "is_cropland": test["is_cropland"].to_numpy(dtype=bool),
        "block_5km": test["block_5km"].to_numpy(dtype=np.int32),
    })


def write_predictions(frames: list[pd.DataFrame], out_path: Path) -> tuple[int, int]:
    """Write all variants atomically; return (rows, bytes)."""
    frame = pd.concat(frames, ignore_index=True)
    table = pa.Table.from_pandas(frame, preserve_index=False, schema=PREDICTION_SCHEMA)
    table = table.cast(PREDICTION_SCHEMA)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(".parquet.tmp")
    pq.write_table(table, tmp, compression="zstd")
    os.replace(tmp, out_path)
    return int(table.num_rows), out_path.stat().st_size


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=None)
    parser.add_argument("--data-dir", default=None)
    parser.add_argument("--temp-dir", default="/tmp/delta_crops_phase8")
    parser.add_argument("--metrics", default="artifacts/phase7/xgb_temporal.json")
    parser.add_argument("--out", default="artifacts/phase8/predictions/temporal_2024.parquet")
    args = parser.parse_args(argv)

    started = time.perf_counter()
    config = load_config(args.config)
    root = Path(args.data_dir) if args.data_dir else Path("data")
    out_path = Path(args.out)

    split_meta, variants = load_freeze(Path(args.metrics))
    split = temporal_split(
        tuple(split_meta["train_years"]) + (split_meta["test_year"],),
        split_meta["test_year"],
    )
    print(f"split {split.name}: train {split.train_years}, test {split.test_year}", flush=True)
    print("frozen rounds: " + ", ".join(
        f"{name}={frozen.rounds}" for name, frozen in variants.items()), flush=True)

    frame = load_frames(root, config, split, Path(args.temp_dir))
    train, test = guard_split(frame, split)
    del frame
    print(f"rows train {len(train):,} test {len(test):,}", flush=True)

    all_checks: list[dict[str, Any]] = []
    frames: list[pd.DataFrame] = []
    for name, frozen in variants.items():
        variant_started = time.perf_counter()
        columns = list(dict.fromkeys(
            ("cell_id", "week_start", "label", "is_cropland", "block_5km", *frozen.features)))
        model = refit(train[columns], frozen.features, frozen.rounds)
        prediction = predict(model, test, frozen.features)
        mask = group_mask(test, HEADLINE_GROUP)
        y = test.loc[mask, "label"].to_numpy(dtype=np.int64)
        metrics = score_metrics(y, prediction[mask], float(y.mean()), prediction[mask])
        checks = gate_rows(frozen, metrics, int(len(y)), int(y.sum()))
        all_checks.extend(checks)
        frames.append(prediction_frame(test, prediction, name))
        print(f"variant {name}: {frozen.rounds} rounds, "
              f"PR-AUC {metrics['pr_auc']:.10f} "
              f"({time.perf_counter() - variant_started:.1f}s)", flush=True)
        del model, prediction

    print("\nreproduction gate (tolerance 1e-9):", flush=True)
    print(format_gate(all_checks), flush=True)
    failed = [check for check in all_checks if not check["passed"]]
    if failed:
        for check in failed:
            print(
                f"GATE FAILED {check['variant']}/{check['metric']}: "
                f"expected {check['expected']:.10f}, actual {check['actual']:.10f}, "
                f"delta {check['delta']:+.3e} (tolerance {TOLERANCE:g})",
                flush=True)
        print("nothing written; parameters, seed, rounds and bars are unchanged.",
              flush=True)
        return 3

    rows, size = write_predictions(frames, out_path)
    print(f"wrote {out_path} ({rows:,} rows, {size / 1e6:.1f} MB) in "
          f"{time.perf_counter() - started:.1f}s, peak RSS "
          f"{resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0:.1f} MB",
          flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

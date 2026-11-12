"""Phase 7 pre-registered baselines (B0-B3) and their metrics.

Everything here follows ``docs/phase7-preregistration.md``: the label is fixed,
the populations are all cells and ``is_cropland``, and metrics are reported on
the pooled set plus the two lag-missingness strata. Models are fitted **within
each group** on training seasons only.

B1 (persistence) is a ranking score, not a probability: its Brier score is n/a.
B3 imputes a missing lagged anomaly with the training median of the same group
(train-only, no future information).
"""

from __future__ import annotations

import argparse
import json
import resource
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from delta_crops.config import Config, load_config
from delta_crops.models.dataset import (
    TARGET_NDVI_ANOMALY,
    Split,
    build_sql,
    connect,
    loso_splits,
    temporal_split,
)

SEED = 20260701
LAG_COLUMNS = (
    "ndvi_lag1", "ndvi_lag2", "ndvi_lag3", "anomaly_lag1",
    "temp_max_mean_c_lag1", "temp_max_mean_c_lag2", "heatwave_days_lag1",
)
FRAME_COLUMNS = ("label", "anomaly_t", "week_index", "is_cropland", *LAG_COLUMNS)
TARGET_RECALL = 0.5
TARGET_PRECISION = 0.5


@dataclass(frozen=True)
class Group:
    """One evaluation cell: a population crossed with a lag-missingness stratum."""

    name: str
    population: str  # "all" | "cropland"
    stratum: str  # "pooled" | "complete" | "incomplete"
    onset: bool = False  # keep only rows with anomaly_t > -0.08


GROUPS = tuple(
    Group(f"{population}/{stratum}", population, stratum)
    for population in ("all", "cropland")
    for stratum in ("pooled", "complete", "incomplete")
)

#: Pre-registered secondary subset: the current week is not already stressed.
ONSET_GROUPS = tuple(
    Group(f"{population}/onset", population, "pooled", onset=True)
    for population in ("all", "cropland")
)


def pr_curve(labels: np.ndarray, scores: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Precision/recall at every distinct score threshold (labels are 0/1)."""
    y = np.asarray(labels, dtype=np.int64)
    order = np.argsort(-np.asarray(scores), kind="mergesort")
    y = y[order]
    sorted_scores = np.asarray(scores)[order]
    cumulative = np.cumsum(y)
    last = np.r_[np.flatnonzero(np.diff(sorted_scores)), len(y) - 1]
    true_positive = cumulative[last]
    seen = last + 1
    precision = true_positive / np.maximum(seen, 1)
    total_positives = int(true_positive[-1]) if len(true_positive) else 0
    if total_positives == 0:
        return precision, np.zeros_like(precision)
    recall = true_positive / total_positives
    return precision, recall


def average_precision(labels: np.ndarray, scores: np.ndarray) -> float | None:
    """Step-wise PR-AUC; a constant score returns the prevalence exactly."""
    y = np.asarray(labels)
    if y.sum() == 0:
        return None
    precision, recall = pr_curve(y, scores)
    return float(np.sum(np.diff(np.r_[0.0, recall]) * precision))


def precision_at_recall(labels: np.ndarray, scores: np.ndarray,
                        target: float = TARGET_RECALL) -> float | None:
    y = np.asarray(labels)
    if y.sum() == 0:
        return None
    precision, recall = pr_curve(y, scores)
    index = int(np.searchsorted(recall, target, side="left"))
    if index >= len(precision):
        return None
    return float(precision[index])


def recall_at_precision(labels: np.ndarray, scores: np.ndarray,
                        target: float = TARGET_PRECISION) -> float | None:
    y = np.asarray(labels)
    if y.sum() == 0:
        return None
    precision, recall = pr_curve(y, scores)
    ok = precision >= target
    return float(recall[ok].max()) if ok.any() else 0.0


def roc_auc(labels: np.ndarray, scores: np.ndarray) -> float | None:
    """Rank (Mann-Whitney) AUC with average ranks for ties."""
    y = np.asarray(labels, dtype=bool)
    scores = np.asarray(scores, dtype=np.float64)
    n_pos = int(y.sum())
    n_neg = len(y) - n_pos
    if n_pos == 0 or n_neg == 0:
        return None
    order = np.argsort(scores, kind="mergesort")
    sorted_scores = scores[order]
    starts = np.r_[0, np.flatnonzero(np.diff(sorted_scores)) + 1]
    ends = np.r_[starts[1:], len(scores)]
    average_ranks = (starts + ends + 1) / 2.0  # 1-based ranks starts+1 .. ends
    ranks = np.empty(len(scores), dtype=np.float64)
    ranks[order] = np.repeat(average_ranks, ends - starts)
    return float((ranks[y].sum() - n_pos * (n_pos + 1) / 2.0) / (n_pos * n_neg))


def brier(labels: np.ndarray, probability: np.ndarray) -> float | None:
    y = np.asarray(labels, dtype=np.float64)
    if len(y) == 0:
        return None
    return float(np.mean((y - np.asarray(probability)) ** 2))


def score_metrics(labels: np.ndarray, scores: np.ndarray, prevalence: float,
                  probability: np.ndarray | None = None) -> dict[str, Any]:
    """All pre-registered metrics for one model on one group."""
    ap = average_precision(labels, scores)
    return {
        "pr_auc": ap,
        "precision_at_recall_0_5": precision_at_recall(labels, scores),
        "recall_at_precision_0_5": recall_at_precision(labels, scores),
        "lift_over_prevalence": None if ap is None or prevalence == 0 else ap / prevalence,
        "roc_auc": roc_auc(labels, scores),
        "brier": None if probability is None else brier(labels, probability),
        "rows": int(len(labels)),
        "positives": int(np.sum(labels)),
    }


def fit_logistic(features: np.ndarray, labels: np.ndarray,
                 iterations: int = 30) -> np.ndarray:
    """IRLS logistic regression; features must already be finite."""
    x = np.asarray(features, dtype=np.float64)
    y = np.asarray(labels, dtype=np.float64)
    beta = np.zeros(x.shape[1], dtype=np.float64)
    for _ in range(iterations):
        eta = np.clip(x @ beta, -30.0, 30.0)
        p = 1.0 / (1.0 + np.exp(-eta))
        weight = np.maximum(p * (1.0 - p), 1e-9)
        gradient = x.T @ (y - p)
        hessian = (x * weight[:, None]).T @ x + np.eye(x.shape[1]) * 1e-6
        step = np.linalg.solve(hessian, gradient)
        beta += step
        if np.max(np.abs(step)) < 1e-8:
            break
    return beta


def _standardise(train: np.ndarray, other: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    mean = np.nanmean(train, axis=0)
    std = np.nanstd(train, axis=0)
    std[std == 0] = 1.0
    return (train - mean) / std, (other - mean) / std


def _logistic_features(frame) -> tuple[np.ndarray, np.ndarray]:
    """anomaly_t, one lagged anomaly, week-of-season; NaNs kept for imputation."""
    return np.column_stack([
        frame["anomaly_t"].to_numpy(dtype=np.float64),
        frame["anomaly_lag1"].to_numpy(dtype=np.float64),
        frame["week_index"].to_numpy(dtype=np.float64),
    ])


def _fit_b3(train, labels: np.ndarray) -> tuple[np.ndarray, np.ndarray, float, np.ndarray]:
    """Return (beta, mean, std, imputation) for B3, fitted on train only.

    ``beta`` includes an intercept as its first element. The intercept matters:
    standardised features have mean zero, so without it ``sigmoid(0) = 0.5``
    instead of the training prevalence, which destroys calibration while
    leaving the ranking (and therefore ROC-AUC / PR-AUC) unchanged.
    """
    x = _logistic_features(train)
    imputation = float(np.nanmedian(x[:, 1])) if np.isfinite(x[:, 1]).any() else 0.0
    x[~np.isfinite(x)] = imputation
    standardised, _ = _standardise(x, x)
    design = np.column_stack([np.ones(len(standardised)), standardised])
    beta = fit_logistic(design, labels)
    return beta, np.nanmean(x, axis=0), np.nanstd(x, axis=0), imputation


def _score_b3(train, test, beta: np.ndarray, mean: np.ndarray, std: np.ndarray,
              imputation: float) -> np.ndarray:
    x = _logistic_features(test)
    x[~np.isfinite(x)] = imputation
    std = np.where(std == 0, 1.0, std)
    design = np.column_stack([np.ones(len(x)), (x - mean) / std])
    z = design @ beta
    return 1.0 / (1.0 + np.exp(-np.clip(z, -30.0, 30.0)))


def b3_scores(train_group, test_group) -> np.ndarray:
    """Fit B3 on the training rows of one group and return its test probabilities.

    Exposed so the block-bootstrap can pair B3 and M on the same resampled
    blocks instead of comparing two unrelated point estimates.
    """
    y_train = train_group["label"].to_numpy(dtype=np.int64)
    beta, mean, std, imputation = _fit_b3(train_group, y_train)
    return _score_b3(train_group, test_group, beta, mean, std, imputation)


def group_mask(frame, group: Group) -> np.ndarray:
    mask = np.ones(len(frame), dtype=bool)
    if group.population == "cropland":
        mask &= frame["is_cropland"].to_numpy(dtype=bool)
    if group.onset:
        mask &= frame["anomaly_t"].to_numpy(dtype=np.float64) > TARGET_NDVI_ANOMALY
    missing = frame[list(LAG_COLUMNS)].isna().any(axis=1).to_numpy(dtype=bool)
    if group.stratum == "complete":
        mask &= ~missing
    elif group.stratum == "incomplete":
        mask &= missing
    return mask


def evaluate_group(train, test, group: Group) -> dict[str, Any]:
    """Fit every baseline on the training rows of this group, score its test rows."""
    train_mask = group_mask(train, group)
    test_mask = group_mask(test, group)
    train_g = train.loc[train_mask]
    test_g = test.loc[test_mask]
    y_train = train_g["label"].to_numpy(dtype=np.int64)
    y_test = test_g["label"].to_numpy(dtype=np.int64)
    result: dict[str, Any] = {
        "rows_train": int(len(train_g)), "rows_test": int(len(test_g)),
        "positives_train": int(y_train.sum()), "positives_test": int(y_test.sum()),
        "prevalence_train": float(y_train.mean()) if len(y_train) else None,
        "prevalence_test": float(y_test.mean()) if len(y_test) else None,
        "models": {},
    }
    if len(test_g) == 0 or len(train_g) == 0:
        return result
    prevalence = float(y_train.mean())
    test_prevalence = float(y_test.mean())

    # B0 majority: constant probability, so its PR-AUC equals the prevalence.
    b0 = np.full(len(test_g), prevalence)
    result["models"]["B0"] = score_metrics(y_test, b0, test_prevalence, b0)

    # B1 persistence: score, not a probability -> Brier n/a.
    b1 = -test_g["anomaly_t"].to_numpy(dtype=np.float64)
    result["models"]["B1"] = score_metrics(y_test, b1, test_prevalence, None)

    # B2 seasonal prior: training positive rate per ISO week.
    rates = train_g.groupby("week_index")["label"].mean()
    b2 = test_g["week_index"].map(rates).fillna(prevalence).to_numpy(dtype=np.float64)
    result["models"]["B2"] = score_metrics(y_test, b2, test_prevalence, b2)

    # B3 logistic regression on anomaly_t, lagged anomaly and week-of-season.
    beta, mean, std, imputation = _fit_b3(train_g, y_train)
    b3 = _score_b3(train_g, test_g, beta, mean, std, imputation)
    result["models"]["B3"] = score_metrics(y_test, b3, test_prevalence, b3)
    result["models"]["B3"]["coefficients"] = beta.tolist()

    # Shuffled-label control: refit on permuted training labels and evaluate
    # against independently permuted test labels. If the labels carry no
    # information the whole path must report chance (PR-AUC ~ prevalence).
    #
    # Scoring that permuted model against the *true* test labels is not a valid
    # null for a ranking model: any non-zero coefficient still imposes a full
    # ordering, so a noise coefficient can align with (or invert) the real
    # feature signal. That number is recorded separately for transparency only.
    rng = np.random.default_rng(SEED)
    shuffled = rng.permutation(y_train)
    shuffled_test = rng.permutation(y_test)
    beta_s, mean_s, std_s, imputation_s = _fit_b3(train_g, shuffled)
    b3_shuffled = _score_b3(train_g, test_g, beta_s, mean_s, std_s, imputation_s)
    result["shuffled_control"] = {
        "pr_auc": average_precision(shuffled_test, b3_shuffled),
        "prevalence_test": float(shuffled_test.mean()),
    }
    result["shuffled_control_real_test_labels"] = {
        "pr_auc": average_precision(y_test, b3_shuffled),
        "prevalence_test": result["prevalence_test"],
        "note": "not a valid null for a ranking model; diagnostic only",
    }
    return result


def frame_for_split(con, sql: str, name: str):
    columns = ", ".join(FRAME_COLUMNS)
    frame = con.sql(
        f"SELECT {columns} FROM ({sql}) t WHERE split = '{name}'"
    ).df()
    return frame


def run_split(root: Path, config: Config, split: Split, temp_dir: Path,
              onset: bool = False) -> dict[str, Any]:
    """Build one fold's dataset from scratch and evaluate every baseline."""
    started = time.perf_counter()
    con = connect(config, temp_dir)
    try:
        sql = build_sql(root, config, split)
        counts = con.sql(
            f"SELECT year, split, count(*) AS rows, sum(label) AS positives "
            f"FROM ({sql}) t GROUP BY year, split ORDER BY year"
        ).df().to_dict("records")
        train = frame_for_split(con, sql, "train")
        test = frame_for_split(con, sql, "test")
    finally:
        con.close()
    selected = ONSET_GROUPS if onset else GROUPS
    groups = {group.name: evaluate_group(train, test, group) for group in selected}
    week_index = (
        test.groupby("week_index")["label"].agg(["size", "mean"]).reset_index()
        .rename(columns={"size": "rows", "mean": "positive_rate"}).to_dict("records")
    )
    return {
        "split": {"name": split.name, "train_years": list(split.train_years),
                  "test_year": split.test_year},
        "onset": onset,
        "seasons": counts,
        "rows_train": int(len(train)), "rows_test": int(len(test)),
        "positive_rate_train": float(train["label"].mean()),
        "positive_rate_test": float(test["label"].mean()),
        "week_index_test": week_index,
        "groups": groups,
        "seconds": round(time.perf_counter() - started, 2),
    }


def _compact_table(report: dict[str, Any]) -> str:
    lines = [f"split {report['split']['name']} "
             f"(train {report['split']['train_years']}, test {report['split']['test_year']})",
             f"rows train {report['rows_train']:,} test {report['rows_test']:,} | "
             f"prevalence train {report['positive_rate_train']:.4f} "
             f"test {report['positive_rate_test']:.4f}", "",
             f"{'group':22s} {'model':4s} {'PR-AUC':>8s} {'P@R.5':>7s} "
             f"{'R@P.5':>7s} {'lift':>7s} {'ROC':>7s} {'Brier':>7s}"]
    for name, group in report["groups"].items():
        for model, metrics in group["models"].items():
            def fmt(value, digits=4):
                return "n/a" if value is None else f"{value:.{digits}f}"
            lines.append(
                f"{name:22s} {model:4s} {fmt(metrics['pr_auc']):>8s} "
                f"{fmt(metrics['precision_at_recall_0_5']):>7s} "
                f"{fmt(metrics['recall_at_precision_0_5']):>7s} "
                f"{fmt(metrics['lift_over_prevalence'], 2):>7s} "
                f"{fmt(metrics['roc_auc']):>7s} {fmt(metrics['brier']):>7s}"
            )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=None)
    parser.add_argument("--data-dir", default=None)
    parser.add_argument("--out", default=None)
    parser.add_argument("--split", choices=("temporal", "loso", "all"), default="temporal")
    parser.add_argument("--onset", action="store_true",
                        help="evaluate only the pre-registered onset subset")
    parser.add_argument("--temp-dir", default="/tmp/delta_crops_phase7")
    args = parser.parse_args(argv)

    config = load_config(args.config)
    root = Path(args.data_dir) if args.data_dir else Path("data")
    temp_dir = Path(args.temp_dir)
    started = time.perf_counter()
    reports = []
    if args.split in ("temporal", "all"):
        reports.append(run_split(root, config, temporal_split(config.seasons.years),
                                 temp_dir, onset=args.onset))
    if args.split in ("loso", "all"):
        for split in loso_splits(config.seasons.years):
            reports.append(run_split(root, config, split, temp_dir, onset=args.onset))

    payload = {
        "seed": SEED,
        "rule": "docs/phase7-preregistration.md",
        "onset": args.onset,
        "reports": reports,
        "seconds": round(time.perf_counter() - started, 2),
        "peak_rss_mb": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0, 1),
    }
    for report in reports:
        print(_compact_table(report), flush=True)
        print(flush=True)
    out = args.out or (
        "artifacts/phase7/baselines_onset_metrics.json" if args.onset
        else "artifacts/phase7/baselines_metrics.json"
    )
    path = Path(out)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=float) + "\n", encoding="utf-8")
    print(json.dumps({"seconds": payload["seconds"], "peak_rss_mb": payload["peak_rss_mb"],
                      "out": str(path)}), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

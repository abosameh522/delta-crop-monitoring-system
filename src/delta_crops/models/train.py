"""S3/S4: XGBoost on the temporal split and leave-one-season-out folds.

One model per variant is trained once per fold on all training rows and then
evaluated on strata, populations and the onset subset without retraining. The
hyperparameters, early-stopping season and refit procedure are frozen in
``docs/phase7-preregistration.md`` (sections 5.3 and 5.4).
"""

from __future__ import annotations

import argparse
import ctypes
import gc
import json
import os
import resource
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import xgboost as xgb

from delta_crops.config import Config, load_config
from delta_crops.models.baselines import (
    Group,
    b3_scores,
    group_mask,
    score_metrics,
)
from delta_crops.models.dataset import (
    FEATURES,
    Split,
    build_sql,
    connect,
    loso_splits,
    temporal_split,
)
from delta_crops.models.spatial import (
    BOOTSTRAP_SEED,
    all_block_ids,
    block_bootstrap,
    partition_blocks,
    per_block_metrics,
    spatial_frames,
)

SEED = 20260701
VALIDATION_YEAR = 2023
EARLY_STOPPING_ROUNDS = 50
MAX_ROUNDS = 600
PARAMS: dict[str, Any] = {
    "objective": "binary:logistic",
    "tree_method": "hist",
    "max_depth": 6,
    "eta": 0.05,
    "seed": SEED,
    "nthread": os.cpu_count() or 1,
    "eval_metric": "aucpr",
}

WEATHER_COLUMNS = (
    "temp_max_mean_c", "temp_max_max_c", "temp_min_mean_c",
    "solar_sum_mj_m2", "precip_sum_mm", "heatwave_days",
    "temp_max_mean_c_lag1", "temp_max_mean_c_lag2",
    "heatwave_days_lag1", "heatwave_days_2w",
)
FULL_FEATURES = list(FEATURES)
NDVI_ONLY_FEATURES = [name for name in FEATURES if name not in WEATHER_COLUMNS]
B3_FEATURES = ["anomaly_t", "anomaly_lag1", "week_index"]
VARIANTS = {"full": FULL_FEATURES, "ndvi_only": NDVI_ONLY_FEATURES}

REPORT_GROUPS = (
    Group("cropland/pooled", "cropland", "pooled"),
    Group("all/pooled", "all", "pooled"),
    Group("cropland/complete", "cropland", "complete"),
    Group("cropland/incomplete", "cropland", "incomplete"),
    Group("cropland/onset", "cropland", "pooled", onset=True),
    Group("all/onset", "all", "pooled", onset=True),
)

METADATA_COLUMNS = ("year", "split", "cell_id", "week_start", "label",
                    "is_cropland", "anomaly_t", "anomaly_lag1", "block_5km")
TARGET_RECALL = 0.5

assert len(FULL_FEATURES) == 26, len(FULL_FEATURES)
assert len(NDVI_ONLY_FEATURES) == 16, len(NDVI_ONLY_FEATURES)
assert not set(NDVI_ONLY_FEATURES) & set(WEATHER_COLUMNS)
assert set(FULL_FEATURES) == set(NDVI_ONLY_FEATURES) | set(WEATHER_COLUMNS)


def release_memory() -> None:
    """Drop freed frames and hand the pages back to the OS between folds.

    Six folds of ~15 M rows would otherwise keep every freed pandas block in the
    process allocator and the OS kills the job; this only frees memory that is
    already unreachable, it changes no computation.
    """
    gc.collect()
    try:
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except OSError:
        pass


@dataclass(frozen=True)
class FitPlan:
    """Seasons passed to early stopping and to the final refit."""

    fit_years: tuple[int, ...]
    validation_year: int
    final_years: tuple[int, ...]

    @property
    def early_stopping_years(self) -> tuple[int, ...]:
        return (*self.fit_years, self.validation_year)


def temporal_plan(train_years: tuple[int, ...],
                  validation_year: int = VALIDATION_YEAR) -> FitPlan:
    """Fit on every training season but the validation one, then refit on all."""
    years = tuple(sorted(train_years))
    if validation_year not in years:
        raise ValueError(f"validation season {validation_year} is not a training season")
    return FitPlan(
        fit_years=tuple(year for year in years if year != validation_year),
        validation_year=int(validation_year),
        final_years=years,
    )


def loso_plan(train_years: tuple[int, ...]) -> FitPlan:
    """Early-stop on the most recent remaining training season, then refit on all."""
    years = tuple(sorted(train_years))
    return temporal_plan(years, validation_year=years[-1])


def split_fit_validation(train_frame, plan: FitPlan):
    fit = train_frame[train_frame["year"] != plan.validation_year]
    validation = train_frame[train_frame["year"] == plan.validation_year]
    if set(fit["year"].unique()) != set(plan.fit_years):
        raise ValueError("fit seasons do not match the plan")
    if set(validation["year"].unique()) != {plan.validation_year}:
        raise ValueError("validation frame contains a season outside the plan")
    return fit, validation


def fold_frames(all_rows, split: Split):
    """Split the built rows exactly as the fold says, and refuse anything else.

    The builder already writes a ``split`` column, but the training path must not
    trust it blindly: a held-out season in the training side is the temporal leak
    this project exists to rule out.
    """
    train = all_rows[all_rows["split"] == "train"].reset_index(drop=True)
    test = all_rows[all_rows["split"] == "test"].reset_index(drop=True)
    if set(train["year"].unique()) != set(split.train_years):
        raise ValueError(f"training rows are not exactly {split.train_years}")
    if set(test["year"].unique()) != {split.test_year}:
        raise ValueError(f"test rows are not exactly {split.test_year}")
    if set(train["year"].unique()) & set(test["year"].unique()):
        raise ValueError("a season appears on both sides of the fold")
    return train, test


def load_frames(root: Path, config: Config, split, temp_dir: Path,
                truncate: tuple[int, object] | None = None,
                require_target: bool = True):
    """Build the split once and keep only the columns training/evaluation need."""
    columns = list(dict.fromkeys(
        (*METADATA_COLUMNS, *FULL_FEATURES)
    ))
    con = connect(config, temp_dir)
    try:
        sql = build_sql(root, config, split, truncate=truncate,
                        require_target=require_target)
        frame = con.sql(f"SELECT {', '.join(columns)} FROM ({sql}) t").df()
    finally:
        con.close()
    return frame


def matrix(frame, features: list[str] | tuple[str, ...]) -> np.ndarray:
    """Dense float32 feature matrix; XGBoost handles NaN natively."""
    return frame[list(features)].to_numpy(dtype=np.float32)


def labels(frame) -> np.ndarray:
    return frame["label"].to_numpy(dtype=np.float32)


def fit_with_early_stopping(fit_frame, validation_frame, features,
                            fit_labels: np.ndarray | None = None,
                            validation_labels: np.ndarray | None = None):
    """Fit one model, stopping on the validation season's PR-AUC."""
    x_fit = matrix(fit_frame, features)
    x_val = matrix(validation_frame, features)
    y_fit = labels(fit_frame) if fit_labels is None else fit_labels
    y_val = labels(validation_frame) if validation_labels is None else validation_labels
    dtrain = xgb.DMatrix(x_fit, label=y_fit, feature_names=list(features))
    dval = xgb.DMatrix(x_val, label=y_val, feature_names=list(features))
    history: dict[str, dict[str, list[float]]] = {}
    booster = xgb.train(
        PARAMS, dtrain, num_boost_round=MAX_ROUNDS,
        evals=[(dval, "validation")],
        early_stopping_rounds=EARLY_STOPPING_ROUNDS,
        evals_result=history, verbose_eval=False,
    )
    rounds = int(booster.best_iteration) + 1
    return booster, rounds, history


def refit(train_frame, features, rounds: int, labels_override: np.ndarray | None = None):
    x_train = matrix(train_frame, features)
    y_train = labels(train_frame) if labels_override is None else labels_override
    dtrain = xgb.DMatrix(x_train, label=y_train, feature_names=list(features))
    return xgb.train(PARAMS, dtrain, num_boost_round=int(rounds))


def predict(booster, frame, features) -> np.ndarray:
    return booster.predict(xgb.DMatrix(matrix(frame, features), feature_names=list(features)))


def permute_within_season(frame, seed: int) -> np.ndarray:
    """Permute labels independently inside each season."""
    rng = np.random.default_rng(seed)
    result = labels(frame).copy()
    years = frame["year"].to_numpy()
    for year in np.unique(years):
        index = np.flatnonzero(years == year)
        result[index] = rng.permutation(result[index])
    return result


def reliability_table(y: np.ndarray, probability: np.ndarray, bins: int = 10) -> list[dict]:
    edges = np.linspace(0.0, 1.0, bins + 1)
    index = np.clip(np.digitize(probability, edges) - 1, 0, bins - 1)
    rows = []
    for b in range(bins):
        mask = index == b
        rows.append({
            "bin": f"{edges[b]:.1f}-{edges[b + 1]:.1f}",
            "rows": int(mask.sum()),
            "sum_probability": float(probability[mask].sum()),
            "sum_positive": float(y[mask].sum()),
            "mean_probability": float(probability[mask].mean()) if mask.any() else None,
            "observed_rate": float(y[mask].mean()) if mask.any() else None,
        })
    return rows


def missingness(frame, features) -> dict[str, float]:
    return {
        name: round(100.0 * float(frame[name].isna().mean()), 3)
        for name in features
    }


def top_importance(booster, features, top: int = 15) -> list[dict]:
    gain = booster.get_score(importance_type="gain")
    rows = [{"feature": name, "gain": float(gain.get(name, 0.0))} for name in features]
    rows.sort(key=lambda row: row["gain"], reverse=True)
    return rows[:top]


def evaluate_predictions(test_frame, prediction: np.ndarray,
                         groups=REPORT_GROUPS) -> dict[str, dict]:
    """Score one fitted model on each pre-registered view without refitting."""
    result: dict[str, dict] = {}
    for group in groups:
        mask = group_mask(test_frame, group)
        y = test_frame.loc[mask, "label"].to_numpy(dtype=np.int64)
        p = prediction[mask]
        if len(y) == 0:
            result[group.name] = {"rows": 0}
            continue
        result[group.name] = {
            "rows": int(len(y)),
            "positives": int(y.sum()),
            "prevalence_test": float(y.mean()),
            "models": {"M": score_metrics(y, p, float(y.mean()), p)},
        }
    return result


def load_baselines_by_split(path: Path) -> dict[str, dict[str, Any]]:
    """Temporal and LOSO baseline groups, keyed by split name."""
    result: dict[str, dict[str, Any]] = {}
    for candidate in (path, path.with_name("baselines_onset_metrics.json")):
        if not candidate.exists():
            continue
        payload = json.loads(candidate.read_text(encoding="utf-8"))
        for report in payload["reports"]:
            name = report["split"]["name"]
            result[name] = {**result.get(name, {}), **report["groups"]}
    if "temporal" not in result:
        raise ValueError("temporal baseline report not found")
    return result


def run_fold(root: Path, config: Config, split: Split, plan: FitPlan,
             selected: dict[str, list[str]], temp_dir: Path,
             baselines: dict[str, dict[str, Any]], skip_sanity: bool) -> dict[str, Any]:
    """Train and score every selected variant on one split, with both controls."""
    fold_started = time.perf_counter()
    print(f"split {split.name}: fit {plan.fit_years}, validation {plan.validation_year}, "
          f"refit {plan.final_years}, test {split.test_year}", flush=True)

    train_all = load_frames(root, config, split, temp_dir)
    train, test = fold_frames(train_all, split)
    del train_all
    fit, validation = split_fit_validation(train, plan)
    print(f"rows train {len(train):,} fit {len(fit):,} validation {len(validation):,} "
          f"test {len(test):,}", flush=True)

    variants: dict[str, Any] = {}
    for name, features in selected.items():
        variant_started = time.perf_counter()
        booster, rounds, history = fit_with_early_stopping(fit, validation, features)
        final_model = refit(train, features, rounds)
        prediction = predict(final_model, test, features)
        groups = evaluate_predictions(test, prediction)
        cropland = group_mask(test, REPORT_GROUPS[0])
        variants[name] = {
            "features": list(features),
            "rounds": {
                "best_iteration": rounds - 1,
                "best_round": rounds,
                "best_validation_pr_auc": float(booster.best_score),
                "rounds_run": len(history.get("validation", {}).get("aucpr", [])),
                "final_validation_pr_auc": (
                    float(history["validation"]["aucpr"][-1])
                    if history.get("validation", {}).get("aucpr") else None
                ),
            },
            "groups": groups,
            "reliability_cropland_pooled": reliability_table(
                test.loc[cropland, "label"].to_numpy(dtype=np.int64),
                prediction[cropland],
            ),
            "missingness_train": missingness(train, features),
            "missingness_test": missingness(test, features),
            "importance_gain_top15": top_importance(final_model, features),
            "seconds": round(time.perf_counter() - variant_started, 2),
        }
        print(f"variant {name}: best round {rounds} "
              f"(validation PR-AUC {booster.best_score:.4f}), "
              f"{variants[name]['seconds']}s", flush=True)
        del booster, final_model, prediction, history
        release_memory()

    sanity: dict[str, Any] = {}
    if not skip_sanity:
        # (i) XGBoost on the three B3 features must match this fold's B3.
        booster3, rounds3, _ = fit_with_early_stopping(fit, validation, B3_FEATURES)
        model3 = refit(train, B3_FEATURES, rounds3)
        prediction3 = predict(model3, test, B3_FEATURES)
        group3 = evaluate_predictions(test, prediction3, (REPORT_GROUPS[0],))
        m3 = group3[REPORT_GROUPS[0].name]["models"]["M"]
        reference = baselines[split.name]["cropland/pooled"]["models"]["B3"]
        floor = reference["pr_auc"] - 0.02
        sanity["b3_features"] = {
            "features": B3_FEATURES, "rounds": rounds3,
            "pr_auc": m3["pr_auc"], "b3_pr_auc": reference["pr_auc"],
            "floor": floor, "passed": bool(m3["pr_auc"] >= floor),
        }
        if not sanity["b3_features"]["passed"]:
            raise RuntimeError(
                f"{split.name} B3-feature sanity failed: "
                f"{m3['pr_auc']:.4f} < {floor:.4f}"
            )

        # (ii) Labels permuted within each training season, and within the test.
        train_permuted = permute_within_season(train, SEED)
        test_permuted = permute_within_season(test, SEED + 1)
        fit_permuted = train_permuted[train["year"].to_numpy() != plan.validation_year]
        validation_permuted = train_permuted[train["year"].to_numpy() == plan.validation_year]
        booster_p, rounds_p, _ = fit_with_early_stopping(
            fit, validation, FULL_FEATURES,
            fit_labels=fit_permuted, validation_labels=validation_permuted,
        )
        model_p = refit(train, FULL_FEATURES, rounds_p, labels_override=train_permuted)
        prediction_p = predict(model_p, test, FULL_FEATURES)
        mask = group_mask(test, REPORT_GROUPS[0])
        y_permuted = test_permuted[mask]
        ap = score_metrics(y_permuted, prediction_p[mask], float(y_permuted.mean()),
                           prediction_p[mask])["pr_auc"]
        prevalence = float(y_permuted.mean())
        sanity["permuted_labels"] = {
            "pr_auc": ap, "prevalence": prevalence, "rounds": rounds_p,
            "absolute_gap": abs(ap - prevalence), "passed": bool(abs(ap - prevalence) <= 0.01),
        }
        if not sanity["permuted_labels"]["passed"]:
            raise RuntimeError(
                f"{split.name} permuted-label sanity failed: {ap:.4f} vs {prevalence:.4f}"
            )
        print(f"sanity {split.name}:", json.dumps(sanity), flush=True)
        del booster3, model3, prediction3, booster_p, model_p, prediction_p
        release_memory()

    week_index = (
        test.groupby("week_index")["label"].agg(["size", "mean"]).reset_index()
        .rename(columns={"size": "rows", "mean": "positive_rate"}).to_dict("records")
    )
    result = {
        "split": {"name": split.name, "train_years": list(split.train_years),
                  "test_year": split.test_year},
        "plan": {"fit_years": list(plan.fit_years),
                 "validation_year": plan.validation_year,
                 "final_years": list(plan.final_years)},
        "rows": {"train": int(len(train)), "fit": int(len(fit)),
                 "validation": int(len(validation)), "test": int(len(test))},
        "variants": variants,
        "sanity": sanity,
        "week_index_test": week_index,
        "seconds": round(time.perf_counter() - fold_started, 2),
    }
    del train, test, fit, validation
    release_memory()
    print(f"{split.name} done in {result['seconds']}s, process peak RSS so far "
          f"{resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0:.0f} MB",
          flush=True)
    return result


def run_spatial_fold(root: Path, config: Config, split: Split, plan: FitPlan,
                     selected: dict[str, list[str]], temp_dir: Path,
                     skip_sanity: bool) -> dict[str, Any]:
    """S5: fit on the training blocks of 2019-2023, score 2024 held-out blocks.

    Every number here is frozen by section 5.5 of the pre-registration. The
    baseline is refit on exactly these training rows so the verdict compares
    like with like; nothing is read from the S3/S4 all-block baselines.
    """
    started = time.perf_counter()
    print(f"split {split.name}: fit {plan.fit_years}, validation {plan.validation_year}, "
          f"refit {plan.final_years}, test {split.test_year} (held-out 5 km blocks)",
          flush=True)

    all_rows = load_frames(root, config, split, temp_dir)
    # The frozen partition is over the AOI lattice, not over loaded rows.
    partition = partition_blocks(all_block_ids(root))
    train, test = spatial_frames(all_rows, partition, split.test_year)
    del all_rows
    fit, validation = split_fit_validation(train, plan)
    print(f"blocks train {len(partition.train_blocks)} held-out {len(partition.held_out_blocks)} "
          f"| rows train {len(train):,} fit {len(fit):,} validation {len(validation):,} "
          f"test {len(test):,}", flush=True)
    print(f"test blocks {sorted(int(b) for b in test['block_5km'].unique())}", flush=True)

    variants: dict[str, Any] = {}
    predictions: dict[str, np.ndarray] = {}
    for name, features in selected.items():
        variant_started = time.perf_counter()
        booster, rounds, history = fit_with_early_stopping(fit, validation, features)
        final_model = refit(train, features, rounds)
        prediction = predict(final_model, test, features)
        predictions[name] = prediction
        groups = evaluate_predictions(test, prediction)
        cropland = group_mask(test, REPORT_GROUPS[0])
        variants[name] = {
            "features": list(features),
            "rounds": {
                "best_iteration": rounds - 1,
                "best_round": rounds,
                "hit_cap": bool(rounds >= MAX_ROUNDS),
                "best_validation_pr_auc": float(booster.best_score),
                "rounds_run": len(history.get("validation", {}).get("aucpr", [])),
            },
            "groups": groups,
            "missingness_train": missingness(train, features),
            "missingness_test": missingness(test, features),
            "importance_gain_top15": top_importance(final_model, features),
            "seconds": round(time.perf_counter() - variant_started, 2),
        }
        print(f"variant {name}: best round {rounds}"
              f"{' (CAP)' if rounds >= MAX_ROUNDS else ''} "
              f"(validation PR-AUC {booster.best_score:.4f}), "
              f"{variants[name]['seconds']}s", flush=True)
        del booster, final_model, prediction, history
        release_memory()

    # The baseline is refit on the S5 training rows and scored on the S5 test
    # rows, inside each reported population exactly as baselines.py does it.
    b3_predictions: dict[str, np.ndarray] = {}
    b3_metrics: dict[str, dict[str, Any]] = {}
    for group in REPORT_GROUPS:
        train_group = train.loc[group_mask(train, group)]
        group_test = test.loc[group_mask(test, group)]
        scores = b3_scores(train_group, group_test)
        full = np.full(len(test), np.nan)
        full[group_mask(test, group)] = scores
        b3_predictions[group.name] = full
        y_group = group_test["label"].to_numpy(dtype=np.int64)
        b3_metrics[group.name] = score_metrics(
            y_group, scores, float(y_group.mean()), scores)
        for variant in selected:
            entry = variants[variant]["groups"].get(group.name, {})
            if entry.get("models"):
                entry["models"]["B3"] = b3_metrics[group.name]
    head_b3 = b3_metrics[REPORT_GROUPS[0].name]
    print(f"B3 refit on S5 rows (cropland/pooled): PR-AUC {head_b3['pr_auc']:.4f} "
          f"(rows {head_b3['rows']:,}, positives {head_b3['positives']:,})", flush=True)

    headline_mask = group_mask(test, REPORT_GROUPS[0])
    predictions["B3"] = b3_predictions[REPORT_GROUPS[0].name]
    per_block = per_block_metrics(test, predictions["full"], headline_mask)
    bootstrap = block_bootstrap(test, predictions, headline_mask)
    onset_entries: dict[str, Any] = {}
    for onset_group in REPORT_GROUPS[4:]:
        mask = group_mask(test, onset_group)
        y = test.loc[mask, "label"].to_numpy(dtype=np.int64)
        entry: dict[str, Any] = {"rows": int(len(y)),
                                 "positives": int(y.sum()),
                                 "prevalence": float(y.mean()) if len(y) else None,
                                 "models": {}}
        for name in selected:
            prediction = predictions[name][mask]
            entry["models"][name] = score_metrics(
                y, prediction, float(y.mean()), prediction)
        entry["models"]["B3"] = b3_metrics[onset_group.name]
        if len(y) and 0 < y.sum() < len(y):
            predictions["B3"] = b3_predictions[onset_group.name]
            entry["bootstrap"] = block_bootstrap(test, predictions, mask)
        onset_entries[onset_group.name] = entry

    sanity: dict[str, Any] = {}
    if not skip_sanity:
        # (i) XGBoost on the three B3 features must match the *S5* B3 refit.
        booster3, rounds3, _ = fit_with_early_stopping(fit, validation, B3_FEATURES)
        model3 = refit(train, B3_FEATURES, rounds3)
        prediction3 = predict(model3, test, B3_FEATURES)
        y_head = test.loc[headline_mask, "label"].to_numpy(dtype=np.int64)
        m3 = score_metrics(y_head, prediction3[headline_mask], float(y_head.mean()),
                           prediction3[headline_mask])
        floor = head_b3["pr_auc"] - 0.02
        sanity["b3_features"] = {
            "features": B3_FEATURES, "rounds": rounds3,
            "pr_auc": m3["pr_auc"], "b3_pr_auc": head_b3["pr_auc"],
            "floor": floor, "passed": bool(m3["pr_auc"] >= floor),
        }
        if not sanity["b3_features"]["passed"]:
            raise RuntimeError(f"{split.name} B3-feature sanity failed: "
                               f"{m3['pr_auc']:.4f} < {floor:.4f}")
        del booster3, model3, prediction3
        release_memory()

        # (ii) Labels permuted within each training season, test permuted separately.
        train_permuted = permute_within_season(train, SEED)
        test_permuted = permute_within_season(test, SEED + 1)
        fit_permuted = train_permuted[train["year"].to_numpy() != plan.validation_year]
        validation_permuted = train_permuted[train["year"].to_numpy() == plan.validation_year]
        booster_p, rounds_p, _ = fit_with_early_stopping(
            fit, validation, FULL_FEATURES,
            fit_labels=fit_permuted, validation_labels=validation_permuted)
        model_p = refit(train, FULL_FEATURES, rounds_p, labels_override=train_permuted)
        prediction_p = predict(model_p, test, FULL_FEATURES)
        y_permuted = test_permuted[headline_mask]
        ap = score_metrics(y_permuted, prediction_p[headline_mask],
                           float(y_permuted.mean()),
                           prediction_p[headline_mask])["pr_auc"]
        prevalence = float(y_permuted.mean())
        sanity["permuted_labels"] = {
            "pr_auc": ap, "prevalence": prevalence, "rounds": rounds_p,
            "absolute_gap": abs(ap - prevalence),
            "passed": bool(abs(ap - prevalence) <= 0.01),
        }
        if not sanity["permuted_labels"]["passed"]:
            raise RuntimeError(f"{split.name} permuted-label sanity failed: "
                               f"{ap:.4f} vs {prevalence:.4f}")
        print(f"sanity {split.name}:", json.dumps(sanity), flush=True)
        del booster_p, model_p, prediction_p
        release_memory()

    result = {
        "split": {"name": split.name, "train_years": list(split.train_years),
                  "test_year": split.test_year},
        "plan": {"fit_years": list(plan.fit_years),
                 "validation_year": plan.validation_year,
                 "final_years": list(plan.final_years)},
        "partition": partition.as_dict(),
        "test_blocks_observed": sorted(int(b) for b in test["block_5km"].unique()),
        "train_blocks_observed": sorted(int(b) for b in train["block_5km"].unique()),
        "heldout_blocks_without_scored_rows": sorted(
            set(partition.held_out_blocks) - {int(b) for b in test["block_5km"].unique()}),
        "train_blocks_without_rows": sorted(
            set(partition.train_blocks) - {int(b) for b in train["block_5km"].unique()}),
        "rows": {"train": int(len(train)), "fit": int(len(fit)),
                 "validation": int(len(validation)), "test": int(len(test)),
                 "test_decision_population": int(head_b3["rows"])},
        "baseline_b3_refit_s5": head_b3,
        "baseline_b3_refit_s5_by_group": b3_metrics,
        "variants": variants,
        "per_block_cropland_pooled": per_block,
        "bootstrap_cropland_pooled": bootstrap,
        "onset": onset_entries,
        "sanity": sanity,
        "week_index_test": (
            test.groupby("week_index")["label"].agg(["size", "mean"]).reset_index()
            .rename(columns={"size": "rows", "mean": "positive_rate"}).to_dict("records")
        ),
        "seconds": round(time.perf_counter() - started, 2),
    }
    del train, test, fit, validation
    release_memory()
    print(f"{split.name} done in {result['seconds']}s, process peak RSS so far "
          f"{resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0:.0f} MB",
          flush=True)
    return result


def summarise_spatial(fold: dict[str, Any], variants: list[str]) -> dict[str, Any]:
    """The frozen S5 verdict: point estimates plus block-bootstrap intervals."""
    headline = "cropland/pooled"
    bootstrap = fold["bootstrap_cropland_pooled"]["metrics"]
    b3 = fold["baseline_b3_refit_s5"]
    checks: dict[str, Any] = {}
    for variant in variants:
        point = fold["variants"][variant]["groups"][headline]["models"]["M"]
        interval = bootstrap[variant]
        checks[variant] = {
            "point": {metric: point[metric] for metric in
                      ("pr_auc", "precision_at_recall_0_5", "brier")},
            "bootstrap": interval,
            "pr_auc_bar": b3["pr_auc"] + 0.05,
            "pr_auc_passed": bool(point["pr_auc"] >= b3["pr_auc"] + 0.05),
            "p_at_r_5_bar": b3["precision_at_recall_0_5"],
            "p_at_r_5_passed": bool(
                point["precision_at_recall_0_5"] > b3["precision_at_recall_0_5"]),
            "brier_bar": b3["brier"],
            "brier_passed": bool(point["brier"] < b3["brier"]),
        }
        checks[variant]["all_passed"] = bool(
            checks[variant]["pr_auc_passed"]
            and checks[variant]["p_at_r_5_passed"]
            and checks[variant]["brier_passed"])
    return {
        "headline": headline,
        "reference": "B3 refit on the S5 training rows, scored on the 2024 held-out blocks",
        "b3": {metric: b3[metric] for metric in
               ("pr_auc", "precision_at_recall_0_5", "brier")},
        "variants": checks,
        "paired_difference_full_minus_B3": (
            fold["bootstrap_cropland_pooled"]["paired_difference_full_minus_B3"]),
    }


def _summary(values: list[float]) -> dict[str, float] | None:
    clean = [float(value) for value in values if value is not None]
    if not clean:
        return None
    return {
        "mean": float(np.mean(clean)),
        "std": float(np.std(clean, ddof=1)) if len(clean) > 1 else 0.0,
        "min": float(np.min(clean)),
        "max": float(np.max(clean)),
    }


def _b3_mean(baselines: dict[str, dict[str, Any]], folds: list[dict[str, Any]],
             metric: str) -> float:
    values = [baselines[fold["split"]["name"]]["cropland/pooled"]["models"]["B3"][metric]
              for fold in folds]
    return float(np.mean(values))


def summarise_loso(folds: list[dict[str, Any]],
                   baselines: dict[str, dict[str, Any]],
                   variants: list[str]) -> dict[str, Any]:
    """Mean/std/min/max per view, plus pooled reliability and the frozen verdict."""
    summary: dict[str, Any] = {"folds": [fold["split"]["name"] for fold in folds],
                               "groups": {}, "per_fold": {}}
    for group in REPORT_GROUPS:
        name = group.name
        entry: dict[str, Any] = {}
        b3 = [baselines[fold["split"]["name"]][name]["models"]["B3"]["pr_auc"]
              for fold in folds]
        entry["B3"] = {"per_fold": dict(zip(summary["folds"], b3)),
                       "pr_auc": _summary(b3)}
        for variant in variants:
            rows = [fold["variants"][variant]["groups"][name]["models"]["M"]
                    for fold in folds]
            entry[variant] = {
                "per_fold": dict(zip(summary["folds"],
                                     [row["pr_auc"] for row in rows])),
                "pr_auc": _summary([row["pr_auc"] for row in rows]),
                "precision_at_recall_0_5": _summary(
                    [row["precision_at_recall_0_5"] for row in rows]),
                "recall_at_precision_0_5": _summary(
                    [row["recall_at_precision_0_5"] for row in rows]),
                "brier": _summary([row["brier"] for row in rows]),
                "roc_auc": _summary([row["roc_auc"] for row in rows]),
                "lift_over_prevalence": _summary(
                    [row["lift_over_prevalence"] for row in rows]),
            }
        summary["groups"][name] = entry

    # Pool the cropland/pooled reliability bins over all held-out seasons.
    pooled = None
    for fold in folds:
        table = fold["variants"]["full"]["reliability_cropland_pooled"]
        if pooled is None:
            pooled = [dict(row) for row in table]
        else:
            for left, right in zip(pooled, table):
                left["rows"] += right["rows"]
                left["sum_probability"] += right["sum_probability"]
                left["sum_positive"] += right["sum_positive"]
    for row in pooled or []:
        row["mean_probability"] = (row["sum_probability"] / row["rows"]
                                   if row["rows"] else None)
        row["observed_rate"] = (row["sum_positive"] / row["rows"]
                                if row["rows"] else None)
    summary["reliability_cropland_pooled_pooled"] = pooled
    summary["reliability_cropland_pooled_loso_2024"] = folds[-1]["variants"]["full"][
        "reliability_cropland_pooled"]

    headline = summary["groups"]["cropland/pooled"]
    b3_pr = headline["B3"]["pr_auc"]["mean"]
    b3_precision = _b3_mean(baselines, folds, "precision_at_recall_0_5")
    b3_brier = _b3_mean(baselines, folds, "brier")
    checks = {}
    for variant in variants:
        entry = headline[variant]
        checks[variant] = {
            "pr_auc": entry["pr_auc"]["mean"],
            "pr_auc_bar": b3_pr + 0.05,
            "pr_auc_passed": bool(entry["pr_auc"]["mean"] >= b3_pr + 0.05),
            "precision_at_recall_0_5": entry["precision_at_recall_0_5"]["mean"],
            "p_at_r_5_bar": b3_precision,
            "p_at_r_5_passed": bool(
                entry["precision_at_recall_0_5"]["mean"] > b3_precision),
            "brier": entry["brier"]["mean"],
            "brier_bar": b3_brier,
            "brier_passed": bool(entry["brier"]["mean"] < b3_brier),
        }
        checks[variant]["all_passed"] = bool(
            checks[variant]["pr_auc_passed"]
            and checks[variant]["p_at_r_5_passed"]
            and checks[variant]["brier_passed"])
    summary["verdict"] = {"reference": "mean B3 over the six folds", "variants": checks}
    return summary


def compact_table(report: dict[str, Any], baselines: dict[str, Any]) -> str:
    def fmt(value, digits=4):
        return "n/a" if value is None else f"{value:.{digits}f}"

    lines = [
        "temporal split (train 2019-2023, validation 2023, test 2024)",
        f"{'group':20s} {'model':7s} {'PR-AUC':>8s} {'P@R.5':>7s} {'R@P.5':>7s} "
        f"{'lift':>7s} {'ROC':>7s} {'Brier':>7s}",
    ]
    for group in REPORT_GROUPS:
        group_name = group.name
        baseline_group = baselines.get(group_name, {})
        for model in ("B0", "B1", "B2", "B3"):
            metrics = baseline_group.get("models", {}).get(model)
            if metrics is None:
                continue
            lines.append(
                f"{group_name:20s} {model:7s} {fmt(metrics['pr_auc']):>8s} "
                f"{fmt(metrics['precision_at_recall_0_5']):>7s} "
                f"{fmt(metrics['recall_at_precision_0_5']):>7s} "
                f"{fmt(metrics['lift_over_prevalence'], 2):>7s} "
                f"{fmt(metrics['roc_auc']):>7s} {fmt(metrics['brier']):>7s}"
            )
        for variant, payload in report["variants"].items():
            metrics = payload["groups"].get(group_name, {}).get("models", {}).get("M")
            if metrics is None:
                continue
            lines.append(
                f"{group_name:20s} M-{variant:5s} {fmt(metrics['pr_auc']):>8s} "
                f"{fmt(metrics['precision_at_recall_0_5']):>7s} "
                f"{fmt(metrics['recall_at_precision_0_5']):>7s} "
                f"{fmt(metrics['lift_over_prevalence'], 2):>7s} "
                f"{fmt(metrics['roc_auc']):>7s} {fmt(metrics['brier']):>7s}"
            )
        lines.append("")
    return "\n".join(lines)


def loso_table(summary: dict[str, Any], variants: list[str]) -> str:
    def fmt(value, digits=4):
        return "n/a" if value is None else f"{value:.{digits}f}"

    folds = summary["folds"]
    lines = [f"{'group':20s} {'model':10s} " + " ".join(f"{name:>9s}" for name in folds)
             + f" {'mean':>9s} {'std':>8s} {'min':>9s} {'max':>9s}"]
    for group in [g.name for g in REPORT_GROUPS]:
        entry = summary["groups"][group]
        series = [("B3", entry["B3"]["pr_auc"], entry["B3"]["per_fold"])]
        for variant in variants:
            series.append((f"M-{variant}", entry[variant]["pr_auc"],
                           entry[variant]["per_fold"]))
        for label, stats, per_fold in series:
            cells = " ".join(f"{fmt(per_fold[name]):>9s}" for name in folds)
            lines.append(
                f"{group:20s} {label:10s} {cells} {fmt(stats['mean']):>9s} "
                f"{fmt(stats['std']):>8s} {fmt(stats['min']):>9s} {fmt(stats['max']):>9s}"
            )
        lines.append("")
    return "\n".join(lines)


def spatial_table(report: dict[str, Any]) -> str:
    """S5: point estimate, block-bootstrap interval and the frozen bar per view."""
    def fmt(value, digits=4):
        return "n/a" if value is None else f"{value:.{digits}f}"

    metrics = report["bootstrap_cropland_pooled"]["metrics"]
    lines = [
        f"S5 spatial holdout: train blocks {report['partition']['n_train_blocks']} "
        f"({report['split']['train_years']}), test blocks "
        f"{report['partition']['n_heldout_blocks']} (2024)",
        f"bootstrap {report['bootstrap_cropland_pooled']['draws_usable']}/"
        f"{report['bootstrap_cropland_pooled']['draws_requested']} usable draws over "
        f"{len(report['bootstrap_cropland_pooled']['blocks'])} blocks, 95% percentile",
        f"{'group':20s} {'model':10s} {'PR-AUC':>8s} {'[2.5':>8s} {'97.5]':>8s} "
        f"{'P@R.5':>7s} {'Brier':>7s}",
    ]
    for group in report["variants"]["full"]["groups"]:
        for model in ("B3", "full", "ndvi_only"):
            point = (report["baseline_b3_refit_s5_by_group"].get(group)
                     if model == "B3" else
                     report["variants"][model]["groups"].get(group, {})
                     .get("models", {}).get("M"))
            if not point:
                continue
            interval = metrics.get(model, {}).get("pr_auc") if model != "B3" else None
            lines.append(
                f"{group:20s} {model:10s} {fmt(point['pr_auc']):>8s} "
                f"{fmt(interval['p2_5']) if interval else 'n/a':>8s} "
                f"{fmt(interval['p97_5']) if interval else 'n/a':>8s} "
                f"{fmt(point['precision_at_recall_0_5']):>7s} "
                f"{fmt(point['brier']):>7s}"
            )
        lines.append("")
    lines.append("onset (separate headline, no effect on the verdict)")
    for name, entry in report["onset"].items():
        for model, point in entry["models"].items():
            lines.append(
                f"{name:20s} {model:10s} {fmt(point['pr_auc']):>8s} "
                f"{'':>8s} {'':>8s} {fmt(point['precision_at_recall_0_5']):>7s} "
                f"{fmt(point['brier']):>7s}"
            )
    lines.append("")
    for metric, interval in report["summary"]["paired_difference_full_minus_B3"].items():
        lines.append(f"paired M-full - B3 {metric:26s} "
                     f"{fmt(interval['mean']) if interval else 'n/a':>8s} "
                     f"[{fmt(interval['p2_5']) if interval else 'n/a'}, "
                     f"{fmt(interval['p97_5']) if interval else 'n/a'}]")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=None)
    parser.add_argument("--data-dir", default=None)
    parser.add_argument("--split", choices=("temporal", "loso", "spatial", "all"),
                        default="temporal")
    parser.add_argument("--variant", choices=(*VARIANTS, "all"), default="all")
    parser.add_argument("--out", default="artifacts/phase7")
    parser.add_argument("--baselines", default="artifacts/phase7/baselines_metrics.json")
    parser.add_argument("--temp-dir", default="/tmp/delta_crops_phase7")
    parser.add_argument("--skip-sanity", action="store_true",
                        help="skip the B3-feature and permutation controls (debug only)")
    args = parser.parse_args(argv)

    config = load_config(args.config)
    root = Path(args.data_dir) if args.data_dir else Path("data")
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    baselines = load_baselines_by_split(Path(args.baselines))
    selected = (dict(VARIANTS) if args.variant == "all"
                else {args.variant: VARIANTS[args.variant]})
    temp_dir = Path(args.temp_dir)
    params = {key: value for key, value in PARAMS.items() if key != "nthread"}

    if args.split in ("temporal", "all"):
        split = temporal_split(config.seasons.years)
        plan = temporal_plan(split.train_years)
        fold = run_fold(root, config, split, plan, selected, temp_dir, baselines,
                        args.skip_sanity)
        temporal = {
            "seed": SEED,
            "split": fold["split"],
            "plan": fold["plan"],
            "params": params,
            "variants": fold["variants"],
            "sanity": fold["sanity"],
            "week_index_test": fold["week_index_test"],
            "seconds": fold["seconds"],
            "peak_rss_mb": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                                 / 1024.0, 1),
        }
        path = out_dir / "xgb_temporal.json"
        path.write_text(json.dumps(temporal, indent=2, default=float) + "\n",
                        encoding="utf-8")
        print(compact_table(temporal, baselines["temporal"]), flush=True)
        print(json.dumps({"seconds": temporal["seconds"],
                          "peak_rss_mb": temporal["peak_rss_mb"],
                          "out": str(path)}), flush=True)

    if args.split in ("spatial", "all"):
        split = temporal_split(config.seasons.years)
        plan = temporal_plan(split.train_years)
        fold = run_spatial_fold(root, config, split, plan, selected, temp_dir,
                               args.skip_sanity)
        summary = summarise_spatial(fold, list(selected))
        spatial = {
            "seed": SEED,
            "spatial_seed": fold["partition"]["seed"],
            "bootstrap_seed": BOOTSTRAP_SEED,
            "params": params,
            "split": fold["split"],
            "plan": fold["plan"],
            "partition": fold["partition"],
            "test_blocks_observed": fold["test_blocks_observed"],
            "train_blocks_observed": fold["train_blocks_observed"],
            "heldout_blocks_without_scored_rows": fold["heldout_blocks_without_scored_rows"],
            "train_blocks_without_rows": fold["train_blocks_without_rows"],
            "rows": fold["rows"],
            "baseline_b3_refit_s5": fold["baseline_b3_refit_s5"],
            "baseline_b3_refit_s5_by_group": fold["baseline_b3_refit_s5_by_group"],
            "variants": fold["variants"],
            "per_block_cropland_pooled": fold["per_block_cropland_pooled"],
            "bootstrap_cropland_pooled": fold["bootstrap_cropland_pooled"],
            "onset": fold["onset"],
            "sanity": fold["sanity"],
            "week_index_test": fold["week_index_test"],
            "summary": summary,
            "seconds": fold["seconds"],
            "peak_rss_mb": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                                 / 1024.0, 1),
        }
        path = out_dir / "xgb_spatial.json"
        path.write_text(json.dumps(spatial, indent=2, default=float) + "\n",
                        encoding="utf-8")
        print(spatial_table(spatial), flush=True)
        print(json.dumps({"verdict": summary["variants"],
                          "seconds": spatial["seconds"],
                          "peak_rss_mb": spatial["peak_rss_mb"],
                          "out": str(path)}, indent=2, default=float), flush=True)

    if args.split in ("loso", "all"):
        folds = []
        for split in loso_splits(config.seasons.years):
            folds.append(run_fold(root, config, split, loso_plan(split.train_years),
                                  selected, temp_dir, baselines,
                                  args.skip_sanity))
        summary = summarise_loso(folds, baselines, list(selected))
        loso = {
            "seed": SEED,
            "params": params,
            "folds": folds,
            "summary": summary,
            "seconds": round(time.perf_counter() - started, 2),
            "peak_rss_mb": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                                 / 1024.0, 1),
        }
        path = out_dir / "xgb_loso.json"
        path.write_text(json.dumps(loso, indent=2, default=float) + "\n",
                        encoding="utf-8")
        print(loso_table(summary, list(selected)), flush=True)
        print(json.dumps({"verdict": summary["verdict"],
                          "seconds": loso["seconds"],
                          "peak_rss_mb": loso["peak_rss_mb"],
                          "out": str(path)}), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

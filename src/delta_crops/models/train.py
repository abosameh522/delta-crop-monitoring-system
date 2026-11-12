"""S3: XGBoost on the temporal split (train 2019-2023, test 2024).

One model per variant is trained once on all training rows and then evaluated
on strata, populations and the onset subset without retraining. The
hyperparameters, early-stopping season (2023) and refit procedure are frozen in
``docs/phase7-preregistration.md`` (section 5.3).
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
import xgboost as xgb

from delta_crops.config import Config, load_config
from delta_crops.models.baselines import (
    Group,
    brier,
    group_mask,
    score_metrics,
)
from delta_crops.models.dataset import FEATURES, build_sql, connect, temporal_split

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
                    "is_cropland", "anomaly_t", "anomaly_lag1")
TARGET_RECALL = 0.5

assert len(FULL_FEATURES) == 26, len(FULL_FEATURES)
assert len(NDVI_ONLY_FEATURES) == 16, len(NDVI_ONLY_FEATURES)
assert not set(NDVI_ONLY_FEATURES) & set(WEATHER_COLUMNS)
assert set(FULL_FEATURES) == set(NDVI_ONLY_FEATURES) | set(WEATHER_COLUMNS)


@dataclass(frozen=True)
class FitPlan:
    """Seasons passed to early stopping and to the final refit."""

    fit_years: tuple[int, ...]
    validation_year: int
    final_years: tuple[int, ...]

    @property
    def early_stopping_years(self) -> tuple[int, ...]:
        return (*self.fit_years, self.validation_year)


def temporal_plan(train_years: tuple[int, ...], validation_year: int = VALIDATION_YEAR) -> FitPlan:
    """Fit on every training season but the validation one, then refit on all."""
    years = tuple(sorted(train_years))
    if validation_year not in years:
        raise ValueError(f"validation season {validation_year} is not a training season")
    return FitPlan(
        fit_years=tuple(year for year in years if year != validation_year),
        validation_year=int(validation_year),
        final_years=years,
    )


def split_fit_validation(train_frame, plan: FitPlan):
    fit = train_frame[train_frame["year"] != plan.validation_year]
    validation = train_frame[train_frame["year"] == plan.validation_year]
    if set(fit["year"].unique()) != set(plan.fit_years):
        raise ValueError("fit seasons do not match the plan")
    if set(validation["year"].unique()) != {plan.validation_year}:
        raise ValueError("validation frame contains a season outside the plan")
    return fit, validation


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


def compact_table(report: dict[str, Any], baselines: dict[str, Any]) -> str:
    def fmt(value, digits=4):
        return "n/a" if value is None else f"{value:.{digits}f}"

    lines = [
        f"temporal split (train 2019-2023, validation 2023, test 2024)",
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


def load_baseline_report(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    groups = None
    for report in payload["reports"]:
        if report["split"]["name"] == "temporal":
            groups = report["groups"]
            break
    if groups is None:
        raise ValueError("temporal baseline report not found")
    onset_path = path.with_name("baselines_onset_metrics.json")
    if onset_path.exists():
        onset = json.loads(onset_path.read_text(encoding="utf-8"))
        for report in onset["reports"]:
            if report["split"]["name"] == "temporal":
                groups = {**groups, **report["groups"]}
                break
    return groups


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=None)
    parser.add_argument("--data-dir", default=None)
    parser.add_argument("--split", choices=("temporal",), default="temporal")
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

    split = temporal_split(config.seasons.years)
    plan = temporal_plan(split.train_years)
    print(f"split {split.name}: fit {plan.fit_years}, validation {plan.validation_year}, "
          f"refit {plan.final_years}, test {split.test_year}", flush=True)

    train_all = load_frames(root, config, split, Path(args.temp_dir))
    train = train_all[train_all["split"] == "train"].reset_index(drop=True)
    test = train_all[train_all["split"] == "test"].reset_index(drop=True)
    fit, validation = split_fit_validation(train, plan)
    print(f"rows train {len(train):,} fit {len(fit):,} validation {len(validation):,} "
          f"test {len(test):,}", flush=True)

    if args.variant == "all":
        selected = dict(VARIANTS)
    else:
        selected = {args.variant: VARIANTS[args.variant]}
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

    sanity: dict[str, Any] = {}
    if not args.skip_sanity:
        # (i) XGBoost on the three B3 features must match B3 within 0.02.
        booster3, rounds3, _ = fit_with_early_stopping(fit, validation, B3_FEATURES)
        model3 = refit(train, B3_FEATURES, rounds3)
        prediction3 = predict(model3, test, B3_FEATURES)
        group3 = evaluate_predictions(test, prediction3, (REPORT_GROUPS[0],))
        m3 = group3[REPORT_GROUPS[0].name]["models"]["M"]
        b3_reference = load_baseline_report(Path(args.baselines))["cropland/pooled"]["models"]["B3"]
        floor = b3_reference["pr_auc"] - 0.02
        sanity["b3_features"] = {
            "features": B3_FEATURES, "rounds": rounds3,
            "pr_auc": m3["pr_auc"], "b3_pr_auc": b3_reference["pr_auc"],
            "floor": floor, "passed": bool(m3["pr_auc"] >= floor),
        }
        if not sanity["b3_features"]["passed"]:
            raise RuntimeError(
                f"B3-feature sanity failed: {m3['pr_auc']:.4f} < {floor:.4f}"
            )

        # (ii) Permuted labels in each season; permuted test labels too.
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
                f"permuted-label sanity failed: {ap:.4f} vs {prevalence:.4f}"
            )
        print("sanity:", json.dumps(sanity), flush=True)

    week_index = (
        test.groupby("week_index")["label"].agg(["size", "mean"]).reset_index()
        .rename(columns={"size": "rows", "mean": "positive_rate"}).to_dict("records")
    )
    payload = {
        "seed": SEED,
        "split": {"name": split.name, "train_years": list(split.train_years),
                  "test_year": split.test_year},
        "plan": {"fit_years": list(plan.fit_years),
                 "validation_year": plan.validation_year,
                 "final_years": list(plan.final_years)},
        "params": {key: value for key, value in PARAMS.items() if key != "nthread"},
        "variants": variants,
        "sanity": sanity,
        "week_index_test": week_index,
        "seconds": round(time.perf_counter() - started, 2),
        "peak_rss_mb": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0, 1),
    }
    path = out_dir / "xgb_temporal.json"
    path.write_text(json.dumps(payload, indent=2, default=float) + "\n", encoding="utf-8")
    baselines = load_baseline_report(Path(args.baselines))
    print(compact_table(payload, baselines), flush=True)
    print(json.dumps({"seconds": payload["seconds"], "peak_rss_mb": payload["peak_rss_mb"],
                      "out": str(path)}), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

# Phase 8 freeze — S3 prediction dump for the dashboard

**Status:** frozen 2026-11-13, before any Phase 8 code or fit was written. This
document fixes what the dashboard's prediction view is allowed to show, where
the predictions come from, and the gate that must pass before any dashboard
work starts. Changing anything here requires a new dated section, not an edit.

Context: Phase 7 shipped metrics, not predictions. `artifacts/phase7/` contains
`xgb_temporal.json`, `xgb_loso.json`, `xgb_spatial.json`, the baseline reports
and the 626 MB `dataset/temporal.parquet`; no model binary and no per-row
prediction was ever persisted. The dashboard therefore needs exactly one
persistence step, and it must prove it reproduces Phase 7 rather than produce a
new result.

---

## 1. What is dumped

| Item | Frozen choice |
| --- | --- |
| Split | **S3 temporal** (train 2019–2023, test 2024) |
| Variants | **`full`** (26 features) and **`ndvi_only`** (16 features) |
| Rows | every **2024 test row** of the built dataset — 3,026,561 rows, all cells, no filtering |
| Fit data | the 2019–2023 rows produced by the Phase 7 loader (`load_frames` → `build_sql`), i.e. the identical frame Phase 7 trained on — see Revision 1 in §6 |
| Rounds | **read from `artifacts/phase7/xgb_temporal.json`**: `variants.full.rounds.best_round` = **533**, `variants.ndvi_only.rounds.best_round` = **275** |
| Hyperparameters | `delta_crops.models.train.PARAMS` unchanged: `binary:logistic`, `hist`, `max_depth=6`, `eta=0.05`, `seed=20260701`, `nthread=os.cpu_count()`, `eval_metric=aucpr` |
| Early stopping | **not run.** The frozen best round is used directly, so the dump cannot select a different model |

`ndvi_only` is included because the Phase 7 finding that it beats `full` is part
of the honest result; showing only `full` would misrepresent the model card.

---

## 2. Output location and columns

One Parquet file, `variant` as a column (not one file per variant):

`artifacts/phase8/predictions/temporal_2024.parquet`

| Column | Type | Meaning |
| --- | --- | --- |
| `cell_id` | int32 | 100 m grid cell id |
| `week_start` | date32 | ISO week start of the **information** week `t` |
| `variant` | string | `full` or `ndvi_only` |
| `prob` | float32 | predicted probability that the mean raw NDVI anomaly over `t+1`/`t+2` is ≤ −0.08 |
| `label` | int8 | the realised outcome at `t+1`/`t+2` (0/1) |
| `is_cropland` | bool | headline population flag |
| `block_5km` | int32 | the Phase 7 spatial unit, for block-level aggregation |

Everything else the dashboard renders (NDVI, anomaly, temperature, cropland
share) is joined from Gold at read time. The dump deliberately duplicates no
Gold column beyond the keys and the label.

Writes are atomic (temp file + `os.replace`) and overwrite on rerun, so the step
is idempotent and a rerun cannot duplicate rows.

---

## 3. Reproduction gate (the only reason this step is allowed to fit)

After refitting each variant and scoring 2024, the step recomputes the
`cropland/pooled` metrics with the **same** `score_metrics` used in Phase 7 and
compares them to the stored artifact:

| Variant | Metric | Must equal | Tolerance |
| --- | --- | ---: | --- |
| `full` | PR-AUC | 0.8213705 | abs Δ < 1e-9 |
| `full` | P@R.5 | 0.8713148 | abs Δ < 1e-9 |
| `full` | Brier | 0.0936884 | abs Δ < 1e-9 |
| `ndvi_only` | PR-AUC | 0.8235561 | abs Δ < 1e-9 |
| `ndvi_only` | P@R.5 | 0.8740270 | abs Δ < 1e-9 |
| `ndvi_only` | Brier | 0.0924261 | abs Δ < 1e-9 |
| both | test rows (`cropland/pooled`) | 2,449,220 | exact |
| both | positives (`cropland/pooled`) | 614,324 | exact |

A miss is reported as the actual value and the signed delta, the command exits
non-zero, and **nothing is written**. Parameters, seed, rounds and bars are not
adjusted to make the gate pass: a mismatch means the dump is not the Phase 7
model, and that is a defect to report, not to tune away.

The full-precision references are read from `xgb_temporal.json` at run time
(`groups."cropland/pooled".models.M`), not hardcoded, so the artifact stays the
single source of truth. The table above rounds them for readability only.

---

## 4. Leakage and integrity guards (refuse, do not warn)

Before any fit:

1. The `split` column is exactly `train` 2019–2023 and `test` 2024; no season on
   both sides.
2. `test_year` is constant 2024 on test rows and absent from train rows.
3. Train years equal the S3 split's `train_years`; test years equal `{2024}`.
4. `(cell_id, week_start)` is unique in the test frame — otherwise the gate's
   row count would be wrong and the map would double-count.
5. Every feature column exists and no label-derived column is used as a feature
   (the frozen `FULL_FEATURES` / `NDVI_ONLY_FEATURES` lists are imported, not
   recopied).
6. Rounds are positive integers and below the frozen 600 cap.

Any failure raises and exits non-zero before a booster is created.

---

## 5. Commands

```bash
# the dump and its gate (Revision 1: same loader as Phase 7)
python -m delta_crops.models.predictions

# equivalent explicit form
python -m delta_crops.models.predictions \
  --data-dir data \
  --metrics artifacts/phase7/xgb_temporal.json \
  --out artifacts/phase8/predictions/temporal_2024.parquet
```

---

## 6. Revision 1 — load path (2026-11-13, before any code)

The original §1/§5 said "read `artifacts/phase7/dataset/temporal.parquet`, no
rebuild". That cannot reproduce the artifact exactly, and the reason is
mechanical:

* the builder writes its Parquet with
  `ORDER BY year, cell_id, week_start` (`dataset.py:361`), so the file is sorted;
* Phase 7 trained on `load_frames()` → `SELECT ... FROM (build_sql(...))`, which
  returns DuckDB's natural hash-join order, i.e. the same rows in a **different
  order**;
* XGBoost's `hist` builds its quantile sketch and histograms from row order, so
  a different ordering can move split thresholds and change the score in the
  last digits — exactly the regime the 1e-9 gate is meant to detect.

Reading the sorted Parquet would therefore test "a model fit on the same rows in
a different order", not the Phase 7 model. Revision 1 keeps the gate's meaning
and uses the identical path Phase 7 used:

* **data:** `load_frames(root, config, temporal_split(years), temp_dir)` from
  `delta_crops.models.train` — the same function, same SQL, same DuckDB version;
* **split:** `fold_frames()`, the same guard Phase 7 used;
* **CLI:** `--data-dir` / `--config` / `--temp-dir` replace `--dataset`; the
  Parquet remains the audit copy, not the training input.

Everything else in this freeze is unchanged: split, variants, frozen rounds,
dumped columns, tolerance and counts.

---

## 7. Scope limits

- No dashboard code reads or writes anything outside `data/gold/**`,
  `artifacts/phase8/predictions/` and `artifacts/phase7/*.json`.
- No new training campaign: this is one deterministic refit per variant, with
  rounds fixed by the artifact. No sanity controls, no early stopping, no S4/S5
  prediction dump.
- No change to `train.py`, `baselines.py`, the dataset builder, the
  pre-registration, or any Phase 3–7 artifact.

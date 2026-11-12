# Phase 7 pre-registration

Frozen 2026-11-12, **before any baseline or model was run**. Nothing below may be
changed after seeing model performance; a later change is a new decision, written
down with the reason.

## 1. Label

* Unit: one **cell-week** row, 100 m grid.
* Prediction time: **end of ISO week t** (Sunday 23:59).
* Target: mean **raw** NDVI anomaly over the next two ISO weeks
  (`t+1` and `t+2`) is **≤ −0.08**.
* Both `t+1` and `t+2` must be observed (`status = 'ok'`); no "next two available
  observations". The anomaly is the raw NDVI difference from a climatology,
  **never the z-score** (D-046).
* −0.08 is a physical NDVI amount, fixed before training. It corresponds to
  roughly the **16th–21st percentile** of the forward anomaly, with a positive
  rate of **15–22 %** per season.
* A row also needs a defined anomaly at week `t` (≥ 3 baseline years), the same
  rule the stored Gold anomaly uses.

## 2. Populations

* Train on **all cells**.
* **Headline metrics: `is_cropland = true` only.** All-cells numbers are always
  reported as a second column, never as the headline.
* Non-cropland rows stay in training and in the all-cells column.

## 3. Baselines (train-only climatology)

* For a **training** row: leave-one-training-year-out baseline — the same cell
  and ISO week in the *other* training seasons.
* For a **test** row: baseline from **all** training seasons.
* Minimum **3** baseline years; a season never enters its own baseline, and a
  test season never enters any baseline.
* The temporal split therefore gives training rows a 4-year pool and test rows a
  5-year pool. This is a known asymmetry, not a bug.

## 4. Features and information time

Everything is knowable at the end of week `t`. No coordinates, no label-derived
column, no forward-week `n_obs`/coverage, no `zscore`.

| Feature | Known at |
| --- | --- |
| `ndvi_mean`, `ndvi_std`, `masked_pct_mean`, `n_obs` (week t) | end of week t |
| `ndvi_lag1..3`, `ndvi_roll2/3`, `ndvi_slope_2w` | ≤ t |
| `anomaly_t`, `baseline_mean`, `n_baseline_years` | current NDVI + training seasons only |
| temperature (`temp_max_mean_c`, `temp_max_max_c`, `temp_min_mean_c`), `solar_sum_mj_m2`, `precip_sum_mm`, `heatwave_days`, their 1–2 week lags and `heatwave_days_2w` | week t and earlier |
| `week_index` 1–20 | calendar |
| `cropland_pct`, `is_cropland` | static |

## 5. Baselines and the XGBoost model

| Model | Definition |
| --- | --- |
| B0 majority | constant score; PR-AUC equals prevalence |
| B1 persistence | score = −`anomaly_t` (current state persists) |
| B2 seasonal prior | training-year positive rate for (ISO week, `is_cropland`); no cell identity |
| B3 logistic regression | `anomaly_t`, one lagged anomaly (`anomaly_t−1`), week-of-season; fitted on training seasons only |
| M XGBoost | `hist`, `binary:logistic`, depth 6, learning rate 0.05, at most 600 rounds |

**Brier for B1 is n/a.** B1 is a ranking score, not a probability; it is compared
by PR-AUC and precision at recall 0.50 only. B0, B2, B3 and M report Brier.

**"Beats the baseline"** means, on the held-out fold, M has

1. PR-AUC at least **0.05 absolute** above the best of B0–B3, **and**
2. higher precision at recall 0.50 than that best baseline, **and**
3. lower Brier score than that best baseline.

If any part fails, the report says the model **does not beat the baseline**.

## 6. Splits and evaluation

* **Temporal holdout:** train 2019–2023, test 2024.
* **Leave-one-season-out:** six folds over 2019–2024, train on the other five.
* XGBoost early stopping uses the **most recent remaining training season** as
  validation; the chosen number of rounds is then used to **refit on all
  training seasons** (including that validation season). The test season is
  touched **once**.
* 5 km blocks (50 × 50 cells) for block-level PR-AUC and block-bootstrap
  intervals; a **spatial-holdout sensitivity run** trains on 75 % of blocks of
  2019–2023 and tests 2024 on the held-out 25 %.
* **Ablation:** NDVI-only features vs NDVI+weather features on identical folds.
  **No weather effect is claimed unless the ablation shows a gain on the
  held-out fold.**

## 7. Metrics

| Metric | Role |
| --- | --- |
| PR-AUC (average precision) | primary |
| precision at recall 0.50 | primary operating point |
| recall at precision 0.50 | secondary operating point |
| Brier score + 10-bin reliability | calibration |
| lift over prevalence | usefulness |
| ROC-AUC | secondary only |
| accuracy | **never used** (81 % negatives would make it meaningless) |

## 8. Reproducibility

Fixed seed **20260701**. Dataset builder:
`python -m delta_crops.models.dataset`. Artifacts stay under gitignored
`artifacts/`; nothing large is committed.

## 9. Measured dataset (S1, 2026-11-12)

The threshold was **not retuned** after recomputing labels from train-only
baselines. Recomputed positive rate per season: 2019 16.18 %, 2020 18.41 %,
2021 22.73 %, 2022 21.07 %, 2023 15.21 %, 2024 21.51 %.

Usable rows: 14,977,696 (2019 2,519,921; 2020 2,728,392; 2021 2,681,558;
2022 2,589,333; 2023 1,431,931; 2024 3,026,561). 2024 matches the stored plan row
set exactly; training rows are fewer because the test season cannot enter their
baseline pool.

## 10. Known limitations

* Weather is a **single grid point** per week: about **120 independent weather
  weeks** (6 seasons × 20), not millions of rows. Cell-level weather evidence is
  therefore weak by construction.
* Only six seasons; 2023 has the fewest usable rows.
* Rainfall has no signal (season totals 0.0–2.1 mm); it stays as a feature for
  completeness but no rainfall effect will be claimed.
* Heatwaves are concentrated in **2021, 2023 and 2024**; heat response is learned
  mostly from three seasons and cannot be separated from year effects.
* Harvest and irrigation are unobserved confounders; a "stress" label can reflect
  either rather than heat.
* 2024 is both the newest and the most extreme season (25 heatwave days): the
  temporal test is also a **distribution-shift test**.
* Per-cell baselines let the model recognise **the same cells** in a new season.
  It does not demonstrate generalisation to new locations; the spatial-holdout
  run is the only partial evidence.
* Feature missingness differs by split (e.g. `ndvi_lag3` missing 12.7 % in
  training vs 20.4 % in 2024) because retained early-season rows differ; XGBoost
  handles missing values natively, but the shift is reported.
* Training rows use a 4-year baseline pool and test rows a 5-year pool in the
  temporal split.

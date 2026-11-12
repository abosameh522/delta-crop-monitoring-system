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
| `anomaly_lag1` (exported for B3 only; not in `FEATURES`) | end of week `t−1`, i.e. ≤ t |
| temperature (`temp_max_mean_c`, `temp_max_max_c`, `temp_min_mean_c`), `solar_sum_mj_m2`, `precip_sum_mm`, `heatwave_days`, their 1–2 week lags and `heatwave_days_2w` | week t and earlier |
| `week_index` 1–20 | calendar |
| `cropland_pct`, `is_cropland` | static |

## 5. Baselines and the XGBoost model

| Model | Definition |
| --- | --- |
| B0 majority | constant score; PR-AUC equals prevalence |
| B1 persistence | score = −`anomaly_t` (current state persists) |
| B2 seasonal prior | training-year positive rate for (ISO week, `is_cropland`); no cell identity |
| B3 logistic regression | `anomaly_t`, one lagged anomaly (`anomaly_t−1`), week-of-season; with an intercept; fitted on training seasons only |
| M XGBoost | `hist`, `binary:logistic`, depth 6, learning rate 0.05, at most 600 rounds |

**Brier for B1 is n/a.** B1 is a ranking score, not a probability; it is compared
by PR-AUC and precision at recall 0.50 only. B0, B2, B3 and M report Brier.

**"Beats the baseline"** means, on the held-out fold, M has

1. PR-AUC at least **0.05 absolute** above the best of B0–B3, **and**
2. higher precision at recall 0.50 than that best baseline, **and**
3. lower Brier score than that best baseline.

If any part fails, the report says the model **does not beat the baseline**.

### 5.1 Decision population (added 2026-11-12, before any model result)

* The verdict is decided on **`is_cropland = true`, stratum `pooled`**, on the
  **temporal holdout** (train 2019–2023, test 2024). Strata `complete` and
  `incomplete` are reported but are **not** decision populations.
* **"The best baseline" on a fold** is the baseline with the highest
  `cropland/pooled` PR-AUC on that fold (B0, B1, B2 or B3).
* The leave-one-season-out result is reported alongside as the **mean and the
  range over the six folds**, and the verdict must also say whether M beats the
  best baseline on PR-AUC in **at least four of the six folds**. That fold count
  is a reporting condition, not a replacement for the temporal decision.
* For the Brier condition, M is compared against **that same best baseline** and
  **also** against the **best Brier among the probabilistic baselines B0, B2 and
  B3**. Both comparisons are reported; the pre-registered condition above stays
  exactly as written.

### 5.2 Onset analysis (secondary, added 2026-11-12)

Added **after the B0–B3 baseline results were seen, but before any model result**.

* Subset: **`anomaly_t > −0.08`** — the current week is not already stressed.
* Report prevalence, PR-AUC, precision at recall 0.50 and lift for B0–B3 and
  later M on that subset, per fold, for **`cropland/pooled`** (all-cells as the
  second column).
* Reason: persistence dominates the pooled metric because a cell's anomaly stays
  similar from week to week through the season. The pooled number therefore
  mostly measures persistence, and the onset subset asks the harder question —
  given a currently unstressed cell, which one becomes stressed next?
* The decision rule in §5.1 is unchanged by this secondary analysis.

### 5.3 S3 execution freeze (2026-11-12, before any model was trained)

Written **before any XGBoost model was fitted or scored**. No model result has
been seen at the time of this commit.

**(a) Frozen hyperparameters.** `hist`, `binary:logistic`, `max_depth = 6`,
`eta = 0.05`, `num_boost_round ≤ 600`, `seed = 20260701`, early-stopping
patience **50** rounds on `aucpr`. Nothing is tuned on the test season and no
hyperparameter is changed after seeing any result.

**(b) Early stopping and refit.** For the temporal split, fit on 2019–2022 with
season **2023** as the validation set, take the best iteration, then **refit on
all of 2019–2023** with that number of rounds and score 2024 **once** per model
variant. 2023 is the thinnest season (about 1.43 M usable rows).

**(c) One model, many views.** M is trained **once on all training rows**
(XGBoost handles missing values natively). It is then evaluated per stratum
(pooled, all-lags-present, at-least-one-lag-missing), per population (cropland
headline, all cells) and on the onset subset (`anomaly_t > −0.08`). **M is not
retrained per stratum or per subset.**

**(d) Numerical bar (computed from `artifacts/phase7/baselines_metrics.json`).**
On `cropland/pooled` temporal, the best baseline is **B3**: PR-AUC **0.7722**,
precision at recall 0.50 **0.8145**, Brier **0.1050**. M must therefore reach
**PR-AUC ≥ 0.8222**, **precision at recall 0.50 > 0.8145** and
**Brier < 0.1050**. All three conditions must hold.

**(e) Ablation variants.** Both variants use identical folds and the same
frozen hyperparameters.

* `ndvi_only` (16 columns, **no** weather):
  `ndvi_mean`, `ndvi_std`, `masked_pct_mean`, `n_obs`, `ndvi_lag1`,
  `ndvi_lag2`, `ndvi_lag3`, `ndvi_roll2`, `ndvi_roll3`, `ndvi_slope_2w`,
  `anomaly_t`, `baseline_mean`, `n_baseline_years`, `week_index`,
  `cropland_pct`, `is_cropland`.
* `full` (26 columns, all four `FEATURES` groups):
  the 16 above plus `temp_max_mean_c`, `temp_max_max_c`, `temp_min_mean_c`,
  `solar_sum_mj_m2`, `precip_sum_mm`, `heatwave_days`,
  `temp_max_mean_c_lag1`, `temp_max_mean_c_lag2`, `heatwave_days_lag1`,
  `heatwave_days_2w`.

Weather columns are exactly those 10. A weather effect is claimed **only if**
`full` beats `ndvi_only` on `cropland/pooled` PR-AUC on 2024. Even then, the
report must say that weather is a **single grid point shared by all cells**
(about 120 independent weeks), so a gain is weak evidence of a causal weather
effect.

**(f) Feature importance** may be printed for information (gain, top 15) but is
**not** interpreted causally.

### 5.4 S4 LOSO execution freeze (2026-11-12, before any LOSO model was fitted)

Written **before any leave-one-season-out model was fitted or scored**. No LOSO
result has been seen at the time of this commit.

**(a) Held-out unit.** One **season**. Six folds `loso-2019` … `loso-2024`: the
test rows are that season, the training rows are the other five. **All cells are
shared between train and test.** No cell is held out spatially and every
neighbour of a test cell is in training. Static features (`cropland_pct`,
`is_cropland`) and every per-cell baseline come from the training seasons.
LOSO therefore measures **temporal generalisation to an unseen season for known
locations**, not spatial generalisation; spatial leakage is **not** removed by
this design. The 5 km-block and spatial-holdout sensitivity runs in §6 remain
separate and are **not** part of S4. The verdict must not be read as evidence of
skill on new locations.

**(b) Early stopping and refit.** Within each fold the validation season is the
**most recent remaining training season** (for `loso-2024` that is 2023). Fit on
the other training seasons, take the best iteration, refit on all five training
seasons and score the held-out season **once** per variant. `loso-2024` has the
same fit/validation/refit seasons as the S3 temporal split, so its numbers are a
pipeline-consistency check and must reproduce S3 (differences only from the
documented deterministic path are a bug).

**(c) Frozen hyperparameters.** Identical to §5.3: `hist`, `binary:logistic`,
`max_depth = 6`, `eta = 0.05`, `num_boost_round ≤ 600`, `seed = 20260701`,
early-stopping patience 50 on `aucpr`. Nothing is tuned on any held-out season.

**(d) Verdict bar — stated explicitly, not inherited silently.** The S3 bar was
derived for the single 2024 temporal test, so it cannot be reused as-is for an
average over six folds. The target, metrics, population and the minimum
meaningful margin are unchanged, so the same margins are applied to the **mean
over the six folds** on `cropland/pooled`:

* mean B3 PR-AUC **0.770509** (std 0.035820, min 0.719293, max 0.824558) →
  M-full must reach mean PR-AUC **≥ 0.820509** (B3 + 0.05, the same margin as
  S3);
* mean B3 precision at recall 0.50 **0.818771** (std 0.042513) → M-full must
  reach a **strictly greater** mean P@R.5;
* mean B3 Brier **0.094783** (std 0.007035) → M-full must reach a **strictly
  lower** mean Brier.

All three conditions must hold. Per-fold values are reported descriptively and
**cannot change the verdict**; no per-fold minimum is required. The margin is
kept at 0.05 because the metric, population and target are the same and
changing it after S3 would be post-hoc.

**(e) Feature variants.** The exact column lists are those frozen in §5.3:
`full` = 26 columns, `ndvi_only` = 16 columns (the 26 minus the 10 weather
columns). Both variants run on all six folds. A weather effect is claimed only
if `full` beats `ndvi_only` on mean `cropland/pooled` PR-AUC, with the §5.3
single-grid-point caveat.

**(f) Sanity controls under LOSO, per fold.** (i) XGBoost on the three B3
features (`anomaly_t`, `anomaly_lag1`, `week_index`) must land within **0.02** of
that fold's stored B3 PR-AUC; its floor is `B3_fold − 0.02`. (ii) Training labels
permuted within each season and test labels permuted independently: PR-AUC must
be within **0.01** of the permuted test prevalence. Any fold failing a control
is a pipeline bug: stop and report. Per-fold and pooled numbers are shown.

**(g) Onset headline.** `cropland/onset` and `all/onset` (`anomaly_t > −0.08`)
are reported **separately** from the pooled table, per fold and on average,
M-full vs B3. They do not enter the verdict.

**(h) Label-window audit.** `anomaly_t` is the **current-week** anomaly; the
label is the mean anomaly of **t+1 and t+2** only. Because `anomaly_t` dominates
gain importance, a test must prove the label is not computed from the current
week. Feature importance stays informational and is not interpreted causally.

### 5.5 S5 execution freeze — spatial holdout on 5 km blocks (2026-11-12, before any S5 fit)

Written **before any spatial-holdout model was fitted or scored**. This section
executes the sensitivity run that §6 pre-registers and that §5.4(a) explicitly
excluded from S4. S5 exists to answer the one question LOSO cannot: **does the
model still work on locations it has never seen?** No S5 result has been seen
at the time of this commit. Nothing here changes the target, the features, the
hyperparameters, the metrics or the verdict margin defined in §5.3/§5.4.

**(a) What is evaluated — the held-out unit is a 5 km block, not a season.**
`block_5km` is already materialised by the dataset builder as
`(cell_row // 50) * 10000 + (cell_col // 50)`, i.e. 50 × 50 cells of the 100 m
grid ≈ 5 × 5 km. The AOI contains **110 blocks** in an 11 × 10 lattice (cell
count per block 22–2500, mean 2175.5; all 110 contain cropland). The split is:

* **Train rows:** every built row of seasons **2019–2023** whose `block_5km` is
  in the **training block set**.
* **Test rows:** every built row of season **2024** whose `block_5km` is in the
  **held-out block set**.
* A block is **never** on both sides, and a held-out block contributes **no
  rows of any season** to fitting, early stopping or refit. That is the spatial
  leakage this run exists to remove.

**(b) Frozen block partition.** The 110 block ids are sorted ascending, permuted
by `numpy.random.default_rng(20260702)` (S3/S4 seed + 1), and the **first 28**
are held out; the remaining **82** train. 28/110 = 25.45 %. The partition is a
pure function of the block id list and that seed, so it is reproducible and
auditable; it is **not** chosen from any result, and it will not be re-drawn.
The realised id list, counts and per-block row totals are written into the
artifact.

**(c) Frozen hyperparameters — unchanged from §5.3/§5.4.** `hist`,
`binary:logistic`, `max_depth = 6`, `eta = 0.05`, **`num_boost_round ≤ 600`**,
`seed = 20260701`, early-stopping patience **50** on `aucpr`.

**Rounds cap: explicitly unchanged at 600.** In S4 folds 2021 and 2022 hit the
cap (596/592 rounds for `full`, 600/595 for `ndvi_only`), so early stopping
never fired there. That is recorded as a known property of those folds. The cap
is **not** raised, lowered or tuned for S5, and it will **not** be changed after
seeing any S5 result.

**(d) Early stopping and refit.** Validation is the **most recent training
season**, 2023, restricted to the **training blocks**. Fit on 2019–2022
training-block rows, take the best iteration, refit on all of 2019–2023
training-block rows with that number of rounds, and score the 2024 held-out
blocks **once** per variant. No held-out block enters any fit.

**(e) Baselines and the verdict bar — stated explicitly, not inherited
silently.** S3's bar was derived on all blocks of the 2024 temporal test; S4's
on the mean over six seasons on all blocks. Neither is the right reference for
a *block-held-out* population, so the baseline is **re-fitted on exactly the S5
training rows and scored on exactly the S5 held-out blocks**. B0–B3 keep their
frozen definitions (`baselines.py`); B3 is refit on S5 train rows. The decision
population is `cropland/pooled` **within the held-out-block 2024 rows**. The
margin is **unchanged at 0.05 PR-AUC**, because the target, the features, the
population definition and the metric are the same as S3/S4, and changing the
margin after two failed campaigns would be post-hoc.

The pre-registered verdict sentence, all three conditions required:

> **M-full beats the baseline on unseen 5 km blocks only if, on 2024 rows of the
> 28 held-out blocks, cropland/pooled mean PR-AUC ≥ B3(block S5) + 0.05, mean
> precision at recall 0.50 > B3(block S5), and mean Brier < B3(block S5). If any
> one condition fails, M-full does not beat the baseline on unseen locations.**

"Mean" here is the mean over the **block bootstrap** resamples defined in (f),
so the point estimate and its interval come from the same estimator. The point
estimate on the full held-out set is reported alongside and must agree with the
bootstrap mean to within Monte-Carlo error; a disagreement larger than that is
a bug, not a result.

**(f) Block-level PR-AUC and block bootstrap.** Because cells inside one 5 km
block are strongly correlated, resampling rows would understate uncertainty.
The uncertainty unit is the block:

* **Per-block PR-AUC:** for every held-out block with at least one positive and
  one negative `cropland/pooled` test row, PR-AUC is computed **inside that
  block alone** and reported with its row/positive counts. Blocks failing that
  condition are listed as not scorable; they are not dropped silently.
* **Block bootstrap:** **1000** resamples of the **28 held-out blocks with
  replacement**, `numpy.random.default_rng(20260703)`, rows of the drawn blocks
  concatenated, and PR-AUC / P@R.5 / Brier recomputed for M-full, M-ndvi_only
  and B3 on each resample. Report the percentile **95 % interval (2.5, 97.5)**
  for every metric and for the paired difference `M − B3` (the same drawn blocks
  score both models, so the difference is paired). Draws that contain no
  positive or no negative rows are skipped and the number of usable draws is
  reported.

**(g) Exact column lists — identical to §5.3/§5.4.** `full` = 26 columns:
`ndvi_mean, ndvi_std, masked_pct_mean, n_obs, ndvi_lag1, ndvi_lag2, ndvi_lag3,
ndvi_roll2, ndvi_roll3, ndvi_slope_2w, anomaly_t, baseline_mean,
n_baseline_years, temp_max_mean_c, temp_max_max_c, temp_min_mean_c,
solar_sum_mj_m2, precip_sum_mm, heatwave_days, temp_max_mean_c_lag1,
temp_max_mean_c_lag2, heatwave_days_lag1, heatwave_days_2w, week_index,
cropland_pct, is_cropland`. The 10 weather columns are
`temp_max_mean_c, temp_max_max_c, temp_min_mean_c, solar_sum_mj_m2,
precip_sum_mm, heatwave_days, temp_max_mean_c_lag1, temp_max_mean_c_lag2,
heatwave_days_lag1, heatwave_days_2w` and `ndvi_only` = the other **16**. The
three B3 features are `anomaly_t, anomaly_lag1, week_index`. Both variants run
on the same S5 split; a weather effect is claimed only if `full` beats
`ndvi_only` on the S5 headline, with the §10 single-grid-point caveat.

**(h) Onset headline, separate.** `cropland/onset` and `all/onset`
(`anomaly_t > −0.08`) are reported as a **separate headline** within the
held-out blocks, point estimate plus bootstrap interval, M-full vs B3. They do
**not** enter the verdict.

**(i) Sanity controls, on the S5 split.** (i) XGBoost on the three B3 features
must land within **0.02** of the **S5 B3 refit** (floor `B3_S5 − 0.02`); comparing
it to the S3 all-block B3 would be the wrong reference and is not used.
(ii) Training labels permuted within each training season and test labels
permuted independently: PR-AUC must be within **0.01** of the permuted test
prevalence. Any failure is a pipeline bug: stop and report.

**(j) A limitation this run cannot remove.** The per-cell climatology baseline
for a 2024 held-out-block row is still built from **that same cell's** 2019–2023
observations, because `build_sql` defines the baseline per cell and season.
S5 therefore tests generalisation to **new locations whose history is known**,
not to a location with no history at all. This is stated in the report and in
the model card; it is the intended reading of §6, and it is the honest ceiling
of what this dataset can demonstrate.

**(k) Reported missingness.** The 2023 season is a **training** season in S5, so
the S4 figure "`ndvi_roll3` missing for 73.41 % of 2023's *scored rows*" is no
longer a test-side number. The report must state both (a) train-side
missingness per training season including 2023, and (b) test-side missingness on
the 2024 held-out-block rows, and say explicitly whether the 73 % shift
persists anywhere on the S5 test side.

## 6. Splits and evaluation

* **Temporal holdout:** train 2019–2023, test 2024.
* **Leave-one-season-out:** six folds over 2019–2024, train on the other five.
  The held-out unit is a **season**, never a region: all cells appear in both
  sides of every LOSO fold, so LOSO is a temporal check, not a spatial one.
* XGBoost early stopping uses the **most recent remaining training season** as
  validation; the chosen number of rounds is then used to **refit on all
  training seasons** (including that validation season). The test season is
  touched **once**.
* 5 km blocks (50 × 50 cells) for block-level PR-AUC and block-bootstrap
  intervals; a **spatial-holdout sensitivity run** trains on 75 % of blocks of
  2019–2023 and tests 2024 on the held-out 25 %.
* **Strata (added 2026-11-12, before any baseline):** every headline and every
  secondary metric is also reported on **"all lag features present"** and on
  **"at least one lag missing"**. A lag feature is any `*lag*` column
  (`ndvi_lag1..3`, `anomaly_lag1`, `temp_max_mean_c_lag1/2`,
  `heatwave_days_lag1`). Models B0, B2 and B3 are fitted **within each stratum on
  training rows only**; the test rows of the same stratum are scored. B1 is
  parameter-free. This prevents a train/test missingness difference from hiding
  inside one pooled number.
* **Reporting rule:** the 2024 result is always shown next to the **week_index
  distribution** of the test rows.
* **Ablation:** NDVI-only features vs NDVI+weather features on identical folds.
  **No weather effect is claimed unless the ablation shows a gain on the
  held-out fold.**
* **Controls.** The shuffled-label control (refit on permuted training labels,
  evaluate against independently permuted test labels) only checks the metric
  and scoring code — it is **not** evidence of no leakage. The leakage evidence
  is the truncation test (`test_features_do_not_use_the_future`, which now also
  compares `anomaly_lag1`), `test_recomputed_baseline_excludes_own_season` and
  `test_test_season_never_enters_a_training_baseline`.

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
* **2024 has 145,406 rows at `week_index = 2` (4.8 % of the season); every
  training season has exactly zero rows at `week_index = 2`.** ISO week 19 has
  zero `status='ok'` cells in every season, and ISO week 20 fails the
  ≥3-baseline-year gate for a leave-one-out training row (its 4-year pool has at
  most 2 usable years) while the 2024 test row succeeds with its 5-year pool
  (2019, 2021, 2023). Because `week_index = 2` has no `week_index = 0` weather
  week, **all** 145,406 of those rows have `temp_max_mean_c_lag2` missing — this
  is the entire 4.8 % test missingness. The strata above make this visible
  instead of pooling it away. **The model has never seen `week_index = 2` in
  training**, so any prediction for those rows is an extrapolation.
* Training rows use a 4-year baseline pool and test rows a 5-year pool in the
  temporal split.

## 11. Week-index diagnostic (2026-11-12, read-only)

Rows retained per `week_index` (share of season) on the S1 dataset:

| week_index | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 (test) |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 0 | 0 | 0 | 0 | 0 | 0 |
| 2 | 0 | 0 | 0 | 0 | 0 | 4.80 % (145,406) |
| 3 | 0 | 6.53 % | 6.65 % | 6.88 % | 0 | 7.34 % |
| 4 | 7.09 % | 4.57 % | 7.00 % | 6.18 % | 0 | 7.32 % |

First four weeks, 2022 vs 2024 (Gold): ISO week 19 has **zero** `status='ok'`
cells in both; ISO week 20 is `ok` for 224,775 cells in 2024 but fails the
3-baseline-year gate for a leave-one-out training row. As a result, the entire
4.8 % `temp_max_mean_c_lag2` missingness in the test season is the 145,406
`week_index = 2` rows, and training has none. Cause demonstrated from the data;
no weather effect is implied.

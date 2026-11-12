# Model card — Phase 7 vegetation-stress model

**Status:** Phase 7 complete. Three pre-registered campaigns were run (S3 temporal, S4
leave-one-season-out, S5 spatial holdout). **All three fail the PR-AUC condition; the model
is not claimed to beat the strongest baseline on the pre-registered primary metric.**
Everything below is quoted from the saved artifacts under `artifacts/phase7/`, not from
memory. The pre-registration that fixes the target, features, splits, bars and decision rule
is [`docs/phase7-preregistration.md`](phase7-preregistration.md); it was frozen before any
model was fitted.

---

## 1. What the model is

A gradient-boosted tree classifier (XGBoost, `binary:logistic`, `tree_method=hist`,
`max_depth=6`, `eta=0.05`, `seed=20260701`, early-stopping patience 50 on `aucpr`,
`num_boost_round ≤ 600`) that answers one question per **cell-week** on the 100 m grid:

> Given everything observed up to the end of ISO week *t*, will this cell's **mean raw NDVI
> anomaly over the next two ISO weeks** be at or below **−0.08**?

It is a **risk scorer**, not a stress diagnosis. It ranks cell-weeks by the probability of a
coming two-week NDVI drop; it does not attribute cause to heat, water or management.

Two variants are trained on identical folds and hyperparameters:

| Variant | Columns | Weather features |
| --- | --- | --- |
| `full` | **26** | all 10 weather columns included |
| `ndvi_only` | **16** | weather excluded (the 26 minus 10) |

The 16 `ndvi_only` columns: `ndvi_mean`, `ndvi_std`, `masked_pct_mean`, `n_obs`, `ndvi_lag1`,
`ndvi_lag2`, `ndvi_lag3`, `ndvi_roll2`, `ndvi_roll3`, `ndvi_slope_2w`, `anomaly_t`,
`baseline_mean`, `n_baseline_years`, `week_index`, `cropland_pct`, `is_cropland`.

The 10 weather columns: `temp_max_mean_c`, `temp_max_max_c`, `temp_min_mean_c`,
`solar_sum_mj_m2`, `precip_sum_mm`, `heatwave_days`, `temp_max_mean_c_lag1`,
`temp_max_mean_c_lag2`, `heatwave_days_lag1`, `heatwave_days_2w`.

`ndvi_only` is **secondary**. It exists to test whether weather adds anything. It is **never**
the basis of a verdict (preregistration §5.3(e), §5.4(e)).

---

## 2. Target

* **Unit:** one cell-week row, 100 m grid, ISO weeks inside the 1 May – 30 Sep season.
* **Information time:** end of ISO week *t* (Sunday 23:59). No feature may look past *t*.
* **Label:** the **mean raw NDVI anomaly over weeks `t+1` and `t+2` is ≤ −0.08**.
* **Both** `t+1` and `t+2` must be observed with `status = 'ok'`. The horizon is never
  "the next two available observations".
* The anomaly is the **raw NDVI difference** from a climatology, **never the z-score**
  (D-044, D-046). −0.08 is a physical NDVI amount fixed before training; it corresponds
  roughly to the 16th–21st percentile of the forward anomaly.
* A row also needs a defined anomaly at week *t* (≥ 3 baseline years), and the baseline is
  built **from training seasons only**: a training row uses a leave-one-training-year-out
  baseline, a test row uses all training seasons. A season never contributes to its own
  baseline (D-015, preregistration §3).

The observed positive rate is **19.59 %** overall: 2019 16.18 %, 2020 18.41 %, 2021 22.73 %,
2022 21.07 %, 2023 15.21 %, 2024 21.51 %.

---

## 3. Data

Built by `python -m delta_crops.models.dataset` into
`artifacts/phase7/dataset/temporal.parquet` (655,793,827 bytes, 29.06 s).

| Season | Split | Usable rows | Positives | Positive % |
| --- | --- | ---: | ---: | ---: |
| 2019 | train | 2,519,921 | 407,626 | 16.18 |
| 2020 | train | 2,728,392 | 502,313 | 18.41 |
| 2021 | train | 2,681,558 | 609,530 | 22.73 |
| 2022 | train | 2,589,333 | 545,571 | 21.07 |
| 2023 | train | 1,431,931 | 217,794 | 15.21 |
| 2024 | test | 3,026,561 | 651,140 | 21.51 |
| **Total** | | **14,977,696** | **2,933,974** | **19.59** |

2023 is the thinnest season (about 1.43 M usable rows). In the **S3 temporal split** it is the
validation season, which is deliberate: the thinnest season is not spent on testing. Under S4
the validation season is instead the most recent *remaining* training season of each fold.

**Populations.** The model is trained on **all cells**. **Headline metrics use
`is_cropland = true` only**; all-cell numbers are always reported alongside. Strata
(`pooled`, `complete` = all lags present, `incomplete` = ≥ 1 lag missing) are reported
descriptively; one model is trained once and evaluated many times, never retrained per
stratum (preregistration §5.3(c)).

---

## 4. Baselines B0–B3

Fitted in `src/delta_crops/models/baselines.py`, on training rows only, inside each
population × lag-missingness stratum (commit `8b0164b`).

| ID | Baseline | What it is |
| --- | --- | --- |
| B0 | Majority | constant = training prevalence; PR-AUC equals test prevalence by construction |
| B1 | Persistence | score = `−anomaly_t` (this week's drop is assumed to continue). It is a **ranking score, not a probability**, so no Brier score is reported |
| B2 | Seasonal prior | per-`week_index` positive rate from training seasons (falls back to the training prevalence for an unseen week) |
| B3 | Logistic regression | `anomaly_t`, `anomaly_lag1`, `week_index`, standardised, **with intercept**; a missing `anomaly_lag1` is imputed with the training median of the same group |

**B3 is the reference.** It is the strongest baseline in every campaign, so every verdict is
M versus B3.

**B3 intercept fix (commit `770d885`).** B3 originally standardised its features but fitted an
**intercept-free** logistic model, so `sigmoid(0) = 0.5` at the feature mean instead of the
training prevalence. Mean predicted probability on 2024 cropland/pooled was **0.519** against
a **0.251** prevalence and every reliability bin over-predicted. Adding the intercept leaves
ranking (ROC-AUC, PR-AUC) unchanged and cuts Brier from **0.1929** to **0.1050**. This fix
landed before any verdict was computed, and every B3 number in this card is post-fix.

S3 (`cropland/pooled`, 2024, 2,449,220 rows, 614,324 positives, prevalence 0.250824):

| Model | PR-AUC | P@R.5 | Brier |
| --- | ---: | ---: | ---: |
| B0 majority | 0.2508243 | 0.2508243 | 0.1884552 |
| B1 persistence | 0.7589650 | 0.7948278 | — (score, not a probability) |
| B2 seasonal prior | 0.2819829 | 0.2887201 | 0.1873158 |
| **B3 logistic** | **0.7722002** | **0.8145373** | **0.1050482** |

---

## 5. Splits

| ID | Split | Held-out unit | Fits | What it can and cannot show |
| --- | --- | --- | --- | --- |
| **S3** | temporal | season **2024** | 2019–2022 fit, 2023 validation, refit 2019–2023 | generalisation to a **new season for known cells**; 2024 is also the most extreme season, so this doubles as a distribution-shift test |
| **S4** | leave-one-season-out, 6 folds | one **season** (2019 … 2024) | other five seasons; validation = most recent remaining season | temporal generalisation averaged over six seasons. **All cells are shared** between train and test, so spatial leakage is **not** removed |
| **S5** | spatial holdout | **5 km block** (50 × 50 cells); 28 of 110 blocks held out, **2024 rows only** | 2019–2023 rows of the 82 training blocks | generalisation to **new locations whose 2019–2023 history is known**. It is not a cold-start test |

The S5 block partition is frozen: 110 block ids sorted, permuted by
`numpy.random.default_rng(20260702)`, first 28 held out (25.45 %). It was not chosen from any
result and will not be re-drawn.

---

## 6. Pre-registered bars and the three verdicts

Bars were fixed in writing before each campaign and were **not** changed after seeing a
result. All three conditions must hold for a pass.

| Campaign | PR-AUC bar | P@R.5 bar | Brier bar |
| --- | --- | --- | --- |
| S3 (test 2024) | ≥ **0.8222002** (B3 + 0.05) | > **0.8145373** | < **0.1050482** |
| S4 (mean of 6 folds) | ≥ **0.8205095** (mean B3 + 0.05) | > **0.8187709** | < **0.0947832** |
| S5 (held-out blocks) | ≥ **0.8147217** (B3 refit + 0.05) | > **0.8070753** | < **0.1036731** |

### S3 — temporal, test season 2024. **FAIL**

`cropland/pooled`, 2,449,220 rows, 614,324 positives.

| Variant | PR-AUC | P@R.5 | Brier | PR-AUC condition | P@R.5 condition | Brier condition | Verdict |
| --- | ---: | ---: | ---: | :---: | :---: | :---: | --- |
| `full` | 0.8213705 | 0.8713148 | 0.0936884 | **FAIL** | pass | pass | **FAIL** |
| `ndvi_only` | 0.8235561 | 0.8740270 | 0.0924261 | pass | pass | pass | pass |

`full` misses the PR-AUC bar by **0.0008298**. Early stopping chose round **533** (583 run);
`ndvi_only` chose round **275** (325 run).

### S4 — leave-one-season-out, mean over six folds. **FAIL**

| Variant | PR-AUC mean | P@R.5 mean | Brier mean | PR-AUC condition | P@R.5 condition | Brier condition | Verdict |
| --- | ---: | ---: | ---: | :---: | :---: | :---: | --- |
| `full` | 0.8191470 | 0.8754861 | 0.0835801 | **FAIL** | pass | pass | **FAIL** |
| `ndvi_only` | 0.8235976 | 0.8812944 | 0.0824871 | pass | pass | pass | pass |

`full` misses the mean PR-AUC bar by **0.0013625**. Per-fold B3 PR-AUC: 2019 0.7498791,
2020 0.7910677, 2021 0.8245582, 2022 0.7660580, 2023 0.7192934, 2024 0.7722002
(mean 0.7705095, std 0.0358198). Per-fold values are descriptive and cannot change the
verdict.

`loso-2024` reproduces the S3 temporal numbers exactly (same fit/validation/refit seasons),
which is the pre-registered pipeline-consistency check.

### S5 — spatial holdout on 5 km blocks. **FAIL**

`cropland/pooled` within the 2024 rows of the held-out blocks: 596,174 rows, 144,091
positives. B3 was **refit on exactly the S5 training rows** and scored on exactly the S5 test
rows: PR-AUC **0.7647217**, P@R.5 **0.8070753**, Brier **0.1036731**.

| Variant | PR-AUC | P@R.5 | Brier | PR-AUC condition | P@R.5 condition | Brier condition | Verdict |
| --- | ---: | ---: | ---: | :---: | :---: | :---: | --- |
| `full` | 0.8007195 | 0.8436102 | 0.0960768 | **FAIL** | pass | pass | **FAIL** |
| `ndvi_only` | 0.8042603 | 0.8470021 | 0.0942075 | **FAIL** | pass | pass | **FAIL** |

`full` misses the PR-AUC bar by **0.0140022**; `ndvi_only` by **0.0104614**. Both variants
pass the P@R.5 and Brier conditions. The fall from S3 to S5 on unseen locations is only
**0.0207** PR-AUC (0.8214 → 0.8007), i.e. the model does transfer spatially; it fails here
because the in-domain margin over B3 (+0.0492 on S3) and the S5 margin (+0.0360) are both
below the +0.05 the project required, not because performance collapses on new blocks.

### The single sentence that summarises Phase 7

> **On all three pre-registered campaigns the model's PR-AUC falls short of
> (B3 + 0.05). It beats B3 on the ranking metric by a real but too-small margin, and it
> passes the operating-point and calibration conditions. The honest conclusion is that this
> model is better than the B3 logistic baseline but not by the margin the project committed
> to before running, and it is not a validated stress-warning system.**

---

## 7. Paired uncertainty (S5 block bootstrap)

Cells inside a 5 km block are strongly correlated, so rows are not independent. S5 therefore
resamples **blocks**, not rows: **1000 / 1000 usable draws** of the 27 scorable held-out
blocks with replacement (`numpy.random.default_rng(20260703)`). Percentile 95 % intervals.

| Quantity | Point | 95 % interval |
| --- | ---: | --- |
| `full` PR-AUC | 0.8007195 | [0.7818991, 0.8211190] |
| `ndvi_only` PR-AUC | 0.8042603 | [0.7863661, 0.8239851] |
| PR-AUC bar | 0.8147217 | — |
| Paired `full` − B3 PR-AUC | **+0.0360180** | [+0.0276012, +0.0438191] |
| Paired `full` − B3 P@R.5 | +0.0369358 | [+0.0213440, +0.0520448] |
| Paired `full` − B3 Brier | −0.0076201 | [−0.0094158, −0.0059748] |

Reading, stated plainly: the bar **0.8147217 lies inside both 95 % PR-AUC intervals** and sits
**above both point estimates**, by 0.0140022 (`full`) and 0.0104614 (`ndvi_only`). The
intervals straddle the bar; the point estimates do not clear it. **The verdict is on the point
estimates and stands as FAIL.** The interval is context, not a reinterpretation.

The paired difference is consistently positive and its interval excludes zero, so the model
does beat B3 in ranking skill — just not by 0.05.

---

## 8. Onset results (separate headline, not part of any verdict)

The onset subset is `anomaly_t > −0.08`: cells that are **already** close to the threshold.
This is the operationally interesting population (do not warn about cells already in a drop),
so it is reported separately.

| Campaign | Population | Rows | Positives | B3 PR-AUC | `full` PR-AUC | `ndvi_only` PR-AUC |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| S3 | cropland/onset | 1,795,668 | 177,133 | 0.3205508 | 0.4373937 | 0.4508199 |
| S4 | cropland/onset (mean) | — | — | 0.3084870 | 0.4228700 | 0.4408980 |
| S5 | cropland/onset | 442,124 | 42,473 | 0.3203791 | 0.3929923 | 0.4082003 |

S5 onset intervals (block bootstrap): `full` [0.3638758, 0.4266090], `ndvi_only`
[0.3778844, 0.4438338], B3 [0.3037888, 0.3396091]; prevalence in that population is
**0.0961**, so `full`'s lift over prevalence is about **4.09×**.

On the all-cells onset population (`all/onset`, 559,419 rows, prevalence 0.0809) S5 gives
`full` 0.3839020 versus B3 0.2932640.

**The onset signal is the strongest part of Phase 7.** Relative to B3 the gain on the onset
population is much larger (S5: +0.0726 PR-AUC) than on the pooled population (+0.0360). This
is descriptive and is not a verdict.

---

## 9. `full` versus `ndvi_only` — no weather effect is claimed

The frozen rule is: a weather effect may be claimed **only if** `full` beats `ndvi_only` on
the headline PR-AUC. It does not, on any campaign:

| Campaign | `full` PR-AUC | `ndvi_only` PR-AUC | Winner |
| --- | ---: | ---: | --- |
| S3 | 0.8213705 | 0.8235561 | `ndvi_only` |
| S4 | 0.8191470 | 0.8235976 | `ndvi_only` |
| S5 | 0.8007195 | 0.8042603 | `ndvi_only` |

`ndvi_only` is better on all three, and it also has the lower Brier in all three. Weather
features therefore add **nothing measurable** here, and the correct statement is:
**no weather effect is claimed.** This is consistent with the structural limitation that
weather is a **single grid point** shared by ~239 k cells, i.e. about **120 independent
weather weeks** across six seasons, and with rainfall being effectively absent (season totals
0.0–2.1 mm). Feature importance (gain) is informational only and is not interpreted causally;
`anomaly_t` dominates it, as expected.

---

## 10. Calibration

10-bin reliability is reported for S3 and S4 (`cropland/pooled`). A bin is well calibrated
when `observed_rate` tracks `mean_probability`.

**S3, `full`, 2024** (well calibrated up to ~0.6, then mildly over-confident):

| Bin | Rows | Mean probability | Observed rate |
| --- | ---: | ---: | ---: |
| 0.0–0.1 | 1,204,181 | 0.0233 | 0.0221 |
| 0.1–0.2 | 247,761 | 0.1453 | 0.1327 |
| 0.2–0.3 | 168,552 | 0.2477 | 0.2117 |
| 0.3–0.4 | 139,079 | 0.3488 | 0.2995 |
| 0.4–0.5 | 115,161 | 0.4488 | 0.3946 |
| 0.5–0.6 | 102,919 | 0.5493 | 0.4988 |
| 0.6–0.7 | 96,936 | 0.6498 | 0.6054 |
| 0.7–0.8 | 97,846 | 0.7501 | 0.7147 |
| 0.8–0.9 | 104,292 | 0.8504 | 0.8298 |
| 0.9–1.0 | 172,493 | 0.9609 | 0.9595 |

**S4, pooled over the six folds**: bins 0.0–0.1 through 0.8–0.9 over-predict by roughly
0.002–0.030 (e.g. 0.5–0.6 predicted 0.5493, observed 0.5206; 0.8–0.9 predicted 0.8517,
observed 0.8394), and the 0.9–1.0 bin is essentially exact (0.9629 vs 0.9643). The pooled
pool contains 7,173,115 rows in the lowest bin.

**S5: documented gap, not a task.** No reliability table exists for S5. The strings
`prediction` and `reliab` occur **0 times** in `artifacts/phase7/xgb_spatial.json`; no
prediction or reliability file exists under `artifacts/` or `data/`; `run_spatial_fold` keeps
predictions in memory only and persists aggregate metrics and bootstrap intervals. Producing
the table would require a prediction-dump step and a re-run (or a separately scoped post-hoc
scoring pass), and neither was performed because Phase 7 was explicitly closed without new
fits. **S5 calibration is therefore unmeasured**, and no S3/S4 table may be substituted for
it.

---

## 11. Sanity controls

Both frozen controls passed in every campaign; a failure would have been treated as a
pipeline bug, not a result. The B3-feature control is implemented exactly as the
pre-registration §5.4(f)(i) defines it — a **one-sided floor** `B3 − 0.02` (`train.py:362`,
`train.py:537`) — so it catches a pipeline that has *lost* B3's signal. It does not forbid the
control from scoring above B3, and in two folds it does (loso-2021 +0.0201, loso-2023 +0.0320
over that fold's B3).

| Campaign | Control | Value | Reference | Result |
| --- | --- | ---: | ---: | --- |
| S3 | XGBoost on the 3 B3 features | PR-AUC 0.7962004 | B3 0.7722002, floor 0.7522002 | pass |
| S3 | Permuted labels | PR-AUC 0.2147977 | prevalence 0.2151779, gap 0.0003802 | pass |
| S4 | XGBoost on the 3 B3 features, per fold | 0.7757136 / 0.8089119 / 0.8446405 / 0.7868671 / 0.7512463 / 0.7962004 | B3 0.7498791 / 0.7910677 / 0.8245582 / 0.7660580 / 0.7192934 / 0.7722002, floor = B3 − 0.02 | pass in all six folds |
| S4 | Permuted labels, per fold | gaps 0.0002753 / 0.0003363 / 0.0000576 / 0.0001483 / 0.0001829 / 0.0003802 | permuted prevalence | pass (all < 0.01) |
| S5 | XGBoost on the 3 B3 features | PR-AUC 0.7828792 | B3(S5) 0.7647217, floor 0.7447217 | pass |
| S5 | Permuted labels | PR-AUC 0.2092269 | prevalence 0.2100461, gap 0.0008192 | pass |

The permuted-label control is the leakage canary: if labels leaked into features, a model
trained on shuffled labels would still score above prevalence. It does not, in any campaign.

---

## 12. Limitations

1. **PR-AUC is below the pre-registered bar in all three campaigns.** This is the headline
   result and it is not softened. The model beats B3 by +0.036 (S5) but was required to beat
   it by 0.05.
2. **S5 has one test year.** The spatial holdout is scored on 2024 only, and 2024 is the most
   extreme season in the record (25 heatwave days). The S5 verdict is a single-season result on
   unseen blocks, not a multi-year estimate.
3. **S5 held-out history is known.** The per-cell climatology for a held-out-block 2024 row is
   still built from that same cell's 2019–2023 observations. S5 tests new **locations with
   known history**, not cold start. This is the honest ceiling of this dataset.
4. **Uneven block weights.** 28 blocks were declared held out but only **27** produced
   scorable rows (`heldout_blocks_without_scored_rows: [80009]`); block 80009 has no
   `cropland/pooled` 2024 row. Scorable blocks range from **253 to 34,075 rows**, and the
   block bootstrap gives every block equal weight regardless of size, so a small block counts
   as much as a 34 k-row block. Four declared training blocks also carry no rows
   (`train_blocks_without_rows: [70009, 90009, 100008, 100009]`), so 78 of the declared 82
   training blocks were actually used.
5. **S5 blocks overlap with known history.** The 5 km blocks partition the AOI, but adjacent
   blocks share a border and the per-cell baseline spans the train blocks. The gain therefore
   reflects interpolation into known neighbourhoods, not extrapolation to a genuinely new
   region.
6. **The 600-round cap binds in places and does not bind in S5.** S4 folds `loso-2021` and
   `loso-2022` hit the cap (`full` 596 / 592 rounds, `ndvi_only` 600 / 595), so early stopping
   never fired there. In S5 the cap did **not** bind: `full` stopped at best round **593**
   (`hit_cap: false`, 600 run) and `ndvi_only` at **599** (`hit_cap: false`, 600 run) — both
   within two rounds of the cap, so a larger cap could plausibly have helped. The cap was
   frozen before any S5 fit and was **not** raised afterwards.
7. **2023 missingness and the `week_index = 2` extrapolation.** In 2024, **145,406 rows
   (4.80 % of the season) sit at `week_index = 2`**, while **every training season has exactly
   zero** rows there. Because `week_index = 2` has no `week_index = 0` weather week, **all**
   of those rows have `temp_max_mean_c_lag2` missing — that is the entire **4.711 %** test
   missingness of that column. Predictions for those rows are an extrapolation. 2023 also
   has the worst lag missingness (in LOSO tests it as the held-out season:
   `ndvi_roll3` missing for **73.414 %** of scored rows, `ndvi_lag1` 37.93 %).
8. **Weather is one grid point.** ~120 independent weather weeks across six seasons; up to
   six seasons total; rainfall is effectively zero. No rainfall or heat effect is claimed.
9. **Confounders.** Harvest and irrigation are unobserved. A "stress" label can reflect either,
   not only heat.
10. **Feature missingness differs by split** (`ndvi_lag3` missing 12.707 % in training vs
    20.486 % on the 2024 test side; `ndvi_lag1` 2.501 % vs 9.214 %). XGBoost handles missing
    values natively, but the shift is real and is reported rather than hidden.
11. **One training row-set per model.** The model is not retrained per stratum and does not
    model cell identity, so it cannot learn per-cell behaviour beyond what the features
    encode.
12. **Cost.** S3 took 1,513.12 s at 9,402.8 MB peak RSS; S4 took 9,020.29 s at 9,616.7 MB;
    S5 took 2,056.22 s at 8,615.9 MB. Baselines took 557.37 s (4,393.8 MB), onset baselines
    320.55 s. This fits a 16-core / 15 GB laptop but is not a lightweight job.

---

## 13. Reproducibility

Prerequisites: the Silver/Gold lake in `data/` (gitignored), the venv from
`PROJECT.md` §5, and the pre-registration document unchanged.

```bash
# 1. Build the modelling dataset and its measured summary (S1)
python -m delta_crops.models.dataset \
  --out artifacts/phase7/dataset \
  --summary-json artifacts/phase7/dataset_summary.json

# 2. Baselines B0-B3 on the temporal split and LOSO
python -m delta_crops.models.baselines --split all \
  --out artifacts/phase7/baselines_metrics.json

# 2b. The same baselines restricted to the onset population
python -m delta_crops.models.baselines --split all --onset \
  --out artifacts/phase7/baselines_onset_metrics.json

# 3. S3 temporal XGBoost (test season 2024)
python -m delta_crops.models.train --split temporal \
  --out artifacts/phase7

# 4. S4 leave-one-season-out, six folds
python -m delta_crops.models.train --split loso \
  --out artifacts/phase7

# 5. S5 spatial holdout on the frozen 5 km blocks
python -m delta_crops.models.train --split spatial \
  --out artifacts/phase7
```

`--split all` runs all three campaigns in one process; steps 3–5 are listed separately
because they were run and committed separately. Every command is deterministic given
`seed = 20260701` (S3/S4), `seed = 20260702` (S5 partition) and
`bootstrap_seed = 20260703` (S5 block bootstrap). Reruns overwrite the same JSON files;
nothing is appended, so re-running cannot duplicate results.

Tests: `pytest -q` (models coverage lives in `tests/test_models.py`,
`tests/test_baselines.py`, `tests/test_spatial.py`, `tests/test_train.py`).
Artifacts are gitignored (`.gitignore:15` for `artifacts/`, `.gitignore:12` for `data/`) and
are never committed; the model card and the pre-registration are the versioned record.

---

## 14. In plain language

**"What did you build?"** A per-cell, per-week risk score for the next two weeks of vegetation
stress in the Nile Delta. It reads Sentinel-2 NDVI history and Open-Meteo weather, builds 26
features that only use information available by Sunday night, and outputs a probability that
the cell's NDVI anomaly will drop by at least 0.08 over the following fortnight.

**"How did you evaluate it honestly?"** Three ways, all fixed in writing before the first fit.
First a temporal split: train 2019–2023, test 2024. Then leave-one-season-out over all six
seasons, so no single year decides the answer. Then a spatial holdout: 28 of 110 five-kilometre
blocks never appear in training, so the model is scored on places it has never seen. The bar
is not "beat a coin flip" — it is "beat a logistic regression on last week's anomaly by at
least 0.05 PR-AUC", and I reported the loss when it did not.

**"Did it work?"** Partly, and I say which part. The model beats the logistic baseline on
ranking by +0.036 PR-AUC with a 95 % block-bootstrap interval of [+0.028, +0.044] — a real
gain. It fails the +0.05 bar in all three campaigns. It passes the operating-point condition
and the calibration condition. So: better than baseline, not by the margin I committed to, and
not validated enough to deploy as a warning system. That is the result, not a footnote.

**"What would you fix next?"** The single biggest lesson is that the weather features add
nothing measurable, because one grid point cannot explain variation across 239,000 cells — the
next version would need gridded or satellite-derived weather/soil moisture, or cell-level
irrigation data. Second, the 600-round cap was binding in two LOSO folds and near-binding in
S5, so a wider cap with proper early stopping is worth one more run. Third, 2023's lag
missingness is a real data-quality problem in the stress-label window.

---

## 15. Artifact index

| Artifact | Contents |
| --- | --- |
| `artifacts/phase7/dataset/temporal.parquet` | the 14,977,696-row modelling table |
| `artifacts/phase7/dataset_summary.json` | S1 measured dataset, per-season positives |
| `artifacts/phase7/baselines_metrics.json` | B0–B3, temporal + six LOSO folds |
| `artifacts/phase7/baselines_onset_metrics.json` | B0–B3 on the onset population |
| `artifacts/phase7/xgb_temporal.json` | S3 metrics, sanity controls, S3 reliability |
| `artifacts/phase7/xgb_loso.json` | S4 per-fold metrics, fold means, verdict, reliability |
| `artifacts/phase7/xgb_spatial.json` | S5 verdict, block bootstrap, onset, per-block metrics, missingness |

Commits: `5651bfc` (dataset/labels), `8b0164b` (baselines), `770d885` (B3 intercept fix),
`98938ce` (onset baselines), `fb54129` (S3 freeze), `ace7352` (S3 run), `675eb4c` (S4 freeze),
`68215bd` (S4 run), `6fd4226` (S5 freeze), `2a30b52` (S5 run).

# Limitations

This page is the single, complete list of what this project does **not** establish. Each entry says
what was measured, why it was not fixed, what it means for the claims, and where the evidence lives.
If a claim is not supported here, it is not supported by the project.

Status of the five items carried from earlier phases, as of 2026-11-13 (Phase 9):

| # | Limitation | Status after Phase 9 |
| --- | --- | --- |
| 1 | 36.70 % DN = 0 on 2021-07-10 | Open, cause not established; no code or output changed |
| 2 | Scheduled flow never ran end-to-end on real data | Open; capability is verified, product behaviour is not |
| 3 | Container concurrency unproven | Open; `global_limit=1` is set, non-overlap was not observed |
| 4 | `gold_regression.py` not re-run after 2024 | **Re-run in Phase 9: 15/15 table-years OK, 0 differences, 48.14 s** — see §4 |
| 5 | S5 reliability gap | Open, documented gap; no S5 prediction file exists |

---

## 1. 36.70 % of invalid in-AOI pixels are DN = 0 on 2021-07-10

**Measured.** On 2021-07-10 (36RTV + 36RUV), 36.70 % of the invalid in-AOI pixels inside NULL cells
are DN = 0. SCL cloud/shadow accounts for 62.12 % of invalid pixels and other masked SCL classes for
1.19 %. Every other one of the 15 alerted dates has ≤ 0.03 % non-cloud/shadow attribution. The audit
and its replay (DN > 0 vs DN > 1) are in [`docs/phase4-quality.md`](phase4-quality.md) and
[`PROJECT.md`](../PROJECT.md) §3.15.

**Why it is not fixed.** The physical cause — a product-side fill, a shadowed surface, or a
processing artefact — is not established. The audit showed the effect is confined to one date and
does not justify a global "DN = 0 is valid reflectance" rule, which would change every season. No
validity rule, Silver file, config value or Gold table was changed for it.

**What it means.** One date's NDVI population is biased toward the cells that did have valid
reflectance. It is not a reason to distrust the six-season trend, and it is not a solved issue
either. Any per-date comparison that includes 2021-07-10 must carry this caveat.

**Where it is surfaced.** This page; `PROJECT.md` §3.15/§7; `docs/phase4-quality.md`; README
limitations row.

## 2. The scheduled flow never ran end-to-end on real data

**Measured.** Phase 6a/6b verified the four-task flow, the retry policy, failure logging, a real
Prefect server with a served deployment, the container suite (`133 passed, 6 skipped,
12 deselected`) and the integration test inside the image (`1 passed`). The real Silver and Gold
were built by hand (2019–2023 on 2026-11-10/11, 2024 on 2026-11-12), not by the schedule.

**Why it is not fixed.** Running the scheduled flow on the real lake means re-ingesting and
rebuilding ~31 GB of data on a timer, which is out of scope for a local portfolio build and would
not add a new claim.

**What it means.** "Runs every five days with retries" describes verified wiring. It is **not** an
observed production behaviour, and no uptime, freshness or alert-delivery claim is made.

**Where it is surfaced.** README "What is proven and what is not"; `docs/phase6-orchestration.md`;
`docs/phase6b-docker.md`; `PROGRESS.md`.

## 3. Container concurrency is unproven

**Measured.** The deployment sets `global_limit=1`, but the only available flow for the overlap test
(`no_op`) finishes in ~0.3 s, so two runs could not be made to overlap. The container suite and
integration test do not exercise concurrency.

**Why it is not fixed.** A meaningful test needs a flow that blocks long enough to overlap, which
would mean a new test-only flow and a longer harness. Not added: it is test scaffolding, not product
behaviour.

**What it means.** The configuration expresses intent; single-flight execution under load was not
observed. Do not cite it as a guarantee.

**Where it is surfaced.** README Docker section; `docs/phase6b-docker.md`; `PROGRESS.md`.

## 4. `gold_regression.py` after season 2024 — re-run, result recorded

**What was open.** The snapshot `data/_regression/gold_before` was captured on 2026-11-11, before
2024 was added on 2026-11-12. The script was not re-run after 2024, and PROJECT noted that
`ndvi_anomaly` 2019–2023 was expected to differ because the leave-one-out pool would now hold six
years.

**What Phase 9 measured.** Re-ran `python scripts/gold_regression.py` against the same snapshot:
**15/15 table-years OK, 0 differences, 48.14 s** (Gold rebuilt in 41.28 s). This includes all five
`ndvi_anomaly` years.

**Why that is not a contradiction.** `build_anomaly` forms the baseline from the years it is asked
to rebuild (`years=2019..2023` here), not from every year present in the lake. Rebasing the 2019–2023
anomalies therefore reproduces the five-year snapshot exactly. What the run does **not** cover is
the 2024 anomaly itself, whose baseline does include the earlier seasons; no snapshot exists for it.

**What it means.** The 2019–2023 Gold tables are reproducible from the current Silver. The earlier
"expected to drift" caveat is retired **for this rebuild path** with evidence, and the 2024 anomaly
remains covered only by the Phase 5 gate and the `adding-season-2024` record.

**Where it is surfaced.** This page; `PROGRESS.md` Phase 9 section; the command is in README testing.

## 5. S5 calibration is unmeasured

**Measured.** `xgb_spatial.json` contains no reliability block: `prediction` and `reliab` occur 0
times in it, no S5 prediction file was ever written, and `run_spatial_fold` keeps probabilities in
memory. S5 itself: 27 of 28 declared held-out blocks scored, block `80009` has no
`cropland/pooled` 2024 row; PR-AUC 0.8007195 against a bar of 0.8147217; paired full − B3
+0.0360180 [+0.0276012, +0.0438191].

**Why it is not fixed.** Producing it means a new deterministic S5 scoring run and a new artifact.
That is a Phase 7-style model run, not documentation work, and it was explicitly left as a
documented gap rather than run under time pressure.

**What it means.** The spatial model's probabilities are uncalibrated and may be systematically
over- or under-confident. Only the S3 probabilities (10-bin table in the dashboard and model card)
have a measured calibration. No S3 table is substituted for S5.

**Where it is surfaced.** Dashboard "Pipeline Health" (as a warning, not an error); model card §12;
README; this page.

---

## Model and data limitations

### 6. The weather features add no skill

`ndvi_only` beats `full` in S3 (0.8235561 vs 0.8213705), S4 (0.8235976 vs 0.8191470) and S5
(0.8042603 vs 0.8007195), and `full` misses the PR-AUC bar in every campaign. The honest conclusion
is that the weather features add no measurable skill on top of NDVI history here — not that weather
does not matter to crops. The project's second question is answered **descriptively** only.

### 7. No visible heatwave response is a description, not a proof of absence

2023 and 2024 NDVI rise through July and stay positive during the recorded heatwave episodes. Over
six seasons this is **no visible heatwave response in the data we have**. It is confounded with
seasonality (NDVI peaks in July–August), orbit mix and a 3–4-year baseline, and it cannot prove that
heat has no effect. Weeks with low coverage must not be compared with neighbours without the
coverage column.

### 8. Small baseline sample and a single test season

Each cell-week baseline uses at most 4–5 seasons (`min_baseline_years = 3`), so anomalies are noisy.
S3 tests exactly one season (2024), the most extreme in the record; S4 averages six folds but still
only six seasons; S5 uses one test year. None of these is a multi-year forecast evaluation.

### 9. 2024 `week_index = 2` is an extrapolation

2024 has 145,406 rows (4.80 %) at `week_index = 2`, a value no training season has, and it accounts
for the whole 4.711 % `temp_max_mean_c_lag2` test missingness. Those predictions are extrapolation
and are not distinguished in the dashboard's probability map.

### 10. The 600-round cap binds in two S4 folds and nearly binds in S5

S4 `loso-2021`/`loso-2022` hit the cap (full 596/592, `ndvi_only` 600/595); S5's best rounds are 593
and 599 of 600 (`hit_cap: false`). Raising the cap after seeing this would be post-hoc selection, so
it was not raised (D-051). A different cap could plausibly change S4/S5 results.

### 11. One AOI, one region, one crop calendar

The AOI is ~50 × 50 km of the central Nile Delta with a summer season (May–September). Results do not
transfer to other regions, winter crops, or field-level decisions. The cropland mask comes from a
2021 land-cover product, so fields that changed use after 2021 are misclassified.

### 12. Silver treats DN = 0 as invalid

DN > 0 is the validity floor for reflectance, not a global calibration. The 14-date audit measured
the cost of DN > 1 instead (at most 0.1914 percentage points more NULL across all cells, 0.0303 on
cropland; up to 965 paired cropland cells/date moving NDVI by > 0.01). The floor was retained
deliberately (D-042); local NDVI can be biased where DN = 0 is common.

### 13. Display blocks are approximations

The map aggregates 100 m cells into 1 km blocks and draws each block as the envelope of its cell
centroids ± half a cell. Edges are approximate, and a block value is the mean over `status='ok'`
cells. An empty block means no usable observation that week, never zero vegetation.

### 14. The prediction dump is gitignored

`artifacts/` is excluded from git. A fresh clone must rerun
`python -m delta_crops.models.predictions` (561.2 s, 8,912 MB peak RSS, 29.6 MB output) before the
Predictions page works. The dump reproduces the Phase 7 S3 metrics with zero delta on all ten gate
checks, so it cannot silently change the reported result.

### 15. The dashboard is a local demo

No authentication, no TLS, no multi-user state, one process. It is not a deployment, and it does not
write to the lake (enforced by a SELECT-only guard and an mtime test).

### 16. Unobserved confounders

Irrigation, harvest timing, crop type and field management are not in the data. Any relationship
between NDVI, heat and rainfall is correlational. There is no validation against yield statistics or
field boundaries, so "stress" means "NDVI anomaly ≤ −0.08", not "crop failure".

---

## How to cite the project honestly

> "A reproducible six-season Sentinel-2 NDVI pipeline for one Nile Delta AOI, with automated quality
> gates, a time-split and spatially held-out stress model that misses its pre-registered PR-AUC bar
> by 0.0008–0.0140, and no demonstrated weather skill. Calibration is measured for the time split
> only."

Do not say: "predicts crop failure", "detects heatwave damage", "production-ready", "runs itself
every five days", or "validated against yields". None of those is supported.

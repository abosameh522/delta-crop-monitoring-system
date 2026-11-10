# Phase 5 — Gold layer: weekly NDVI, anomaly and weather

Status: **DONE** (2026-11-11). Scope was Gold only; Phase 6 has not started.

Commands (all real runs, from the repository root):

```bash
.venv/bin/python -m delta_crops.gold.cli gate        # weekly NULL decomposition (build gate)
.venv/bin/python -m delta_crops.gold.cli orbits --diagnose
.venv/bin/python -m delta_crops.gold.cli build --force
.venv/bin/python -m delta_crops.gold.cli verify
.venv/bin/python -m pytest tests/test_gold.py -q
```

## 1. What is built

| Table | Path | Rows | Key |
| --- | --- | --- | --- |
| `weekly_ndvi` | `data/gold/weekly_ndvi/year=Y/weekly_ndvi.parquet` | 4,786,080 per year; 23,930,400 total | `(cell_id, week_start)` |
| `ndvi_anomaly` | `data/gold/ndvi_anomaly/year=Y/ndvi_anomaly.parquet` | 4,786,080 per year; 23,930,400 total | `(cell_id, week_start)` |
| `weekly_weather` | `data/gold/weekly_weather/year=Y/weekly_weather.parquet` | 20 per year; 100 total | `week_start` |

All three are Parquet written per year and read through DuckDB. Gold covers **all 239,304 kept
cells**, not only cropland, so later cell-level work needs no rebuild; analysis filters on
`cells.is_cropland` (194,778 cells). `is_cropland` is joined, never copied, so there is one owner
of the flag.

`weekly_ndvi` columns: `cell_id`, `week_start` (Monday), `ndvi_mean`, `ndvi_std`, `n_obs`,
`masked_pct_mean`, `status`, `orbit_mix` (`R021`, `R064`, `R021|R064`, or NULL), `n_orbits`.
`ndvi_anomaly` adds `baseline_mean`, `baseline_std`, `anomaly`, `zscore`, `n_baseline_years`,
`status`. `weekly_weather` carries the five weather aggregates, `heatwave_days` and `n_days`.

Only ISO weeks **19–38** are in Gold: those 20 weeks sit fully inside 1 May–30 Sep in all five
years. Weeks 17/18/39/40 are partial in at least one year and would make a same-week cross-year
comparison unequal; they stay in Silver.

## 2. Weekly rule, exactly as implemented

- Week = ISO Monday–Sunday; `week_start` is the Monday.
- `n_obs` = distinct acquisition dates in that week with a non-NULL cell NDVI.
- `masked_pct_mean` = mean of the **actual Silver `masked_pct`** over *all* acquisition dates of
  that week for the cell, including dates whose NDVI is NULL. A NULL date is not assumed to be
  100 % masked: a cell with 40 % valid pixels is NULL with `masked_pct = 60`, and 60 is what goes
  into the mean. Measured: 270,339 / 1,531,69 / 190,707 / 312,231 / 209,751 NULL-NDVI
  cell-dates (2019–2023) have `masked_pct < 100`; the rest are fully masked.
- `status`: `n_obs < 2` → `insufficient_observations`; else `masked_pct_mean > 50` →
  `too_cloudy`; else `ok`. The masked cap is the deciding factor rarely: 458 / 123 / 157 /
  1,172 / 42 cell-weeks across 2019–2023.
- `ndvi_mean` and `ndvi_std` are **NULL unless status is `ok`**. `n_obs` and `masked_pct_mean`
  are always written so a NULL can be explained. Nothing is interpolated or filled.
- `ndvi_mean` is the mean of per-date cell NDVI (equal weight per date), not a pixel-weighted mean.
- `ndvi_std` is the sample std across those dates (`NULL` when `n_obs = 1`, but such a week is
  already `insufficient_observations` and has no value).

## 3. Build gate: decomposition of the cell-week NULL share

The gate runs before any Gold file is written and aborts if a season exceeds 30 %. It splits
NULLs into **structural** weeks (fewer than 2 AOI-wide acquisition dates, so every cell in the
week is NULL) and **per-cell** loss inside weeks that do have enough AOI acquisitions.
`struct%` is the structural share of all cell-weeks in that season; `nonstruct` is the NULL share
inside the remaining weeks; `total%` is the overall share.

| Season | Pop | Weeks | Structural weeks | struct% | nonstruct NULL% | total NULL% | insuff | too_cloudy |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2019 | all | 20 | 1 | 5.00 | 8.71 | 13.27 | 395,410 | 458 |
| 2019 | cropland | 20 | 1 | 5.00 | 8.77 | 13.33 | 324,415 | 247 |
| 2020 | all | 20 | 1 | 5.00 | 10.68 | 15.14 | 485,362 | 123 |
| 2020 | cropland | 20 | 1 | 5.00 | 10.94 | 15.39 | 404,738 | 19 |
| 2021 | all | 20 | 0 | 0.00 | 8.54 | 8.54 | 408,368 | 157 |
| 2021 | cropland | 20 | 0 | 0.00 | 8.66 | 8.66 | 337,295 | 32 |
| 2022 | all | 20 | 2 | 10.00 | 12.04 | 20.84 | 517,505 | 1,172 |
| 2022 | cropland | 20 | 2 | 10.00 | 12.36 | 21.13 | 432,491 | 921 |
| 2023 | all | 20 | 3 | 15.00 | 14.75 | 27.54 | 600,074 | 42 |
| 2023 | cropland | 20 | 3 | 15.00 | 15.02 | 27.76 | 497,182 | 32 |

For 2023 that is 15.0 points structural and 12.5 points per-cell (0.85 × 14.75), i.e. **15.0 of
27.54 points (54 %)** are weeks with fewer than two AOI acquisitions and **12.5 points (46 %)**
are per-cell loss. The earlier pre-plan wording ("loss comes from per-cell masking") was wrong;
this table is the corrected one. Worst season is 2023 at **27.54 % (all) / 27.76 % (cropland)**,
below the 30 % gate, so the weekly rule was **not** relaxed. Full JSON:
`data/gold/_reports/weekly_gate_<run>.json` (gitignored).

## 4. Relative orbits and what the diagnostic actually shows

P5-0 is a metadata-only STAC pass: one query per season year, `s2:product_uri` parsed with
`_R(\d{3})_`, plus `datetime`, `view:incidence_angle`, `view:azimuth`. Result: **574
date-tile rows**, one relative orbit per date (no date carries two orbits), **R021** and
**R064**, sensing times 08:41 / 08:51 UTC as in D-035. The cache is written to
`data/silver/_cache/orbits.parquet`; **Bronze remains read-only**, and no imagery is read.

**Cropland mean NDVI per date, within year, by orbit:**

| Year | R021 dates | R021 mean | R064 dates | R064 mean | Gap (R064 − R021) |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2019 | 30 | 0.5246 | 29 | 0.5378 | +0.0132 |
| 2020 | 30 | 0.5124 | 29 | 0.5233 | +0.0109 |
| 2021 | 31 | 0.4942 | 29 | 0.5007 | +0.0065 |
| 2022 | 31 | 0.5098 | 28 | 0.5306 | +0.0208 |
| 2023 | 29 | 0.5108 | 27 | 0.5515 | +0.0407 |

Those gaps are **not** an orbit offset on their own: each orbit's dates fall at different places
in the season, so phenology confounds them.

**The requested 5-day pairing does not isolate the orbit effect.** In this series a date exactly
5 days later is the **same relative orbit seen by the other platform** (S2A/S2B share an orbital
plane), and only 12 such pairs exist in five years — all same-orbit (2019: 2, 2020: 1, 2021: 2,
2022: 3, 2023: 4). Their mean differences (−0.004, −0.002, −0.000, −0.031, −0.023) are therefore
temporal change, not orbit differences.

Adjacent dates 2–3 days apart **do** cross orbits, and the sign is consistent in all five years:

| Year | 2 d pairs (R064→R021) | mean Δ NDVI | 3 d pairs (R021→R064) | mean Δ NDVI |
| --- | ---: | ---: | ---: | ---: |
| 2019 | 28 | −0.0099 | 28 | +0.0061 |
| 2020 | 29 | −0.0052 | 27 | +0.0043 |
| 2021 | 29 | −0.0082 | 28 | +0.0054 |
| 2022 | 28 | −0.0051 | 27 | +0.0075 |
| 2023 | 25 | −0.0055 | 25 | +0.0093 |

R064 reads higher than R021 by roughly **0.005–0.010 NDVI** in every year and in both pair
directions, but each pair also spans 2–3 days of crop change, so the orbit component is **not
separable** from phenology with this design. Therefore: **the earlier claim that the orbit offset
is small or absent on cropland is not supported, and is withdrawn.** What can be said is that
the observed cross-orbit difference is of order 0.01 NDVI, that weekly means draw on a mixture of
orbits (both columns are labelled), and that the leave-one-year-out anomaly compares the same ISO
week across years, where the orbit mix is broadly similar but never identical.

## 5. Heatwave definition and its evidence

**Definition (D-045, written before any NDVI response was inspected):** a heatwave day is a day
with daily `temp_max_c >= 40.0 °C` that lies inside a run of **at least 3 consecutive such days**,
all inside 1 May–30 Sep. `heatwave_days` counts those days per ISO week. Runs are detected on the
full daily series and then split at week boundaries; a run is not restarted on Monday.

Threshold evidence from the 765 real season-days (weather only, no NDVI):

| Candidate | Episodes | Days | Share of season-days |
| --- | ---: | ---: | ---: |
| Tmax ≥ 35, ≥ 3 days | 32 | 559 | 73 % |
| Tmax ≥ 38, ≥ 3 days | 35 | 220 | 29 % |
| **Tmax ≥ 40, ≥ 3 days** | **9** | **47** | **6.1 %** |
| Tmax ≥ 40, ≥ 2 days | 19 | 73 | 9.5 % |

Pooled daily maxima: median 37.0, p90 **40.2**, p95 41.3 °C. 40.0 °C is the pooled 90th
percentile rounded to a round number, chosen because 35 °C is normal summer weather and 38 °C
with a 3-day run still covers 29 % of days. Per season the rule yields 3 / 5 / 18 / 3 / 18
heatwave days (2019–2023); 2023 has four episodes: **14–18 Jul, 22–28 Jul, 5–7 Aug, 6–8 Sep**.

**Weekly split at ISO boundaries:** `heatwave_days` is counted per ISO week, so a run that crosses
a week boundary is split by day, not restarted. The 5–7 Aug run puts 2 days in the week of
2023-07-31 and 1 day in the week of 2023-08-07; the 6–8 Sep run puts 3 days in the week of
2023-09-04. The weekly values for 2023 are therefore 3 (Jul 10), 4 (Jul 17), 5 (Jul 24), 2 (Jul 31),
1 (Aug 07), 3 (Sep 04) — summing to 18. The single-day value in the week of 2023-08-07 is **not**
a separate episode; it is the tail of the Aug 5–7 run. Do not describe it as one.

**Weather provenance and interpretation limits:** one Open-Meteo grid point at the AOI centre
(~30.97 °N, 31.14 °E after model snapping), not an AOI average. ISO week 19 starts between 4 and
10 May depending on the year (2019-05-06, 2020-05-04, 2021-05-10, 2022-05-09, 2023-05-08), so a
same-ISO-week baseline carries a calendar shift of up to 6 days. With only 5 seasons × 20 weeks =
100 weekly points, any heat/NDVI relationship is confounded with seasonality (NDVI rises to a
July–August peak and then falls at harvest). Read the **anomaly**, not raw NDVI, and treat the
pattern as descriptive, not causal.

**Rainfall:** 5.4 mm summed over all 765 season-days, daily maximum 0.7 mm, **no day above
1 mm** in any season. `precip_sum_mm` is carried and shown, but this window cannot estimate a
rainfall effect; reporting a slope would be meaningless.

## 6. Anomaly

Leave-one-year-out, per cell and ISO week: baseline is the mean of `ndvi_mean` in the other four
years for the same cell and same ISO week, using only years whose status is `ok`; equal weight
per year, **no smoothing** across weeks. `baseline_std` is the sample std of those yearly values.
`n_baseline_years < 3` → `no_baseline` with NULL baseline/anomaly/zscore; `zscore` is also NULL
when `baseline_std = 0`.

Status precedence: the **current week's** status is propagated first
(`insufficient_observations` / `too_cloudy`), then `no_baseline`, then `ok`. The `baseline_mean`
of 2024 excludes 2024 itself — verified by a synthetic test where the baseline is 0.30, not
0.35. This baseline is **descriptive only**. Phase 7 must recompute any baseline feature from
training years only (D-015, D-044).

## 7. Acceptance — real output

```
2019 weekly: rows=4,786,080 keys=4,786,080 ndvi=[-0.893, 0.998] ok=4,150,908 insuff=634,714 cloudy=458 weeks=20 bad_null=0 bad_ok=0 bad_insuff=0
2020 weekly: rows=4,786,080 keys=4,786,080 ndvi=[-0.971, 0.993] ok=4,061,291 insuff=724,666 cloudy=123 weeks=20 bad_null=0 bad_ok=0 bad_insuff=0
2021 weekly: rows=4,786,080 keys=4,786,080 ndvi=[-0.984, 0.989] ok=4,377,555 insuff=408,368 cloudy=157 weeks=20 bad_null=0 bad_ok=0 bad_insuff=0
2022 weekly: rows=4,786,080 keys=4,786,080 ndvi=[-0.978, 0.997] ok=3,788,795 insuff=996,113 cloudy=1,172 weeks=20 bad_null=0 bad_ok=0 bad_insuff=0
2023 weekly: rows=4,786,080 keys=4,786,080 ndvi=[-0.966, 0.998] ok=3,468,052 insuff=1,317,986 cloudy=42 weeks=20 bad_null=0 bad_ok=0 bad_insuff=0
```

`keys` equals `rows` in every year (no duplicate `(cell_id, week_start)`), and `bad_null`,
`bad_ok`, `bad_insuff` are all zero: no non-`ok` row carries a value, no `ok` row lacks one, and
no week with fewer than 2 observations is labelled anything but `insufficient_observations`.

Anomaly ranges: `baseline_mean` within [−0.969, 0.978] and `anomaly` within
[−1.175, 0.988] for all years, i.e. baseline stays in [−1, 1] and anomaly is well inside
[−2, 2]. No `ok` anomaly row has a NULL anomaly/zscore. `no_baseline` counts:
650,569 / 345,038 / 705,241 / 169,067 / 355,995 (2019–2023).

**zscore percentiles (pooled across years, `anomaly IS NOT NULL`):**  
p1=-8.2, p10=-1.8, p25=-0.90, p50=-0.02, p75=0.88, p90=1.8, p99=7.5

**zscore is indicative only.** The heavy tails (p1 = −8.2, p99 = +7.5) come from `baseline_std`
being computed from only 3–4 yearly values per cell/week. The **anomaly (raw difference) is the
primary Gold signal**; `zscore` must not be presented as stress detection or used as a formal
test statistic. It is a rough guide for investigation, nothing more.

Timings on this machine (4 DuckDB threads): gate **9.5 s**, weather **0.4 s**, weekly
**29.5 s** (5.5–6.1 s per year), anomaly **10.0 s**, full `build --force` **50.3 s**. A second
`build` skips every existing output; `--force` reproduces identical files in the tests.

Sample query (`gold/queries.py`) returns **20 rows in 0.34 s**; 2023 cropland mean NDVI peaks at
0.72–0.73 in weeks 24–31 and falls to 0.47 by 18 September, with the four heatwave episodes
visible in `heatwave_days` (3, 4, 5, 2, 1, 3 across weeks 28–36) alongside the weekly mean `tmax`.
Coverage is printed next to every NDVI value (100 %, 93.4 %, 17.2 %, 0 % in the affected weeks)
so a mean is never read without the share of cropland cells behind it.

**What the 2023 data show (descriptive only):** NDVI and anomaly rise through July and stay positive
during the four heatwave episodes (weeks 28–36). There is **no visible NDVI drop or anomaly
reversal** during or after the heatwaves. This is a descriptive comparison over five seasons;
it **cannot prove absence of an effect** — the signal may be confounded with seasonality (NDVI
peaks in July–August), the orbit mix (84–95 % mixed in `ok` weeks), and the small baseline sample
(3–4 years). **Weeks with low coverage (e.g., 2023-07-03 at 17.2 %) must not be compared with
neighbouring weeks without the coverage column**; the mean is based on a small and potentially
biased cell subset.

Tests: **7 new Gold tests**, full suite **65 passed in 2.04 s**:

1. heatwave runs (+ exactly-threshold day, 2-day run ignored);
2. ISO week 19–38 keys and Sunday/Monday assignment;
3. status rule at the boundaries (2 obs and exactly 50 % masked is `ok`);
4. weekly build contract on a synthetic 4-cell × 4-year dataset (values NULL unless `ok`,
   orbit mix, `too_cloudy` from the all-dates masked mean);
5. anomaly excludes the current year, baseline 0.30 not 0.35, current-week status precedence;
6. idempotent rebuild (identical checksums);
7. weekly weather heatwave days (3-day run = 3, 2-day run = 0) and the sample query shape/coverage.

## 8. Limitations carried forward

- Five seasons give at most 4 baseline years and only 100 weekly points; anomaly with a 3–4 year
  baseline is noisy and `zscore` is indicative.
- Leave-one-year-out is not a forecast baseline and must not be used as a model feature.
- No summer rainfall signal (5.4 mm total), so the rainfall half of the question cannot be
  answered from this window.
- One weather grid point for a 48 × 50 km AOI, and a same-ISO-week calendar shift of up to 6 days.
- Orbit mix is tagged but not corrected; the cropland orbit offset is not separable from
  phenology here and the earlier "small/absent" claim is withdrawn.
- WorldCover 2021 is static; 100 m cells mix fields; DN-floor on dark pixels and the unexplained
  2021-07-10 DN = 0 share (PROJECT.md §7) remain open.
- Heatwave runs truncated by the 1 May / 30 Sep window edges cannot qualify; no such truncated
  run affected the five measured seasons, but the rule is window-bounded.
- `zscore` and `anomaly` inherit the noisy small-sample baseline; treat outliers as candidates for
  investigation, not findings.

## 9. Interview explanation

- **What we built:** we turned 70 million daily cell observations into a weekly table that is
  honest about holes. If a cell does not have at least two usable satellite dates in a week, we
  write NULL and a reason instead of inventing a number.
- **Why the gate matters:** before building anything we measured how much of the weekly table
  would be empty. 2023 loses 15 % of its cell-weeks because the satellites simply did not give us
  two clear looks that week — that is structural, not a bug — and another 12.5 % because clouds
  covered individual cells. At 27.5 % total we stayed under our 30 % stop line and did **not**
  loosen the rule.
- **The anomaly:** each week is compared with the same week in the other four years for the same
  cell, excluding the year being scored. That answers "is this week unusual for this place at this
  time of year?" without leaking the year into its own baseline. For modelling we will rebuild it
  from training years only.
- **Heatwaves:** we defined a heatwave day as 40 °C or more inside a run of at least three days,
  chosen because 35 °C happens on three out of four summer days and 38 °C runs still cover a third
  of the season. That gives 47 heatwave days in five years, 18 of them in 2023, which matches four
  visible July–September episodes.
- **The orbit trap:** the same ground is imaged from two different orbital tracks, and we found
  NDVI reads about 0.005–0.010 higher from one than the other on cropland. We label the orbit mix
  in every weekly row rather than pretending the measurements are interchangeable, and we withdrew
  an earlier claim that the difference was negligible.
- **What I would say if asked about rain:** there is essentially no summer rain here — 5 mm in five
  seasons — so we report it but refuse to fit a rainfall effect that the data cannot support.

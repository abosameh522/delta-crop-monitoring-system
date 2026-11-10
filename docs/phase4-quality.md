# Phase 4 — Silver data quality (2026-11-11)

Phase 4 is complete; Phase 5 has not started. Silver remains DN>0; no output was
rebuilt. Threshold changes were explicit user decisions after measurement,
recorded in PROJECT.md D-036 onward. Alerting is not evidence of a pipeline bug.

## Run commands

```bash
uv pip install -e ".[dev]"
.venv/bin/python -m delta_crops.quality.cli
.venv/bin/python -m delta_crops.quality.dn_floor
.venv/bin/python -m delta_crops.quality.corruption_demo
.venv/bin/python -m pytest -q
```

The quality CLI accepts `--data-dir` and `--config`. It returns 0 with alerts,
1 on critical failures. The Python API raises `QualityFailure` after writing
its JSON report, so a future orchestrator can stop downstream execution.
No orchestration has been added. Each run writes a unique atomic report under
`data/silver/_reports/quality/`; gap files retain exact per-cell dates and splits.

`corruption_demo` copies all 293 Silver date files, cells.parquet and the full
ledger into a TemporaryDirectory (about 657 MB for imagery tables). It changes
one NDVI to 1.25 ONLY in that copy, invokes the actual CLI, asserts exit 1,
checks the original source SHA-256 is unchanged, saves the acceptance report,
and removes the temporary copy. Never corrupt the real lake for this test.

## Gap definition and diagnosis

A gap is the number of consecutive calendar days without a usable NDVI value.
Usable means exactly `ndvi IS NOT NULL` after Silver's 50% valid-pixel gate.
Between usable dates A and B, length is `(B-A).days-1`. Leading and trailing
missing days inside May 1–September 30 count. A never-usable cell has a 153-day
gap; an always-usable daily cell has zero. Longest-gap ties choose the earliest.

Every gap splits into (1) days with no acquisition date in the ingestion ledger
and (2) ledger acquisition days when this cell's NDVI is NULL. The first part is
information only; it is not a missing-partition failure. Gap output is never
filled. A structurally missing/corrupt output aborts temporal analysis rather
than disguising a pipeline failure as a natural data gap.

**May 7–17, 2020 = 11 consecutive missing calendar days**, bracketed by usable
May 6 and May 18. The 43,642 cells are NULL on **May 8** (RUV only) and **May 16**
(both tiles): 9 no-acquisition days + 2 NULL acquisition dates.

| Geometric coverage | Cells |
| --- | ---: |
| 36RTV only | 15,272 |
| Both / overlap | 15,073 |
| 36RUV only | 13,297 |

35,573 are cropland; 8,069 are not. Extent of their 100 m cell rectangles,
EPSG:32636: **west 295100, south 3403400, east 343800, north 3453700 m**.
The exact 43,642 IDs, coverage labels and cropland flags are saved in
`data/silver/_reports/quality/gap_cohort_may2020.parquet`.
It is **not** a 43,642-cell RTV-only footprint. May 8's invalidity is dominated
by uncovered pixels in 15,272 cells and SCL masking in 28,370. On May 16 SCL
masking dominates 43,586 cells and DN=0 dominates 56. Recomputed valid counts
match saved Silver exactly. The 8 uncovered pixel observations on May 16 are
small edge-window slivers, not an uncovered cell footprint.

Coverage here means at least one in-AOI 10 m pixel inside a tile's rectangular
AOI-window extent; it does not imply that DN or SCL is usable. The 12 known
single-tile dates naturally lose coverage where the other tile is required;
that known effect explains only a subset of this cohort, not the whole group.
No unexpected shared swath footprint or Silver count bug was found.

## Long tails

| Season | Cells with >30-day gaps | >30-day episodes | Cropland | Coverage | Longest example |
| --- | ---: | ---: | ---: | --- | --- |
| 2019 | 16 | 17 | 0 | all RUV-only | cell 403374, Jul 24–Sep 30 = 69 days |
| 2020 | 19 | 19 | 0 | all RUV-only | cell 282313, Jun 26–Sep 30 = 97 days |
| 2021 | 24 | 24 | 0 | all RUV-only | cells 282313/282314/400377/493351, Jul 21–Sep 30 = 72 days |
| 2022 | 0 | 0 | 0 | — | — |
| 2023 | 0 | 0 | 0 | — | — |

[Exact cell IDs, all 60 long-gap episodes, dates, attribution and UTM rectangles](phase4-long-tail-cells.csv)
are versioned beside this report. The full acquisition-date observations and
pixel-cause measurements are in the gitignored `gap_diagnosis.json`.

These cells have usable dates elsewhere in their seasons, all have 100 in-AOI
pixels, and none is persistently NULL for the whole season. On **1,177 NULL
acquisition cell-dates in their chosen longest gaps**, invalidity is SCL-dominated
in every case. Across those observations only **15 pixels** have DN=0; **80,179**
positive-DN pixels are SCL-masked, mostly class 2 (dark area), then shadow/cloud.
This does **not** support persistent swath-edge DN=0 as the explanation. Valid
counts reconstructed from B04/B08/SCL match saved Silver for all audited dates.
The secondary 2019 cell 282313 episode (Jun 14–Jul 15, 32 days) is listed in the
CSV; the pixel-cause audit covered the chosen longest episode for each cell.

## Threshold distributions

Shares below use strictly **> threshold** as requested for “exceeding”. The
configured alert uses **>=13 days**; no observed longest gap equals 13 here,
so the >13 and >=13 percentages coincide on this dataset. Denominators are
239,304 all cells and 194,778 cropland cells in every season.

| Season | >10 all / crop % | >13 all / crop % | >21 all / crop % | >30 all / crop % |
| --- | --- | --- | --- | --- |
| 2019 | 1.1960 / 1.1788 | 1.0497 / 1.0129 | 0.0109 / 0.0000 | 0.0067 / 0.0000 |
| 2020 | 27.1809 / 27.3060 | 4.0563 / 4.0939 | 0.0100 / 0.0000 | 0.0079 / 0.0000 |
| 2021 | 1.7037 / 1.7512 | 1.6886 / 1.7384 | 0.0176 / 0.0046 | 0.0100 / 0.0000 |
| 2022 | 7.6434 / 7.9865 | 1.9118 / 1.9751 | 0.0618 / 0.0616 | 0.0000 / 0.0000 |
| 2023 | 12.6935 / 13.1647 | 4.7396 / 4.8399 | 0.0000 / 0.0000 | 0.0000 / 0.0000 |

The post-measurement decision is **>10 days information only**, **>=13 days cell
alert**, and an additional alert when **>10%** of all/cropland cells violate the
13-day rule. Thirteen missing days guarantee a full Monday–Sunday ISO week for
any start weekday; 12 do not (unit-tested for all seven start weekdays). This
links the rule to future weekly usability; it does not guarantee two valid
observations per week. Original >10-day percentages and AOI-only maxima are
retained in D-036. The former ~20% review stop gate is resolved.

## Checks, severities and performance

Explicit physical schema: cell_id int32, date date32, ndvi float32,
valid_pixels uint8, total_pixels uint8, masked_pct float32. Only ndvi values may
be NULL. Expectations are declared in quality/checks.py, independent of inputs;
the date must be physically inside the file, not synthesized from its path.

Critical: schema/dtypes, required NULLs, NDVI outside [-1,1] or nonfinite,
duplicate (cell_id,date), wrong row count, valid_pixels>total_pixels, foreign
cell IDs, wrong date contents and missing expected ledger-date outputs.
Expected outputs come from the **293 ledger dates**, never existing-file globs.
A file that exists but has bad schema, rows or date contents still fails.

Alerts: masked_pct>50%; dual-tile NULL>50% OR above the leave-one-out seasonal
median+6×raw MAD (>=5 other dual-tile dates). Zero MAD means strictly above the
peer median; insufficient peers use only the absolute criterion. Single-tile
dates are separate context and receive no NULL-rate alert for being single-tile.
All numeric thresholds, alert parameters and severities are explicit in config.

DuckDB scans selected Parquet columns and aggregates each date independently,
keeping distinct-key state near 239k rows instead of 70M. Pandera checks the
small per-date violation-count frame, requiring zero critical violations.
Gap SQL processes one season at a time with window functions, calendar prefix
counts and endpoint sentinels; there is no daily 239k-cell calendar expansion.
Default limits are 4 threads and 2 GB DuckDB memory; RSS was not measured.

Actual output:

```text
Quality PASSED: 293 ledger dates, 70,116,072 rows; 4 alert checks
Seconds: 18.757
58 passed in 1.87s
Critical quality failure: ndvi_range
Corruption CLI exit code: 1
Original Silver unchanged.
```

Scan aggregates: **12.847 s**; gap analysis: **5.811 s**; total checks: **18.757 s**.
Overall NDVI NULL share: **13.0100%**. No critical failure; **9,122,114** rows
have masked_pct>50%; **15** dual-tile dates have NULL alerts. Both populations
have gap alerts, but no season exceeds 10% gap prevalence. All 12 single-tile
dates remain visible and do not receive single-tile NULL-rate alerts. Alerts
are retained rather than suppressed to make the dataset look cleaner.

## DN=1 floor audit — no rebuild

Fixed 14 dates: the six original July dates plus May/June/August/September in
both eras. This is the minimum to preserve all six while covering each month.
Replay uses the same AOI mask, first-valid tile order, SCL mask, arithmetic and
50% cell gate as Silver. DN>0 replay reproduces saved valid counts and NDVI;
DN>1 is computed in memory only. Audit time **20.16 s**.

| Date | Net mosaic pixels excluded | All cells non-NULL→NULL | All NULL Δ pp | Crop non-NULL→NULL | Crop NULL Δ pp | Paired crop mean NDVI Δ | Crop cells abs(ΔNDVI)>0.01 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2019-05-19 | 0 | 0 | 0.000000 | 0 | 0.000000 | 0.00000000 | 0 |
| 2019-06-18 | 48,431 | 312 | 0.130378 | 1 | 0.000513 | 0.00007357 | 367 |
| 2019-07-01 | 16 | 0 | 0.000000 | 0 | 0.000000 | -0.00000018 | 0 |
| 2019-07-03 | 66,908 | 396 | 0.165480 | 4 | 0.002054 | 0.00015351 | 523 |
| 2019-07-06 | 1,478 | 2 | 0.000836 | 2 | 0.001027 | 0.00000293 | 71 |
| 2019-08-12 | 448 | 2 | 0.000836 | 0 | 0.000000 | 0.00000023 | 2 |
| 2019-09-06 | 36,672 | 255 | 0.106559 | 1 | 0.000513 | 0.00003509 | 153 |
| 2023-05-23 | 0 | 0 | 0.000000 | 0 | 0.000000 | 0.00000000 | 0 |
| 2023-06-22 | 81,371 | 458 | 0.191388 | 9 | 0.004621 | 0.00012947 | 965 |
| 2023-07-02 | 74,453 | 427 | 0.178434 | 8 | 0.004107 | 0.00015068 | 701 |
| 2023-07-05 | 1,050 | 8 | 0.003343 | 8 | 0.004107 | -0.00001779 | 26 |
| 2023-07-07 | 28,253 | 195 | 0.081486 | 59 | 0.030291 | -0.00005355 | 389 |
| 2023-08-06 | 1,038 | 6 | 0.002507 | 0 | 0.000000 | -0.00000003 | 2 |
| 2023-09-05 | 34,699 | 263 | 0.109902 | 0 | 0.000000 | 0.00001968 | 69 |

The JSON report also retains total non-NULL/NULL counts and pixel counts under
both rules, per-tile excluded pixel observations, baseline mosaic DN=1 pixels,
mean NDVI under both rules and paired maximum differences. Tile overlaps can
supply a replacement pixel under DN>1, so net mosaic exclusions differ from
summing exclusions across tiles. Paired differences exclude changed NULL status;
the separate NULL transition counts prevent that exclusion hiding effects.

The broader audit supersedes rough earlier 500–700-cell estimates: up to **965**
paired cropland cells change >0.01 on a date. Recommend **keep DN>0, no rebuild**
for the current aggregate analysis: crop NULL increase is at most **0.0303 pp**
and paired mean changes stay near 0.00015 or below. Local changes remain relevant
for future cell-level predictions, so this is not proof of negligible local bias.
No DN rule changed, and Silver partitions were never written by the audit.

## Evidence and limitations

Local gitignored reports from this run:

- `quality_20261110T221116Z_9cb29e54.json` and its five per-cell gap files.
- `corruption_acceptance_20261110T221053Z_9fbee464.json`.
- `dn_floor_20261110T220722Z_271bfcff.json`.
- `gap_diagnosis.json` and the prior `gap_measurement_20261110T215222Z.json`.

UTC filenames can show November 10 while the local Cairo date is November 11.
Reports reference local data, not portable bundled raw observations. Phase 3's
stable-target test still requires its separate local measurement artifact.
The DN audit covers 14 dates, not every acquisition; SCL cause diagnosis is
observational, not ground truth. Robust outliers identify unusual missingness,
not necessarily broken processing. No Gold, orchestration or modeling was added.

## In plain language

“I separated bad pipeline output from naturally missing satellite observations.
Wrong schemas, duplicate keys and missing expected partitions stop the run;
clouds and long observation gaps produce transparent alerts. I measured the real
gap distribution before we changed a threshold, kept that decision history, and
split missing days into absent acquisitions versus NULL cell observations.
DuckDB scans the large files while Pandera validates compact summaries. Finally,
I deliberately corrupted a disposable data copy to prove the CLI fails safely,
and measured a stricter DN rule without altering production data.”

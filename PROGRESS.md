# Progress

## Status

- **Phase:** 6 — orchestration **IN PROGRESS**. 6a (Prefect flow) accepted and CLOSED;
  6b (Docker) implemented, **awaiting the user's Docker verification**.
- **Current step:** 6b files are written and unit-tested; the Docker daemon cannot be used from
  this session, so build/up/healthcheck/stats/tests inside the image are the user's to run.
  Command list lives in `docs/phase6b-docker.md` §Compose and in the handoff message.
- **6b layout:** one container runs `python -m delta_crops.orchestration.serve` →
  `flow.serve(interval=5 days, global_limit=1, pause_on_shutdown=True)` with the ephemeral
  Prefect API (SQLite) inside `PREFECT_HOME=./.prefect`. Chosen over server+worker because the
  6a spike proved `global_limit=1` ordering on 3.9.2 and this needs one process, not three.
  No RAM figure is claimed; `docker stats --no-stream` is the measurement.
- **6b images:** `Dockerfile` = `runtime` (default; `python:3.12-slim`, uv from a pinned image,
  `uv sync --frozen --no-dev`, non-root `app` with build-arg uid/gid, no `data/`) and `test`
  (`+ uv sync --frozen --extra dev`, pytest only there). `.dockerignore` drops `data/`, `.venv/`,
  `.git/`, `.prefect/`, caches, `.env`, `tests/`, `docs/`.
- **6b compose:** `orchestrator` mounts `./data:/app/data`, `./config:/app/config:ro`,
  `./.prefect:/var/lib/prefect`; sets `DELTA_CROPS_DATA_DIR=/app/data`,
  `DELTA_CROPS_CONFIG=/app/config/aoi.yaml`, `PREFECT_HOME=/var/lib/prefect`; restart
  `unless-stopped`, `init: true`, no published ports, healthcheck
  `python -m delta_crops.orchestration.healthcheck`. `tests` service (profile `test`) mounts
  `tests/` and the real lake **read-only**.
- **6b tests:** `tests/test_phase6b.py` — 10 tests, no Docker needed: five-day interval,
  `global_limit=1`/`pause_on_shutdown`, `data_dir()` env resolution, options passed to
  `flow.serve`, healthcheck healthy/ unreachable/ unwritable home, Dockerfile stage invariants,
  `.dockerignore` coverage, compose mounts + env. Default suite total after 6b: see Tests line.
- **Schedule anchor decision:** registration happens on every container start, so the interval is
  re-anchored there; accepted because every run is idempotent (complete season = `no_op`,
  in-progress = skip existing assets/verified partitions). Fixed anchor via
  `IntervalSchedule(anchor_date=...)` is future work. `pause_on_shutdown=True` means
  `docker compose stop` pauses the schedule and `up -d` resumes it (no catch-up burst).
- **6a base (accepted):** four tasks calling the real CLI `main()` in-process
  (ingest → Silver → quality → Gold), only exit 3 retried (1/2/4 s), `PipelineError` otherwise,
  quality `retries=0`, every run appends `_logs/pipeline_runs.jsonl` in `try/finally`,
  `gold_verify`/`bronze_verify` fields, partial weather via `--allow-partial-weather-year`,
  real end-to-end integration test with only network mocked.
- **Tests now:** default `142 passed, 1 deselected`; quick loop `-m "not flow and not slow"`
  fast (<10 s); slow `-m slow` 1 test. Gold regression 15/15 identical (last run 47.15 s).
- **Half-done / NOT DONE:** Phase 6b Docker verification (build, `up -d`, health, `docker stats`,
  in-container imports and suite, then `down`). Phase 7+ not started. Legacy Silver outputs still
  need explicit adoption.
- **Next:** user runs the 6b command list and pastes build/ps/logs/stats/test output;
  then 6b is closed and Phase 7 can start on request. No unverified claim is recorded as fact.

Legend: `TODO` · `IN PROGRESS` · `DONE` · `BLOCKED`

| Phase | Title | Status | Notes |
| --- | --- | --- | --- |
| 0 | Scope and setup | DONE | Committed on `main` (`c2c3ea8`) |
| 1 | Architecture design (no code) | DONE | Source, baseline, grid, schemas, BOA offset, risks documented; accepted |
| 2 | Ingestion (Bronze) | DONE | 1 722 assets, 25.81 GB, verified complete 2019–2023 |
| 3 | Bronze → Silver (cleaning) | DONE | 293/293 dates built; 39 tests passed; revised within-orbit test 7 passes both groups. Earlier failures and post-hoc qualification retained in D-035. |
| 4 | Data quality | DONE | All 293 ledger dates / 70,116,072 rows passed critical checks; 58 tests pass; corruption CLI exit 1; DN audit and D-036 onward documented. |
| 5 | Gold layer | DONE | Weekly gate passed (max 27.54%), anomaly leave-one-out, heatwave ≥40°C/≥3d, orbit cache + diagnostic. 7 new tests; 65 total pass. |
| 6 | Orchestration | IN PROGRESS | 6a accepted (Prefect 3.9.2, four tasks, retries, run log). 6b Docker built and unit-tested; awaiting user verification. |
| 7 | Modeling | TODO | Baseline computed from training years only |
| 8 | Dashboard | TODO | |
| 9 | Packaging and documentation | TODO | |

## Phase 0 — Scope and setup

- [x] AOI-A chosen, south edge shifted to `30.75` (Tanta fully inside).
- [x] Focus season 2023, plus 2019–2023 baseline seasons.
- [x] Repo structure, `pyproject.toml`, `.gitignore`, `.env.example`, `PROJECT.md`, `PROGRESS.md`, `README.md`.
- [x] `pytest` runs with tests covering config validity and bbox size.

Notes:
- Committed on `main` as `c2c3ea8`; no remote configured and nothing pushed.
- Bbox measures ~47.7 km × ~49.8 km (~2 375 km²), inside the 50×50 km target.

## Phase 1 — Architecture design

- [x] Data source comparison verified against live STAC APIs (Earth Search chosen, CDSE fallback).
- [x] 2019–2023 L2A availability confirmed for the AOI window.
- [x] BOA offset behaviour checked on real pixels; documented the metadata/data disagreement.
- [x] 100 m grid, row/size estimates, storage layout and partitioning.
- [x] Exact Bronze/Silver/Gold schemas and primary keys.
- [x] Weather split into its own Silver table.
- [x] ESA WorldCover cropland mask verified and measured over the AOI.
- [x] Cloud filter 20 % vs 40 % compared per season; 40 % recommended.
- [x] Weekly minimum-observation rule and null semantics.
- [x] Leakage rule for Phase 7 added to risks.
- [x] `config/aoi.yaml` and `tests/test_config.py` updated for seasons, grid and thresholds.
- [x] User review of `PROJECT.md` architecture (accepted 2026-11-10).
- [x] Review follow-ups: 238k cell-count rationale, cross-era water sanity check, issue link + date,
      Phase 5 NULL-rate gate, Phase 9 WorldCover attribution.

Notes:
- Architecture is documentation only; no pipeline code written.
- Open data points deferred: heat-wave definition (Phase 5), stress threshold (Phase 7).

## Phase 2 — Ingestion (Bronze)

- [x] Earth Search STAC search over the AOI + season, `eo:cloud_cover < 40`, dedupe on
      `(tile, date)` keeping the highest processing baseline.
- [x] Windowed reads of **B04, B08, SCL only** (no full-scene downloads), written as COGs under
      `data/bronze/sentinel2/tile=/year=/date=/`.
- [x] Open-Meteo daily weather per season year (one call per year, validated for completeness).
- [x] **Append-only JSONL log** (`data/bronze/_logs/ingestion_log.jsonl`), one fsynced record per
      completed `(item, band)`, compacted to Parquet as a separate step.
- [x] Idempotent re-runs: skip when the logged size + SHA-256 match the file on disk.
- [x] `nodata_pct` per band, `resolution_m`, `duration_s` in the log (no NDVI/masking in this phase).
- [x] 23 offline tests, including a simulated interruption, a torn-final-line recovery test and
      request-body tests for the STAC query builder.
- [x] GDAL/vsicurl settings documented in `PROJECT.md` §3.13 with measured request counts.
- [x] Bronze COGs written DEFLATE + predictor 2 + tiled, **no overviews**; losslessness proved on
      48/48 files (decoded-array SHA-256 unchanged) and 26.3 % smaller.
- [x] Open-Meteo terms verified; recorded as D-017 and added to the Phase 9 checklist.
- [x] Two-era smoke runs (2019 and 2023) with real numbers, re-run after the compression change.

Smoke results (window 15–24 June, cloud < 40 %, final COG format):

| | 2019 | 2023 |
| --- | --- | --- |
| Scenes / dates | 8 / 4 | 8 / 4 |
| Assets | 24 (365.0 MB) | 24 (361.5 MB) |
| Item ids | `…_1_L2A` | `…_0_L2A` |
| Baseline | 05.00 | 05.09 |
| STAC asset names (`red`/`nir`/`scl`) | identical | identical |
| `raster:bands` scale / offset | 0.0001 / −0.1 | 0.0001 / −0.1 |
| `boa_offset_applied` | true | true |
| Resolution | B04/B08 10 m, SCL 20 m | same |
| nodata | 0 in B04/B08 (mean 3.65 % incl. 13.5–15.7 % on 36RUV dates) | 0 in B04/B08 (mean 4.01 %, up to 16.43 %) |
| Wall time (4 workers) | 254 s | 313 s |
| Re-run | 0 downloaded / 24 skipped in 3.0 s | 0 downloaded / 24 skipped |

**Era differences are naming and baseline only.** Asset names, scale, offset flag, resolutions and
nodata are identical between 2019 and 2023, which confirms the reflectometry rule (scale only, never
the advertised −0.1) applies to both eras without a branch. The one real difference is the processing
baseline (`05.00` → `05.09`), which is exactly why dedupe keeps the highest baseline.

### Weather in Bronze (proven for all five years)

`ingest` fetches weather itself for every year its window touches, and there is also a standalone
command that fetches weather without any imagery (both idempotent through the same ledger):

```bash
.venv/bin/python -m delta_crops.ingestion.cli weather            # all configured season years
.venv/bin/python -m delta_crops.ingestion.cli weather --year 2023
```

Real state of `data/bronze/weather/year=YYYY/open_meteo_daily.json` (checked 2026-11-10):

| Year | Days | Range | Validation | Nulls |
| --- | --- | --- | --- | --- |
| 2019 | 153 | 2019-05-01 .. 2019-09-30 | passed | 0 |
| 2020 | 153 | 2020-05-01 .. 2020-09-30 | passed | 0 |
| 2021 | 153 | 2021-05-01 .. 2021-09-30 | passed | 0 |
| 2022 | 153 | 2022-05-01 .. 2022-09-30 | passed | 0 |
| 2023 | 153 | 2023-05-01 .. 2023-09-30 | passed | 0 |

Validation is `weather.validate_payload`: it raises unless the daily arrays exist, have exactly
`(end − start).days + 1` entries, start on 1 May and end on 30 Sep, and contain all five variables.
Open-Meteo snaps the request to its model grid: the AOI centre (30.975 °N, 31.110 °E) returns
`30.9666, 31.1374`, ~3 km from the geometric centre — acceptable for a 48 km AOI, noted for the
weather table's provenance.

### Full backfill — COMPLETE (run by the user, verified 2026-11-10)

The user ran `scripts/backfill.sh`; all five seasons are in Bronze and `verify` reports
**"Bronze is complete for this year"** for 2019–2023: **1 722 assets**, **25.81 GB**, 293 acquisition
dates, mean nodata 3.6–4.4 % per year, weather present for all five years. Per year on disk:
2019 5.07, 2020 5.25, 2021 5.36, 2022 5.27, 2023 4.86 GB. Measured per tile-scene: **36RTV 21.8 MB**,
**36RUV 67.8 MB**, **88.1 MB per acquisition date**.

Expected work from STAC (checked 2026-11-10, cloud < 40 %) with measured sizes:

| Year | Tile-scenes | Assets | Bronze on disk |
| --- | --- | --- | --- |
| 2019 | 113 | 339 | 5.07 GB |
| 2020 | 116 | 348 | 5.25 GB |
| 2021 | 119 | 357 | 5.36 GB |
| 2022 | 117 | 351 | 5.27 GB |
| 2023 | 109 | 327 | 4.86 GB |
| **Total** | **574** | **1 722** | **25.81 GB** |

Runbook used (kept for re-runs; years run **sequentially in one window** — the 4-worker cap is per
process, and five parallel years would open ~20 connections against one shared ledger):

```bash
cd ~/delta-crop-monitoring-system
mkdir -p data/bronze/_logs

# Sequential years in ONE window, laptop kept awake, per-year log + combined log.
tmux new-session -d -s bronze -c ~/delta-crop-monitoring-system \
  'systemd-inhibit --what=sleep:idle --why="Bronze backfill" \
     bash scripts/backfill.sh 2>&1 | tee data/bronze/_logs/backfill.log'

tmux attach -t bronze     # Ctrl-b d to detach; the run continues
```

`scripts/backfill.sh` loops `2019 2020 2021 2022 2023` in order, uses `.venv/bin/python` by absolute
path (no dependence on the tmux environment), logs each year to `data/bronze/_logs/ingest_<year>.log`,
continues after a failed year, and compacts the ledger at the end. Pass years explicitly to run a
subset: `bash scripts/backfill.sh 2022 2023`.

Progress check (reads the JSONL only, no network):

```bash
cd ~/delta-crop-monitoring-system
.venv/bin/python -m delta_crops.ingestion.cli status          # assets, GB, mean nodata per year
.venv/bin/python -m delta_crops.ingestion.cli status --year 2023
du -sh data/bronze/sentinel2
tail -f data/bronze/_logs/ingest_2023.log
```

Completeness check (re-queries STAC and compares expected assets with logged + checksummed files;
exit code 1 if anything is missing). Run per year, or after the whole backfill:

```bash
cd ~/delta-crop-monitoring-system
for y in 2019 2020 2021 2022 2023; do
  .venv/bin/python -m delta_crops.ingestion.cli verify --year "$y" || echo "INCOMPLETE: $y"
done
```

Resume after any interruption (Ctrl-C, sleep, network drop): just run the same `ingest --year Y`
again — complete assets are skipped by size + SHA-256, and the JSONL is append-only, so a torn last
line is discarded on read. Only that one year needs re-running; the ledger is shared, so no other year
is affected.

Notes:
- Weather is already in Bronze for all five years; every `ingest --year` re-checks it and skips it.
- Disk: ~26 GB total. Check with `df -h .` before starting 2022/2023.
- A full year at 4 workers took roughly 1–1.2 h (109–119 tile-scenes).
- The accidental 5-minute 2019 run from the review session is now simply part of the completed 2019
  season — no extra or duplicate files.

## Upcoming phase requirements (agreed during Phase 1 review)

- **Original Phase 3 requirement (superseded by D-034/D-035, retained for audit):** per-era real-scene check **and** the
  cross-era water sanity check (fixed 1 km water box, 1–7 July 2019 vs 2023; |Δ red reflectance| ≤ 0.02,
  |Δ NDVI| ≤ 0.05, ≥ 1 000 valid water pixels per year).
- **Phase 5 (gate):** report the weekly NULL rate per season and per cell-week by `status` **before**
  proposing any relaxation of the ≥ 2-observations rule.
- **Phase 9:** README must include ESA WorldCover 10 m 2021 v200 attribution (CC BY 4.0) and
  **"Weather data by Open-Meteo.com" (CC BY 4.0)** in the data-sources/attribution section.

## Phase 3 completion — 2026-11-10

- Silver build and previous acceptance measurements retained (293 dates; no rebuild).
- Config-only commit 4c45fad preceded the revised comparison. No tolerances,
  Option C pixels, DN>1 test validity, or Silver DN>0 rules changed.
- Test 7 read the existing report and cross-checked every orbit against product
  URIs and sensing times. Both eligible groups passed; neither was dropped.
- Commands actually run:
  - `.venv/bin/python -m pytest tests/test_silver.py::test_cross_era_stable_target_runs_and_is_within_tolerance -s -q`: **1 passed in 0.29s**.
  - `.venv/bin/python -m pytest -q --deselect=tests/test_silver.py::test_cross_era_stable_target_runs_and_is_within_tolerance`: **38 passed, 1 deselected in 0.30s**.
- The second command intentionally omitted the already-run test to honor the
  one-run instruction. Collectively all 39 tests passed; no skips or failures.
- Limitations: post-hoc criterion; local gitignored report needed for test 7;
  dark-target DN=1 floor; orbit-dependent bright-surface red; ~1.05 GB peak RSS.
- Interview explanation: we turn masked satellite pixels into consistent 100 m
  cell observations. A reference-target check initially failed because the
  observations mixed two orbit tracks. We kept the failures, froze a revised
  same-orbit rule before comparing again, and evaluated both tracks without
  changing tolerances. Passing this check supports conditional comparability,
  not proof that every surface or orbit is interchangeable.
- Completion commit message: `test: validate frozen within-orbit rule and document D-035`.
- Phase 4 has not started.

## Phase 4 preflight — gap measurement (2026-11-10)

Report: `data/silver/_reports/quality/gap_measurement_20261110T215222Z.json`.
Per-cell longest gaps with exact start/end and attribution are in sibling
`gap_longest_YEAR_20261110T215222Z.parquet` files. These reports are gitignored.

Usable means `ndvi IS NOT NULL` under the existing 50% valid-pixel gate.
Gap days are calendar days strictly between usable dates, plus leading/trailing
season days. For tied longest gaps, the earliest is retained. Every gap is split
into no-ledger-acquisition days and acquisition days with NULL cell NDVI.
No daily cell grid was materialized; DuckDB window functions ran one season at
a time, 4 threads, 2 GB memory limit. Ledger dates, not existing-file globs,
defined the expected input list; all 293 expected paths existed. This is a
presence preflight, not the planned complete critical output-validation check.

| Season | Ledger dates | p50 | p90 | p99 | Max | Cells >10 days | Share |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2019 | 59 | 7 | 9 | 14 | 69 | 2,862 | 1.196% |
| 2020 | 59 | 9 | 11 | 14 | 97 | 65,045 | 27.181% |
| 2021 | 60 | 6 | 7 | 14 | 72 | 4,077 | 1.704% |
| 2022 | 59 | 7 | 9 | 14 | 29 | 18,291 | 7.643% |
| 2023 | 56 | 9 | 12 | 16 | 21 | 30,376 | 12.693% |

Successful measurement command: `.venv/bin/python /tmp/measure_phase4_gaps.py`
(log `/tmp/phase4_gap_measurement.log`); total **5.61 s**. Assertions checked
that every split sums to the gap length, split counts are nonnegative, every
cell has a longest-gap record, and total no-acquisition cell-days equal
(season days − ledger dates) × kept cells. The temporary measurement script
is not a versioned CLI; the Phase 4 implementation is still pending.

2020 violating longest gaps average 9.497 days without acquisitions and 2.176
acquisition days with NULL NDVI. The most common is May 7–17 (11 missing days:
9 without acquisitions + 2 NULL acquisition dates), affecting 43,642 cells.
Stopped as required; no gap threshold/config or checks implemented.

## Phase 4 completion — 2026-11-11 (Cairo)

- Built Pandera/DuckDB checks, explicit Arrow schema, season gap attribution,
  contextual NULL alerts, atomic per-run reports and nonzero critical CLI exits.
- Diagnosis: May 7–17, 2020 = 11 missing calendar days (9 no acquisition + 2
  NULL acquisitions on May 8/16). Cohort: 43,642 cells, 35,573 cropland; geometric
  split 15,272 RTV-only / 15,073 overlap / 13,297 RUV-only. It is not an RTV-only
  footprint. Reconstructed valid counts match Silver. Long tails >30 days:
  16/19/24/0/0 cells across 2019–2023, all non-cropland RUV-only; SCL masking,
  mostly class 2, explains chosen longest episodes, not persistent DN=0.
- User-approved post-measurement gap decision logged as D-036; prior placeholder
  decision IDs moved to D-037/D-038 so the requested D-036 records the gap rule.
  The ~20% threshold-review stop gate is explicitly resolved.
- New quality tests: 19. Final full suite: `.venv/bin/python -m pytest -q` →
  **58 passed in 1.87s**.
- `.venv/bin/python -m delta_crops.quality.corruption_demo` → disposable full
  copy, NDVI 1.25 detected, **CLI exit 1**, source SHA-256 unchanged. Acceptance
  JSON retained under `data/silver/_reports/quality/`.
- `.venv/bin/python -m delta_crops.quality.cli` → **PASSED: 293 ledger dates,
  70,116,072 rows; 4 alert checks; 18.757 s** (scan 12.847 s, gaps 5.811 s).
  15 dual-tile NULL alerts; 12 single-tile dates context only; total NULL 13.0100%.
  Gap violations >=13 days: all cells 1.05/4.06/1.69/1.91/4.74%; cropland
  1.01/4.09/1.74/1.98/4.84%. No seasonal prevalence alert (>10%).
- `.venv/bin/python -m delta_crops.quality.dn_floor` → 14 fixed dates, **20.16 s**,
  replay matches Silver, no rebuild. Report retains both rules' NULL/valid counts,
  excluded pixels and NDVI changes. Recommendation: keep DN>0; local effects remain.
- Documentation: `docs/phase4-quality.md` includes the interview explanation;
  `docs/phase4-long-tail-cells.csv` lists all 60 >30-day episodes (59 cell-seasons).
- Feature commit: e004f3b. Suggested completion commit message:
  `docs: complete Phase 4 diagnosis, audit evidence and runbook`.
- Limitations: alert thresholds revised post-measurement; DN audit is sampled;
  SCL cause classification is observational; reports/raw data are local and
  gitignored; Phase 3 test 7 still needs its persisted real-data artifact.
- No Gold tables, orchestration, modeling or Silver rebuild. Phase 5 not started.

# Progress

## Status

- **Phase:** 6 — orchestration **CLOSED** (6a accepted earlier, 6b verified on the host on
  2026-11-11). **Season 2024 added and built on 2026-11-12** (see the 2024 bullet below).
  **Phase 7 — modeling CLOSED 2026-11-13**: S1–S5 run, all three campaigns fail the PR-AUC
  condition only; model card at [`docs/model-card.md`](docs/model-card.md).
- **Season 2024 (2026-11-12, run by Ahmed by hand, no flow, no Docker):** config years are now
  `[2019, 2020, 2021, 2022, 2023, 2024]` with `focus: 2023` (commit `701d7b7`). Ingest
  `305 downloaded, 46 skipped, 0 failed` in 4451.5 s; `verify --year 2024` 351/351 assets from
  117 scenes; Bronze 4.9 GB (36RTV 1.2 + 36RUV 3.7); ledger 2079 lines (1727 + 351 + 1
  `weather:2024`). Weather `silver/weather.parquet` 918 rows (153 × 6). Silver 2024
  `59 built, 0 failed in 83.8s`; **352 ledger dates** (293 + 59), 352 sidecars; 2019–2023 Silver
  files hash-identical. Quality **PASSED: 352 ledger dates, 84,235,008 rows; 4 alert checks** (the
  content of the 4 alerts was not inspected). Weekly gate PASSED, 2024 NULL share 14.78 % /
  14.93 % cropland. Orbits refreshed; orbit check on 2024 weekly = 0 rows. Gold weather/weekly/
  anomaly each `1 built, 5 rebuilt, 0 failed`; 4,786,080 rows per year for six years; **18** Gold
  sidecars. 2019–2023 `weekly_ndvi`/`weekly_weather` compared against `data/_regression/gold_before`
  with `EXCEPT` both ways: 0 differences for all ten table-years. **`ndvi_anomaly` 2019–2023 is
  expected to differ** (leave-one-out pool is now six years, not five) and was not compared.
  `gold_regression.py` was not re-run. Record: `docs/adding-season-2024.md`.
- **Test fix for the new year list:** `tests/test_config.py` now asserts the real contract
  (baseline starts at 2019, is contiguous and unique, `focus in years`) instead of pinning
  `[2019..2023]` (commit `85746e3`). Counts are unchanged: host full suite `150 passed,
  1 deselected`; quick loop `139 passed, 12 deselected`. No assertion was weakened.
- **6b measured on the host (Ahmed, rebuilt images with `HOST_UID=1000 HOST_GID=1000`):**
  container suite `133 passed, 6 skipped, 12 deselected` (139 total) with the 6 skips being the
  host-only Docker-file tests and their explicit reason;
  `tests/test_orchestration_integration.py` in the image `1 passed` (~8.5 s, four real CLIs,
  mocked network); host full suite `150 passed, 1 deselected`; quick loop `139 passed,
  12 deselected`.
- **6b memory (idle only):** orchestrator 104 MiB, server 234.5 MiB of 15.33 GiB by
  `docker stats --no-stream`. Never measured under load; no other RAM claim exists.
- **6b failure logging:** missing config inside the orchestrator container exits 1 with a
  `FileNotFoundError` naming the file, before any download. It is a raw traceback, not a friendly
  message — a polish item, not a blocker.
- **6b layout (final):** `server` = `prefect server start --host 0.0.0.0 --port 4200 --no-ui`
  with `./.prefect:/var/lib/prefect` (scheduler + SQLite); `orchestrator` =
  `python -m delta_crops.orchestration.serve` with `PREFECT_API_URL=http://server:4200/api`
  and `PREFECT_HOME=/tmp/prefect` (client scratch, never a shared SQLite);
  `depends_on: server: service_healthy`; `tests` profile mounts `tests/` and the real lake
  read-only and sets no `PREFECT_API_URL` (its harness starts its own server).
- **Why a real server (source, Prefect 3.9.2):** `get_client` starts an ephemeral
  `SubprocessASGIServer` only when `PREFECT_API_URL` is empty
  (`prefect/client/orchestration/__init__.py:245-258`), its port is random
  (`prefect/server/api/server.py:1123-1125`), and the scheduler is not registered for ephemeral
  servers (`prefect/server/services/scheduler.py:212-215` + `perpetual_services.py:78-92`), which
  `prefect/runner/runner.py:383-386` warns about. The first compose version therefore crash-looped
  and, had we only removed the variable, would have left a silently dead schedule.
- **Healthchecks:** API answers, and for the runner the deployment exists and is unpaused. They
  are **not** a heartbeat: a wedged runner with a registered deployment still reads healthy.
- **Not proven / open from 6b:** container concurrency (`global_limit=1` is configured and visible;
  `no_op` runs last ~0.3 s so the in-container attempt was inconclusive — only the local spike
  proves the ordering); a real-pipeline smoke inside the container (deliberately skipped, see
  caution below); `uv` image tag is minor-pinned (`ghcr.io/astral-sh/uv:0.9`), not digest-pinned.
- **Caution recorded:** never run `python -m delta_crops.orchestration` with the default config
  against an empty/unfamiliar `--data-dir`; it starts a real multi-year ingest. Use a tiny fixture
  config, the exit-code tests, or a nonexistent config for failure checks. One real example:
  manual run `copper-mussel` CRASHED after ~1.5 h (interrupted) after starting ~5.7 GB of ingest;
  `beneficial-silkworm` FAILED in 47 ms on the missing-config check. Both are manual-run side
  effects, not schedule failures; the scheduled deployment was healthy (3 future SCHEDULED runs,
  one manual `no_op` COMPLETED). The real ledger stayed at 1727 lines, git stayed clean, and
  `data/_logs/pipeline_runs.jsonl` held only the two `no_op` records.
- **Test-image rule:** rebuild `delta-crops:6b-test` after ANY `src/` change
  (`HOST_UID=1000 HOST_GID=1000 docker compose --profile test build tests`). A 4-hour-old image
  executed pre-fix code and reported 9 failures (5 missing repo files, 4 from old code); after the
  rebuild only the 5 repo-file tests failed, which produced the host-only skip policy (`dc171f9`).
  A green container suite proves the package/CLIs in the image, not the compose wiring.
- **Adopted on the real lake (2026-11-11):** Gold regression before adopting was 15/15 OK, 0 diffs
  (weekly_ndvi/ndvi_anomaly 4,786,080 rows per year, weekly_weather 20 rows per year, 44.93 s).
  A `/tmp/adopt_rehearsal` copy was adopted first; the real lake then had 0 sidecars and a
  1727-line ledger. Silver `all --adopt`: `0 built, 0 skipped, 0 rebuilt (input change),
  0 unfingerprinted, 293 adopted, 0 failed in 23.3s (peak RSS 183 MB)`, exit 0, sidecars
  59/59/60/59/56 by year. Gold `weather` → `weekly` → `anomaly`, no `--year`/`--force`: each
  `errors []`, `built 0`, `skipped 0`, `unfingerprinted 0`, `adopted 5`, `failed 0`; 15 sidecars.
  A SHA-256 over the sorted list of non-sidecar files was identical before and after for
  `data/silver` and `data/gold`; ledger still 1727 lines; git clean. The 23.3 s is warm-cache;
  there is no cold-disk timing. Procedure and scope: `docs/adopting-an-existing-lake.md`.
- **Open after Phase 6:** real Silver/Gold are adopted and 2024 is built, but the **scheduled flow
  has still never run end-to-end on real data** — the schedule would now pick 2025 once it is
  added to the config (2024 is complete: all 153 weather days present and the season end has
  passed); container concurrency unproven; healthcheck is not a heartbeat; `uv` image
  tag minor-pinned (`ghcr.io/astral-sh/uv:0.9`). Known CLI quirks (not blockers, not fixed):
  `gold.cli weather` ignores `--year` and **builds** a missing year instead of adopting it;
  `--summary-json` is written only by `gold.cli build`; Gold exits 0 when `failed > 0`;
  `--adopt --force` rebuilds; Gold fingerprints follow Silver mtime/size.

- **6a base (accepted):** four tasks calling the real CLI `main()` in-process
  (ingest → Silver → quality → Gold), only exit 3 retried (1/2/4 s), `PipelineError` otherwise,
  quality `retries=0`, every run appends `_logs/pipeline_runs.jsonl` in `try/finally`,
  `gold_verify`/`bronze_verify` fields, partial weather via `--allow-partial-weather-year`,
  real end-to-end integration test with only network mocked.
- **Tests then (historical, Phase 6a — do not read as current):** default
  `142 passed, 1 deselected`; quick loop `-m "not flow and not slow"` fast (<10 s); slow
  `-m slow` 1 test; Gold regression 15/15 identical (47.15 s). Current counts are in
  the status section above: host full suite `150 passed, 1 deselected`; quick loop `139 passed,
  12 deselected`.
- **Half-done / NOT DONE:** Phase 6b Docker verification is complete; the real Silver/Gold are
  adopted (2026-11-11) and season 2024 is built through Gold (2026-11-12), but the scheduled flow
  has never run end-to-end on real data. `gold_regression.py` was not re-run after 2024 and the
  `ndvi_anomaly` 2019–2023 files were not compared with the old snapshot. **Phase 7 is closed
  (2026-11-13); Phase 8+ not started.** S5 has no reliability table because predictions were not
  persisted — a documented gap, not a pending task.
- **Next:** nothing is pending inside Phase 7 — the model card, the measured results,
  the decisions D-047…D-051 and the stale status lines are all written and committed. Phase 8
  (dashboard) starts only on approval for Phase 8. No unverified claim is recorded as fact.

Legend: `TODO` · `IN PROGRESS` · `DONE` · `BLOCKED`

| Phase | Title | Status | Notes |
| --- | --- | --- | --- |
| 0 | Scope and setup | DONE | Committed on `main` (`c2c3ea8`) |
| 1 | Architecture design (no code) | DONE | Source, baseline, grid, schemas, BOA offset, risks documented; accepted |
| 2 | Ingestion (Bronze) | DONE | 1 722 assets, 25.81 GB, verified complete 2019–2023 |
| 3 | Bronze → Silver (cleaning) | DONE | 293/293 dates built; 39 tests passed; revised within-orbit test 7 passes both groups. Earlier failures and post-hoc qualification retained in D-035. |
| 4 | Data quality | DONE | All 293 ledger dates / 70,116,072 rows passed critical checks; 58 tests pass; corruption CLI exit 1; DN audit and D-036 onward documented. |
| 5 | Gold layer | DONE | Weekly gate passed (max 27.54%), anomaly leave-one-out, heatwave ≥40°C/≥3d, orbit cache + diagnostic. 7 new tests; 65 total pass. |
| 6 | Orchestration | DONE | 6a accepted; 6b verified on the host: container suite 133 passed / 6 skipped / 12 deselected, integration 1 passed, idle RAM 104 + 234.5 MiB. Real Silver/Gold adopted 2026-11-11 (293 + 15 sidecars, outputs byte-identical). Open: scheduled flow never run on real data, container concurrency, heartbeat, uv tag pin. |
| 7 | Modeling | DONE | S1 14,977,696 rows; B0–B3 (`8b0164b`, `770d885`, `98938ce`); S3 `ace7352` 0.8213705 vs bar 0.8222002; S4 `68215bd` 0.8191470 vs bar 0.8205095; S5 `2a30b52` 0.8007195 vs bar 0.8147217. **All three FAIL on PR-AUC only**; P@R.5 and Brier pass. Model card `docs/model-card.md`. |
| 8 | Dashboard | TODO | |
| 9 | Packaging and documentation | TODO | |

Phases 2–5 figures above describe the original 2019–2023 build. Season 2024 (2026-11-12) adds 351
Bronze assets and 59 Silver dates: 352 ledger dates and 84,235,008 quality rows in total. Per-year
measured detail is in the status section and `docs/adding-season-2024.md`.

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

## Phase 7 completion — 2026-11-13 (models)

Pre-registration: [`docs/phase7-preregistration.md`](docs/phase7-preregistration.md), frozen
2026-11-12 before any baseline or model was run. Full record:
[`docs/model-card.md`](docs/model-card.md). All numbers below are quoted from
`artifacts/phase7/*.json`; artifacts are gitignored and never committed.

**S1 — dataset and labels (`5651bfc`).** Target: mean **raw** NDVI anomaly over `t+1`/`t+2`
≤ **−0.08**, both weeks `status='ok'` (D-047). 14,977,696 usable rows, 2,933,974 positives
(19.59 %). Per season: 2019 2,519,921 / 16.18 %; 2020 2,728,392 / 18.41 %; 2021 2,681,558 /
22.73 %; 2022 2,589,333 / 21.07 %; 2023 1,431,931 / 15.21 %; 2024 3,026,561 / 21.51 % (test).
29.06 s, 3,266.5 MB.

**S2 — baselines B0–B3 (`8b0164b`, `770d885`, `98938ce`).** On S3 `cropland/pooled`, PR-AUC:
B0 0.2508243, B1 0.7589650, B2 0.2819829, **B3 0.7722002** (P@R.5 0.8145373, Brier 0.1050482).
B3 is the reference for every verdict. `770d885` added B3's missing intercept: ranking unchanged,
Brier 0.1929 → 0.1050, mean predicted probability 0.519 → calibrated (D-048). Onset baselines in
`98938ce`; 557.37 s + 320.55 s, 4,393.8 MB.

**S3 — temporal split, test 2024 (`fb54129` freeze, `ace7352` run).** Bar 0.8222002 /
0.8145373 / 0.1050482. `full` 0.8213705 / 0.8713148 / 0.0936884 → **FAIL**, misses PR-AUC by
**0.0008298**; `ndvi_only` 0.8235561 / 0.8740270 / 0.0924261 → passes all three. 1,513.12 s,
9,402.8 MB.

**S4 — leave-one-season-out, six folds (`675eb4c` freeze, `68215bd` run).** Bar 0.8205095 /
0.8187709 / 0.0947832, derived on the six-fold mean, not reused from S3 (D-049). `full`
0.8191470 / 0.8754861 / 0.0835801 → **FAIL**, misses by **0.0013625**; `ndvi_only` 0.8235976 /
0.8812944 / 0.0824871 → passes all three. B3 mean 0.7705095 (std 0.0358198). `loso-2024`
reproduces S3 exactly (pipeline-consistency check). 9,020.29 s, 9,616.7 MB.

**S5 — spatial holdout on 5 km blocks (`6fd4226` freeze, `2a30b52` run).** Partition frozen:
28 of 110 blocks held out, `default_rng(20260702)` (D-050); B3 refit on S5 training rows (D-049).
Test 596,174 rows / 144,091 positives. Bar 0.8147217 / 0.8070753 / 0.1036731. `full` 0.8007195 /
0.8436102 / 0.0960768 → **FAIL**, misses by **0.0140022**; `ndvi_only` 0.8042603 / 0.8470021 /
0.0942075 → **FAIL** by 0.0104614. Block bootstrap 1000/1000 usable draws: full PR-AUC
[0.7818991, 0.8211190], ndvi_only [0.7863661, 0.8239851]; the bar lies inside both intervals and
above both point estimates. Paired `full − B3` PR-AUC **+0.0360180** [+0.0276012, +0.0438191].
2,056.22 s, 8,615.9 MB.

**Onset headline (separate, not a verdict).** S5 `cropland/onset` 442,124 rows / 42,473 positives
(prevalence 0.0961): `full` 0.3929923 vs B3 0.3203791. S5 `all/onset`: 0.3839020 vs 0.2932640.
S3 onset: 0.4373937 vs 0.3205508. S4 onset mean: 0.4228700 vs 0.3084870.

**No weather effect is claimed.** `ndvi_only` beats `full` on all three campaigns
(0.8235561 > 0.8213705; 0.8235976 > 0.8191470; 0.8042603 > 0.8007195) and has the lower Brier
each time. Weather is one grid point (~120 independent weeks).

**Sanity controls passed in every campaign.** XGBoost on the three B3 features clears its frozen
floor `B3 − 0.02` in every campaign and every fold (S3 0.7962004 vs 0.7722002; S5 0.7828792 vs
0.7647217); permuted labels score at prevalence (S3 gap 0.0003802; S5 gap 0.0008192). No leakage
was detected.

**Documented gaps, not tasks.**
- S5 has **no reliability table**: predictions were not persisted (`prediction` and `reliab`
  occur 0 times in `xgb_spatial.json`, no prediction files under `artifacts/` or `data/`, and
  `run_spatial_fold` keeps arrays in memory). No re-run was performed, so S5 calibration is
  unmeasured. S3 and S4 have full 10-bin tables.
- S5 declared 28 held-out blocks, 27 were scorable (`80009` has no `cropland/pooled` 2024 row);
  78 of the declared 82 training blocks carried rows.
- The 600-round cap binds in `loso-2021`/`loso-2022` and nearly binds in S5 (593 and 599 of 600,
  `hit_cap: false`); left unchanged (D-051).
- 2024 has 145,406 rows (4.80 %) at `week_index = 2` and no training season has any, so those
  predictions are an extrapolation and account for the whole 4.711 % `temp_max_mean_c_lag2`
  test missingness.

**Tests.** `.venv/bin/python -m pytest -q` → **191 passed, 1 deselected in 25.68 s**. Phase 7
coverage lives in `tests/test_models.py`, `tests/test_baselines.py`, `tests/test_spatial.py`,
`tests/test_train.py`.

**Limitations.** All three campaigns miss the pre-registered PR-AUC bar; S5 is a single test year
(2024, the most extreme season); S5 held-out blocks still use their own 2019–2023 climatology, so
it is new locations with known history, not cold start; block bootstrap weights 27 unequal blocks
equally (253–34,075 rows each); 2023 lag missingness reaches 73.414 % (`ndvi_roll3`) when it is the
held-out season; harvest and irrigation are unobserved confounders. No Phase 8 dashboard work was
started.

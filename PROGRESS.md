# Progress

## Status

- **Phase:** 3 — Bronze → Silver **IN PROGRESS** (not DONE — test 7 failed)
- **Current step:** test 7 (cross-era stable target) ran and FAILED; full build complete; acceptance checks all passed except test 7
- **Done:** Phases 0–2 committed and verified. Phase 3 code: `transform/{grid,reflectance,bronze_reader,silver,cells,weather,stability,cli}.py`, `silver:` config block, 12 new tests (36 total including test 7). Grid measured 487 × 508 → 239 304 kept cells, 194 778 cropland. Cells and weather tables built. Full Silver build 293/293 dates, 0 failed, 445 s. All acceptance checks passed except test 7.
- **Half-done:** nothing (Phase 3 code complete; test 7 is the only failing item)
- **Blocked:** cross-era stable-target test (test 7) — Option B config frozen and committed (`8d80592`: class 80, erosion 3 px, window 1-7 July, min_valid_pixels 1000, pooling at_least_one_date, tolerances |delta red| <= 0.02, |delta NDVI| <= 0.05). Test now runs (no longer skips) but **FAILS**: delta median red = 0.0708 (> 0.02), delta median NDVI = 0.9167 (> 0.05). Phase 3 left IN PROGRESS pending analysis. Not assumed to be a processing bug — water reflectance legitimately varies with turbidity/algae.
- **Tests:** 36 total, 35 passing, 1 failing (test_cross_era_stable_target_runs_and_is_within_tolerance)
- **Next:** analyse test 7 failure cause with user; do NOT start Phase 4 until test 7 passes or is resolved.

Legend: `TODO` · `IN PROGRESS` · `DONE` · `BLOCKED`

| Phase | Title | Status | Notes |
| --- | --- | --- | --- |
| 0 | Scope and setup | DONE | Committed on `main` (`c2c3ea8`) |
| 1 | Architecture design (no code) | DONE | Source, baseline, grid, schemas, BOA offset, risks documented; accepted |
| 2 | Ingestion (Bronze) | DONE | 1 722 assets, 25.81 GB, verified complete 2019–2023 |
| 3 | Bronze → Silver (cleaning) | IN PROGRESS | 239 304 kept cells; NDVI + SCL mask + weather; full build 293/293; test 7 FAILED (delta red 0.0708 > 0.02, delta NDVI 0.9167 > 0.05) — left IN PROGRESS pending analysis |
| 4 | Data quality | TODO | |
| 5 | Gold layer | TODO | Leakage rule applies to model features |
| 6 | Orchestration | TODO | Prefect 3 |
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

- **Phase 3 (blocking):** reflectometry tests must include the per-era real-scene check **and** the
  cross-era water sanity check (fixed 1 km water box, 1–7 July 2019 vs 2023; |Δ red reflectance| ≤ 0.02,
  |Δ NDVI| ≤ 0.05, ≥ 1 000 valid water pixels per year).
- **Phase 5 (gate):** report the weekly NULL rate per season and per cell-week by `status` **before**
  proposing any relaxation of the ≥ 2-observations rule.
- **Phase 9:** README must include ESA WorldCover 10 m 2021 v200 attribution (CC BY 4.0) and
  **"Weather data by Open-Meteo.com" (CC BY 4.0)** in the data-sources/attribution section.

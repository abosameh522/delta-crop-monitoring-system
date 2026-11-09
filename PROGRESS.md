# Progress

## Status

- **Phase:** 2 — Ingestion (Bronze) — **code complete, awaiting review**
- **Current step:** Phase report recorded; Phase 3 pending approval
- **Done:** Phases 0–1 accepted and committed. Phase 2 modules, 17 offline tests, and two real smoke
  runs (2019 + 2023, 24 assets each) are complete. Idempotent re-runs verified (0 downloads, 24 skips).
- **Half-done:** full 5-season backfill is **not run** (by design) — commands are in the Phase 2
  section below for the user to run in tmux.
- **Tests:** 17 passing (last run 2026-11-10)
- **Next:** Phase 3 plan (Bronze → Silver) pending approval.

Legend: `TODO` · `IN PROGRESS` · `DONE` · `BLOCKED`

| Phase | Title | Status | Notes |
| --- | --- | --- | --- |
| 0 | Scope and setup | DONE | Committed on `main` (`c2c3ea8`) |
| 1 | Architecture design (no code) | DONE | Source, baseline, grid, schemas, BOA offset, risks documented; accepted |
| 2 | Ingestion (Bronze) | DONE | Earth Search + Open-Meteo; JSONL log; measured runs; awaiting review |
| 3 | Bronze → Silver (cleaning) | TODO | Must implement reflectance rule + Phase 3 offset tests |
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
- [x] 17 offline tests, including a simulated interruption and a torn-final-line recovery test.
- [x] GDAL/vsicurl settings documented in `PROJECT.md` §3.13 with measured request counts.
- [x] Open-Meteo terms verified; recorded as D-017 and added to the Phase 9 checklist.
- [x] Two-era smoke runs (2019 and 2023) with real numbers.

Smoke results (window 15–24 June, cloud < 40 %):

| | 2019 | 2023 |
| --- | --- | --- |
| Scenes / dates | 8 / 4 | 8 / 4 |
| Assets | 24 (495.1 MB) | 24 (490.6 MB) |
| Item ids | `…_1_L2A` | `…_0_L2A` |
| Baseline | 05.00 | 05.09 |
| BS assets (`red`/`nir`/`scl`) | identical names | identical names |
| `raster:bands` scale / offset | 0.0001 / −0.1 | 0.0001 / −0.1 |
| `boa_offset_applied` | true | true |
| Resolution | B04/B08 10 m, SCL 20 m | same |
| nodata | 0 in B04/B08 (mean 3.65 % incl. 13.5–15.7 % on 36RUV dates) | 0 in B04/B08 (mean 4.01 %, up to 16.43 %) |
| Wall time | 638 s sequential | 253 s with 4 workers |
| Re-run | 0 downloaded / 24 skipped in 3.0 s | 0 downloaded / 24 skipped in 2.1 s |

**Era differences are naming and baseline only.** Asset names, scale, offset flag, resolutions and
nodata are identical between 2019 and 2023, which confirms the reflectometry rule (scale only, never
the advertised −0.1) applies to both eras without a branch. The one real difference is the processing
baseline (`05.00` → `05.09`), which is exactly why dedupe keeps the highest baseline.

### Full backfill (run manually)

Expected work from STAC (checked 2026-11-10, cloud < 40 %):

| Year | Tile-scenes | Assets | Est. Bronze |
| --- | --- | --- | --- |
| 2019 | 113 | 339 | ~7.0 GB |
| 2020 | 116 | 348 | ~7.2 GB |
| 2021 | 119 | 357 | ~7.4 GB |
| 2022 | 117 | 351 | ~7.3 GB |
| 2023 | 109 | 327 | ~6.8 GB |
| **Total** | **574** | **1 722** | **~36 GB**, ~5 h at 4 workers |

```bash
cd ~/delta-crop-monitoring-system && source .venv/bin/activate

# One tmux session, one year per window. Re-run any year safely; it skips completed assets.
tmux new-session -d -s bronze
for y in 2019 2020 2021 2022 2023; do
  tmux new-window -t bronze -n "y$y" \
    "python -m delta_crops.ingestion.cli ingest --year $y 2>&1 | tee data/bronze/_logs/ingest_$y.log"
done
tmux attach -t bronze          # Ctrl-b n / p to move between years
```

Progress check (reads the JSONL, no network):

```bash
python -m delta_crops.ingestion.cli status          # per-year asset counts + GB + mean nodata
du -sh data/bronze/sentinel2                        # bytes actually on disk
```

Completeness check (re-queries STAC, compares expected vs logged+checksummed files; exit code 1 if
anything is missing). Run per year after that year's window finishes:

```bash
for y in 2019 2020 2021 2022 2023; do
  python -m delta_crops.ingestion.cli verify --year $y || echo "INCOMPLETE: $y"
done
python -m delta_crops.ingestion.cli compact         # refresh ingestion_log.parquet
```

Notes:
- Do not run two windows for the **same year** at once; the JSONL append is safe, but duplicate
  downloads waste bandwidth. Different years are independent.
- If a run is interrupted, just re-run it: the log is append-only and complete assets are skipped.
- Disk: ~36 GB total. Check with `df -h .` before starting 2022/2023.

## Upcoming phase requirements (agreed during Phase 1 review)

- **Phase 3 (blocking):** reflectometry tests must include the per-era real-scene check **and** the
  cross-era water sanity check (fixed 1 km water box, 1–7 July 2019 vs 2023; |Δ red reflectance| ≤ 0.02,
  |Δ NDVI| ≤ 0.05, ≥ 1 000 valid water pixels per year).
- **Phase 5 (gate):** report the weekly NULL rate per season and per cell-week by `status` **before**
  proposing any relaxation of the ≥ 2-observations rule.
- **Phase 9:** README must include ESA WorldCover 10 m 2021 v200 attribution (CC BY 4.0) and
  **"Weather data by Open-Meteo.com" (CC BY 4.0)** in the data-sources/attribution section.

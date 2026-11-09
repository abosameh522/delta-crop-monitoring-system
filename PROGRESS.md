# Progress

## Status

- **Phase:** 2 — Ingestion (planning)
- **Current step:** Phase 2 plan sent, pending approval
- **Done:** Phases 0–1 accepted and committed; architecture, schemas and risks are in `PROJECT.md`
- **Half-done:** none
- **Tests:** 8 passing (last run 2026-11-10, before any Phase 2 code)
- **Next:** add ingestion dependencies and implement Earth Search
  windowed ingestion + Open-Meteo fetch into Bronze, with metadata log and idempotent re-runs.

Legend: `TODO` · `IN PROGRESS` · `DONE` · `BLOCKED`

| Phase | Title | Status | Notes |
| --- | --- | --- | --- |
| 0 | Scope and setup | DONE | Committed on `main` (`c2c3ea8`) |
| 1 | Architecture design (no code) | DONE | Source, baseline, grid, schemas, BOA offset, risks documented; awaiting review |
| 2 | Ingestion (Bronze) | TODO | Earth Search primary; CDSE credentials optional |
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

## Upcoming phase requirements (agreed during Phase 1 review)

- **Phase 3 (blocking):** reflectometry tests must include the per-era real-scene check **and** the
  cross-era water sanity check (fixed 1 km water box, 1–7 July 2019 vs 2023; |Δ red reflectance| ≤ 0.02,
  |Δ NDVI| ≤ 0.05, ≥ 1 000 valid water pixels per year).
- **Phase 5 (gate):** report the weekly NULL rate per season and per cell-week by `status` **before**
  proposing any relaxation of the ≥ 2-observations rule.
- **Phase 9:** README must include ESA WorldCover 10 m 2021 v200 attribution (CC BY 4.0).

# Progress

Legend: `TODO` · `IN PROGRESS` · `DONE` · `BLOCKED`

| Phase | Title | Status | Notes |
| --- | --- | --- | --- |
| 0 | Scope and setup | DONE | AOI-A + summer 2023 locked; 5 tests pass; Phase 1 pending |
| 1 | Architecture design (no code) | TODO | |
| 2 | Ingestion (Bronze) | TODO | Needs Copernicus credentials in `.env` |
| 3 | Bronze → Silver (cleaning) | TODO | |
| 4 | Data quality | TODO | |
| 5 | Gold layer | TODO | |
| 6 | Orchestration | TODO | |
| 7 | Modeling | TODO | |
| 8 | Dashboard | TODO | |
| 9 | Packaging and documentation | TODO | |

## Phase 0 — Scope and setup

- [x] Propose two AOIs and let the user choose → **AOI-A**, south edge shifted to `30.75` (Tanta fully inside).
- [x] Propose one growing season → **S1**, 2023-05-01 → 2023-09-30.
- [x] Store bbox and season in `config/aoi.yaml`.
- [x] Create repo structure, `pyproject.toml`, `.gitignore`, `.env.example`, `PROJECT.md`, `PROGRESS.md`, `README.md`.
- [x] `pytest` runs with tests covering config validity and bbox size.

Notes:
- Bbox measures ~47.7 km × ~49.8 km (~2 375 km²), inside the 50×50 km target.
- Only `pyyaml` and `pytest` are installed; phase-specific libraries are added when used.
- Deferred to later phases: 3–5 years of history for anomaly baselines (Phase 5).

# Nile Delta Crop Monitoring — Project Context

## The question

**How does crop health in a selected Nile Delta area change over a growing season, and how do heat
waves and rainfall affect it?**

We answer it with real satellite and weather data, not synthetic numbers.

## Scope (locked in Phase 0)

| Item | Value |
| --- | --- |
| Area of interest | Central Nile Delta around Tanta / El-Mahalla El-Kubra (**AOI-A**) |
| Bounding box (WGS84) | west `30.86`, south `30.75`, east `31.36`, north `31.20` |
| Approx. size | ~47.7 km east–west × ~49.8 km north–south (~2 375 km²) |
| Study season | **S1**: 2023-05-01 → 2023-09-30 (summer rice / maize / cotton) |
| Imagery | Sentinel-2 L2A via Copernicus Data Space STAC (B04, B08, SCL) |
| Weather | Open-Meteo daily archive at the AOI centre |
| Storage | Local Parquet lake, queried with DuckDB |
| History | One season now; extend to 3–5 years in a later phase for anomaly baselines |

### In scope

- Reproducible Bronze → Silver → Gold pipeline.
- Automated data-quality checks that fail loudly.
- A 5-day orchestrated schedule.
- A simple, honestly evaluated vegetation-stress model.
- A dashboard with a map, charts, predictions and pipeline health.

### Out of scope (for now)

- Full-scene downloads, multi-satellite fusion, cloud infrastructure (S3/MinIO), real-time streaming.
- AOIs outside the chosen bounding box.

## Repository layout

```
.
├── PROJECT.md           # this file: question, scope, decisions log
├── PROGRESS.md          # phase checklist and notes
├── README.md            # short landing page until Phase 9
├── config/aoi.yaml      # AOI bounding box + study season
├── data/                # local lake: bronze | silver | gold (gitignored)
├── dags/                # orchestration (Phase 6)
├── docs/                # reports, model card, screenshots
├── src/delta_crops/
│   ├── ingestion/       # Phase 2: STAC + Open-Meteo → Bronze
│   ├── transform/       # Phase 3: Bronze → Silver, Phase 5: Gold
│   ├── quality/         # Phase 4: data-quality checks
│   ├── features/        # Phase 7: modelling features
│   ├── models/          # Phase 7: baseline + XGBoost
│   └── dashboard/       # Phase 8: Streamlit app
└── tests/               # pytest suite, one file per concern
```

`src/delta_crops/` is used as a single installed package (instead of loose `src/ingestion` etc.)
so imports are unambiguous and `pip install -e .` works from any working directory.

## Environment setup

Requires Python 3.10–3.13 (3.12 recommended; the geo stack used from Phase 2 has no 3.14 wheels yet).

```bash
uv venv --python 3.12 .venv
source .venv/bin/activate
uv pip install -e ".[dev]"
cp .env.example .env      # then fill in your Copernicus credentials
pytest -q
```

Plain `python -m venv` + `pip install -e ".[dev]"` works too.

## Decisions log

| ID | Date | Decision | Why |
| --- | --- | --- | --- |
| D-001 | 2026-11-10 | Project root is this existing repository, not a nested `delta-crops/` folder | Avoids a redundant directory level; git history and the existing workspace already point here |
| D-002 | 2026-11-10 | AOI-A, south edge moved to `30.75` to include all of Tanta | AOI must be ~50×50 km and contain the reference city; requested by the user |
| D-003 | 2026-11-10 | First season is summer 2023 (May–Sep) | Near-cloudless Sentinel-2 coverage and peak heat (Jul–Aug), which directly tests the heat-wave question |
| D-004 | 2026-11-10 | Python 3.10–3.13, 3.12 recommended | Raster/geo dependencies (rasterio, odc-stac) lack 3.14 wheels; upgrading mid-project is costly |
| D-005 | 2026-11-10 | Package under `src/delta_crops/` with phase sub-packages | Clean editable install and unambiguous imports as the codebase grows |
| D-006 | 2026-11-10 | Only `pyyaml` (runtime) and `pytest` (dev) declared in Phase 0 | No scope creep: each phase adds the libraries it actually uses |

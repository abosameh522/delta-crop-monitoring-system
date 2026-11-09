# Nile Delta Crop Monitoring

End-to-end satellite crop-monitoring pipeline for a ~50×50 km area of the central Nile Delta, Egypt:
Sentinel-2 NDVI + Open-Meteo weather → Bronze/Silver/Gold Parquet lake → DuckDB → stress model → dashboard.

**Current status:** Phase 1 complete (architecture documented, awaiting review). The full README arrives in Phase 9.

- Question, scope and decisions: [`PROJECT.md`](PROJECT.md)
- Phase checklist: [`PROGRESS.md`](PROGRESS.md)

## Quick start

```bash
uv venv --python 3.12 .venv
source .venv/bin/activate
uv pip install -e ".[dev]"
cp .env.example .env   # add your Copernicus Data Space credentials
pytest -q
```

## Layout

`config/` AOI and season · `src/delta_crops/` pipeline code · `tests/` pytest suite · `dags/` orchestration ·
`data/` local lake (gitignored) · `docs/` reports and model card.

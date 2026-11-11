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

Fast local loop (skips the slower Prefect flow tests):

```bash
pytest -q -m "not flow and not slow"
```

## Orchestration

Run the scheduled pipeline once (it processes only configured seasons that are
still in progress; today that is a logged no-op):

```bash
python -m delta_crops.orchestration
python -m delta_crops.orchestration --reprocess-year 2021   # explicit complete year
```

Operational notes:

- **All incomplete years are processed.** If two configured seasons are open at
  once, both are ingested and Silver-built in the same run.
- **Disk-full writes are not retried.** An `OSError` from a Bronze write is a
  failed asset (exit 4); free disk space and re-run the ingest.
- **Silver identity is metadata-based.** Fingerprints use partition names, file
  sizes and `mtime_ns`, not content hashes. Copying `data/silver` to another
  machine changes mtimes and makes Gold rebuild unless fingerprints are copied
  or adopted (`--adopt`); preserving mtimes avoids the rebuild.


## Layout

`config/` AOI and season · `src/delta_crops/` pipeline code · `tests/` pytest suite · `dags/` orchestration ·
`data/` local lake (gitignored) · `docs/` reports and model card.

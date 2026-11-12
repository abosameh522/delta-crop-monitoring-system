# Season 2024 — record

Status: **DONE on 2026-11-12.** Ingest, weather, Silver, quality, gate, orbit
cache and Gold were run by hand (no flow, no Docker). The container stack stayed
down throughout. `config/aoi.yaml` now lists six seasons, 2019–2024, and is
committed.

Pre-flight (2026-11-11): backups in `~/backups/pre2024` (540 MB, verified with
an empty `diff -rq` against the original `/tmp` copy), payload hash
`~/backups/pre2024/payload_2019_2023.sha256` with **2031** lines (1722 Bronze +
293 Silver NDVI + `silver/cells.parquet` + 15 Gold), ledger 1727 lines.

## Measured results

| Step | Command | Measured |
| --- | --- | --- |
| Ingest | `ingestion.cli ... ingest --year 2024` | `305 downloaded, 46 skipped, 0 failed` in 4451.5 s, exit 0 |
| Ingest verify | `ingestion.cli ... verify --year 2024` | `Expected 351 assets from 117 scenes; complete 351`, exit 0 |
| Bronze 2024 | — | 1.2 GB (tile 36RTV) + 3.7 GB (tile 36RUV) = **4.9 GB**; in line with the ~5 GB estimate |
| Ledger | — | **2079** lines = 1727 + 351 assets + 1 `weather:2024`; the first 1773 lines byte-identical before/after the resume; no file outside `year=2024` touched |
| Silver weather | `transform.cli ... weather` | `silver/weather.parquet` **918 rows** = 153 × 6 |
| Silver 2024 | `transform.cli ... year --year 2024` | `59 built, 0 skipped, 0 rebuilt (input change), 0 unfingerprinted, 0 adopted, 0 failed in 83.8s (peak RSS 1090 MB)` |
| Silver status | `transform.cli ... status --year 2024` | `352/352` files (352 ledger dates = 293 + 59); 352 sidecars; 2019–2023 files hash-identical; no `.tmp` |
| Quality | `quality.cli ...` | `PASSED: 352 ledger dates, 84,235,008 rows; 4 alert checks`, `failed_checks` empty, 21.5 s, `quality_20261112T020141Z_47c4a558.json` |
| Weekly gate | `gold.cli ... gate` | PASSED, `weekly_gate_20261112T020232Z_65b82678.json`; 2024 NULL share 14.78 % (all cells) / 14.93 % (cropland); 1,659,210 NULL cell-dates, mean masked 97.46 % on NULL |
| Orbits | `gold.cli ... orbits --refresh` | exit 0; old cache backed up to `/tmp/orbits_before_2024.parquet` |
| Orbit check | weekly 2024, `n_obs > 0 AND (orbit_mix IS NULL OR n_orbits = 0)` | **0 rows** |
| Gold weather | `gold.cli ... weather` | 1 built, 5 rebuilt (input change), 0 failed |
| Gold weekly | `gold.cli ... weekly` | 1 built, 5 rebuilt (input change), 0 failed |
| Gold anomaly | `gold.cli ... anomaly` | 1 built, 5 rebuilt (input change), 0 failed |
| Row counts | — | `weekly_ndvi` and `ndvi_anomaly` **4,786,080 rows per year** for all six years; Gold sidecars **18** |

Regression against `data/_regression/gold_before` (never written): `weekly_ndvi`
and `weekly_weather` for 2019–2023 compared with DuckDB `EXCEPT` in both
directions → **0 differences for all ten table-years**.

### What changed and what did not

The new `silver/weather.parquet` changes the weather SHA-256 and the config hash,
so all five older `weekly_weather` and `weekly_ndvi` files were **rewritten**,
with identical row values (measured above). `ndvi_anomaly` for 2019–2023 was
**not** compared and is **expected to differ** from the old snapshot: the
leave-one-year-out baseline pool now has six years instead of five. No claim is
made that the anomaly is unchanged.

## Incident log (lessons for next time)

1. The first attempt used the system `python` and failed with
   `ModuleNotFoundError: delta_crops`. Always call `.venv/bin/python`; no venv is
   active in the shell.
2. The second attempt was interrupted with Ctrl-C after 46 assets (exit 130).
   There were no `.tmp` files left and the ledger was valid JSON; the resumed run
   reported `305 downloaded, 46 skipped, 0 failed`. The design worked as
   intended: each asset is written to `.tmp` then `os.replace`d, and the ledger
   record is appended only after the final file exists.
3. Pasting several lines after `tmux new` or `newgrp` loses lines — send the
   command block one line at a time or use a script file.
4. Piping to `tee` makes Python buffer stdout, so a log can stay empty if the
   process is killed. Use `python -u` (or `PYTHONUNBUFFERED=1`) when logging a
   long download.

## Not done / not claimed

- The container stack stayed down and was not touched.
- The Prefect flow has never run on real data.
- `scripts/gold_regression.py` was not re-run after 2024 (the targeted
  `EXCEPT` comparison above covers `weekly_ndvi`/`weekly_weather` only).
- No Phase 7 work.

## Decisions kept from the plan

- **Rollback never deletes before a verified copy exists.** Restore into
  `data/gold.restore`, verify against `~/backups/pre2024/gold`, then swap; same
  pattern for `data/silver/_reports`.
- **`ingestion_log.parquet` is stale after the 2024 append and is not
  compacted.** Nothing in Silver/Quality/Gold reads it: `compact_log` is the only
  writer (`src/delta_crops/ingestion/bronze.py:360-370`, called from
  `ingestion/cli.py:153`), and only `tests/test_ingestion.py:192` reads it.
  Silver reads the JSONL (`transform/bronze_reader.py:51-57`), Quality calls
  `load_ledger` (`quality/checks.py:94`), Gold's gate calls it
  (`gold/gate.py:34`).
- **Orbit check after weekly** (above), because `weekly_fingerprint` has no orbit
  hash (`gold/fingerprint.py:133-141`); a late `orbits --refresh` would not
  trigger a rebuild.
- **Stop criteria were fixed before seeing the data**: any critical quality
  check > 0; 2024 cropland 13-day gap share > 10 %; 2024 masked rows > 23 % of
  cell-dates; fewer than 50 2024 acquisition dates or a missing 2024 partition;
  weekly gate > 30 %; 2024 weather days ≠ 153 or any null weather value; any
  2019–2023 `weekly_ndvi`/`weekly_weather` difference against the snapshot
  (then no anomaly). All passed.

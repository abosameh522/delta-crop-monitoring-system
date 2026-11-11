# Adding season 2024 — plan (accepted with changes)

Status: **pre-flight done (2026-11-11); config, ingest and the rest not started.**
The config edit, the ingest (Ahmed, tmux) and every downstream step follow
`go 2024 config`.

## Pre-flight result (read-only, 2026-11-11)

- Free space: `412G` of `475G` (63 G used, 14 %) — enough for the ~0.6 GB copies
  and the 2024 download.
- Backups in `/tmp/pre2024` (540 MB total; `gold` is 522 MB):
  `ingestion_log.jsonl`, `ingestion_log.parquet`, `silver_weather.parquet`,
  `orbits.parquet`, `silver_reports/`, `gold/`.
- Ledger: 1727 lines; `sha256(ingestion_log.jsonl)` =
  `f7d46103beec582a1c61eeeea6a94f4862a09444e1a308dc2f1a2963bfe173b4`, identical
  for the copy.
- `diff -rq data/gold /tmp/pre2024/gold` → empty (exit 0).
- Payload hash file `/tmp/pre2024/payload_2019_2023.sha256`: **2031 lines**
  (1722 Bronze + 293 Silver NDVI + `silver/cells.parquet` + 15 Gold), 0 sidecars,
  0 paths containing `year=2024`. The earlier "expected 309" was an arithmetic
  mistake (it counted only Silver + Gold); 2031 is the correct total.
- 23.3 s to hash ~26.9 GB warm.
- `data/_regression/` was not touched.

## Changes to the earlier plan

1. **Rollback never deletes before a verified copy exists.** Restore Gold and
   Silver reports into a side directory, verify content, then swap.
2. **`ingestion_log.parquet` goes stale after the 2024 append and is not
   compacted.** Nothing in Silver/Quality/Gold reads it: `compact_log` is the
   only writer (`src/delta_crops/ingestion/bronze.py:360-370`, called from
   `ingestion/cli.py:153`) and only `tests/test_ingestion.py:192` reads it.
   Silver reads the JSONL (`transform/bronze_reader.py:51-57`), Quality calls
   `load_ledger` (`quality/checks.py:94`), Gold's gate calls it
   (`gold/gate.py:34`), ingestion `status`/`verify` read the JSONL
   (`ingestion/cli.py:163-174, 189`). Stale Parquet is therefore inert; do not
   run `compact` during this step.
3. **Explicit post-weekly orbit check for 2024**, because `weekly_fingerprint`
   has no orbit hash (`gold/fingerprint.py:133-141`) and a late
   `orbits --refresh` would not trigger a rebuild.
4. **One more stop criterion:** if `weekly_ndvi` or `weekly_weather` for
   2019–2023 differs at all from `data/_regression/gold_before` after the
   rebuild, stop and do not run anomaly. The stop criteria are otherwise exactly
   as agreed.

## Commands

### 1. Config

`config/aoi.yaml:19` → `years: [2019, 2020, 2021, 2022, 2023, 2024]`. `focus`
stays 2023 unless the dashboard default should move to 2024.

### 2. Ingest (Ahmed, tmux)

```bash
python -m delta_crops.ingestion.cli --config config/aoi.yaml --data-dir data ingest --year 2024
python -m delta_crops.ingestion.cli --config config/aoi.yaml --data-dir data verify --year 2024
```

Year filter: `ingestion/cli.py:40-41` → `seasons.bounds(2024)` =
2024-05-01..2024-09-30; STAC body uses that window
(`ingestion/earth_search.py:113-116`); files land under
`bronze/.../year={scene.day.year}` (`ingestion/bronze.py:124-131`); weather only
for years in the window (`ingestion/cli.py:34-35, 104-105`). Size ~5.2 GB is the
five-year average, not a 2024 measurement.

### 3. Silver, quality, Gold

```bash
python -m delta_crops.transform.cli --config config/aoi.yaml --data-dir data weather
python -m delta_crops.transform.cli --config config/aoi.yaml --data-dir data year --year 2024
python -m delta_crops.transform.cli --config config/aoi.yaml --data-dir data status --year 2024
python -m delta_crops.quality.cli    --config config/aoi.yaml --data-dir data --summary-json /tmp/quality_2024.json
python -m delta_crops.gold.cli       --config config/aoi.yaml --data-dir data gate
python -m delta_crops.gold.cli       --config config/aoi.yaml --data-dir data orbits --refresh
python -m delta_crops.gold.cli       --config config/aoi.yaml --data-dir data weather
python -m delta_crops.gold.cli       --config config/aoi.yaml --data-dir data weekly
python -m delta_crops.gold.cli       --config config/aoi.yaml --data-dir data anomaly
```

`orbits --refresh` must run **before** weekly (it rewrites the whole cache with
all configured years).

### 4. Post-weekly orbit check (expect 0)

```sql
SELECT count(*) AS violations
FROM read_parquet('data/gold/weekly_ndvi/year=2024/weekly_ndvi.parquet')
WHERE n_obs > 0 AND (orbit_mix IS NULL OR n_orbits = 0);
```

Any non-zero result means the orbit cache lacked 2024 dates; rerun
`orbits --refresh`, delete the 2024 weekly output and rebuild it, because the
fingerprint will not detect the orbit change.

### 5. Stop criteria (fixed before seeing data)

Any critical Quality check > 0; 2024 cropland 13-day gap share > 10 %; 2024
masked rows > 23 % of cell-dates; fewer than 50 2024 acquisition dates or a
missing 2024 Silver partition; weekly gate NULL share > 30 %; 2024 weather days
≠ 153 or any null weather value; **any difference in 2019–2023
`weekly_ndvi`/`weekly_weather` against `data/_regression/gold_before`** (then no
anomaly).

### 6. Rollback (side directory, verify, then swap)

```bash
mkdir -p /tmp/post2024

# Gold: build and verify a restore copy first — never delete the live one first
rm -rf data/gold.restore
cp -a /tmp/pre2024/gold data/gold.restore
diff -rq data/gold.restore /tmp/pre2024/gold                      # empty
(cd data/gold.restore && find . -type f -print0 | sort -z | xargs -0 sha256sum) \
  > /tmp/post2024/gold_restore.sha256
# same sha256 list restricted to gold/ from the pre-flight payload file:
grep ' gold/' /tmp/pre2024/payload_2019_2023.sha256 | sed 's# gold/# ./#' \
  > /tmp/post2024/gold_expected.sha256
diff /tmp/post2024/gold_expected.sha256 /tmp/post2024/gold_restore.sha256   # empty
mv data/gold data/gold.bad.2024
mv data/gold.restore data/gold
rm -rf data/gold.bad.2024

# Silver reports: same rule
rm -rf data/silver/_reports.restore
cp -a /tmp/pre2024/silver_reports data/silver/_reports.restore
diff -rq data/silver/_reports.restore /tmp/pre2024/silver_reports # empty
mv data/silver/_reports data/silver/_reports.bad.2024
mv data/silver/_reports.restore data/silver/_reports
rm -rf data/silver/_reports.bad.2024

# the rest of the rollback (Bronze 2024, ledger, weather, orbits, config)
```

Then re-run the payload hash and diff it against
`/tmp/pre2024/payload_2019_2023.sha256` (empty), and confirm the ledger is back
to 1727 lines. `data/_regression/` is never written by any of this.

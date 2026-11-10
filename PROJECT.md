# Nile Delta Crop Monitoring — Project Context

## 1. The question

**How does crop health in a selected Nile Delta area change over a growing season, and how do heat
waves and rainfall affect it?**

We answer it with real satellite and weather data, not synthetic numbers.

## 2. Scope (locked in Phase 0)

| Item | Value |
| --- | --- |
| Area of interest | Central Nile Delta around Tanta / El-Mahalla El-Kubra (**AOI-A**) |
| Bounding box (WGS84) | west `30.86`, south `30.75`, east `31.36`, north `31.20` |
| Approx. size | ~47.7 km east–west × ~49.8 km north–south (**2 375 km²**) |
| Study seasons | **2019–2023**, same window **1 May – 30 Sep** each year |
| Focus season | **2023** (2019–2022 + leave-one-out 2023 form the baseline) |
| Imagery | Sentinel-2 L2A, bands B04 / B08 / SCL, windowed COG reads |
| Weather | Open-Meteo daily archive at the AOI centre |
| Storage | Local Parquet lake, queried with DuckDB |
| Grid | 100 m cells in EPSG:32636 (UTM 36N), ~238 000 cells |

### In scope

- Reproducible Bronze → Silver → Gold pipeline.
- Automated data-quality checks that fail loudly.
- A 5-day orchestrated schedule.
- A simple, honestly evaluated vegetation-stress model.
- A dashboard with a map, charts, predictions and pipeline health.

### Out of scope (for now)

- Full-scene downloads, multi-satellite fusion, cloud infrastructure (S3/MinIO), real-time streaming.
- Field-level segmentation; 100 m cells are neighbourhood indicators, not individual fields.
- AOIs outside the chosen bounding box.

---

## 3. Architecture

### 3.1 Data flow

```mermaid
flowchart LR
    subgraph Sources
        ES[Earth Search STAC<br/>Sentinel-2 L2A COGs]
        OM[Open-Meteo<br/>daily archive]
        WC[ESA WorldCover 2021<br/>10 m land cover]
    end

    ES -->|"windowed B04/B08/SCL reads"| B[(Bronze<br/>raw COGs + ingestion log)]
    OM -->|"JSON"| B
    WC -->|"one-time COG window"| S2

    B -->|"clip, scale to reflectance,<br/>NDVI, SCL mask, 100 m aggregate"| S[(Silver<br/>cells · ndvi · weather)]
    S2[(Silver cells<br/>is_cropland)] --> S
    S -->|"weekly aggregation,<br/>anomaly, weather join"| G[(Gold<br/>weekly_ndvi · anomaly · weather)]

    S --> Q[Quality checks<br/>Pandera]
    G --> D[Dashboard<br/>Streamlit + Folium]
    G --> M[Model<br/>baseline + XGBoost]
    M --> D
```

`ingest → bronze → silver → quality → gold` is one Prefect flow (Phase 6), scheduled every 5 days.
Bronze is append-only and idempotent; Silver and Gold are rebuildable from Bronze.

### 3.2 Data sources — verified, not assumed

Both candidates were queried on 2026-11-10 with the real AOI and window.

| | **Earth Search v1** (recommended) | **Copernicus Data Space (CDSE)** |
| --- | --- | --- |
| STAC endpoint | `https://earth-search.aws.element84.com/v1` | `https://catalogue.dataspace.copernicus.eu/stac` |
| Search auth | none | none |
| Download auth | **none** — public HTTPS COGs | **account required**: OAuth token for OData, or S3 access/secret keys for `s3://eodata` |
| Asset format | Cloud-Optimized GeoTIFF (deflate, overviews) | **JPEG2000** in the SAFE product; no COG asset exists for `sentinel-2-l2a` (checked all 428 collections) |
| Windowed reads of B04/B08/SCL | **yes** — HTTP range request returned `206` | not practical: JP2 + no COG → a whole product zip (~0.8–1.2 GB) per date |
| L2A coverage, May–Sep 2019–2023 | 47–56 usable dates/season | 47–53 usable dates/season |
| Terms | free, full and open Sentinel data via AWS Registry of Open Data; free-to-use API with **no SLA** | free and open Copernicus data; registered account + terms of use |
| Tiles over AOI | `36RTV`, `36RUV` | `36RTV`, `36RUV` |

**Recommendation: Earth Search as primary.** It is the only one of the two that supports our actual
access pattern — reading ~23 MB of three bands instead of ~1 GB per date. CDSE is documented as the
fallback (same Sentinel-2 products, authoritative JP2), and its credentials stay optional in `.env`.

Trade-off accepted: we depend on a third-party public mirror without an SLA. Mitigation: retries with
exponential backoff, checksums in the ingestion log, and a CDSE code path behind the same interface.

### 3.3 Reflectance normalization — the docs and the data disagree

Sentinel-2 L2A processing baseline **04.00** (25 Jan 2022) introduced a **+1000 DN BOA offset**. A naive
implementation applies the STAC `raster:bands.offset = -0.1` for 2022+ scenes. **That is wrong for Earth
Search's `sentinel-2-l2a` collection**, and the real pixels prove it:

| Item | Baseline | `earthsearch:boa_offset_applied` | `raster:bands` offset | Valid DN < 1000 | Water median DN | NDVI > 1 if offset applied |
| --- | --- | --- | --- | --- | --- | --- |
| `S2A_36RTV_20190507_2_L2A` | 05.00 | `true` | −0.1 | 79.5 % | 768 / 736 (red/nir) | 90.5 % of vegetation pixels |
| `S2B_36RTV_20220715_0_L2A` | 04.00 | `true` | −0.1 | 69 % red, 82 % nir | 862 / 771 | — |
| `S2A_36RTV_20230715_0_L2A` | 05.09 | `true` | −0.1 | 76.1 % | 892 / 869 | 94.1 % of vegetation pixels |

A truly offset-baked product would have almost every valid DN ≥ 1000. Applying the advertised −0.1
pushes water reflectance negative and gives NDVI > 1 for ~90 % of cropland pixels — physically
impossible. This is a known, open metadata defect in this legacy collection
([Element84/earth-search#71](https://github.com/Element84/earth-search/issues/71)); the pixels are
already harmonized while `raster:bands` still advertises the offset.

**Normalization rule (all years, all tiles):**

```
reflectance = DN × 0.0001          # scale only; never subtract 0.1 for Earth Search COGs
ndvi        = (nir − red) / (nir + red)   # red, nir already in reflectance
```

- Do **not** use `raster:bands.offset` or `boa_offset_applied` from this collection.
- Do **not** clip negatives to 0 before NDVI; use the SCL mask instead.
- When the same acquisition has several processing baselines, keep the **highest baseline**. Measured:
  the 2019 smoke scenes (baseline **05.00**) and 2023 smoke scenes (**05.09**) are both **10 m** in
  B04/B08 with `scale 0.0001` / `offset −0.1` and `boa_offset_applied = true`. A 2019-05-07 tile has
  three items (02.12, 02.12, 05.00), so duplicates are real and a deterministic winner is required.
  **Correction (2026-11-10):** an earlier note claimed the 02.12 item was 5 490 × 5 490 at 20 m; that
  is **not reproducible** — re-querying Earth Search shows `S2A_36RTV_20190507_1_L2A` (02.12) with
  `proj:shape [10980, 10980]` and `spatial_resolution 10`. The claim is removed; the dedupe rule now
  rests only on "newest baseline wins", not on an assumed resolution difference.
- If we ever switch to CDSE JP2, the opposite applies: baseline ≥ 04.00 there **does** contain the
  +1000 offset, so subtract 1 000 DN.

**Phase 3 test requirements (added to the acceptance criteria):**

1. Unit: reflectance conversion on synthetic DN returns `DN / 10 000`, is never negative for `DN > 0`,
   and NDVI stays in `[-1, 1]`.
2. Integration: for one real item per baseline era (2019, 2021, 2022, 2023), assert ≥ 50 % of valid
   DN < 1000 (harmonized evidence) and ≥ 99.9 % of NDVI in `[-1, 1]`.
3. Dedupe: when two baselines exist for one `(tile, date)`, the highest baseline is selected and its
   asset is 10 m.
4. Cross-era sanity check: a **fixed 1 km reference box of open water** inside the AOI (chosen in
   Phase 3 from WorldCover class 80 pixels that stay water in every season) is sampled in the same
   calendar week (1–7 July) of **2019 and 2023**. Require ≥ 1 000 valid water pixels in each year and:
   `|Δ median red reflectance| ≤ 0.02` and `|Δ median NDVI| ≤ 0.05`. A failure means the radiometry is
   not comparable across eras and blocks Phase 3.

Earth Search's newer `sentinel-2-c1-l2a` collection has consistent metadata but **zero scenes for
2022 over this AOI** (verified), so it cannot replace the legacy collection for our 5-year baseline.

### 3.4 Storage layout and partitioning

```
data/bronze/
  sentinel2/tile=36RUV/year=2019/date=2019-05-04/B04.tif   # windowed COG, reflectance-ready DN
  sentinel2/tile=36RUV/year=2019/date=2019-05-04/B08.tif
  sentinel2/tile=36RUV/year=2019/date=2019-05-04/SCL.tif
  weather/year=2019/open_meteo_daily.json
  _logs/ingestion_log.jsonl        # append-only, one record per completed asset
  _logs/ingestion_log.parquet      # compacted view (rebuildable from the JSONL)
data/silver/
  cells.parquet
  ndvi/year=2023/month=07/*.parquet
  weather/year=2023/weather.parquet
data/gold/
  weekly_ndvi/year=2023/*.parquet
  ndvi_anomaly/year=2023/*.parquet
  weekly_weather/year=2023/weekly_weather.parquet
```

- Bronze is raw and append-only. The log is a **JSONL file with one fsynced record per completed
  `(item, band)`**, written *after* the COG is atomically renamed into place; a crash can therefore lose
  at most the asset being written, never the log. `ingestion_log.parquet` is produced by a separate
  `compact` step, so a torn line or an interrupted run can never corrupt it. A re-run reads the JSONL
  (last record per `asset_key` wins), verifies size + SHA-256, and skips complete assets.
- Silver/Gold are **overwritten** per `(year, month)` / `(year)` for idempotency — rerunning a date
  replaces its rows instead of appending duplicates.
- Silver partitions follow the requirement (year/month). Gold is year-partitioned; weekly tables are
  small and date-filtered.

### 3.5 Grid design

- **100 m cells, EPSG:32636**, origin snapped to the AOI's south-west corner in UTM. 478 × 498 ≈
  **238 000 cells**.
- **Why 238k and not 250k:** the earlier 500 × 500 = 250 000 figure assumed a full 50.0 × 50.0 km
  square. The approved AOI is **47.72 km east–west × 49.76 km north–south**, and the grid is clipped
  to that rectangle after reprojection to UTM 36N: `ceil(47.72 / 0.1) = 478` columns ×
  `ceil(49.76 / 0.1) = 498` rows = **238 044 cells**. The UTM envelope differs from the WGS84 bbox by
  under 100 m per edge; the origin is snapped outward and partial edge cells are **retained**, not
  dropped, so no cropland on the AOI boundary is lost. Every row-count estimate on this page uses
  238 000 cells, never 250 000.
- NDVI is computed at native **10 m**, then mean-aggregated to 100 m; the SCL mask (native 20 m) is
  upsampled to 10 m for masking.
- Why not 10 m/20 m: 20 m → 6.25 M cells per date, ~1.5 B rows over 5 years — too heavy for a laptop.
  Why not 250 m/1 km: a 250 m cell is 6.25 ha and a 1 km cell 100 ha, while typical Delta holdings are
  0.1–1 ha; both smear field-scale stress into noise.
- **Cost accepted:** a 100 m cell (~1 ha) still mixes several small fields. We report a neighbourhood
  health signal, not field-level diagnosis; this is stated again in the dashboard and README.

Row estimates (≈293 acquisition dates across 5 seasons at the 40 % cloud filter, each date normally
covering both tiles):

| Layer | Rows | Est. size |
| --- | --- | --- |
| Bronze | 574 tile-scenes = 1 722 assets | **25.8 GB** (measured on the full backfill) |
| Silver `ndvi` | 238 k × 293 ≈ **70 M** | ~2–3 GB Parquet |
| Silver `cells` | 238 k | < 20 MB |
| Silver `weather` | 765 days (5 seasons × 153) | < 1 MB |
| Gold weekly (all cells) | 238 k × 110 weeks ≈ **26 M** | ~1–2 GB |
| Gold weekly (cropland only) | ~193 k × 110 ≈ **21 M** | ~0.8–1.5 GB |

**Measured, not estimated** (real smoke runs on 2026-11-10, window 15–24 June):

| Run | Assets | Size | Wall time | Throughput |
| --- | --- | --- | --- | --- |
| 2019, 4 workers (final format) | 24 | 365.0 MB | 254 s | 1.4 MB/s |
| 2023, 4 workers (final format) | 24 | 361.5 MB | 313 s | 1.2 MB/s |
| 2019, sequential (pre-compression) | 24 | 495.1 MB | 638 s | 0.78 MB/s |
| 2019 re-run (all skipped) | 24 | 0 MB | 3.0 s | size + SHA-256 only |

Per **tile-scene** (one tile, one date, three bands), measured over the complete 574-scene backfill:
**36RTV ≈ 21.8 MB**, **36RUV ≈ 67.8 MB** — 36RUV is 3.3× larger because its AOI window is
5 070 × 4 860 px against 5 070 × 1 463 px for 36RTV. Per **acquisition date** (both tiles):
**88.1 MB mean over 293 real dates**. Correction: an earlier draft quoted 44 / 137 MB per date — those
were totals for the 8-date smoke sample, not per-scene means.

**Full backfill, verified 2026-11-10:** 1 722 assets = 574 tile-scenes × 3 bands, **25.81 GB** on disk
(2019 5.07, 2020 5.25, 2021 5.36, 2022 5.27, 2023 4.86 GB), 293 acquisition dates. Effective rate
from the smoke runs (24 assets, 254–313 s) puts the whole backfill at **~5–6 h** with 4 workers,
~13 h sequentially. Phase 1's 46 MB/date estimate was 2× too low; the 36 GB figure from the first
smoke run was 28 % too high because those COGs still carried overviews. Both corrections are measured.

The machine has **443 GB free** (see the Phase 1 report), so disk is not a constraint; time is the
thing to plan around. Concurrency is network-bound: `--workers 4` measured 2.5× faster than
sequential, and back-to-back runs varied 254–313 s for the same 24 assets, so plan with the slower
figure. Raising `--workers` further gains little and risks Earth Search throttling.

### 3.6 Cropland mask

Source: **ESA WorldCover 10 m 2021 v200**, class `40` (cropland), tile `N30E030`, CC BY 4.0, served as
a COG from `esa-worldcover.s3.eu-central-1.amazonaws.com` (verified `200 OK`, 47.4 MB).

Measured over the AOI:

- **78.5 %** of 10 m pixels are cropland; built-up 15.0 %, wetland 1.9 %, trees 2.6 %, water 0.6 %.
- At 100 m, **81.0 %** of cells have ≥ 50 % cropland pixels → **~193 000 cropland cells**, ~45 000
  non-cropland.
- A cell is `is_cropland = true` when `cropland_pct ≥ 50`; `cropland_pct` and `worldcover_class` are
  kept so the threshold can be changed without re-reading the COG.
- Filtering to cropland removes ~19 % of cells, mostly urban fringes and canals, and prevents built-up
  pixels from diluting NDVI averages. Gold aggregates use cropland cells only; Silver keeps all cells
  with the flag so the effect is auditable.

Limitations: WorldCover is a **2021 static snapshot**, so land-use change after 2021 and mixed 100 m
cells introduce classification error. `is_cropland` is nullable when the mask has no coverage.
Attribution requirement: the README (Phase 9) must credit **ESA WorldCover 10 m 2021 v200, CC BY 4.0**.

### 3.7 Cloud filter and weekly sampling

Scene-level `eo:cloud_cover` is tile-wide, not AOI-wide, so a "cloudy" scene can still be clear over
Tanta. Verified unique acquisition dates per season:

| Season | cloud < 20 % | cloud < 40 % | Gain |
| --- | --- | --- | --- |
| 2019 | 55 | 59 | +4 |
| 2020 | 55 | 59 | +4 |
| 2021 | 56 | 60 | +4 |
| 2022 | 51 | 59 | +8 |
| 2023 | 47 | 56 | +9 |

**Recommendation: 40 % scene filter.** In 2023 it lifts weeks with ≥ 2 usable acquisitions from 17 to
19 of 22, and 2022 from 17 to 20; per-pixel SCL masking plus the masked-percentage checks remove the
cloudy pixels that the looser filter lets in. The 20 % filter is kept as a config option.

**Weekly rule (no silent fills):**

- Week = ISO week, Monday–Sunday. A `(cell, week)` value is produced only if the cell has **≥ 2
  distinct acquisition dates with at least one unmasked 10 m pixel** in that week, and the weekly mean
  `masked_pct ≤ 50 %`.
- Otherwise the row is still written with `ndvi_mean = NULL`, `n_obs` set, and
  `status = 'insufficient_observations'` or `'too_cloudy'`. Nothing is interpolated.
- Anomaly requires a non-null current-week value **and** a baseline mean from **≥ 3 baseline years**;
  otherwise `anomaly = NULL` with `status = 'no_baseline'`.
- Weather is always complete (Open-Meteo reanalysis), so missing weather is a hard error, not a null.
- **Phase 5 gate:** report the NULL rate per season and per cell-week (after the per-cell SCL mask),
  with counts by `status`, and show it to the user **before** changing the minimum-observations rule.

### 3.8 Schemas

All timestamps are UTC. `date` is the Sentinel-2 acquisition date, not the ingestion date.

#### Bronze — `ingestion_log.parquet` (PK: `asset_key`)

| Column | Type | Notes |
| --- | --- | --- |
| `asset_key` | string | `{item_id}:{band}` for imagery, `weather:{year}` for weather |
| `ingested_at_utc` | timestamp[us, UTC] | run timestamp |
| `source` | string | `earth-search` \| `open-meteo` |
| `collection` | string | `sentinel-2-l2a` \| `open-meteo-archive` |
| `item_id` | string, null | STAC item id |
| `tile` | string, null | `36RTV` / `36RUV` |
| `acquired_at` | timestamp[us, UTC], null | scene datetime |
| `band` | string, null | `B04` \| `B08` \| `SCL` |
| `path` | string | local relative path |
| `bytes` | int64 | file size |
| `sha256` | string, null | checksum when computed |
| `cloud_cover` | float32, null | `eo:cloud_cover` |
| `processing_baseline` | string, null | e.g. `05.09` |
| `boa_offset_applied` | boolean, null | recorded for audit only — **not used** for conversion |
| `stac_href` | string, null | source asset URL |
| `nodata_pct` | float32, null | % of nodata pixels *inside the AOI window* (0 for valid scenes) |
| `resolution_m` | float32, null | native pixel size of the asset (10 m B04/B08, 20 m SCL) |
| `duration_s` | float32, null | read + write time for that asset (inflated under concurrency) |
| `http_requests` | int32, null | filled for Open-Meteo (1); imagery left null and measured separately |

The JSONL record is written after the COG is verified on disk; `compaction` deduplicates by
`asset_key` (latest record wins) into the Parquet table above.

Raw Bronze files are COGs per `(tile, year, date, band)`; no Parquet.

#### Silver — `cells.parquet` (PK: `cell_id`)

| Column | Type | Notes |
| --- | --- | --- |
| `cell_id` | int32 | `cell_row * 1000 + cell_col`; stable within the AOI |
| `cell_row` | uint16 | 0-based row in the 100 m grid |
| `cell_col` | uint16 | 0-based column |
| `centroid_lon` | float64 | WGS84 centroid |
| `centroid_lat` | float64 | WGS84 centroid |
| `cropland_pct` | float32, null | % of 10 m cropland pixels in the cell |
| `is_cropland` | boolean, null | `cropland_pct ≥ 50`; null if mask unavailable |
| `worldcover_class` | uint8, null | majority WorldCover class |

#### Silver — `ndvi/year=/month=/` (PK: `(cell_id, date)`)

| Column | Type | Notes |
| --- | --- | --- |
| `cell_id` | int32 | FK → `cells` |
| `date` | date | acquisition date |
| `ndvi` | float32, null | mean of unmasked 10 m NDVI; null if no valid pixel |
| `valid_pixels` | uint16 | unmasked 10 m pixels (0–100) |
| `total_pixels` | uint16 | 100 by construction |
| `masked_pct` | float32 | 100 × (1 − valid/total) |

No weather columns here: weather lives in its own table and is joined in Gold or at query time — this
keeps Silver rows narrow and avoids repeating ~5 values per row ~70 M times.

#### Silver — `weather/year=/` (PK: `date`)

| Column | Type | Notes |
| --- | --- | --- |
| `date` | date | local day (Africa/Cairo) |
| `temp_max_c` | float32 | °C |
| `temp_min_c` | float32 | °C |
| `temp_mean_c` | float32 | °C |
| `precip_mm` | float32 | mm/day |
| `solar_mj_m2` | float32 | shortwave radiation sum, MJ/m²/day |

#### Gold — `weekly_ndvi/year=/` (PK: `(cell_id, week_start)`)

| Column | Type | Notes |
| --- | --- | --- |
| `cell_id` | int32 | |
| `week_start` | date | Monday |
| `ndvi_mean` | float32, null | mean of valid observations |
| `ndvi_std` | float32, null | observation spread within week |
| `n_obs` | uint8 | distinct valid acquisition dates |
| `masked_pct_mean` | float32, null | mean masked share across observations |
| `status` | string | `ok` \| `insufficient_observations` \| `too_cloudy` |

#### Gold — `ndvi_anomaly/year=/` (PK: `(cell_id, week_start)`)

| Column | Type | Notes |
| --- | --- | --- |
| `cell_id` | int32 | |
| `week_start` | date | |
| `ndvi_mean` | float32, null | current week |
| `baseline_mean` | float32, null | same ISO week in baseline years |
| `baseline_std` | float32, null | |
| `anomaly` | float32, null | `ndvi_mean − baseline_mean` |
| `zscore` | float32, null | `anomaly / baseline_std` (null if std = 0) |
| `n_baseline_years` | uint8 | ≥ 3 required |
| `status` | string | `ok` \| `no_baseline` \| `insufficient_observations` |

#### Gold — `weekly_weather/year=/` (PK: `week_start`)

| Column | Type | Notes |
| --- | --- | --- |
| `week_start` | date | |
| `temp_max_mean_c` | float32 | mean of daily maxima |
| `temp_max_max_c` | float32 | hottest day of the week |
| `temp_min_mean_c` | float32 | |
| `precip_sum_mm` | float32 | weekly total |
| `solar_sum_mj_m2` | float32 | weekly total |
| `heatwave_days` | uint8 | days above the documented threshold (defined in Phase 5) |

### 3.9 Tool choices (one line each)

| Tool | Why |
| --- | --- |
| `pystac-client` | Standard STAC search/pagination, works against Earth Search and CDSE |
| `odc-stac` + `rioxarray` | Windowed COG reads into labelled arrays without downloading scenes |
| `rasterio` / GDAL | CRS reprojection, resampling to the 100 m grid, SCL masking |
| `numpy` | Vectorised NDVI and aggregation |
| `pyarrow` + Parquet | Columnar, compressed, partitioned storage |
| `duckdb` | Sub-second SQL over Parquet with no server |
| `pandera` | Schema and value checks expressed as code and run as a pipeline gate |
| `prefect` 3 | Python-native orchestration; lighter locally than Airflow, same retry/backoff model |
| `pydantic-settings` + `python-dotenv` | Typed config and `.env` loading, no hardcoded secrets |
| `pytest` | Phase-level tests |
| `streamlit` + `folium` | Fast interactive map/dashboard demo |
| `scikit-learn` + `xgboost` | Naive baseline and gradient-boosted model |
| `uv` | Fast, reproducible environments (Python 3.12) |

### 3.10 Quality gates (detailed in Phase 4)

Critical checks fail the run: NDVI ∈ [−1, 1]; `masked_pct` below the configured cap; duplicate
`(cell_id, date)` keys; stable schema; daily weather completeness; weekly `n_obs` consistency. Gaps
longer than 10 days are **flagged, never filled**. Each run writes a JSON + Markdown quality report.

### 3.11 Risks

| Risk | Likelihood | Impact | Mitigation |
| --- | --- | --- | --- |
| Cloud cover leaves whole weeks empty | Medium | Medium | 40 % scene filter + SCL masking; week 20/32 can still be empty — emit NULL + status, report coverage per week |
| Duplicate / overlapping acquisitions | High | High | Dedupe on `(tile, date)` keeping the highest processing baseline; never append duplicates (idempotent writer) |
| SCL mask quality (thin cirrus, shadows, 20 m upsampled) | Medium | High | Mask classes 0/1/3/8/9/10/11; track `masked_pct`; fail/flag weeks above 50 %; compare against NDVI outliers |
| BOA offset metadata is wrong in Earth Search | High (confirmed) | High | Scale-only normalization + Phase 3 tests + checksummed Bronze; CDSE JP2 fallback subtracts the offset |
| Data volume / runtime | Low | Medium | **~26 GB** Bronze (measured 90.8 MB/date after compression), 443 GB free; windowed reads cut bytes ~50× vs full scenes; backfill ~5–6 h with 4 workers |
| Earth Search has no SLA / rate limits | Medium | High | Retries with exponential backoff, bounded concurrency, ingestion log resume, CDSE fallback |
| 100 m cells mix fields | High | Medium | Documented as neighbourhood signal; `cropland_pct` exposed for filtering; 20 m mode is future work |
| WorldCover 2021 is static | Medium | Medium | Keep class + percentage so thresholds can change; note in dashboard legend |
| Weekly sampling drops sparse weeks | Medium | Medium | Explicit `status`, never interpolated; coverage metrics in quality report |
| **Leakage: baseline used as a model feature** | Medium | **High** | Leave-one-year-out baseline is for **exploratory Gold only**. In Phase 7, any baseline/anomaly feature must be computed from **training years only** (expanding window per split year). Violating this gives inflated metrics |
| "Stress" target is ambiguous | High | High | Define precisely in Phase 7 (e.g. NDVI drop ≥ 0.1 vs previous 2 weeks, cropland only); harvest/senescence confounds documented in the model card |

### 3.12 Leakage rule (model phase)

The leave-one-year-out baseline in Gold is valid for **descriptive analysis** ("how does 2023 compare
to other years?"). It is **not** valid as a feature for the Phase 7 model: using the test year in a
baseline leaks the future. For modelling, every baseline/anomaly feature is recomputed per split using
only the training years before that split (expanding window, always time-ordered). This rule is a
release blocker for Phase 7 and is repeated in the model card.

---

### 3.13 Remote reads: GDAL /vsicurl settings and measurements

Every remote read runs inside one `rasterio.Env` block defined once in
`src/delta_crops/ingestion/raster.py` (`GDAL_ENV`):

| Setting | Value | Why |
| --- | --- | --- |
| `GDAL_DISABLE_READDIR_ON_OPEN` | `EMPTY_DIR` | Never list a directory over HTTP; the COG URL is one object |
| `CPL_VSIL_CURL_ALLOWED_EXTENSIONS` | `.tif` | Stops GDAL probing for `.aux.xml`/`.ovr` sidecars (extra HEADs) |
| `GDAL_HTTP_MAX_RETRY` / `GDAL_HTTP_RETRY_DELAY` | `3` / `1` | Absorb transient 429/5xx and connection resets |
| `GDAL_HTTP_TIMEOUT` / `GDAL_HTTP_CONNECTTIMEOUT` | `60` / `10` s | Fail a dead socket instead of hanging the backfill |
| `GDAL_HTTP_MULTIRANGE` | `YES` | Coalesce adjacent block reads into one multipart request |
| `VSI_CACHE` / `VSI_CACHE_SIZE` | `TRUE` / 50 MB | Reuse blocks across repeated reads of the same file |
| `GDAL_HTTP_HEADERS` | `Accept-Encoding: identity` | Avoid double compression; GDAL inflates deflate itself |

Measured with `CPL_CURL_VERBOSE=YES` on 2026-11-10 (2023 scenes):

| Asset | Window | COG size | HTTP calls | Bytes transferred |
| --- | --- | --- | --- | --- |
| `S2A_36RTV…B04` | 5 070 × 1 463 px | 14.8 MB | 14 (1 HEAD + 13 ranged GETs) | 18.3 MB |
| `S2B_36RUV…B08` + `…SCL` | 5 070 × 4 375 px / 2 535 × 2 187 px | 51.5 MB + 1.1 MB | 36 (2 HEAD + 34 ranged GETs) | 63.5 MB |

So one acquisition date (two tiles × B04 + B08 + SCL) costs roughly **80–100 HTTP range requests and
~250 MB transferred**, producing ~91 MB of Bronze COGs. A `--workers 4` run stayed well under any
plausible per-IP rate limit (≈1 request/s sustained) and never needed a retry.

### 3.14 Bronze COG format (write side)

`write_cog` in `src/delta_crops/ingestion/raster.py` writes: **COG, DEFLATE, `predictor=2`, `zlevel=6`,
512 × 512 tiles, no overviews**, nodata preserved, and an atomic `os.replace` from a `.tmp` sibling.

Why these settings, measured on the 2019 and 2023 smoke files:

| Variant (36RTV B04, 5 070 × 1 463 px, 14.8 MB of raw DN) | Size |
| --- | --- |
| DEFLATE + predictor 2 + overviews (first implementation) | 14.81 MB |
| DEFLATE + predictor 2, **no overviews** (final) | 10.87 MB |
| ZSTD level 9 + predictor 2, no overviews (rejected) | 10.33 MB |

- The first implementation *was* compressed — Sentinel DN is noisy 13-bit data, so the band only
  deflates to ~73 % — but the auto-built overviews added **3.9 MB**, exactly cancelling the gain and
  making the file look uncompressed. Silver reads native 10 m windows and never uses pyramids, and any
  later map/dashboard zoom reads Gold tables, so overviews are pure cost here. `overviews="NONE"`.
- ZSTD is 5 % smaller but needs a GDAL build with libzstd to read; DEFLATE is readable by every GIS
  tool, which matters for a portfolio repo. 5 % of 26 GB is ~1.3 GB — not worth the compatibility risk.
- **Losslessness proved, not assumed:** all 48 smoke assets were re-encoded and the SHA-256 of the
  decoded array compared before/after — 48/48 identical, 985.7 MB → 726.4 MB (**26.3 % smaller**).
  The same check was repeated after deleting the smoke data and re-downloading it from Earth Search:
  all 48 decoded arrays still match the pre-change hashes, so the whole download → decode → write path
  is bit-exact.

---

## 4. Repository layout

```
.
├── PROJECT.md           # question, scope, architecture, decisions log
├── PROGRESS.md          # phase checklist and notes
├── README.md            # short landing page until Phase 9
├── config/aoi.yaml      # AOI, seasons, grid, quality thresholds
├── data/                # local lake: bronze | silver | gold (gitignored)
├── dags/                # orchestration (Phase 6)
├── docs/                # reports, model card, screenshots
├── scripts/backfill.sh  # sequential, logged, resumable Bronze backfill
├── src/delta_crops/
│   ├── ingestion/       # Phase 2: Earth Search + Open-Meteo → Bronze
│   ├── transform/       # Phase 3: Bronze → Silver, Phase 5: Gold
│   ├── quality/         # Phase 4: data-quality checks
│   ├── features/        # Phase 7: modelling features
│   ├── models/          # Phase 7: baseline + XGBoost
│   └── dashboard/       # Phase 8: Streamlit app
└── tests/               # pytest suite, one file per concern
```

`src/delta_crops/` is a single installed package so imports are unambiguous and `pip install -e .`
works from any working directory.

## 5. Environment setup

Requires Python 3.10–3.13 (3.12 recommended; the geo stack has no 3.14 wheels yet).

```bash
uv venv --python 3.12 .venv
source .venv/bin/activate
uv pip install -e ".[dev]"
cp .env.example .env      # optional: only needed if we fall back to CDSE
pytest -q
```

## 6. Decisions log

| ID | Date | Decision | Why |
| --- | --- | --- | --- |
| D-001 | 2026-11-10 | Project root is this repository, not a nested `delta-crops/` folder | Avoids a redundant directory level |
| D-002 | 2026-11-10 | AOI-A with south edge at `30.75` | ~50×50 km and Tanta fully inside |
| D-003 | 2026-11-10 | First season is summer 2023 (May–Sep) | Clear skies and peak heat |
| D-004 | 2026-11-10 | Python 3.10–3.13, 3.12 recommended | Raster stack lacks 3.14 wheels |
| D-005 | 2026-11-10 | Package under `src/delta_crops/` | Clean editable install |
| D-006 | 2026-11-10 | Only used libraries are declared per phase | No scope creep |
| D-007 | 2026-11-10 | **Earth Search is the primary imagery source**, CDSE is the fallback | Only Earth Search supports windowed COG reads without auth; CDSE JP2 means ~1 GB/date |
| D-008 | 2026-11-10 | Baseline seasons 2019–2023, same May–Sep window | 2019–2023 availability verified; 5 seasons give a usable anomaly baseline |
| D-009 | 2026-11-10 | Reflectance = `DN × 0.0001`; ignore Earth Search `raster:bands.offset` | Real pixels show data already harmonized; advertised −0.1 produces negative reflectance and NDVI > 1. Defect: <https://github.com/Element84/earth-search/issues/71> (verified 2026-11-10) |
| D-010 | 2026-11-10 | 100 m grid in EPSG:32636 | Balances field size against 5-year row counts |
| D-011 | 2026-11-10 | Weather in its own Silver table, joined in Gold | Avoids repeating 5 weather values in ~70 M NDVI rows |
| D-012 | 2026-11-10 | Cropland flag from ESA WorldCover 10 m 2021 (CC BY 4.0) | Free, 10 m, covers AOI; removes urban/canal pixels from averages |
| D-013 | 2026-11-10 | 40 % scene-level cloud filter + SCL per-pixel mask | Adds 4–9 dates/season vs 20 %; SCL handles residual cloud |
| D-014 | 2026-11-10 | Weekly value requires ≥ 2 valid observations; otherwise NULL + status | Honest gaps beat interpolated fake signal |
| D-015 | 2026-11-10 | Leave-one-year-out baseline is exploratory only; model features use training years only | Prevents temporal leakage in Phase 7 |
| D-016 | 2026-11-10 | Prefect 3 for orchestration | Lighter than Airflow locally; same retry/backoff semantics |
| D-017 | 2026-11-10 | Open-Meteo free API, CC BY 4.0, attribution "Weather data by Open-Meteo.com" | Terms verified on the official site 2026-11-10: free tier is **non-commercial only**, < 10 000 calls/day, 5 000/hour, 600/minute; the data is CC BY 4.0. Our usage is 1 call/year of a season. Attribution is on the Phase 9 checklist next to WorldCover |
| D-018 | 2026-11-10 | Bronze log is append-only JSONL, compacted to Parquet separately | One fsynced record per completed asset survives any crash; rewriting a Parquet file per asset would leave a corrupt log after Ctrl-C |
| D-019 | 2026-11-10 | 4 concurrent asset downloads (`--workers 4`) | Measured 2.5× wall-clock speedup (638 s → 254–313 s per 24 assets); network-bound, so more workers add little and risk throttling |
| D-020 | 2026-11-10 | Bronze COGs: DEFLATE + `predictor=2` + 512 tiles + **no overviews**; ZSTD rejected | Overviews cost 3.9 MB on a 10.9 MB band and Silver never reads them (26.3 % smaller without). ZSTD is only 5 % smaller but needs libzstd in the reader, so DEFLATE wins on portability |
| D-021 | 2026-11-10 | Weather is fetched by `ingest` for every year in the window **and** by a standalone `weather` command | Lets the cheap weather half of ingestion run (and be proven) without touching imagery; same ledger, so both are idempotent |

## 7. Open questions for later phases

- Exact heat-wave definition for `heatwave_days` (Phase 5).
- Exact stress threshold and forecast horizon (Phase 7).
- Whether a 20 m "hotspot" Gold table is worth the extra volume (propose only if needed).

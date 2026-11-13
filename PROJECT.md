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
4. Original cross-era requirement (superseded by D-034/D-035, retained for audit): a **fixed 1 km reference box of open water** inside the AOI (chosen in
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

- **100 m cells, EPSG:32636**, origin snapped outward to the AOI envelope's south-west corner in UTM,
  then **clipped to the AOI polygon**: 487 × 508 = 247 396 envelope cells, of which **239 304
  intersect the AOI** and are kept (8 092 corner cells are outside the rotated rectangle).
- **Measured 2026-11-10** with `python -m delta_crops.transform.cli grid`:

  | Quantity | Value |
  | --- | --- |
  | UTM bounds (west, south, east, north) | 295 148.7, 3 403 045.1, 343 753.4, 3 453 740.4 |
  | UTM extent | 48.6 × 50.7 km |
  | Grid origin (`x0`, `y0`) | 295 100, 3 403 000 |
  | Envelope | 487 × 508 = **247 396** cells |
  | Kept (intersect AOI, `all_touched`) | **239 304** |
  | AOI 10 m pixels inside polygon | **23 840 326** = 2 384.0 km² |
  | Mean in-AOI pixels per kept cell | 99.62 (max 100) |

  **Correction (measured, supersedes earlier drafts):** an earlier draft said 478 × 498 ≈ 238 044
  cells and "~2.3 km uncovered". The real numbers are 487 × 508, and the old grid left roughly
  **0.85 km east + 0.94 km north** outside the grid (9 columns × 0.1 km, 10 rows × 0.1 km), not 2.3 km.
  The kept-cell count (239 304) sits inside the expected 236 000–242 000 band, so no stop was needed.
  The earlier "~245 000 / ~200 000 cropland / ~72 M rows" estimate was wrong on all three counts.
- NDVI is computed at native **10 m**, then mean-aggregated to 100 m. The SCL mask (native 20 m) is
  **resampled to the 10 m B04 grid with nearest neighbour** rather than block-replicated: in this
  collection the 20 m SCL origin sits half a 10 m pixel (10 m) away from the B04 origin, so block
  replication would shift every class by one pixel and silently bias the mask.
- Why not 10 m/20 m: 20 m → 6.25 M cells per date, ~1.5 B rows over 5 years — too heavy for a laptop.
  Why not 250 m/1 km: a 250 m cell is 6.25 ha and a 1 km cell 100 ha, while typical Delta holdings are
  0.1–1 ha; both smear field-scale stress into noise.
- **Cost accepted:** a 100 m cell (~1 ha) still mixes several small fields. We report a neighbourhood
  health signal, not field-level diagnosis; this is stated again in the dashboard and README.

Row estimates (293 acquisition dates across 5 seasons at the 40 % cloud filter, each date normally
covering both tiles):

| Layer | Rows | Est. size |
| --- | --- | --- |
| Bronze | 574 tile-scenes = 1 722 assets | **25.8 GB** (measured on the full backfill) |
| Silver `ndvi` | 239 304 × 293 = **70 116 072** | ~2–3 GB Parquet |
| Silver `cells` | 239 304 | 6.6 MB |
| Silver `weather` | 765 days (5 seasons × 153) | < 1 MB |
| Gold weekly (all cells) | 239 304 × 110 weeks ≈ **26 M** | ~1–2 GB |
| Gold weekly (cropland only) | 194 778 × 110 ≈ **21 M** | ~0.8–1.5 GB |


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

### 3.5.1 WorldCover on the 10 m NDVI lattice (reprojection recipe)

ESA WorldCover is served as a 10 m **EPSG:4326** COG. To align its classes with the NDVI pixels:

1. Convert the analysis envelope from EPSG:32636 to EPSG:4326 with `transform_bounds` and read only
   that window from the COG (`N30E030`, 47.4 MB).
2. `rasterio.warp.reproject` with **nearest neighbour** onto the **same 10 m UTM lattice** used for
   NDVI (`from_origin(295 100, 3 453 800, 10, 10)`), so WorldCover pixel *(r, c)* and NDVI pixel
   *(r, c)* cover the same square metre of ground. Nearest is mandatory here: interpolating class
   codes would fabricate classes that do not exist.
3. Reshape the 5 080 × 4 870 class array into 508 × 10 × 487 × 10 blocks and count classes per block
   **over in-AOI pixels only** — so `cropland_pct`'s denominator is the cell's in-AOI pixel count, not
   a blind 100.
4. `worldcover_class` = per-cell majority class; **ties break to the lowest class code** (argmax over
   the ascending class list), which is deterministic and reproducible.
5. `is_cropland = cropland_pct >= 50`.

The result is cached as `data/silver/_cache/worldcover_<x0>_<y0>_487x508.npy`, so `cells` rebuilds in
~2 s without re-reading the COG.

Measured AOI class shares (in-AOI 10 m pixels): cropland 78.48 %, bare 15.00 %, built 3.68 %,
wetland 1.88 %, trees 1.04 %, water 0.63 %, sparse 0.20 %, shrub 0.13 %.

### 3.6 Cropland mask

Source: **ESA WorldCover 10 m 2021 v200**, class `40` (cropland), tile `N30E030`, CC BY 4.0, served as
a COG from `esa-worldcover.s3.eu-central-1.amazonaws.com` (verified `200 OK`, 47.4 MB).

Measured over the AOI:

- **78.48 %** of 10 m pixels are cropland; bare 15.00 %, built 3.68 %, wetland 1.88 %, trees 1.04 %,
  water 0.63 %, sparse 0.20 %, shrub 0.13 %.
- At 100 m, **194 778 of 239 304 kept cells (81.4 %) have ≥ 50 % cropland pixels** — measured with
  `cli cells` on 2026-11-10. `cropland_pct` p10/p50/p90 = 5 / 98 / 100 %.
- A cell is `is_cropland = true` when `cropland_pct ≥ 50`; `cropland_pct` and `worldcover_class` are
  kept so the threshold can be changed without re-reading the COG.
- Filtering to cropland removes ~19 % of cells, mostly urban fringes and canals, and prevents built-up
  pixels from diluting NDVI averages. Gold aggregates use cropland cells only; Silver keeps all cells
  with the flag so the effect is auditable.

Limitations: WorldCover is a **2021 static snapshot**, so land-use change after 2021 and mixed 100 m
cells introduce classification error. `is_cropland` is nullable when the mask has no coverage.
Attribution requirement: the README (Phase 9) must credit **ESA WorldCover 10 m 2021 v200, CC BY 4.0**.
The reprojection recipe that puts WorldCover on the 10 m NDVI lattice is in §3.5.1.

**DN floor on dark targets:** Earth Search COGs (baseline 5.0 and 5.9) apply a BOA offset and
clip negative reflectance at 0, which maps to DN = 1 (DN = 0 is nodata). Dark targets (water,
deep shadow) therefore have B08 clamped at DN = 1, producing artificially low NIR reflectance
and NDVI ≈ −1 regardless of true surface state. This makes NDVI unreliable for dark targets.
The cross-era test excludes DN ≤ 1 pixels via `require_dn_gt_one`, but the floor remains a
fundamental limitation for any water/dark-target analysis. Measured on 2023-07-07 36RUV:
25,078 B08 pixels at DN=1 (0.10% of scene), 32.8% of SCL=6 water pixels.

**Measured 2026-11-10** (`cli cells`): 239 304 rows, **194 778 cropland cells** (81.4 % of kept
cells), cropland_pct p10/p50/p90 = 5 / 98 / 100 %. Median in-AOI pixels per cell 100.

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
| `date` | date | acquisition date (also stored inside each file) |
| `ndvi` | float32, null | mean of valid 10 m NDVI; **null** when below the valid-fraction gate or no valid pixel |
| `valid_pixels` | uint8 | 10 m pixels that are SCL-unmasked **and** have a valid NDVI (0–100) |
| `total_pixels` | uint8 | in-AOI 10 m pixels in the cell (0–100; < 100 on the AOI boundary) |
| `masked_pct` | float32 | 100 × (1 − valid_pixels / total_pixels); 100 when the tile footprint misses the cell |

Every kept cell is written for **every** date, so `rows = dates × 239 304` even on a date where one
tile is missing (the uncovered cells then hold `valid_pixels = 0`, `ndvi = NULL`, `masked_pct = 100`).

**Masking rules (config `silver.masked_scl_classes`).** Masked: 0 no-data, 1 saturated, 2 dark area,
3 cloud shadow, 8 cloud medium, 9 cloud high, 10 thin cirrus, 11 snow/ice. Kept: 4 vegetation, 5 bare,
6 water, 7 unclassified. `ndvi = (NIR − Red) / (NIR + Red)` on reflectance, and a pixel only counts
when both bands are present (`DN > 0`) and the denominator is positive — so a 0/0 pixel is null, not
0, and cannot drag a cell mean.

**Tile mosaic (`silver.tile_priority = ["36RTV", "36RUV"]`).** Both tiles are read into one global 10 m
canvas; **first valid pixel wins**, so 36RTV has priority in the ~9.8 km overlap (20.1 % of the AOI).
The alternative (averaging the overlap) would halve sensor noise but change the meaning of every
overlap cell mid-series; it is deferred until the measured overlap bias justifies it.

**Valid-fraction gate:** `ndvi` is null when fewer than `silver.min_valid_fraction = 0.50` of a cell's
in-AOI pixels are valid. `masked_pct` is still written so a null can be explained.

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

### 3.15 Bronze → Silver processing rules (Phase 3, implemented and measured)

Implemented in `src/delta_crops/transform/`: `grid.py` (grid + AOI masks), `reflectance.py` (scale,
NDVI, SCL mask), `bronze_reader.py` (ledger-verified reads), `silver.py` (per-date mosaic and 100 m
aggregation), `cells.py`, `weather.py`, `stability.py`, `cli.py`.

**Per-date pipeline.** For each ledger date: verify every Bronze asset (size, and SHA-256 by default),
read the AOI windows of B04/B08 at 10 m and SCL resampled to the B04 grid with nearest neighbour, mask
with `silver.masked_scl_classes`, place both tiles on one global 10 m canvas (first valid pixel wins,
priority 36RTV → 36RUV), average each 10 × 10 block into a 100 m cell, then write one Parquet file.

**Measured (2026-11-10, this machine):**

| Item | Measured |
| --- | --- |
| Wall time per date | 1.5–2.0 s (median ~1.6 s); single-tile dates 0.4–1.5 s |
| Peak RSS (whole run) | ~1.05 GB (**deviation**: target was < 1 GB) |
| SHA-256 verification of one date (6 assets, ~88 MB) | **0.066 s** vs 0.001 s without → negligible |
| Rows per date | 239 304 (every kept cell, always) |
| NDVI range on 2023-07-02 | −0.988 … 0.992 |

**Idempotency.** Each file is written to a `*.parquet.tmp` sibling and `os.replace`d into place, so a
crash never leaves a half-written file. A date whose output exists and reports the expected row count
is skipped (re-run of July 2023: 7/12 skipped, 8.3 s); `--force` rebuilds. Bronze is only ever read,
and only through the ledger — a missing file, size mismatch or checksum mismatch makes the date fail
loudly instead of silently producing a short file.

**Resource deviation:** peak RSS of ~1.05 GB (measured 1052 MB avg / 1063 MB max from `dates.parquet`,
and 1063 MB from the full-build summary) exceeds the < 1 GB memory budget target. The dominant
consumers are the two 10 m float32 canvases (~40 MB each) plus PyArrow/GC overhead. Acceptable on a
15 GB machine, but worth addressing if deploying to smaller instances.

**Single-tile dates.** The cloud filter is per tile, so some dates have only one tile (e.g. 2023-07-05
is 36RTV only; 2019-09-14 is 36RUV only). Those dates still write all 239 304 cells; cells the present
tile does not cover get `valid_pixels = 0`, `ndvi = NULL`, `masked_pct = 100`, and the date is logged
with the tile list it actually used.

**SCL evidence before freezing the mask.** `cli scl` reports per-year class shares over in-AOI pixels.
2023 (partial, July): class 4 = 73.81 %, class 5 = 19.90 %, class 6 = 0.15 %, class 7 = 0.80 %,
class 2 = 0.001 %, class 3 = 0.22 %, class 8/9 = 0.80/0.39 %. Both gate classes are far below the 5 %
threshold, so the mask in config stands unchanged. Flooded rice is therefore *not* being labelled
water/dark in this AOI at a scale that matters.

**Tile overlap audit (36RTV vs 36RUV).** Over every dual-tile date the code records the median absolute
NDVI difference in the overlapping pixels. July 2023: 11 dual-tile dates, median-across-dates
**0.0064**, worst date 2023-07-20 at **0.0191**. That is below the 0.02 threshold, so first-valid-pixel
priority (no averaging) stands. The full-series numbers are in `data/silver/_reports/tile_overlap.parquet`.

---

### 3.16 Dashboard (Phase 8)

The dashboard is a **read-only** consumer: Gold tables, the published pipeline-health summary, and
the frozen Phase 7 artifacts. It never runs a pipeline stage, never recomputes a check, and never
writes to `data/` (enforced by a single-SELECT SQL guard and a test that snapshots the lake's
mtimes across all four pages).

```bash
python -m delta_crops.dashboard.health   # publish data/gold/_reports/pipeline_health.json
delta-crops-dashboard                    # http://localhost:8501
```

**Pipeline Health is a projection, not a new check (Option A).** Phase 4 reports live under
`data/silver/_reports/quality/` and the Gold gate under `data/gold/_reports/`, but the dashboard may
read only Gold. `dashboard/health.py` therefore reads the two newest reports (sorted by run id,
which starts with a UTC timestamp) and writes one compact summary with no wall-clock field, so a
rerun produces byte-identical output. The quality and gate code is untouched.

**Pages.** Map (weekly NDVI or anomaly on 1 km blocks), Season (cropland-mean NDVI vs Tmax, plus
heatwave/rainfall bars), Predictions (S3 probability map, scoreboard, calibration), Pipeline Health
(quality checks, season gaps, Gold gate, model status, S5 gap).

**1 km blocks.** 239,304 cells per week is too many polygons for a browser map, so cells are
aggregated into 10 × 10 blocks of the 100 m grid (2,483 blocks in the AOI, 2,459 cropland in
2024-07-01), each drawn as the envelope of its cell centroids plus half a cell. The weekly value is
the mean over `status = 'ok'` cells, so an empty block means no usable observation, not zero
vegetation.

**Predictions cover 17 weeks, the map 20.** Gold holds 20 weeks for 2024 (2024-05-06 →
2024-09-16). The frozen S3 target needs the mean anomaly over `t+1` and `t+2`, so a cell-week is
scorable only when both weeks exist: `2024-09-09` and `2024-09-16` have no `t+2` inside the season,
and `2024-05-06` has no anomaly at all (0 of 239,304 cells — no baseline years for that ISO week).
The Predictions page states this in place; no week is padded or interpolated. On 2024-07-01 that is
2,309 of 2,459 cropland blocks with a prediction.

**S5 calibration stays a documented gap.** `xgb_spatial.json` contains no reliability block and no
S5 prediction file was ever written, so the Health page reports the gap and does not substitute the
S3 table for it.

**Known limitations.** The prediction dump is gitignored (`artifacts/`), so a fresh clone must rerun
`python -m delta_crops.models.predictions` (≈9.4 min, 8.9 GB peak) before the Predictions page
works; without it the page shows a clear error. The map's block polygons are centroid envelopes, not
true cell footprints, so block edges are approximate (± half a 100 m cell). No server-side
authentication is configured: the app is a local demo, not a deployment.

### 3.17 Packaging and documentation (Phase 9)

Phase 9 adds no pipeline logic. It makes the existing work reproducible and honestly described:

- **README** carries the question, the generated results table, three real screenshots, the Mermaid
  data flow, the trade-off table, exact data credits, a quick start, both lake paths, the dashboard,
  Compose, limitations, future work and the layout. **No results number is typed by hand**:
  `scripts/readme_results.py` renders the block between `<!-- results:start -->` and
  `<!-- results:end -->` from `xgb_temporal.json`, `xgb_loso.json`, `xgb_spatial.json` and the B3
  reference in `baselines_metrics.json`. `--check` exits non-zero on drift, and
  `tests/test_docs.py` runs the same comparison, so a stale README fails the suite.
- **`docs/limitations.md`** is the canonical list: the 36.70 % DN = 0 date, the unscheduled real-data
  flow, the unproven container concurrency, the S5 calibration gap, this phase's `gold_regression`
  result, and the model/data limits. The README links to it instead of maintaining two versions.
- **Demo**: `scripts/demo.sh` checks the lake, dump and metrics, publishes health, prints the timed
  shot list, and serves the app; `--check` verifies without serving. `docs/demo-2min.md` is the
  narration, including the descriptive heatwave line and a do-not-say list.
- **Screenshots** (`docs/images/{map,predictions,health}.png`) are headless-Chromium captures of the
  running app at its default weeks (Map 2024-07-15, Predictions 2024-07-08; Health full-height so the
  S5 gap is in frame). They are captures, not mock-ups.
- **Compose** gains an optional `dashboard` profile. `health-publish` is a one-shot writer of
  `data/gold/_reports/pipeline_health.json`; `dashboard` serves `delta-crops:dashboard` on
  `127.0.0.1:8501` with `data/` and `artifacts/` mounted **read-only**. The default pipeline path is
  unchanged, and `delta-crops:local` stays Streamlit-free: the dashboard is a separate build stage.
- **Licence**: code is MIT (`LICENSE`, "Ahmed Sameh"), data keeps its own licences. The required
  credits — ESA WorldCover 10 m 2021 v200 (CC BY 4.0, with the WorldCover credit line) and "Weather
  data by Open-Meteo.com" (CC BY 4.0) — are in the README and pinned by a test.

A fresh clone was verified against the documented steps: clone, `uv venv`, editable install,
`pytest -q` → **231 passed, 9 skipped, 2 deselected** with no lake and no artifacts. Only the
data-dependent steps reused the existing lake (see the Phase 9 section of `PROGRESS.md`); nothing was
downloaded and no table was rebuilt for the acceptance run.

---

## 4. Repository layout

```
.
├── PROJECT.md           # question, scope, architecture, decisions log
├── PROGRESS.md          # phase checklist and notes
├── README.md            # short landing page until Phase 9
├── artifacts/           # Phase 7 model artifacts: metrics JSON, dataset (gitignored)
├── config/aoi.yaml      # AOI, seasons, grid, quality thresholds
├── data/                # local lake: bronze | silver | gold (gitignored)
├── docs/                # phase reports, pre-registration, model card
├── scripts/backfill.sh  # sequential, logged, resumable Bronze backfill
├── src/delta_crops/
│   ├── ingestion/       # Phase 2: Earth Search + Open-Meteo → Bronze
│   ├── transform/       # Phase 3: Bronze → Silver, Phase 5: Gold
│   ├── quality/         # Phase 4: data-quality checks
│   ├── orchestration/   # Phase 6: Prefect flow, four tasks, run log
│   ├── features/        # empty; Phase 7 features live in models/dataset.py
│   ├── models/          # Phase 7: dataset, baselines, train (temporal/LOSO/spatial)
│   └── dashboard/       # Phase 8: Streamlit app, views, read-only loaders
└── tests/               # pytest suite, one file per concern
```

There is no `dags/` directory: orchestration is the Prefect flow in
`src/delta_crops/orchestration/`, not Airflow DAG files.

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

Dashboard (Phase 8):

```bash
uv pip install -e ".[dev,dashboard]"
python -m delta_crops.dashboard.health   # publish the Gold health summary once
delta-crops-dashboard                    # http://localhost:8501, from the repo root
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
| D-022 | 2026-11-10 | Grid is 487 × 508 = 247 396 envelope cells, **clipped to the AOI polygon → 239 304 kept cells** | The envelope is an axis-aligned box around a rotated AOI; 8 092 corner cells are outside the polygon and would be permanent NULLs. Kept count is inside the agreed 236–242 k band |
| D-023 | 2026-11-10 | `silver.min_valid_fraction = 0.50`; `ndvi` is NULL below it, `masked_pct` is always written | Half a cell of valid pixels is the minimum for a defensible 100 m mean; keeping `masked_pct` means a NULL can always be explained rather than looking like missing data |
| D-024 | 2026-11-10 | `valid_pixels` = SCL-unmasked **and** valid NDVI (DN > 0 both bands, denominator > 0); the cell mean divides by this same count | One definition used everywhere; a 0/0 pixel is null, not 0, so it cannot drag the mean. `masked_pct = 100·(1 − valid/in-AOI total)` counts only in-AOI pixels |
| D-025 | 2026-11-10 | SCL is warped to the 10 m B04 grid with **nearest neighbour**, not block-replicated | Measured: the 20 m SCL origin is 10 m off the B04 origin, so 2×2 replication would shift every class by one pixel. The naive "SCL = 2× resolution, aligned" assertion failed on 2023-07-02 and was replaced with the reprojection |
| D-026 | 2026-11-10 | Mask set `{0,1,2,3,8,9,10,11}` dropped; `{4,5,6,7}` kept | Gates on measured SCL shares: class 2 = 0.001 %, class 7 = 0.80 % in 2023, far from the 5 % stop line, so flooded rice is not being lost |
| D-027 | 2026-11-10 | Tile mosaic is **first valid pixel wins**, priority 36RTV → 36RUV | Measured overlap median difference 0.0064 across dates (worst 0.0191) < 0.02, so averaging buys noise reduction worth less than the mid-series change in meaning it would cause |
| D-028 | 2026-11-10 | SHA-256 verification **on by default**, `--no-verify-checksum` for dev | Costs 0.066 s/date (measured) — negligible against a 1.6 s date — and catches a truncated or swapped Bronze file, which would otherwise silently corrupt Silver |
| D-029 | 2026-11-10 | Per-date Parquet written atomically (`*.tmp` + `os.replace`); rerun skips valid outputs, `--force` rebuilds | Idempotency with no partial files after Ctrl-C; skipping is keyed on the exact expected row count, not just file presence |
| D-030 | 2026-11-10 | Single-tile dates still write all 239 304 cells | Keeps `rows = dates × cells` true for every date, so Gold can left-join on `(cell_id, date)` without special cases; uncovered cells are `valid_pixels=0, ndvi=NULL, masked_pct=100` |
| D-031 | 2026-11-10 | Chinese `date` column stored inside every Parquet, partitions `year=/month=/` | `date` survives a file being moved out of its hive path, and DuckDB still prunes on the partition keys |
| D-032 | 2026-11-10 | Cross-era stable-target test **blocked**, not loosened | Measured: best 1 km box is 27.1 % water / 15.1 % desert, far from the required 90 %. Tolerances and the 1 km requirement are unchanged; alternatives are put to the user |
| D-033 | 2026-11-10 | `duckdb>=1.1` added to dependencies | Silver acceptance requires timing a real query on the hive layout; DuckDB is the query engine named in the architecture |
| D-034 | 2026-11-10 | Stable target switched from Option B (water, class 80) to Option C (built-up, class 50) | Option B failed: B08 DN==1 floor on dark water pixels (clamping at 1 after BOA offset) and temporal instability of WorldCover water (43% look like vegetation on some dates, only 47 persistently water pixels in 2019, 0 in 2023). Option C (built-up, class 50, erosion 3 px, CC >= 1 km2, window 1-14 July, DN>1 required, median-of-date-medians pooling) has 1,057,017 pixels, 6 qualifying dates in 2019 and 4 in 2023, NDVI delta 0.0034 (PASS), red delta 0.0302 (FAIL, tolerance 0.02). Original Option B failure (delta red 0.0708, delta NDVI 0.9167) and diagnosis retained in repo history. DN-floor limitation (NDVI on dark pixels unreliable) added to known limitations. |
| D-035 | 2026-11-10 | Adopt post-hoc within-orbit stable-target check | Full history, original failures and limitations are retained in the D-035 section below. |
| D-036 | 2026-11-11 | Revise the original 10-day gap rule AFTER real-data measurement: >10 days information only, >=13 days cell-season alert, >10% seasonal prevalence additional alert; never critical or filled | Measured >10-day shares were 2019 1.20%, 2020 27.18%, 2021 1.70%, 2022 7.64%, 2023 12.69%; AOI-wide gap maxima 4/7/4/4/6 days. Ten days is near the longest-gap median scale (p50=9 in 2020/2023; not literally a median of 10) and would flag common missingness. Thirteen consecutive missing days always contain a complete Monday–Sunday ISO week regardless of start weekday (12 does not); tied to Phase 5 weekly usability, not a fit to the current pass rate. Gap statistics cover all cells and cropland separately. The prior ~20% review gate is resolved. |
| D-037 | 2026-11-10 | Keep Silver DN>0; no rebuild, per user decision after Phase 3 acceptance | DN=1 floor remains a known limitation; Phase 4 must quantify excluded pixels, valid-count changes, non-NULL → NULL transitions and NULL-share changes under DN>1 without changing Silver. |
| D-038 | 2026-11-10 | Phase 4 gap usability is NDVI IS NOT NULL; stop for threshold review when >~20% of cells exceed proposed 10 days | Real-data preflight found 27.181% in 2020 (65,045/239,304 cells); initial gate subsequently resolved by the user in D-036. Gaps are data alerts, AOI acquisition absence is information only, and neither is filled. Gap splits count no-ledger days separately from NULL acquisition dates, including season edges. |
| D-039 | 2026-11-11 | Pandera validates compact DuckDB per-file aggregates; Arrow validates explicitly declared physical schema | Laptop scans avoid loading 70M rows into pandas; per-file DISTINCT state is bounded to 239,304 keys. Measured 70,116,072 rows in 18.76 s with 4 threads and 2 GB DuckDB limit. Pandera 0.29.0 tested. |
| D-040 | 2026-11-11 | Structural/data-integrity failures are critical; missingness is alerts/information; persist JSON before raising | Schema/dtypes, NDVI range, duplicate keys, row count, valid_pixels>total_pixels, required NULLs, foreign cell IDs, wrong date contents and missing ledger-date outputs stop the CLI with nonzero exit. Input list comes from all 293 ledger dates, not file discovery. Schema is six explicit fields; counts uint8, NDVI/masked_pct float32, cell_id int32, physical date date32. |
| D-041 | 2026-11-11 | Approved alert defaults: masked_pct>50%; dual-tile NULL>50% OR leave-one-out seasonal median+6×raw MAD with >=5 other dates | Single-tile dates get separate context, never a NULL-rate alert for single-tile status. With zero MAD use strictly above peer median; with too few peers use absolute criterion only. All values and severities are in config; actual run retains 15 dual-tile NULL alerts and all 12 single-tile dates as context. |
| D-042 | 2026-11-11 | Keep Silver DN>0/no rebuild after a fixed 14-date DN-floor measurement; report local effects | Six original July dates plus one date per other month in both eras. DN>0 replay reproduced saved counts/NDVI. DN>1 added at most 0.1914 percentage points NULL across all cells, 0.0303 points on cropland; up to 965 paired cropland cells/date changed NDVI by >0.01. Supports retaining Silver for current aggregate use, with local prediction limitations. No validity rule or Silver output changed. |
| D-043 | 2026-11-11 | Weekly rule: ISO weeks 19–38, ≥2 observations, mean masked_pct ≤50, status NULL unless ok | Structural NULL share (weeks with <2 AOI dates) and per-cell NULL share measured separately (see Phase 5 gate table). 2023: 15.0 points structural, 12.5 points per-cell = 27.54 % total; cropland 27.76 %. max_weekly_masked_pct is the deciding factor for 458 / 123 / 157 / 1,172 / 42 cell-weeks (2019–2023). No interpolation or fill. |
| D-044 | 2026-11-11 | Leave-one-year-out anomaly: same ISO week, ok years only, no smoothing, current-week status wins | Baseline mean of up to 4 years; <3 → no_baseline. Current-week status (insufficient/too_cloudy) overrides no_baseline; only ok status exposes anomaly/zscore. Descriptive only; Phase 7 must recompute from training years. |
| D-045 | 2026-11-11 | Heatwave: daily Tmax ≥ 40.0 °C inside a run of ≥ 3 consecutive days (window 1 May–30 Sep) | Chosen from real 765-day pool: 47 days (6.1 %) in 9 episodes; 35 °C runs cover 73 %, 38 °C runs 29 %. 2023: 18 heatwave days in 4 episodes (14–18 Jul, 22–28 Jul, 5–7 Aug, 6–8 Sep). Runs detected on full daily series; weekly `heatwave_days` splits runs at ISO week boundaries (e.g., 5–7 Aug puts 2 days in week 2023-07-31, 1 day in 2023-08-07). The 1-day week is **not** a separate episode. 40 °C is the pooled p90 (40.2) rounded. |
| D-046 | 2026-11-11 | Orbit metadata cached under Silver; per-week orbit_mix and n_orbits; diagnostic is descriptive; zscore indicative only | STAC product_uri parsed, no date has >1 orbit. 5-day pairing is same orbit (not cross-orbit). Cross-orbit pairs (2–3 d) show R064 reads ~0.005–0.010 higher than R021 in all 5 years, but phenology confounds it; earlier claim that offset is small/absent on cropland is withdrawn. Labelled, not corrected. zscore has heavy tails (p1 = −8.2, p99 = +7.5) because baseline_std is from 3–4 years; anomaly (raw difference) is the primary signal; zscore must not be used as stress detection. |
| D-047 | 2026-11-12 | Phase 7 target: per cell-week, mean **raw** NDVI anomaly over `t+1` and `t+2` ≤ **−0.08**, both weeks `status='ok'`, features stop at week `t`, baseline from training seasons only | Threshold is a physical NDVI amount fixed before training, roughly the 16th–21st percentile of the forward anomaly with a 15–22 % positive rate, so it is neither rare nor trivial. Both horizon weeks must be observed, so the label never means "next two available observations". The raw difference is used because D-044/D-046 established that `zscore` has unusable tails at 3–4 baseline years. Leave-one-training-year-out for train rows and all training seasons for test rows prevents a season entering its own baseline (D-015). Frozen in `docs/phase7-preregistration.md` §1–§3 before any fit. |
| D-048 | 2026-11-12 | Baseline B3 is a logistic regression on `anomaly_t, anomaly_lag1, week_index` **with an intercept**; the intercept-free fit is a defect, fixed in `770d885` | B3 standardised its features but fitted no intercept, so `sigmoid(0)=0.5` at the feature mean instead of the training prevalence. Mean predicted probability on 2024 cropland/pooled was 0.519 against a 0.251 prevalence and every reliability bin over-predicted. Adding the intercept leaves ranking (ROC-AUC, PR-AUC) unchanged and cuts Brier from 0.1929 to 0.1050. Because B3 is the reference for every verdict, a mis-calibrated B3 would have made the Brier condition meaningless. Fixed before any verdict was computed; all reported B3 numbers are post-fix. |
| D-049 | 2026-11-12 | Verdict bars are "B3 + 0.05 PR-AUC, P@R.5 strictly greater, Brier strictly lower", derived **per campaign** from that campaign's own B3, and never changed after seeing a result | S3's bar (0.8222002 / 0.8145373 / 0.1050482) applies to the single 2024 temporal test. S4 re-derives it on the **mean over six folds** (0.8205095 / 0.8187709 / 0.0947832) because an S3 bar is not an average-over-folds reference, while keeping the 0.05 margin since target, metric and population are unchanged. S5 re-fits B3 on **exactly its own training rows** and scores **exactly its own held-out blocks**, because neither the S3 all-block bar nor the S4 season bar is the right reference for a block-held-out population; the margin stays 0.05 because changing it after two failures would be post-hoc. Result: all three campaigns fail PR-AUC only (S3 by 0.0008298, S4 by 0.0013625, S5 by 0.0140022) and pass P@R.5 and Brier. Freezes in `fb54129`, `675eb4c`, `6fd4226`. |
| D-050 | 2026-11-12 | S5 spatial holdout uses a **frozen** 5 km block partition: 110 block ids sorted, permuted with `default_rng(20260702)`, first 28 held out (25.45 %), rest train; never re-drawn | `block_5km = (cell_row // 50) * 10000 + (cell_col // 50)`, already materialised by the dataset builder, so the partition is a pure function of the block id list and one seed and is auditable in the artifact. It is not chosen from any result. Held-out blocks contribute no rows of any season to fitting, early stopping or refit, which is the spatial leakage S4 cannot remove. Realised caveats reported, not fixed: 28 declared but 27 blocks scorable (80009 has no cropland/pooled 2024 row) and 4 declared training blocks carry no rows. |
| D-051 | 2026-11-12 | `num_boost_round ≤ 600` is frozen for S3, S4 **and** S5 and is not raised after seeing a cap hit; cap hits are reported as a property of the run | S4 folds `loso-2021`/`loso-2022` hit the cap (full 596/592, ndvi_only 600/595), so early stopping never fired there. S5 did **not** hit it (full best round 593, ndvi_only 599, `hit_cap: false` of 600 run), but sits within two rounds of it, so a larger cap could plausibly help. Raising the cap after seeing a near-hit would be post-hoc model selection, so it was left unchanged and recorded as a known limitation instead. Every variant uses identical hyperparameters: `hist`, `binary:logistic`, `max_depth=6`, `eta=0.05`, seed 20260701, patience 50 on `aucpr`. |
| D-052 | 2026-11-13 | Phase 8 prediction dump refits the frozen S3 variants with rounds read from `xgb_temporal.json` (533 / 275), through the Phase 7 loader, behind a 1e-9 gate; nothing is written on a miss | Phase 7 persisted metrics, not predictions, so the dashboard needed one persistence step. Re-running early stopping could pick different rounds; reading the sorted `dataset/temporal.parquet` changes XGBoost's histogram sketch because the builder writes `ORDER BY year, cell_id, week_start`; and fitting anything new would make the dashboard a different model. The dump therefore uses `load_frames` → `build_sql` → `fold_frames` and the stored rounds, and must reproduce all six metrics plus the 2,449,220 / 614,324 counts exactly. Measured: every delta 0.0 (see freeze §6). |
| D-053 | 2026-11-13 | Pipeline Health uses Option A: a publisher projects the newest Phase 4 quality report and Gold gate report into `data/gold/_reports/pipeline_health.json`; the checks themselves are untouched and no timestamp is written | The dashboard may read only Gold, but the quality evidence lives under `data/silver/_reports/`. Re-running checks would duplicate Phase 4 logic and could disagree with it; moving the reports would rewrite Phase 4's contract. A projection keeps one source of truth. Omitting the wall clock and sorting candidates by `run_id` (a UTC timestamp prefix) makes reruns byte-identical, so publishing cannot create drift. |
| D-054 | 2026-11-13 | The map aggregates the 100 m grid into 1 km blocks (10 × 10 cells), one polygon per block | A week holds 239,304 cells; drawing each as a polygon is slow in the browser and unreadable at AOI zoom. 1 km keeps within-block heterogeneity visible (2,483 blocks, 2,459 cropland on 2024-07-01), matches the 100 m → 5 km hierarchy used by S5, and stays honest: the value is the mean over `status='ok'` cells, and an empty block means no usable observation, never zero NDVI. Polygons are centroid envelopes ± half a cell, so edges are approximate. |
| D-055 | 2026-11-13 | Dashboard modules live in `dashboard/views/` with a sidebar `radio`, not Streamlit's `pages/` convention | `pages/` triggers Streamlit's automatic multipage navigation, which cannot be exercised page-by-page in `AppTest` and would give four URLs with no shared state. A radio keeps one entry point (`delta-crops-dashboard`), makes the page choice testable, and keeps paths in one place. |
| D-056 | 2026-11-13 | The read-only guarantee is enforced by a single-SELECT guard plus an mtime test, not by `duckdb.connect(read_only=True)` | `read_only=True` only applies to a database *file*, and the dashboard deliberately opens none — it queries Parquet. So `_select_only()` refuses any statement that is not one `SELECT`/`WITH`, and a test snapshots the mtimes of `data/` and `artifacts/phase8/` across all four pages and asserts they are unchanged. |
| D-057 | 2026-11-13 | The Predictions page exposes 17 weeks and says why; the map slider keeps all 20 Gold weeks | Gold has 20 weeks for 2024, but the target needs `t+1` and `t+2` to be observable: `2024-09-09` and `2024-09-16` have no `t+2` inside the season, and `2024-05-06` has no anomaly (0/239,304 cells, no baseline years for that ISO week). Truncating the map slider or padding predictions would hide a real coverage difference; stating it is the honest option and it is pinned by a test. |
| D-058 | 2026-11-13 | The README results table is **generated** by `scripts/readme_results.py` from the four Phase 7 artifacts, never typed | A hand-copied metric drifts silently and there is no way to tell a stale README from a changed model. The script renders a marked block; `--check` exits non-zero on drift and `tests/test_docs.py` asserts the committed block equals a fresh render and that each row's metric is the artifact's value. Regenerating is one command, so the docs cannot become the weakest link. |
| D-059 | 2026-11-13 | The dashboard is an optional Compose profile (`--profile dashboard`) with a separate `delta-crops:dashboard` image; the default pipeline path is unchanged | A portfolio reviewer wants one command that includes the app, but the pipeline image should not carry Streamlit or plotly and the orchestrator should not depend on the dashboard. A separate build stage and profile gives `docker compose up -d` the same meaning as before, and `--profile dashboard` adds `health-publish` then the app. |
| D-060 | 2026-11-13 | In Compose the dashboard mounts `data/` and `artifacts/` **read-only**, and a one-shot `health-publish` service is the only writer (one JSON) | The app's own guarantee is a SELECT-only guard plus an mtime test; a writable bind mount would make that a convention rather than a boundary. Publishing is a separate step the app depends on (`service_completed_successfully`), so the dashboard never computes or writes the health summary itself. |
| D-061 | 2026-11-13 | Code licence is **MIT** (`LICENSE`, "Ahmed Sameh"); data keeps its own licences | The Phase 9 brief left the licence as an unfilled placeholder with MIT as the first option, so MIT was taken and recorded here rather than left blank. No third-party data is redistributed: `data/` is gitignored and rebuilt, and the README carries the WorldCover and Open-Meteo credits. Reversible: replace `LICENSE` and the README §Licence if a different licence is wanted. |
| D-062 | 2026-11-13 | The real-data Silver stability test joins the `slow` marker instead of being skipped, and the README states measured counts | The default suite must pass on a clone with no `data/`, but skipping the test outright would hide a real-data failure. `slow` excludes it from the default run, keeps it selectable with `-m slow`, and — because it has no skip guard — still fails if selected without the report. Counts are documented per environment (231/9/2 fresh, 240/2 with the lake) so a reader can tell a real failure from a missing input. |

### D-035 — Post-hoc within-orbit stable-target comparison (2026-11-10)

The original water-box requirement (class 80, fixed 1 km reference box, July 1–7)
could not supply a defensible stable water target. The subsequent water-target
comparison failed: delta red **0.0708**, delta NDVI **0.9167**. This history remains
part of the evidence; changing the target does not erase that failure.

Option C retained built-up class 50 pixels after 3-pixel erosion and removal of
components smaller than 10,000 pixels, DN > 1 in both bands, SCL masking, and
median-of-date-medians pooling. Its July 1–14 comparison failed on red:
**delta red 0.0302 > 0.02**, while **delta NDVI 0.0034 <= 0.05** passed.

Diagnosis: Earth Search `s2:product_uri` confirms two relative orbits: **R021**
(sensing time 08:41 UTC) and **R064** (08:51 UTC). Built-up red separates by orbit
within both years. Platform alone does not explain the groups. Sun elevation has
not been shown to explain them, and a physical cause such as view angle/BRDF is
**not proven**. Partial viewing-angle metadata does not establish causality.

The user adopted the following rule; config-only commit **4c45fad** froze it
before the first within-orbit cross-era differences were calculated:

- Same Option C pixel set, DN > 1 validity, SCL mask and tile mosaic priority.
- June 1–August 31 in 2019 and 2023; group by relative orbit derived from
  `s2:product_uri`, cross-checked against sensing time.
- A qualifying date has valid fraction >= 0.5 and both tiles or RUV only.
- Each orbit needs >= 2 qualifying dates in EACH year. Every eligible orbit is
  evaluated; no orbit may be dropped after viewing results. No eligible groups
  is a failure, not a vacuous pass.
- Per qualifying date median, then median of those date-medians per year.
- Pass only if ALL eligible groups satisfy BOTH absolute tolerances:
  red <= 0.02 and NDVI <= 0.05. Tolerances are unchanged.

This is explicitly a **post-hoc change after the first failures** and therefore
proves less than a pre-planned independent test. It is a conditional same-orbit
sanity check, not proof of universal cross-era or cross-orbit equivalence.

Test 7 ran exactly once under this rule, using the persisted real measurements:

| Orbit | Dates 2019 / 2023 | Red 2019 / 2023 | NDVI 2019 / 2023 | Absolute red delta | Absolute NDVI delta | Verdict |
| --- | --- | --- | --- | --- | --- | --- |
| R021 | 18 / 17 | 0.25469999 / 0.25340000 | 0.08618258 / 0.08346797 | 0.00129999 | 0.00271462 | PASS |
| R064 | 18 / 17 | 0.20069999 / 0.20220000 | 0.09267525 / 0.09473685 | 0.00150000 | 0.00206160 | PASS |

Measurements are in gitignored `data/silver/_reports/stable_target_dates.parquet`
(115 unique May–September dates). Its `relative_orbit` column is checked against
saved product URIs. Missing report, invalid provenance, unexpected orbits, or
inconsistent sensing times fail the test clearly; it never skips. This local
artifact must be restored/recreated for a fresh checkout to run test 7; a
versioned report-generation command remains a reproducibility limitation.

Limitations carried forward:

- **DN=1 floor on dark targets:** NDVI on clipped dark pixels can be unreliable.
  Stable-target DN > 1 validity does not change Silver's DN > 0 rule.
- **Orbit-dependent red on bright surfaces:** Phase 5 must not compare raw red
  across orbits; retain orbit context when interpreting such measurements.
- Existing sampled cropland measurements show mean cell NDVI changes around
  **0.0001**, but roughly **500–700 cells/date** can change by > 0.01 on affected
  dates (491, 707 and 531 in the sampled examples; other dates have fewer).
  Recommendation: **do not rebuild all Silver solely on this evidence**. The
  aggregate change is small, but local effects matter for later cell-level
  stress predictions. The user decides whether to change validity and rebuild;
  neither has been done.

### Phase 4 quality implementation and measured results

See [the Phase 4 report and runbook](docs/phase4-quality.md) for the exact schema,
config defaults, gap definition, May 2020 geography, per-season threshold shares,
all long-tail cell IDs/dates, DN-floor measurements and verified command output.
All 293 ledger dates and 70,116,072 rows passed critical checks in 18.76 seconds;
58 tests passed. The disposable full-copy corruption test exited 1 on NDVI=1.25
and verified the original file SHA-256 was unchanged. Alerts remain in reports.
The 14-date audit supports keeping DN>0/no rebuild for current aggregate work,
with local DN-floor limitations explicitly retained.

### Phase 5 Gold layer and measured results

See [the Phase 5 report](docs/phase5-gold.md) for the exact schemas, gate decomposition,
orbit diagnostic, heatwave definition and evidence, anomaly construction, sample query
output, and limitations. All 65 tests pass; full `build --force` completes in ~50 s.

### Phase 7 models and measured results

The target, features, splits, bars and decision rule were frozen in
[`docs/phase7-preregistration.md`](docs/phase7-preregistration.md) **before any model was
fitted**. The full record, including calibration, limitations and the interview explanation,
is the [model card](docs/model-card.md). Every number below is from `artifacts/phase7/`.

**Target (D-047).** Per cell-week, using only information up to the end of ISO week `t`: the
mean **raw** NDVI anomaly over weeks `t+1` and `t+2` is **≤ −0.08** (both weeks must be
`status = 'ok'`; never the z-score). Baseline from training seasons only.

**Dataset (S1).** 14,977,696 usable rows, 2,933,974 positives (19.59 %): 2019 2,519,921;
2020 2,728,392; 2021 2,681,558; 2022 2,589,333; 2023 1,431,931; 2024 3,026,561 (test).
Built by `5651bfc`; measured summary in `artifacts/phase7/dataset_summary.json`.

**Baselines (S2, `8b0164b`, `770d885`, `98938ce`).** B0 majority, B1 persistence (`−anomaly_t`,
a ranking score with no Brier), B2 seasonal prior, B3 logistic regression on
`anomaly_t, anomaly_lag1, week_index`. On S3
`cropland/pooled`: B0 PR-AUC 0.2508243, B1 0.7589650, B2 0.2819829, **B3 0.7722002**
(P@R.5 0.8145373, Brier 0.1050482). B3 had a missing intercept until `770d885`; fixing it left
ranking unchanged and cut Brier from 0.1929 to 0.1050. B3 is the reference for every verdict.

**Campaigns and verdicts — all three FAIL on the PR-AUC condition only.**

| Campaign | Split | Bar (PR-AUC / P@R.5 / Brier) | `full` | `ndvi_only` | Verdict |
| --- | --- | --- | --- | --- | --- |
| S3 `ace7352` | train 2019–2023, test 2024 | 0.8222002 / 0.8145373 / 0.1050482 | 0.8213705 / 0.8713148 / 0.0936884 | 0.8235561 / 0.8740270 / 0.0924261 | **FAIL** (misses by 0.0008298) |
| S4 `68215bd` | leave-one-season-out, 6 folds, mean | 0.8205095 / 0.8187709 / 0.0947832 | 0.8191470 / 0.8754861 / 0.0835801 | 0.8235976 / 0.8812944 / 0.0824871 | **FAIL** (misses by 0.0013625) |
| S5 `2a30b52` | 28 of 110 five-km blocks held out, 2024 only | 0.8147217 / 0.8070753 / 0.1036731 | 0.8007195 / 0.8436102 / 0.0960768 | 0.8042603 / 0.8470021 / 0.0942075 | **FAIL** (misses by 0.0140022) |

Freezes: S3 `fb54129`, S4 `675eb4c`, S5 `6fd4226`. In every campaign `full` passes the
operating-point and calibration conditions and fails only PR-AUC. S5's bar 0.8147217 lies
inside both 95 % block-bootstrap intervals (full [0.7818991, 0.8211190], ndvi_only
[0.7863661, 0.8239851]) and above both point estimates; the verdict stays on the point
estimates. The paired `full − B3` PR-AUC difference on S5 is **+0.0360180** [+0.0276012,
+0.0438191] over 1000/1000 usable block draws — a real gain that is smaller than the 0.05
margin the project required.

**Onset headline (separate, not part of any verdict).** On S5 `cropland/onset` (442,124 rows,
prevalence 0.0961) `full` reaches PR-AUC 0.3929923 vs B3 0.3203791; `all/onset` 0.3839020 vs
0.2932640. S3 onset: `full` 0.4373937 vs B3 0.3205508. The onset population is where the model
adds the most over B3 (+0.0726 on S5), and it is reported separately as pre-registered.

**No weather effect is claimed.** `ndvi_only` beats `full` on all three campaigns (0.8235561
vs 0.8213705; 0.8235976 vs 0.8191470; 0.8042603 vs 0.8007195) and has the lower Brier each
time. Weather is a single grid point (~120 independent weeks), so this is unsurprising but it
is stated, not hidden.

**Sanity controls passed everywhere**: XGBoost on the three B3 features clears its frozen floor
(B3 − 0.02) in every campaign and every fold (S3 0.7962004 vs B3 0.7722002; S5 0.7828792 vs B3
0.7647217), and permuted labels score at prevalence (S3 gap 0.0003802; S5 gap 0.0008192).

**Documented gaps.** S5 has no reliability table: predictions were not persisted
(`prediction` and `reliab` occur 0 times in `xgb_spatial.json`) and no re-run was performed, so
S5 calibration is unmeasured. S5 declared 28 held-out blocks but 27 produced scorable rows
(block 80009 has none) and 78 of the declared 82 training blocks were used. Full limitations:
model card §12.

### Phase 8 dashboard and measured results

The S3 prediction dump and its gate are frozen in
[`docs/phase8-predictions-freeze.md`](docs/phase8-predictions-freeze.md); the design is §3.16.

**Dump (measured 2026-11-13).** `artifacts/phase8/predictions/temporal_2024.parquet`: 6,053,122
rows (3,026,561 per variant), 29.6 MB, 17 weeks (2024-05-13 → 2024-09-02), 225,582 cells, no
duplicate `(variant, cell_id, week_start)`, probabilities in [6.1e-6, 0.9999]. Refit cost 561.2 s
with 8,912 MB peak RSS. All ten gate checks matched with **delta 0.0**: `full` 0.8213704513 /
0.8713148213 / 0.0936884443, `ndvi_only` 0.8235560892 / 0.8740271972 / 0.0924262925, and
2,449,220 rows / 614,324 positives for both. Re-scoring the written file reproduces the same
numbers, so the dump is the Phase 7 model, not a lookalike.

**Health publish (measured 2026-11-13).** `python -m delta_crops.dashboard.health` wrote
`data/gold/_reports/pipeline_health.json` from `quality_20261112T020141Z_47c4a558.json` (status
`passed`, 16 checks, 352 dates) and `weekly_gate_20261112T020232Z_65b82678.json` (status `passed`);
two consecutive runs produced identical bytes.

**Pages (verified with `AppTest`).** Map renders 2,483 blocks (2,459 cropland) for 2024-07-01 and
exposes 20 slider weeks; Season draws NDVI + Tmax and the heatwave/rainfall bars; Predictions shows
the 17-week slider, the 17-vs-20 explanation, the S3 scoreboard and a 10-bin calibration table;
Pipeline Health shows the quality checks, season gaps, gate table and the S5 gap. `AppTest`
additionally asserts that all four pages leave `data/` and `artifacts/phase8/` mtimes untouched, and
that a dump with `prob = 1.4` is refused with `probabilities outside [0, 1]`. Full suite: **234
passed, 1 deselected**.

**Not claimed.** No S5 reliability table, no new model, no metric changed by the dashboard. The
dashboard only displays the frozen Phase 7 result and adds no evidence about crop stress.

## 7. Open questions for later phases

- ~~Exact stress threshold and forecast horizon (Phase 7).~~ **Resolved in Phase 7:** mean raw
  NDVI anomaly ≤ −0.08 over `t+1`/`t+2`, both weeks `status='ok'` (`docs/phase7-preregistration.md`
  §1, D-047).
- Whether a 20 m "hotspot" Gold table is worth the extra volume (propose only if needed).
- **Open, unexplained (logged at Phase 4 acceptance, 2026-11-11):** on **2021-07-10**
  (36RTV + 36RUV) **36.70 %** of the invalid in-AOI pixels inside NULL cells are DN = 0;
  SCL cloud/shadow accounts for 62.12 % and other masked SCL classes for 1.19 %. The physical
  cause of that DN = 0 share is **not established**. Every other one of the 15 alerted dates has
  ≤ 0.03 % non-cloud/shadow attribution. No code, config or Silver output was changed; this is a
  known limitation, not a resolved issue, and it must be carried into the Phase 5 and Phase 9
  limitations sections.
- **zscore is not stress detection:** `zscore` has heavy tails (p1 = −8.2, p99 = +7.5) because
  `baseline_std` comes from only 3–4 yearly values per cell/week. The **anomaly (raw difference)
  is the primary Gold signal**; `zscore` must not be presented as stress detection or used as a
  formal test statistic. It is a rough guide for investigation, nothing more (D-046).
- **Main-question interpretation:** the 2023 data show **no visible heatwave response** — NDVI and
  anomaly rise through July and stay positive during the four heatwave episodes. This is
  **descriptive over five seasons**; it **cannot prove absence of an effect**. The signal is
  confounded with seasonality (NDVI peaks in July–August), orbit mix (84–95 % mixed in `ok`
  weeks), and the small baseline sample (3–4 years). **Weeks with low coverage (e.g., 2023-07-03
  at 17.2 %) must not be compared with neighbouring weeks without the coverage column**; the
  mean is based on a small and potentially biased cell subset.


### Phase 6a verification correction (2026-11-11)

The earlier Phase 6a completion claim is withdrawn: the four-task flow, Gold
fingerprints/as_of wiring, asset retry, partial-weather integration and pipeline
failure logging are NOT DONE. Silver fingerprint fixture checks now pass after
replacing an empty-input comparison. Historical Gold rebuilt in temporary output
matches all 15 table/year snapshots; anomaly inputs now correctly use rebuilt
weekly tables in the regression script. Local Prefect deployment concurrency=1
was demonstrated with two non-overlapping runs; it does not establish product
orchestration. Full evidence and remaining limitations: docs/phase6-orchestration.md.

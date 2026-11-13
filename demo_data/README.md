# Demo sample

A small, committed subset of this project's own outputs so the dashboard runs from a fresh clone
with no data lake and no model run.

```bash
scripts/run_demo.sh        # http://localhost:8501
```

The app shows a **"Demo sample: subset of cells, 2024 only"** banner while it reads this directory,
so the sample is never mistaken for the full AOI.

## What is in here

| Path | Contents |
| --- | --- |
| `silver/cells.parquet` | 2 500 cells (2 407 cropland) of the 100 m grid: rows 20–69, columns 400–449 |
| `gold/weekly_ndvi/year=2024/` | the same cells, all 20 Gold weeks of the 2024 season |
| `gold/ndvi_anomaly/year=2024/` | the same cells and weeks, with the anomaly columns |
| `gold/weekly_weather/year=2024/` | the 20 season weeks for the AOI weather grid point |
| `gold/_reports/pipeline_health.json` | the published Phase 4/Gold health summary, unchanged |
| `artifacts/phase7/` | `xgb_temporal.json`, `baselines_metrics.json`, `xgb_spatial.json`, unchanged |
| `artifacts/phase8/predictions/temporal_2024.parquet` | the frozen S3 dump, filtered to the same cells |
| `DEMO_SAMPLE.json` | the marker the dashboard reads to show the banner, plus the window used |

Total size: 2 638 650 bytes (2.64 MB).

Nothing is synthetic or recalculated. Every value is a row-for-row copy of the corresponding file
under `data/` and `artifacts/`; only the spatial window and the season are narrowed. The metric JSONs
and `pipeline_health.json` describe the full analysis and are copied as-is, because the dashboard
reports their published numbers verbatim.

## How it was produced

```bash
python scripts/make_demo_sample.py           # reads data/ and artifacts/, writes demo_data/
python scripts/make_demo_sample.py --cells 50 --year 2024 --out demo_data
```

The window is the most cropland-heavy 50 x 50 cell square on a 1 km boundary, chosen deterministically,
so re-running the script on the same lake reproduces these files.

## Attribution and licences

The sample is derived data and keeps the licences of the sources it came from. The code in this
repository is MIT (see [`../LICENSE`](../LICENSE)); the data below is not.

| Source | What the sample contains from it | Licence and required credit |
| --- | --- | --- |
| **ESA WorldCover 10 m 2021 v200** | `worldcover_class`, `cropland_pct` and `is_cropland` per cell | **CC BY 4.0** — © ESA WorldCover project 2021 / Contains modified Copernicus Sentinel data (2021) processed by ESA WorldCover consortium. <https://esa-worldcover.org/en/data-access> · <https://creativecommons.org/licenses/by/4.0/> |
| **Sentinel-2 L2A** (ESA, via Earth Search / Element 84 on AWS Open Data) | the B04/B08/SCL-derived NDVI, anomaly and prediction rows | Copernicus Sentinel data, processed by ESA. <https://earth-search.aws.element84.com/v1> |
| **Open-Meteo** | the 2024 weekly temperature, rainfall, radiation and heatwave fields | **"Weather data by Open-Meteo.com"**, licensed **CC BY 4.0** — <https://open-meteo.com/> · <https://creativecommons.org/licenses/by/4.0/> |

If you reuse this subset, keep the credits above. To rebuild the full tables from the sources, follow
the pipeline in [`../README.md`](../README.md).

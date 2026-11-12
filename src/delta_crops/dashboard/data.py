"""Read-only data access for the dashboard.

Every loader is a pure function of files on disk plus a Spark-free DuckDB
query; nothing here writes to the lake. DuckDB's ``read_only=True`` flag only
applies to a database file, and the dashboard deliberately opens none, so the
read-only guarantee is enforced by refusing any SQL that is not a single SELECT
statement (``_select_only``) and by the tests, which snapshot the lake's mtimes.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd
import streamlit as st

PREDICTION_COLUMNS = ("cell_id", "week_start", "variant", "prob", "label",
                      "is_cropland", "block_5km")
PREDICTION_VARIANTS = ("full", "ndvi_only")
KM_BLOCKS = 10  # 10 x 10 cells of the 100 m grid = 1 km
HALF_CELL_LON = 0.00053  # half a 100 m cell in degrees at ~31 N
HALF_CELL_LAT = 0.00045

DEFAULT_ROOT = "data"
DEFAULT_PREDICTIONS = "artifacts/phase8/predictions/temporal_2024.parquet"
DEFAULT_ARTIFACTS = "artifacts/phase7"


def _select_only(sql: str) -> None:
    """Refuse anything that is not a single read-only SELECT."""
    statement = sql.strip().lower()
    if not statement.startswith(("select", "with")):
        raise ValueError("dashboard queries must start with SELECT or WITH")
    for forbidden in ("insert ", "update ", "delete ", "create ", "drop ",
                      "alter ", "copy ", "attach ", "detach ", "pragma "):
        if forbidden in statement:
            raise ValueError(f"dashboard queries may not contain {forbidden.strip()!r}")


def query(sql: str, params: list[Any] | None = None) -> pd.DataFrame:
    """Run one SELECT against the lake in a throwaway in-memory connection."""
    _select_only(sql)
    connection = duckdb.connect()
    try:
        return connection.execute(sql, params or []).df()
    finally:
        connection.close()


def _parquet(root: str, table: str) -> str:
    return f"read_parquet('{root}/gold/{table}/year=*/*.parquet')"


def _year_files(root: str, table: str, years: tuple[int, ...]) -> str:
    paths = ", ".join(f"'{root}/gold/{table}/year={year}/*.parquet'" for year in years)
    return f"read_parquet([{paths}])"


@st.cache_data(show_spinner=False)
def season_weeks(root: str, year: int) -> list[str]:
    """The 20 ISO week starts Gold holds for one season, as ISO strings."""
    frame = query(
        f"SELECT DISTINCT week_start FROM {_year_files(root, 'weekly_ndvi', (year,))}"
        " ORDER BY 1"
    )
    return [pd.Timestamp(value).date().isoformat() for value in frame["week_start"]]


@st.cache_data(show_spinner=False)
def map_layer(root: str, year: int, week_start: str, layer: str,
              cropland_only: bool) -> pd.DataFrame:
    """One week of NDVI or anomaly aggregated to 1 km blocks, with their bounds."""
    table, column = ("weekly_ndvi", "ndvi_mean") if layer == "ndvi" else ("ndvi_anomaly", "anomaly")
    filter_sql = "AND c.is_cropland" if cropland_only else ""
    sql = f"""
        WITH c AS (
            SELECT cell_id, cell_row, cell_col, is_cropland,
                   centroid_lon, centroid_lat
            FROM read_parquet('{root}/silver/cells.parquet')
        ),
        b AS (
            SELECT cell_row // {KM_BLOCKS} AS km_row, cell_col // {KM_BLOCKS} AS km_col,
                   min(centroid_lon) AS lon0, max(centroid_lon) AS lon1,
                   min(centroid_lat) AS lat0, max(centroid_lat) AS lat1
            FROM c GROUP BY 1, 2
        ),
        w AS (
            SELECT cell_id, {column} AS value
            FROM {_year_files(root, table, (year,))}
            WHERE week_start = CAST(? AS DATE) AND status = 'ok' AND {column} IS NOT NULL
        ),
        j AS (
            SELECT c.cell_row // {KM_BLOCKS} AS km_row, c.cell_col // {KM_BLOCKS} AS km_col,
                   w.value
            FROM w JOIN c ON c.cell_id = w.cell_id
            WHERE TRUE {filter_sql}
        )
        SELECT b.km_row, b.km_col, b.lon0, b.lon1, b.lat0, b.lat1,
               avg(j.value) AS value, count(j.value) AS n_cells
        FROM b JOIN j ON j.km_row = b.km_row AND j.km_col = b.km_col
        GROUP BY 1, 2, 3, 4, 5, 6
        HAVING count(j.value) > 0
        ORDER BY 1, 2
    """
    frame = query(sql, [week_start])
    for column_name in ("lon0", "lon1", "lat0", "lat1", "value"):
        frame[column_name] = frame[column_name].astype(float)
    frame["n_cells"] = frame["n_cells"].astype(int)
    return frame


def geojson(frame: pd.DataFrame) -> dict[str, Any]:
    """Turn block centroids into 1 km polygons (centroid envelope + half a cell)."""
    features = []
    for row in frame.itertuples(index=False):
        west, east = row.lon0 - HALF_CELL_LON, row.lon1 + HALF_CELL_LON
        south, north = row.lat0 - HALF_CELL_LAT, row.lat1 + HALF_CELL_LAT
        features.append({
            "type": "Feature",
            "geometry": {"type": "Polygon", "coordinates": [[
                [west, south], [east, south], [east, north], [west, north], [west, south],
            ]]},
            "properties": {
                "value": float(row.value),
                "cells": int(row.n_cells),
                "km_row": int(row.km_row),
                "km_col": int(row.km_col),
            },
        })
    return {"type": "FeatureCollection", "features": features}


@st.cache_data(show_spinner=False)
def season_frame(root: str, years: tuple[int, ...]) -> pd.DataFrame:
    """Weekly cropland-mean and all-cell NDVI over the AOI, joined to weather."""
    sql = f"""
        WITH c AS (SELECT cell_id, is_cropland FROM read_parquet('{root}/silver/cells.parquet')),
        w AS (
            SELECT cell_id, week_start, ndvi_mean, status, year
            FROM {_year_files(root, 'weekly_ndvi', years)}
        ),
        agg AS (
            SELECT w.week_start,
                   avg(w.ndvi_mean) FILTER (WHERE w.status = 'ok' AND c.is_cropland) AS ndvi_cropland,
                   avg(w.ndvi_mean) FILTER (WHERE w.status = 'ok') AS ndvi_all,
                   count(w.ndvi_mean) FILTER (WHERE w.status = 'ok' AND c.is_cropland) AS n_cropland
            FROM w JOIN c ON c.cell_id = w.cell_id
            GROUP BY 1
        ),
        weather AS (SELECT * FROM {_parquet(root, 'weekly_weather')})
        SELECT agg.*, weather.temp_max_mean_c, weather.temp_max_max_c,
               weather.precip_sum_mm, weather.heatwave_days
        FROM agg LEFT JOIN weather ON weather.week_start = agg.week_start
        ORDER BY agg.week_start
    """
    frame = query(sql)
    frame["week_start"] = pd.to_datetime(frame["week_start"])
    frame["year"] = frame["week_start"].dt.year
    return frame


def validate_predictions(frame: pd.DataFrame) -> pd.DataFrame:
    """Refuse a corrupted dump instead of drawing it."""
    missing = [name for name in PREDICTION_COLUMNS if name not in frame.columns]
    if missing:
        raise ValueError(f"prediction file is missing columns: {missing}")
    if frame.empty:
        raise ValueError("prediction file is empty")
    probability = frame["prob"]
    if probability.isna().any():
        raise ValueError("prediction file has missing probabilities")
    if not probability.between(0.0, 1.0).all():
        bad = probability[(probability < 0.0) | (probability > 1.0)]
        raise ValueError(f"probabilities outside [0, 1]: {bad.head().tolist()}")
    if frame.duplicated(subset=["variant", "cell_id", "week_start"]).any():
        raise ValueError("prediction file has duplicate (variant, cell_id, week_start) rows")
    unknown = set(frame["variant"].unique()) - set(PREDICTION_VARIANTS)
    if unknown:
        raise ValueError(f"unknown variants: {sorted(unknown)}")
    if not set(frame["label"].unique()) <= {0, 1}:
        raise ValueError("labels outside 0/1")
    return frame


@st.cache_data(show_spinner=False)
def load_predictions(path: str, variant: str) -> pd.DataFrame:
    """Validated predictions for one variant (all 2024 test rows)."""
    frame = pd.read_parquet(
        path, columns=list(PREDICTION_COLUMNS),
    )
    validate_predictions(frame)
    return frame[frame["variant"] == variant].reset_index(drop=True)


@st.cache_data(show_spinner=False)
def prediction_weeks(path: str, variant: str) -> list[str]:
    frame = load_predictions(path, variant)
    return [pd.Timestamp(value).date().isoformat() for value in sorted(frame["week_start"].unique())]


@st.cache_data(show_spinner=False)
def prediction_map(root: str, path: str, variant: str, week_start: str,
                   cropland_only: bool) -> pd.DataFrame:
    """One predicted week aggregated to the same 1 km blocks as the map page."""
    filter_sql = "AND c.is_cropland" if cropland_only else ""
    sql = f"""
        WITH c AS (
            SELECT cell_id, cell_row, cell_col, is_cropland, centroid_lon, centroid_lat
            FROM read_parquet('{root}/silver/cells.parquet')
        ),
        b AS (
            SELECT cell_row // {KM_BLOCKS} AS km_row, cell_col // {KM_BLOCKS} AS km_col,
                   min(centroid_lon) AS lon0, max(centroid_lon) AS lon1,
                   min(centroid_lat) AS lat0, max(centroid_lat) AS lat1
            FROM c GROUP BY 1, 2
        ),
        p AS (
            SELECT cell_id, prob
            FROM read_parquet('{path}')
            WHERE variant = ? AND week_start = CAST(? AS DATE)
        ),
        j AS (
            SELECT c.cell_row // {KM_BLOCKS} AS km_row, c.cell_col // {KM_BLOCKS} AS km_col,
                   p.prob AS value
            FROM p JOIN c ON c.cell_id = p.cell_id
            WHERE TRUE {filter_sql}
        )
        SELECT b.km_row, b.km_col, b.lon0, b.lon1, b.lat0, b.lat1,
               avg(j.value) AS value, count(j.value) AS n_cells
        FROM b JOIN j ON j.km_row = b.km_row AND j.km_col = b.km_col
        GROUP BY 1, 2, 3, 4, 5, 6
        HAVING count(j.value) > 0
        ORDER BY 1, 2
    """
    frame = query(sql, [variant, week_start])
    for column_name in ("lon0", "lon1", "lat0", "lat1", "value"):
        frame[column_name] = frame[column_name].astype(float)
    frame["n_cells"] = frame["n_cells"].astype(int)
    return frame


def reliability(frame: pd.DataFrame, bins: int = 10) -> pd.DataFrame:
    """The Phase 7 10-bin reliability table, recomputed from the dump."""
    probability = frame["prob"].to_numpy(dtype=float)
    labels = frame["label"].to_numpy(dtype=int)
    edges = np.linspace(0.0, 1.0, bins + 1)
    index = np.clip(np.digitize(probability, edges) - 1, 0, bins - 1)
    rows = []
    for number in range(bins):
        mask = index == number
        rows.append({
            "bin": f"{edges[number]:.1f}-{edges[number + 1]:.1f}",
            "rows": int(mask.sum()),
            "mean_probability": float(probability[mask].mean()) if mask.any() else None,
            "observed_rate": float(labels[mask].mean()) if mask.any() else None,
        })
    return pd.DataFrame(rows)


@st.cache_data(show_spinner=False)
def load_health(root: str) -> dict[str, Any]:
    """The published Gold health summary, validated before display."""
    path = Path(root) / "gold" / "_reports" / "pipeline_health.json"
    if not path.exists():
        raise FileNotFoundError(
            f"{path} is missing; run `python -m delta_crops.dashboard.health`"
        )
    health = json.loads(path.read_text(encoding="utf-8"))
    quality = health.get("quality") or {}
    if health.get("schema_version") != 1:
        raise ValueError("unexpected pipeline_health.json schema_version")
    if quality.get("status") not in {"passed", "failed"}:
        raise ValueError("pipeline_health.json has no quality status")
    if not quality.get("checks"):
        raise ValueError("pipeline_health.json has no quality checks")
    return health


@st.cache_data(show_spinner=False)
def s3_scoreboard(artifacts: str) -> dict[str, Any]:
    """S3 metrics, the frozen bars and the pass/fail conditions, from artifacts."""
    temporal = json.loads((Path(artifacts) / "xgb_temporal.json").read_text(encoding="utf-8"))
    baselines = json.loads((Path(artifacts) / "baselines_metrics.json").read_text(encoding="utf-8"))
    b3 = baselines["reports"][0]["groups"]["cropland/pooled"]["models"]["B3"]
    bars = {
        "pr_auc": b3["pr_auc"] + 0.05,
        "precision_at_recall_0_5": b3["precision_at_recall_0_5"],
        "brier": b3["brier"],
    }
    variants = {}
    for name, entry in temporal["variants"].items():
        metrics = entry["groups"]["cropland/pooled"]["models"]["M"]
        passed = {
            "pr_auc": metrics["pr_auc"] >= bars["pr_auc"],
            "precision_at_recall_0_5": metrics["precision_at_recall_0_5"] > bars["precision_at_recall_0_5"],
            "brier": metrics["brier"] < bars["brier"],
        }
        variants[name] = {"metrics": metrics, "passed": passed, "rounds": entry["rounds"]["best_round"]}
    return {"b3": b3, "bars": bars, "variants": variants, "rows": temporal["variants"]["full"]["groups"]["cropland/pooled"]["rows"]}


@st.cache_data(show_spinner=False)
def available_years(root: str) -> list[int]:
    """Seasons present as ``gold/weekly_ndvi/year=*/`` partitions, ascending."""
    base = Path(root) / "gold" / "weekly_ndvi"
    years = [
        int(path.name.split("=", 1)[1])
        for path in base.glob("year=*")
        if path.is_dir() and path.name.split("=", 1)[1].isdigit()
    ]
    return sorted(years)


@st.cache_data(show_spinner=False)
def s5_summary(artifacts: str) -> dict[str, Any]:
    """S5 verdict plus whether a reliability table exists at all.

    The spatial artifact nests its result under ``summary``: the headline group
    name, the per-variant point estimates and bars, and the paired bootstrap
    difference. There is no top-level ``verdict`` key, and there is no
    reliability block at all — the latter is the documented gap.
    """
    spatial_path = Path(artifacts) / "xgb_spatial.json"
    text = spatial_path.read_text(encoding="utf-8")
    spatial = json.loads(text)
    summary = spatial["summary"]
    full = summary["variants"]["full"]
    return {
        "headline": summary["headline"],
        "variants": summary["variants"],
        "b3": summary["b3"],
        "paired": summary["paired_difference_full_minus_B3"]["pr_auc"],
        "reliability_measured": "reliab" in text.lower(),
        "blocks_scored": len(spatial["per_block_cropland_pooled"]),
        "blocks_declared": spatial["partition"]["n_heldout_blocks"],
        "full_point": full["point"],
        "full_pr_auc_bar": full["pr_auc_bar"],
    }

"""Weekly NDVI per 100 m cell: the first Gold table.

One row per (cell, ISO week) for every kept cell and every configured week, so a
missing week is an explicit NULL plus a status, never an absent row and never a
filled value. Week = ISO Monday-Sunday.
"""

from __future__ import annotations

import os
import time
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from delta_crops.config import Config, Gold, require_gold

WEEKLY_SCHEMA = pa.schema(
    [
        ("cell_id", pa.int32()),
        ("week_start", pa.date32()),
        ("ndvi_mean", pa.float32()),
        ("ndvi_std", pa.float32()),
        ("n_obs", pa.uint8()),
        ("masked_pct_mean", pa.float32()),
        ("status", pa.string()),
        ("orbit_mix", pa.string()),
        ("n_orbits", pa.uint8()),
    ]
)

#: One status rule, used by the build SQL and rendered by the tests.
STATUS_CASE = (
    "CASE WHEN coalesce({n_obs}, 0) < {min_obs} THEN 'insufficient_observations' "
    "WHEN {masked} > {max_masked} THEN 'too_cloudy' ELSE 'ok' END"
)

#: A weekly value exists only when the week passes both gates; otherwise NULL.
OK_CASE = "coalesce({n_obs}, 0) >= {min_obs} AND {masked} <= {max_masked}"


def iso_week_start(day: date) -> date:
    """Monday of the ISO week containing ``day``."""
    return day - timedelta(days=day.weekday())


def season_weeks(year: int, first: int, last: int) -> list[date]:
    """Sorted Monday starts of ISO weeks ``first``..``last`` inside one season."""
    start = date(year, 5, 1)
    end = date(year, 9, 30)
    weeks: set[date] = set()
    day = start
    while day <= end:
        if first <= day.isocalendar().week <= last:
            weeks.add(iso_week_start(day))
        day += timedelta(days=1)
    return sorted(weeks)


def weekly_path(root: Path, year: int) -> Path:
    return root / "gold" / "weekly_ndvi" / f"year={year}" / "weekly_ndvi.parquet"


def anomaly_path(root: Path, year: int) -> Path:
    return root / "gold" / "ndvi_anomaly" / f"year={year}" / "ndvi_anomaly.parquet"


def weather_path(root: Path, year: int) -> Path:
    return root / "gold" / "weekly_weather" / f"year={year}" / "weekly_weather.parquet"


def _connect(gold: Gold) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET threads = ?", [gold.duckdb_threads]).fetchall()
    con.execute("SET memory_limit = ?", [gold.duckdb_memory_limit]).fetchall()
    return con


def _orbits_relation(con: duckdb.DuckDBPyConnection, cache: Path) -> str:
    """Register one relative orbit per date; a conflicting date is a hard error."""
    if not cache.is_file():
        raise FileNotFoundError(
            f"missing orbit cache {cache}; run `python -m delta_crops.gold.cli orbits` first"
        )
    table = pq.read_table(cache, columns=["date", "relative_orbit"])
    con.register("orbit_cache", table)
    conflicts = con.execute(
        """SELECT count(*) FROM (
               SELECT date FROM orbit_cache GROUP BY date
               HAVING count(DISTINCT relative_orbit) > 1)"""
    ).fetchall()[0][0]
    if conflicts:
        raise ValueError(f"{conflicts} dates carry more than one relative orbit in {cache}")
    con.execute(
        """CREATE OR REPLACE TEMP TABLE date_orbits AS
           SELECT DISTINCT date, 'R' || lpad(CAST(relative_orbit AS VARCHAR), 3, '0') AS label
           FROM orbit_cache"""
    ).fetchall()
    return "date_orbits"


def build_weekly_ndvi(
    root: Path,
    config: Config,
    years: list[int] | None = None,
    force: bool = False,
) -> dict:
    """Write `gold/weekly_ndvi/year=Y/` for each year; skip valid existing output."""
    gold = require_gold(config)
    years = list(years or config.seasons.years)
    cells = pq.read_table(root / "silver" / "cells.parquet", columns=["cell_id"])
    kept = len(cells)
    if not kept:
        raise ValueError("silver/cells.parquet is empty")
    summary: dict = {"years": [], "seconds": 0.0}
    started = time.perf_counter()
    con = _connect(gold)
    try:
        con.register("cells", cells)
        _orbits_relation(con, root / gold.orbit_cache)
        for year in years:
            weeks = season_weeks(year, gold.week_first, gold.week_last)
            con.register("weeks", pa.table({"week_start": pa.array(weeks, pa.date32())}))
            target = weekly_path(root, year)
            expected = kept * len(weeks)
            if not force and target.is_file() and pq.ParquetFile(target).metadata.num_rows == expected:
                summary["years"].append(dict(year=year, rows=expected, skipped=True))
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            glob = str(root / "silver" / "ndvi" / f"year={year}" / "**" / "*.parquet")
            started_year = time.perf_counter()
            con.execute("CREATE OR REPLACE TEMP TABLE obs AS "
                "WITH per_date AS ("
                "  SELECT cell_id, date, date_trunc('week', date) AS week_start, ndvi, masked_pct"
                "  FROM read_parquet(?) WHERE week(date) BETWEEN ? AND ?"
                "), orbited AS ("
                "  SELECT p.*, o.label AS orbit FROM per_date p"
                "  LEFT JOIN date_orbits o USING (date)"
                ") "
                "SELECT cell_id, week_start,"
                "  count(DISTINCT date) FILTER (WHERE ndvi IS NOT NULL) AS n_obs,"
                "  avg(ndvi) AS ndvi_mean,"
                "  stddev_samp(ndvi) AS ndvi_std,"
                "  avg(masked_pct) AS masked_pct_mean,"
                "  string_agg(DISTINCT orbit, '|' ORDER BY orbit)"
                "      FILTER (WHERE ndvi IS NOT NULL AND orbit IS NOT NULL) AS orbit_mix,"
                "  count(DISTINCT orbit) FILTER (WHERE ndvi IS NOT NULL AND orbit IS NOT NULL)"
                "      AS n_orbits "
                "FROM orbited GROUP BY 1, 2",
                [glob, gold.week_first, gold.week_last]).fetchall()
            as_of = date(year, 9, 30)
            status = (f"CASE WHEN w.week_start + INTERVAL 6 DAY > DATE '{as_of.isoformat()}' "
                      "THEN 'not_yet_observed' ELSE " + STATUS_CASE.format(
                n_obs="o.n_obs", min_obs=gold.min_observations,
                masked="o.masked_pct_mean", max_masked=gold.max_masked_pct,
            ) + " END")
            ok = OK_CASE.format(
                n_obs="o.n_obs", min_obs=gold.min_observations,
                masked="o.masked_pct_mean", max_masked=gold.max_masked_pct,
            )
            tmp = target.with_suffix(".parquet.tmp")
            try:
                con.execute(
                    "COPY (SELECT c.cell_id, w.week_start,"
                    f" CAST(CASE WHEN {ok} THEN o.ndvi_mean END AS REAL) AS ndvi_mean,"
                    f" CAST(CASE WHEN {ok} THEN o.ndvi_std END AS REAL) AS ndvi_std,"
                    " CAST(coalesce(o.n_obs, 0) AS UTINYINT) AS n_obs,"
                    " CAST(o.masked_pct_mean AS REAL) AS masked_pct_mean,"
                    f" {status} AS status,"
                    " o.orbit_mix,"
                    " CAST(coalesce(o.n_orbits, 0) AS UTINYINT) AS n_orbits"
                    " FROM cells c CROSS JOIN weeks w"
                    " LEFT JOIN obs o ON o.cell_id = c.cell_id AND o.week_start = w.week_start"
                    " ORDER BY c.cell_id, w.week_start)"
                    " TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
                    [str(tmp)],
                ).fetchall()
                rows = pq.ParquetFile(tmp).metadata.num_rows
                if rows != expected:
                    raise ValueError(f"{year}: wrote {rows} rows, expected {expected}")
                os.replace(tmp, target)
            finally:
                tmp.unlink(missing_ok=True)
            summary["years"].append(dict(year=year, rows=expected,
                seconds=round(time.perf_counter() - started_year, 2), skipped=False))
    finally:
        con.close()
    summary["seconds"] = round(time.perf_counter() - started, 2)
    return summary

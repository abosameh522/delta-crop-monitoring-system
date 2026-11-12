"""Phase 7 dataset: labels and features for the 2-week vegetation-stress target.

Target (frozen before training): the mean **raw** NDVI anomaly over the next two
ISO weeks (t+1, t+2) is <= ``TARGET_NDVI_ANOMALY``, with both weeks observed
(``status = 'ok'``). Raw anomaly, never z-score.

Baseline rules inside one split (D-044 recomputed for Phase 7):

* a **training** row subtracts a leave-one-training-year-out baseline: the same
  cell and ISO week in the other training seasons;
* a **test** row subtracts the baseline built from **all** training seasons;
* both need at least ``min_baseline_years`` usable seasons (3 by default), and
  a row is kept only when the **current** week also has a baseline, so
  ``anomaly_t`` is always defined (the same rule the stored Gold anomaly uses).

No row's baseline ever contains its own season, and no test season ever enters a
baseline. Features only use observations from the current season at or before
week ``t`` plus baselines from training seasons, which are in the past for a test
season.

The dataset is written to Parquet under ``artifacts/`` (gitignored), never under
``data/``.
"""

from __future__ import annotations

import argparse
import json
import resource
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import duckdb

from delta_crops.config import Config, load_config

#: Frozen physical NDVI amount, fixed before any model was trained.
TARGET_NDVI_ANOMALY = -0.08
#: Both t+1 and t+2 must be observed; no "next two available observations".
HORIZON_WEEKS = 2
#: One 5 km block at the 100 m grid (50 x 50 cells), used by Phase 7 evaluation.
BLOCK_CELLS = 50

FEATURES = (
    "ndvi_mean", "ndvi_std", "masked_pct_mean", "n_obs",
    "ndvi_lag1", "ndvi_lag2", "ndvi_lag3",
    "ndvi_roll2", "ndvi_roll3", "ndvi_slope_2w",
    "anomaly_t", "baseline_mean", "n_baseline_years",
    "temp_max_mean_c", "temp_max_max_c", "temp_min_mean_c",
    "solar_sum_mj_m2", "precip_sum_mm", "heatwave_days",
    "temp_max_mean_c_lag1", "temp_max_mean_c_lag2",
    "heatwave_days_lag1", "heatwave_days_2w",
    "week_index", "cropland_pct", "is_cropland",
)


@dataclass(frozen=True)
class Split:
    """One temporal split: train seasons and the season held out for testing."""

    name: str
    train_years: tuple[int, ...]
    test_year: int

    def __post_init__(self) -> None:
        if not self.train_years:
            raise ValueError("a split needs at least one training season")
        if self.test_year in self.train_years:
            raise ValueError(f"test season {self.test_year} must not be a training season")
        if len(set(self.train_years)) != len(self.train_years):
            raise ValueError("training seasons must be unique")

    @property
    def years(self) -> tuple[int, ...]:
        return (*self.train_years, self.test_year)


def baseline_years(split: Split, row_year: int) -> tuple[int, ...]:
    """Seasons allowed in the climatology of a row.

    Training rows exclude their own season (leave-one-out); test rows use every
    training season. Raises if ``row_year`` is outside the split.
    """
    if row_year == split.test_year:
        return split.train_years
    if row_year in split.train_years:
        return tuple(year for year in split.train_years if year != row_year)
    raise ValueError(f"season {row_year} is not part of split {split.name}")


def temporal_split(years: tuple[int, ...], test_year: int | None = None) -> Split:
    """Hold out the newest season; train on the others."""
    years = tuple(sorted(years))
    test = years[-1] if test_year is None else int(test_year)
    return Split(name="temporal", train_years=tuple(y for y in years if y != test), test_year=test)


def loso_splits(years: tuple[int, ...]) -> list[Split]:
    """One split per season: train on the other seasons, test on this one."""
    years = tuple(sorted(years))
    return [
        Split(name=f"loso-{year}", train_years=tuple(y for y in years if y != year), test_year=year)
        for year in years
    ]


def _year_list(years: tuple[int, ...]) -> str:
    return ", ".join(str(int(year)) for year in years)


def _truncate_clause(truncate: tuple[int, date] | None) -> str:
    if truncate is None:
        return ""
    year, week_start = truncate
    return f"WHERE NOT (year = {int(year)} AND week_start > DATE '{week_start.isoformat()}')"


def build_sql(
    root: Path,
    config: Config,
    split: Split,
    truncate: tuple[int, date] | None = None,
    require_target: bool = True,
) -> str:
    """Return the SQL that builds one split's label/feature table.

    ``truncate=(season, week_start)`` drops every observation from that season
    after ``week_start``. It exists for the leakage test, which rebuilds a row's
    features from an input view that could not have known the future.
    ``require_target=False`` keeps rows whose next two weeks are unknown; the
    leakage test needs those rows to compare feature values at the cut-off.
    """
    min_years = int(config.gold.min_baseline_years) if config.gold else 3
    gold = root / "gold"
    weekly = str(gold / "weekly_ndvi" / "year=*" / "*.parquet")
    weather = str(gold / "weekly_weather" / "year=*" / "*.parquet")
    anomaly = str(gold / "ndvi_anomaly" / "year=*" / "*.parquet")
    cells = str(root / "silver" / "cells.parquet")
    all_years = _year_list(split.years)
    train_years = _year_list(split.train_years)
    cutoff = _truncate_clause(truncate)
    cutoff_weather = _truncate_clause(truncate)
    threshold = TARGET_NDVI_ANOMALY
    target_filter = (
        """
      AND w.fwd_a1 IS NOT NULL AND w.fwd_a2 IS NOT NULL
      AND w.fwd_s1 = 'ok' AND w.fwd_s2 = 'ok'
      AND w.fwd_w1 = CAST(w.week_start + INTERVAL 7 DAY AS DATE)
      AND w.fwd_w2 = CAST(w.week_start + INTERVAL 14 DAY AS DATE)"""
        if require_target
        else ""
    )

    return f"""
WITH ndvi AS (
    SELECT year AS season, cell_id, week_start, week(week_start) AS iso_week,
           ndvi_mean, ndvi_std, n_obs, masked_pct_mean, status
    FROM read_parquet('{weekly}', hive_partitioning = true)
    {cutoff}
), wx AS (
    SELECT year AS season, week_start, temp_max_mean_c, temp_max_max_c,
           temp_min_mean_c, precip_sum_mm, solar_sum_mj_m2, heatwave_days
    FROM read_parquet('{weather}', hive_partitioning = true)
    {cutoff_weather}
), train_obs AS (
    SELECT season, cell_id, iso_week, ndvi_mean
    FROM ndvi
    WHERE status = 'ok' AND season IN ({train_years})
), totals AS (
    SELECT cell_id, iso_week, sum(ndvi_mean) AS total, count(*) AS n
    FROM train_obs
    GROUP BY 1, 2
), baseline AS (
    SELECT o.season, o.cell_id, o.iso_week,
           (t.total - o.ndvi_mean) / (t.n - 1) AS base_mean,
           t.n - 1 AS n_years
    FROM train_obs o JOIN totals t USING (cell_id, iso_week)
    WHERE t.n > 1
    UNION ALL
    SELECT {int(split.test_year)} AS season, t.cell_id, t.iso_week,
           t.total / t.n AS base_mean,
           t.n AS n_years
    FROM totals t
), with_base AS (
    SELECT n.season, n.cell_id, n.week_start, n.iso_week,
           n.ndvi_mean, n.ndvi_std, n.n_obs, n.masked_pct_mean, n.status,
           b.base_mean, b.n_years,
           CASE WHEN n.status = 'ok' AND b.n_years >= {min_years}
                THEN n.ndvi_mean - b.base_mean END AS anomaly
    FROM ndvi n JOIN baseline b USING (season, cell_id, iso_week)
    WHERE n.season IN ({all_years})
), windowed AS (
    SELECT w.*,
           LAG(w.ndvi_mean, 1) OVER p AS ndvi_lag1,
           LAG(w.ndvi_mean, 2) OVER p AS ndvi_lag2,
           LAG(w.ndvi_mean, 3) OVER p AS ndvi_lag3,
           LAG(w.anomaly, 1) OVER p AS anomaly_lag1,
           LEAD(w.anomaly, 1) OVER p AS fwd_a1,
           LEAD(w.anomaly, 2) OVER p AS fwd_a2,
           LEAD(w.status, 1) OVER p AS fwd_s1,
           LEAD(w.status, 2) OVER p AS fwd_s2,
           LEAD(w.week_start, 1) OVER p AS fwd_w1,
           LEAD(w.week_start, 2) OVER p AS fwd_w2
    FROM with_base w
    WINDOW p AS (PARTITION BY w.season, w.cell_id ORDER BY w.week_start)
), label_rows AS (
    SELECT w.*,
           (w.fwd_a1 + w.fwd_a2) / 2.0 AS fwd_anomaly_2w
    FROM windowed w
    WHERE w.status = 'ok' AND w.anomaly IS NOT NULL{target_filter}
), weather_lag AS (
    SELECT season, week_start,
           temp_max_mean_c, temp_max_max_c, temp_min_mean_c,
           precip_sum_mm, solar_sum_mj_m2, heatwave_days,
           LAG(temp_max_mean_c, 1) OVER q AS temp_max_mean_c_lag1,
           LAG(temp_max_mean_c, 2) OVER q AS temp_max_mean_c_lag2,
           LAG(heatwave_days, 1) OVER q AS heatwave_days_lag1,
           heatwave_days + coalesce(LAG(heatwave_days, 1) OVER q, 0) AS heatwave_days_2w
    FROM wx
    WINDOW q AS (PARTITION BY season ORDER BY week_start)
), stored AS (
    SELECT year AS season, cell_id, week_start, anomaly, status,
           LEAD(anomaly, 1) OVER sp AS a1,
           LEAD(anomaly, 2) OVER sp AS a2,
           LEAD(status, 1) OVER sp AS s1,
           LEAD(status, 2) OVER sp AS s2
    FROM read_parquet('{anomaly}', hive_partitioning = true)
    WHERE year IN ({all_years})
    WINDOW sp AS (PARTITION BY year, cell_id ORDER BY week_start)
), stored_fwd AS (
    SELECT season, cell_id, week_start,
           (a1 + a2) / 2.0 AS fwd_anomaly_2w_stored
    FROM stored
    WHERE a1 IS NOT NULL AND a2 IS NOT NULL AND s1 = 'ok' AND s2 = 'ok'
)
SELECT
    l.season AS year,
    l.cell_id,
    l.week_start,
    CASE WHEN l.season = {int(split.test_year)} THEN 'test' ELSE 'train' END AS split,
    CAST({int(split.test_year)} AS SMALLINT) AS test_year,
    CAST(CASE WHEN l.fwd_anomaly_2w IS NULL THEN NULL
              WHEN l.fwd_anomaly_2w <= {threshold} THEN 1 ELSE 0 END AS TINYINT) AS label,
    CAST(l.fwd_anomaly_2w AS FLOAT) AS fwd_anomaly_2w,
    CAST(l.anomaly AS FLOAT) AS anomaly_t,
    CAST(l.base_mean AS FLOAT) AS baseline_mean,
    CAST(l.n_years AS TINYINT) AS n_baseline_years,
    CAST(l.ndvi_mean AS FLOAT) AS ndvi_mean,
    CAST(l.ndvi_std AS FLOAT) AS ndvi_std,
    CAST(l.masked_pct_mean AS FLOAT) AS masked_pct_mean,
    CAST(l.n_obs AS TINYINT) AS n_obs,
    CAST(l.ndvi_lag1 AS FLOAT) AS ndvi_lag1,
    CAST(l.ndvi_lag2 AS FLOAT) AS ndvi_lag2,
    CAST(l.ndvi_lag3 AS FLOAT) AS ndvi_lag3,
    CAST(l.anomaly_lag1 AS FLOAT) AS anomaly_lag1,
    CAST((l.ndvi_mean + l.ndvi_lag1) / 2.0 AS FLOAT) AS ndvi_roll2,
    CAST((l.ndvi_mean + l.ndvi_lag1 + l.ndvi_lag2) / 3.0 AS FLOAT) AS ndvi_roll3,
    CAST((l.ndvi_mean - l.ndvi_lag2) / 2.0 AS FLOAT) AS ndvi_slope_2w,
    CAST(l.iso_week - 18 AS SMALLINT) AS week_index,
    CAST(wl.temp_max_mean_c AS FLOAT) AS temp_max_mean_c,
    CAST(wl.temp_max_max_c AS FLOAT) AS temp_max_max_c,
    CAST(wl.temp_min_mean_c AS FLOAT) AS temp_min_mean_c,
    CAST(wl.solar_sum_mj_m2 AS FLOAT) AS solar_sum_mj_m2,
    CAST(wl.precip_sum_mm AS FLOAT) AS precip_sum_mm,
    CAST(wl.heatwave_days AS TINYINT) AS heatwave_days,
    CAST(wl.temp_max_mean_c_lag1 AS FLOAT) AS temp_max_mean_c_lag1,
    CAST(wl.temp_max_mean_c_lag2 AS FLOAT) AS temp_max_mean_c_lag2,
    CAST(wl.heatwave_days_lag1 AS TINYINT) AS heatwave_days_lag1,
    CAST(wl.heatwave_days_2w AS TINYINT) AS heatwave_days_2w,
    CAST(c.cropland_pct AS FLOAT) AS cropland_pct,
    CAST(c.is_cropland AS BOOLEAN) AS is_cropland,
    CAST((CAST(c.cell_row AS INTEGER) // {BLOCK_CELLS}) * 10000
         + (CAST(c.cell_col AS INTEGER) // {BLOCK_CELLS}) AS INTEGER) AS block_5km,
    CAST(sf.fwd_anomaly_2w_stored AS FLOAT) AS fwd_anomaly_2w_stored
FROM label_rows l
JOIN read_parquet('{cells}') c USING (cell_id)
LEFT JOIN weather_lag wl ON wl.season = l.season AND wl.week_start = l.week_start
LEFT JOIN stored_fwd sf
       ON sf.season = l.season AND sf.cell_id = l.cell_id AND sf.week_start = l.week_start
ORDER BY l.season, l.cell_id, l.week_start
"""


def connect(config: Config, temp_dir: Path | None = None) -> duckdb.DuckDBPyConnection:
    """Open DuckDB with the project's Gold limits and an explicit spill directory."""
    con = duckdb.connect()
    if config.gold:
        con.execute("SET threads = ?", [config.gold.duckdb_threads]).fetchall()
        con.execute("SET memory_limit = ?", [config.gold.duckdb_memory_limit]).fetchall()
    if temp_dir is not None:
        temp_dir.mkdir(parents=True, exist_ok=True)
        con.execute("SET temp_directory = ?", [str(temp_dir)]).fetchall()
    return con


def balance_sql() -> str:
    """Per-season label balance, recomputed next to the stored anomaly."""
    return f"""
SELECT year, split, count(*) AS rows,
       CAST(sum(label) AS BIGINT) AS positives,
       round(100.0 * avg(label), 2) AS positive_pct,
       count(fwd_anomaly_2w_stored) AS rows_with_stored,
       round(100.0 * avg(CASE WHEN fwd_anomaly_2w_stored <= {TARGET_NDVI_ANOMALY}
                         THEN 1.0 ELSE 0.0 END)
             FILTER (WHERE fwd_anomaly_2w_stored IS NOT NULL), 2) AS stored_positive_pct
FROM ({{sql}}) t
GROUP BY year, split
ORDER BY year
"""


def summarise(con: duckdb.DuckDBPyConnection, sql: str) -> list[dict[str, Any]]:
    """Return the per-season balance rows plus the population totals."""
    rows = con.sql(balance_sql().format(sql=sql)).to_arrow_table().to_pylist()
    for row in rows:
        recomputed = row["positive_pct"]
        stored = row["stored_positive_pct"]
        row["shift_points"] = (
            None if stored is None or recomputed is None else round(recomputed - stored, 2)
        )
    return rows


def build(
    root: Path,
    config: Config,
    split: Split,
    out_dir: Path | None = None,
    temp_dir: Path | None = None,
) -> dict[str, Any]:
    """Build one split, optionally write Parquet, and return a measured summary."""
    started = time.perf_counter()
    con = connect(config, temp_dir)
    try:
        sql = build_sql(root, config, split)
        seasons = summarise(con, sql)
        result: dict[str, Any] = {
            "split": {"name": split.name, "train_years": list(split.train_years),
                      "test_year": split.test_year},
            "rule": {"threshold": TARGET_NDVI_ANOMALY, "horizon_weeks": HORIZON_WEEKS,
                     "min_baseline_years": config.gold.min_baseline_years if config.gold else 3},
            "seasons": seasons,
            "rows": sum(row["rows"] for row in seasons),
            "positives": sum(row["positives"] for row in seasons),
        }
        result["positive_pct"] = round(
            100.0 * result["positives"] / result["rows"], 2
        ) if result["rows"] else None
        if out_dir is not None:
            target = out_dir / f"{split.name}.parquet"
            target.parent.mkdir(parents=True, exist_ok=True)
            tmp = target.with_suffix(".parquet.tmp")
            columns = ", ".join(
                ["year", "cell_id", "week_start", "split", "test_year", "label"]
                + list(FEATURES) + ["anomaly_lag1", "block_5km", "fwd_anomaly_2w"]
            )
            con.execute(
                f"COPY (SELECT {columns} FROM ({sql}) t ORDER BY year, cell_id, week_start) "
                f"TO '{tmp}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880)"
            ).fetchall()
            tmp.replace(target)
            result["path"] = str(target)
            result["bytes"] = target.stat().st_size
        result["seconds"] = round(time.perf_counter() - started, 2)
        result["peak_rss_mb"] = round(
            resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0, 1
        )
        return result
    finally:
        con.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=None, help="AOI config path")
    parser.add_argument("--data-dir", default=None, help="data lake root (default: ./data)")
    parser.add_argument("--out", default=None, help="write <split>.parquet under this directory")
    parser.add_argument("--summary-json", default=None, help="write the measured summary here")
    parser.add_argument("--train-years", type=int, nargs="+", default=None)
    parser.add_argument("--test-year", type=int, default=None)
    args = parser.parse_args(argv)

    config = load_config(args.config)
    root = Path(args.data_dir) if args.data_dir else Path("data")
    years = tuple(args.train_years or config.seasons.years)
    if args.train_years:
        test = int(args.test_year or max(years))
        split = Split(name="temporal", train_years=years, test_year=test)
    else:
        split = temporal_split(config.seasons.years, args.test_year)

    summary = build(
        root, config, split,
        out_dir=Path(args.out) if args.out else None,
        temp_dir=Path("/tmp/delta_crops_phase7"),
    )
    text = json.dumps(summary, indent=2)
    print(text, flush=True)
    if args.summary_json:
        path = Path(args.summary_json)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

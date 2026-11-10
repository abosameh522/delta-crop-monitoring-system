"""Phase 5 gate: decompose the cell-week NULL share before Gold is built.

Two causes are separated per season and population (all kept cells, cropland):
structural weeks (fewer than the required AOI-wide acquisition dates, so every
cell is NULL by construction) and per-cell loss inside weeks that do have enough
AOI acquisitions. NULL-NDVI cell-dates with masked_pct < 100 are counted too, so
"NULL" is never confused with "no data at all".
"""

from __future__ import annotations

import json
import time
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import uuid4

import duckdb
import pyarrow as pa

from delta_crops.config import Config, require_gold
from delta_crops.gold.weekly import iso_week_start, season_weeks
from delta_crops.transform.bronze_reader import imagery_dates, load_ledger


class WeeklyGateFailure(RuntimeError):
    """Raised when a season's weekly NULL share exceeds the configured gate."""


def weekly_gate(root: Path, config: Config, write: bool = True) -> dict:
    """Measure and return the gate report; raise if any season exceeds the cap."""
    gold = require_gold(config)
    started = time.perf_counter()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8]
    ledger = load_ledger(root)
    acquisitions: dict[int, set[date]] = {}
    for day in imagery_dates(ledger):
        start, end = config.seasons.bounds(day.year)
        if start <= day <= end:
            acquisitions.setdefault(day.year, set()).add(day)

    week_rows: list[dict] = []
    for year in config.seasons.years:
        weeks = season_weeks(year, gold.week_first, gold.week_last)
        for week_start in weeks:
            count = sum(1 for day in acquisitions.get(year, set()) if iso_week_start(day) == week_start)
            week_rows.append(dict(season=year, week_start=week_start, acquisitions=count,
                                  structural=count < gold.min_observations))

    cells_path = root / "silver" / "cells.parquet"
    cells = duckdb.connect().execute(
        "SELECT count(*) AS kept, count(*) FILTER (WHERE is_cropland) AS cropland FROM read_parquet(?)",
        [str(cells_path)],
    ).fetchall()[0]
    kept, cropland = int(cells[0]), int(cells[1])
    if not kept:
        raise ValueError("silver/cells.parquet is empty")

    con = duckdb.connect()
    con.execute("SET threads = ?", [gold.duckdb_threads]).fetchall()
    con.execute("SET memory_limit = ?", [gold.duckdb_memory_limit]).fetchall()
    try:
        con.register("week_flags", pa.Table.from_pylist(week_rows))
        glob = str(root / "silver" / "ndvi" / "year=*" / "**" / "*.parquet")
        con.execute("CREATE OR REPLACE TEMP TABLE obs AS "
            "SELECT year(o.date) AS season, o.cell_id, date_trunc('week', o.date) AS week_start,"
            " c.is_cropland,"
            " count(DISTINCT o.date) FILTER (WHERE o.ndvi IS NOT NULL) AS n_obs,"
            " avg(o.masked_pct) AS masked_mean "
            "FROM read_parquet(?) o JOIN read_parquet(?) c USING (cell_id) "
            "WHERE week(o.date) BETWEEN ? AND ? "
            "GROUP BY 1, 2, 3, 4",
            [glob, str(cells_path), gold.week_first, gold.week_last]).fetchall()
        diagnostics = {row[0]: dict(
            null_cell_dates=int(row[1]), null_partial_masked=int(row[2]),
            null_full_masked=int(row[3]),
            mean_masked_pct_on_null=None if row[4] is None else round(float(row[4]), 2),
        ) for row in con.execute(
            "SELECT year(date), count(*) FILTER (WHERE ndvi IS NULL),"
            " count(*) FILTER (WHERE ndvi IS NULL AND masked_pct < 100),"
            " count(*) FILTER (WHERE ndvi IS NULL AND masked_pct = 100),"
            " avg(masked_pct) FILTER (WHERE ndvi IS NULL) "
            "FROM read_parquet(?) WHERE week(date) BETWEEN ? AND ? GROUP BY 1 ORDER BY 1",
            [glob, gold.week_first, gold.week_last]).fetchall()}

        seasons = []
        for year in config.seasons.years:
            weeks = [row for row in week_rows if row["season"] == year]
            structural_weeks = sum(row["structural"] for row in weeks)
            row: dict = dict(year=year, weeks=len(weeks), structural_weeks=structural_weeks,
                             populations={})
            for label, predicate, population_cells in (
                ("all", "TRUE", kept), ("cropland", "o.is_cropland", cropland)
            ):
                structural_cell_weeks = structural_weeks * population_cells
                result = con.execute(
                    "SELECT count(*),"
                    " count(*) FILTER (WHERE o.n_obs < ?),"
                    " count(*) FILTER (WHERE o.n_obs >= ? AND o.masked_mean > ?),"
                    " count(*) FILTER (WHERE o.n_obs < ? OR o.masked_mean > ?) "
                    "FROM obs o JOIN week_flags f USING (season, week_start) "
                    f"WHERE NOT f.structural AND o.season = ? AND {predicate}",
                    [gold.min_observations, gold.min_observations, gold.max_masked_pct,
                     gold.min_observations, gold.max_masked_pct, year]).fetchall()[0]
                nonstructural, insufficient, too_cloudy, null_rows = (int(v) for v in result)
                nonstructural_weeks = len(weeks) - structural_weeks
                expected = nonstructural_weeks * population_cells
                if nonstructural != expected:
                    raise ValueError(
                        f"{year} {label}: {nonstructural} non-structural cell-weeks, expected {expected}"
                    )
                total_cell_weeks = len(weeks) * population_cells
                total_null = structural_cell_weeks + null_rows
                row["populations"][label] = dict(
                    cells=population_cells,
                    structural_cell_weeks=structural_cell_weeks,
                    structural_share_pct=round(100 * structural_cell_weeks / total_cell_weeks, 2),
                    nonstructural_cell_weeks=nonstructural,
                    nonstructural_null=null_rows,
                    nonstructural_null_pct=round(100 * null_rows / nonstructural, 2),
                    insufficient_observations=insufficient,
                    too_cloudy=too_cloudy,
                    total_cell_weeks=total_cell_weeks,
                    total_null=total_null,
                    total_null_pct=round(100 * total_null / total_cell_weeks, 2),
                )
            row["null_cell_dates"] = diagnostics.get(year)
            seasons.append(row)

        failing = [dict(year=row["year"], population=label, null_pct=values["total_null_pct"])
                   for row in seasons for label, values in row["populations"].items()
                   if values["total_null_pct"] > gold.max_gate_null_pct]
        report = dict(run_id=run_id, status="failed" if failing else "passed",
                      gate_max_null_pct=gold.max_gate_null_pct, failing=failing,
                      min_observations=gold.min_observations, max_masked_pct=gold.max_masked_pct,
                      weeks=[gold.week_first, gold.week_last], kept_cells=kept, cropland_cells=cropland,
                      seasons=seasons, timings=dict(total_seconds=None))
    finally:
        con.close()
    report["timings"]["total_seconds"] = round(time.perf_counter() - started, 2)
    if write:
        directory = root / "gold" / "_reports"
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"weekly_gate_{run_id}.json"
        path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        report["report_path"] = str(path)
    if failing:
        raise WeeklyGateFailure(
            "Weekly NULL share exceeds the gate: "
            + ", ".join(f"{f['year']} {f['population']} {f['null_pct']}%" for f in failing)
        )
    return report


def format_gate_table(report: dict) -> str:
    """Render the decomposition as a fixed-width table for the CLI."""
    lines = [
        f"weekly gate ({report['min_observations']}+ dates, masked <= {report['max_masked_pct']}%,"
        f" weeks {report['weeks'][0]}-{report['weeks'][1]}): {report['status'].upper()}",
        "",
        f"{'season':>7} {'pop':>9} {'struct%':>8} {'nonstruct':>9} {'rows':>12}"
        f" {'insuff':>11} {'cloudy':>9} {'total%':>7}",
    ]
    for row in report["seasons"]:
        for label, values in row["populations"].items():
            lines.append(
                f"{row['year']:>7} {label:>9} {values['structural_share_pct']:>7.2f}%"
                f" {values['nonstructural_null_pct']:>8.2f}% {values['nonstructural_null']:>12,}"
                f" {values['insufficient_observations']:>11,} {values['too_cloudy']:>9,}"
                f" {values['total_null_pct']:>6.2f}%"
            )
    return "\n".join(lines)

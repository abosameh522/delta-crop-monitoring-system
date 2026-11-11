"""Leave-one-year-out NDVI anomaly: descriptive Gold, never a model feature.

The baseline for a cell and ISO week is the mean of the same ISO week in the
other seasons, using only complete seasons (all weather days present) whose
weekly status is `ok`; no smoothing across weeks. An incomplete season never
enters another year's baseline. Phase 7 must recompute any baseline feature from
training years only (D-015, D-044).
"""

from __future__ import annotations

import os
import time
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from delta_crops.config import Config, require_gold
from delta_crops.gold.as_of import complete_years
from delta_crops.gold.fingerprint import (
    anomaly_fingerprint,
    fingerprint_path,
    fingerprints_match,
    read_fingerprint,
    verify_anomaly_inputs,
    write_fingerprint,
)
from delta_crops.gold.weekly import anomaly_path

ANOMALY_SCHEMA = pa.schema(
    [
        ("cell_id", pa.int32()),
        ("week_start", pa.date32()),
        ("ndvi_mean", pa.float32()),
        ("baseline_mean", pa.float32()),
        ("baseline_std", pa.float32()),
        ("anomaly", pa.float32()),
        ("zscore", pa.float32()),
        ("n_baseline_years", pa.uint8()),
        ("status", pa.string()),
    ]
)


def build_anomaly(
    root: Path,
    config: Config,
    years: list[int] | None = None,
    force: bool = False,
    adopt: bool = False,
) -> dict:
    """Write `gold/ndvi_anomaly/year=Y/` and its fingerprint; skip verified output."""
    gold = require_gold(config)
    years = list(years or config.seasons.years)
    complete = tuple(sorted(complete_years(root, config, config.seasons.years)))
    if complete:
        baseline_filter = "AND yr IN (" + ", ".join(str(y) for y in complete) + ")"
    else:
        baseline_filter = "AND FALSE"
    glob = str(root / "gold" / "weekly_ndvi" / "year=*" / "*.parquet")
    summary: dict = {
        "table": "ndvi_anomaly", "years": [], "errors": [],
        "built": 0, "skipped": 0, "rebuilt_due_to_input_change": 0,
        "unfingerprinted": 0, "adopted": 0, "failed": 0, "seconds": 0.0,
    }
    started = time.perf_counter()
    con = duckdb.connect()
    con.execute("SET threads = ?", [gold.duckdb_threads]).fetchall()
    con.execute("SET memory_limit = ?", [gold.duckdb_memory_limit]).fetchall()
    try:
        con.execute(
            "CREATE OR REPLACE TEMP VIEW all_weeks AS "
            f"SELECT *, year(week_start) AS yr, week(week_start) AS wk FROM read_parquet('{glob}')"
        ).fetchall()
        for year in years:
            target = anomaly_path(root, year)
            cur_rows = con.execute("SELECT count(*) FROM all_weeks WHERE yr = ?", [year]).fetchall()[0][0]
            if not cur_rows:
                raise ValueError(f"no weekly_ndvi rows for {year}; build weekly_ndvi first")
            stored = read_fingerprint(fingerprint_path(target))
            build_reason = None
            if not force and target.is_file():
                if stored is None and not adopt:
                    summary["unfingerprinted"] += 1
                    print(f"{year} anomaly skip (unfingerprinted)", flush=True)
                    summary["years"].append(dict(year=year, rows=cur_rows, status="unfingerprinted",
                                                 skipped=True, complete=year in complete))
                    continue
                if stored is None and adopt:
                    problem = verify_anomaly_inputs(root, year, complete)
                    if problem:
                        summary["failed"] += 1
                        summary["errors"].append(problem)
                        summary["years"].append(dict(year=year, rows=0, status="failed",
                                                     skipped=True, complete=year in complete))
                        continue
                    write_fingerprint(
                        fingerprint_path(target), anomaly_fingerprint(root, config, year, complete)
                    )
                    summary["adopted"] += 1
                    print(f"{year} anomaly adopt (verified inputs)", flush=True)
                    summary["years"].append(dict(year=year, rows=cur_rows, status="adopted",
                                                 skipped=True, complete=year in complete))
                    continue
                current = anomaly_fingerprint(root, config, year, complete)
                valid = pq.ParquetFile(target).metadata.num_rows == cur_rows
                if fingerprints_match(stored, current) and valid:
                    summary["skipped"] += 1
                    summary["years"].append(dict(year=year, rows=cur_rows, status="skipped",
                                                 skipped=True, complete=year in complete))
                    continue
                build_reason = ("input changed" if not fingerprints_match(stored, current)
                                else "invalid output")
            target.parent.mkdir(parents=True, exist_ok=True)
            tmp = target.with_suffix(".parquet.tmp")
            try:
                con.execute(
                    "COPY ("
                    "WITH base AS ("
                    "  SELECT cell_id, wk,"
                    "    count(DISTINCT yr) FILTER (WHERE status = 'ok') AS n_years,"
                    "    avg(ndvi_mean) FILTER (WHERE status = 'ok') AS bmean,"
                    "    stddev_samp(ndvi_mean) FILTER (WHERE status = 'ok') AS bstd"
                    "  FROM all_weeks WHERE yr <> ? " + baseline_filter + " GROUP BY 1, 2"
                    "), cur AS (SELECT * FROM all_weeks WHERE yr = ?) "
                    "SELECT c.cell_id, c.week_start,"
                    " CAST(c.ndvi_mean AS REAL) AS ndvi_mean,"
                    " CAST(b.bmean AS REAL) AS baseline_mean,"
                    " CAST(b.bstd AS REAL) AS baseline_std,"
                    " CAST(CASE WHEN c.status = 'ok' AND coalesce(b.n_years, 0) >= ?"
                    "      THEN c.ndvi_mean - b.bmean END AS REAL) AS anomaly,"
                    " CAST(CASE WHEN c.status = 'ok' AND coalesce(b.n_years, 0) >= ?"
                    "      AND b.bstd > 0 THEN (c.ndvi_mean - b.bmean) / b.bstd END AS REAL) AS zscore,"
                    " CAST(coalesce(b.n_years, 0) AS UTINYINT) AS n_baseline_years,"
                    " CASE WHEN c.status <> 'ok' THEN c.status"
                    "      WHEN coalesce(b.n_years, 0) < ? THEN 'no_baseline'"
                    "      ELSE 'ok' END AS status"
                    " FROM cur c LEFT JOIN base b USING (cell_id, wk)"
                    " ORDER BY c.cell_id, c.week_start)"
                    " TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
                    [year, year, gold.min_baseline_years, gold.min_baseline_years,
                     gold.min_baseline_years, str(tmp)],
                ).fetchall()
                rows = pq.ParquetFile(tmp).metadata.num_rows
                if rows != cur_rows:
                    raise ValueError(f"{year}: wrote {rows} anomaly rows, expected {cur_rows}")
                os.replace(tmp, target)
            finally:
                tmp.unlink(missing_ok=True)
            write_fingerprint(
                fingerprint_path(target), anomaly_fingerprint(root, config, year, complete)
            )
            if build_reason == "input changed":
                summary["rebuilt_due_to_input_change"] += 1
                status = "rebuilt_due_to_input_change"
            else:
                summary["built"] += 1
                status = "built"
            summary["years"].append(dict(year=year, rows=cur_rows, skipped=False,
                                         status=status, complete=year in complete))
    finally:
        con.close()
    summary["seconds"] = round(time.perf_counter() - started, 2)
    return summary

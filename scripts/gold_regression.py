#!/usr/bin/env python3
"""Gold regression test: rebuild Gold for 2019-2023 into a temp dir and compare
against data/_regression/gold_before using DuckDB EXCEPT in both directions
plus row counts. Exit 0 only if all differences are zero."""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb
import pyarrow.parquet as pq

from delta_crops.config import load_config
from delta_crops.gold import anomaly, weather, weekly


TABLES = ["weekly_ndvi", "ndvi_anomaly", "weekly_weather"]
YEARS = list(range(2019, 2024))


def _connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET threads = 4")
    con.execute("SET memory_limit = '8GB'")
    return con


def _compare_table(con: duckdb.DuckDBPyConnection, snapshot_root: Path, test_root: Path, table: str, year: int) -> tuple[int, int, int]:
    """Return (rows_snapshot, rows_test, diff_count) for one table/year."""
    snapshot_path = snapshot_root / table / f"year={year}" / f"{table}.parquet"
    test_path = test_root / "gold" / table / f"year={year}" / f"{table}.parquet"

    if not snapshot_path.is_file() or not test_path.is_file():
        return (-1, -1, -1)

    snap_rows = con.execute(f"SELECT count(*) FROM read_parquet('{snapshot_path}')").fetchall()[0][0]
    test_rows = con.execute(f"SELECT count(*) FROM read_parquet('{test_path}')").fetchall()[0][0]

    # EXCEPT both directions
    diff_a = con.execute(
        f"SELECT count(*) FROM (SELECT * FROM read_parquet('{snapshot_path}') EXCEPT SELECT * FROM read_parquet('{test_path}'))"
    ).fetchall()[0][0]
    diff_b = con.execute(
        f"SELECT count(*) FROM (SELECT * FROM read_parquet('{test_path}') EXCEPT SELECT * FROM read_parquet('{snapshot_path}'))"
    ).fetchall()[0][0]

    return (snap_rows, test_rows, diff_a + diff_b)


def rebuild_gold(temp_root: Path, data_root: Path, years: list[int], config) -> dict:
    """Rebuild Gold tables into temp_root/gold using the real Silver from data_root."""
    # Inputs remain read-only; every Gold output is rooted in TemporaryDirectory.
    (temp_root / "silver").symlink_to((data_root / "silver").resolve(), target_is_directory=True)
    weekly.build_weekly_ndvi(temp_root, config, years, force=True)
    anomaly.build_anomaly(temp_root, config, years, force=True)
    weather.build_weekly_weather(temp_root, config, force=True)


def run_regression(snapshot_root: Path, data_root: Path, years: list[int] | None = None) -> bool:
    years = years or YEARS
    config = load_config()

    print(f"Snapshot root: {snapshot_root}")
    print(f"Data root:     {data_root}")
    print(f"Years:         {years}")

    with TemporaryDirectory(prefix="gold_regression_") as tmp:
        temp_root = Path(tmp)
        print(f"Temp root:     {temp_root}")

        started = time.perf_counter()
        rebuild_gold(temp_root, data_root, years, config)
        build_s = time.perf_counter() - started
        print(f"Gold rebuilt in {build_s:.2f}s")

        con = _connect()
        all_ok = True

        for table in TABLES:
            for year in years:
                snap_rows, test_rows, diffs = _compare_table(con, snapshot_root, temp_root, table, year)
                if diffs == -1:
                    print(f"  {table} year={year}: MISSING FILES (snapshot={snap_rows}, test={test_rows})")
                    all_ok = False
                    continue
                if diffs == 0 and snap_rows == test_rows:
                    print(f"  {table} year={year}: OK ({snap_rows:,} rows)")
                else:
                    print(f"  {table} year={year}: DIFFS={diffs} (snapshot={snap_rows:,}, test={test_rows:,})")
                    all_ok = False

        total_s = time.perf_counter() - started
        print(f"Total time: {total_s:.2f}s (build {build_s:.2f}s)")
        return all_ok


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, default=Path("data/_regression/gold_before"),
                        help="Root of the frozen Gold snapshot")
    parser.add_argument("--data-dir", type=Path, default=Path("data"),
                        help="Root of the data lake (reads Silver, writes temp Gold)")
    parser.add_argument("--years", type=int, nargs="+", default=None,
                        help="Years to rebuild (default: 2019-2023)")
    args = parser.parse_args()

    if not args.snapshot.is_dir():
        print(f"Snapshot not found: {args.snapshot}", file=sys.stderr)
        return 2
    if not args.data_dir.is_dir():
        print(f"Data dir not found: {args.data_dir}", file=sys.stderr)
        return 2

    ok = run_regression(args.snapshot, args.data_dir, args.years)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
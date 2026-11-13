#!/usr/bin/env python3
"""Build the small committed demo sample the dashboard runs on.

The sample is a real subset of the local lake, never synthetic: a square window
of cells from ``silver/cells.parquet``, the matching rows of the 2024 Gold
tables, the matching rows of the frozen S3 prediction dump, and the metric JSONs
plus ``pipeline_health.json`` that the dashboard reads. Numbers, labels and
probabilities are copied unchanged.

Usage (from the repository root, after a normal pipeline run):

    python scripts/make_demo_sample.py
    python scripts/make_demo_sample.py --cells 50 --year 2024 --out demo_data
"""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

CELL_WINDOW = 50  # 50 x 50 cells of the 100 m grid = 5 x 5 km
MARKER = "DEMO_SAMPLE.json"
GOLD_TABLES = ("weekly_ndvi", "ndvi_anomaly", "weekly_weather")
METRIC_FILES = ("xgb_temporal.json", "baselines_metrics.json", "xgb_spatial.json")


def choose_window(cells: pd.DataFrame, size: int) -> tuple[int, int]:
    """The most cropland-heavy ``size`` x ``size`` window, aligned to 1 km blocks.

    Origins are restricted to multiples of 10 cells so the 5 km window covers
    exactly ``size // 10`` map blocks per axis, which keeps the demo map tidy.
    Ties break by the lowest row, then the lowest column.
    """
    n_rows = int(cells["cell_row"].max()) + 1
    n_cols = int(cells["cell_col"].max()) + 1
    cropland = np.zeros((n_rows, n_cols), dtype=np.int32)
    cropland[cells["cell_row"].to_numpy(), cells["cell_col"].to_numpy()] = (
        cells["is_cropland"].to_numpy().astype(np.int32)
    )
    integral = cropland.cumsum(axis=0).cumsum(axis=1)

    def window_sum(row: int, col: int) -> int:
        r1, c1 = row + size - 1, col + size - 1
        total = int(integral[r1, c1])
        if row:
            total -= int(integral[row - 1, c1])
        if col:
            total -= int(integral[r1, col - 1])
        if row and col:
            total += int(integral[row - 1, col - 1])
        return total

    best = (-1, 0, 0)
    for row in range(0, n_rows - size + 1, 10):
        for col in range(0, n_cols - size + 1, 10):
            score = window_sum(row, col)
            if score > best[0]:
                best = (score, row, col)
    return best[1], best[2]


def subset(table: pa.Table, cell_ids: pa.Array) -> pa.Table:
    """Rows whose ``cell_id`` is in ``cell_ids``, schema untouched."""
    mask = pc.is_in(table["cell_id"], value_set=cell_ids)
    return table.filter(mask)


def write(table: pa.Table, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)


def build(data_root: Path, artifacts: Path, out: Path, size: int, year: int) -> dict:
    if out.exists():
        shutil.rmtree(out)

    cells = pd.read_parquet(data_root / "silver" / "cells.parquet",
                            columns=["cell_id", "cell_row", "cell_col", "is_cropland"])
    row0, col0 = choose_window(cells, size)
    row1, col1 = row0 + size - 1, col0 + size - 1
    window = cells[
        cells["cell_row"].between(row0, row1) & cells["cell_col"].between(col0, col1)
    ]
    cell_ids = pa.array(sorted(int(value) for value in window["cell_id"]))

    cells_table = pq.read_table(data_root / "silver" / "cells.parquet")
    write(subset(cells_table, cell_ids), out / "silver" / "cells.parquet")

    for table in GOLD_TABLES:
        source = data_root / "gold" / table / f"year={year}" / f"{table}.parquet"
        table_data = pq.read_table(source)
        if "cell_id" in table_data.column_names:
            table_data = subset(table_data, cell_ids)
        write(table_data, out / "gold" / table / f"year={year}" / f"{table}.parquet")

    health = data_root / "gold" / "_reports" / "pipeline_health.json"
    target = out / "gold" / "_reports" / "pipeline_health.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(health, target)

    for name in METRIC_FILES:
        destination = out / "artifacts" / "phase7" / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(artifacts / "phase7" / name, destination)

    predictions = artifacts / "phase8" / "predictions" / "temporal_2024.parquet"
    destination = out / "artifacts" / "phase8" / "predictions" / "temporal_2024.parquet"
    write(subset(pq.read_table(predictions), cell_ids), destination)

    marker = {
        "kind": "demo-sample",
        "summary": f"{size} x {size} cells of the 100 m grid (~5 x 5 km), season {year} only",
        "source": "a spatial and temporal subset of the local lake and the frozen Phase 7/8 outputs",
        "cell_window": {"row0": row0, "row1": row1, "col0": col0, "col1": col1},
        "n_cells": int(len(window)),
        "n_cropland_cells": int(window["is_cropland"].sum()),
        "year": year,
        "attribution": "demo_data/README.md",
    }
    (out / MARKER).write_text(json.dumps(marker, indent=2) + "\n", encoding="utf-8")
    return marker


def total_size(path: Path) -> int:
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", default="data", type=Path)
    parser.add_argument("--artifacts", default="artifacts", type=Path)
    parser.add_argument("--out", default="demo_data", type=Path)
    parser.add_argument("--cells", default=CELL_WINDOW, type=int)
    parser.add_argument("--year", default=2024, type=int)
    options = parser.parse_args(argv)

    marker = build(options.data_root, options.artifacts, options.out,
                   options.cells, options.year)
    size = total_size(options.out)
    print(f"demo sample: {marker['summary']}")
    print(f"cells: {marker['n_cells']:,} ({marker['n_cropland_cells']:,} cropland), "
          f"window rows {marker['cell_window']['row0']}-{marker['cell_window']['row1']}, "
          f"cols {marker['cell_window']['col0']}-{marker['cell_window']['col1']}")
    print(f"total size: {size:,} bytes ({size / 1e6:.2f} MB)")
    for path in sorted(options.out.rglob("*")):
        if path.is_file():
            print(f"  {path.stat().st_size:>9,}  {path}")
    if size > 15 * 1000 * 1000:
        print("ERROR: sample exceeds the 15 MB budget")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

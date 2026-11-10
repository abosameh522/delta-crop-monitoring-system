"""Command line entry point for Bronze -> Silver."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

from delta_crops.config import Config, data_dir, load_config
from delta_crops.transform import bronze_reader, cells, silver, weather
from delta_crops.transform.grid import PIXELS_PER_CELL, load_or_build


def _select_dates(command: str, args: argparse.Namespace, ledger: dict) -> list[date]:
    all_dates = bronze_reader.imagery_dates(ledger)
    if command == "date":
        return [date.fromisoformat(args.date)]
    if command == "month":
        return [d for d in all_dates if d.year == args.year and d.month == args.month]
    if command == "year":
        return [d for d in all_dates if d.year == args.year]
    return all_dates


def _total_pixels(pixel_mask: np.ndarray, grid) -> np.ndarray:
    return pixel_mask.reshape(grid.nrows, PIXELS_PER_CELL, grid.ncols, PIXELS_PER_CELL).sum(axis=(1, 3)).astype(np.int32)


def cmd_grid(args: argparse.Namespace, config: Config, root: Path) -> int:
    grid, keep, pixels = load_or_build(config.bbox, root / "silver" / "_cache")
    total = _total_pixels(pixels, grid)
    print(json.dumps(
        {
            "x0": grid.x0,
            "y0": grid.y0,
            "cols": grid.ncols,
            "rows": grid.nrows,
            "envelope_cells": grid.ncols * grid.nrows,
            "kept_cells": int(keep.sum()),
            "aoi_10m_pixels": int(pixels.sum()),
            "aoi_area_km2": round(float(pixels.sum()) * 100 / 1e6, 1),
            "cells_with_total_gt_0": int((total > 0).sum()),
            "mean_total_pixels_per_kept_cell": round(float(total[keep].mean()), 2),
        },
        indent=2,
    ))
    return 0


def cmd_cells(args: argparse.Namespace, config: Config, root: Path) -> int:
    grid, keep, pixels = load_or_build(config.bbox, root / "silver" / "_cache")
    rows = cells.build_cells(grid, config.bbox, keep, pixels, root)
    table = pq.read_table(root / "silver" / "cells.parquet")
    cropland = table.column("is_cropland").to_pylist()
    print(f"cells.parquet: {rows} rows, cropland cells {sum(1 for value in cropland if value)}")
    return 0


def cmd_weather(args: argparse.Namespace, config: Config, root: Path) -> int:
    rows = weather.build_weather(config, root)
    print(f"weather.parquet: {rows} rows")
    return 0


def cmd_build(args: argparse.Namespace, config: Config, root: Path) -> int:
    grid, keep, pixels = load_or_build(config.bbox, root / "silver" / "_cache")
    ledger = bronze_reader.load_ledger(root)
    dates = _select_dates(args.command, args, ledger)
    if not dates:
        print("no dates selected", file=sys.stderr)
        return 2
    total = _total_pixels(pixels, grid)
    print(f"Processing {len(dates)} dates over {int(keep.sum())} kept cells")
    summary = silver.build(
        dates=dates,
        config=config,
        data_root=root,
        keep_cells=keep,
        pixel_mask=pixels,
        total_px=total,
        grid=grid,
        verify_checksum=not args.no_verify_checksum,
        force=args.force,
    )
    print(
        f"Done: {summary['processed']} processed, {summary['skipped']} skipped, "
        f"{summary['failed']} failed in {summary['wall_seconds']:.1f}s "
        f"(peak RSS {summary['peak_rss_mb']:.0f} MB)"
    )
    for error in summary["errors"]:
        print(f"  {error}")
    return 0 if summary["failed"] == 0 else 1


def cmd_status(args: argparse.Namespace, config: Config, root: Path) -> int:
    ledger = bronze_reader.load_ledger(root)
    dates = bronze_reader.imagery_dates(ledger)
    present = 0
    for day in dates:
        if silver.ndvi_path(root, day).exists():
            present += 1
    print(f"ledger dates {len(dates)}, Silver files present {present}")
    if args.year:
        dates = [d for d in dates if d.year == args.year]
        present = sum(1 for d in dates if silver.ndvi_path(root, d).exists())
        print(f"year={args.year}: {present}/{len(dates)}")
    return 0


def cmd_scl(args: argparse.Namespace, config: Config, root: Path) -> int:
    path = silver.reports_dir(root) / "scl_shares.parquet"
    if not path.exists():
        print("no SCL report yet", file=sys.stderr)
        return 2
    table = pq.read_table(path).to_pylist()
    years = sorted({row["year"] for row in table})
    print(f"{'class':>5} " + " ".join(f"{year:>7}" for year in years))
    for scl_class in range(12):
        values = []
        for year in years:
            row = next(r for r in table if r["year"] == year and r["scl_class"] == scl_class)
            values.append(f"{row['pct_of_aoi_pixels']:7.2f}")
        print(f"{scl_class:>5} " + " ".join(values))
    gate = []
    for year in years:
        dark = next(r for r in table if r["year"] == year and r["scl_class"] == 2)["pct_of_aoi_pixels"]
        unclassified = next(r for r in table if r["year"] == year and r["scl_class"] == 7)["pct_of_aoi_pixels"]
        if dark > config.silver.max_scl_dark_pct or unclassified > config.silver.max_scl_unclassified_pct:
            gate.append(f"{year}: class2={dark:.2f}% class7={unclassified:.2f}%")
    if gate:
        print("GATE FAILED (class 2 or 7 above threshold): " + "; ".join(gate), file=sys.stderr)
        return 3
    print("gate OK: class 2 and 7 within thresholds for every year")
    return 0


def cmd_stable(args: argparse.Namespace, config: Config, root: Path) -> int:
    """Survey candidate cross-era stable targets without freezing anything."""
    from delta_crops.transform.grid import make_grid
    from delta_crops.transform.stability import survey

    grid, keep, pixels = load_or_build(config.bbox, root / "silver" / "_cache")
    classes = cells.worldcover_on_grid(grid, config.bbox, root / "silver" / "_cache")
    for code, name in ((80, "water"), (60, "bare/desert")):
        for box_m in (1000, 500, 300):
            fraction, row, col, in_aoi = survey(classes, pixels, code, box_m // grid.pixel_size_m)
            print(
                f"{name:11s} {box_m:>5d} m box: best in-AOI share {fraction:5.1%} "
                f"at 10m px ({row},{col}) in-AOI px {in_aoi}"
            )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="delta-crops-silver")
    parser.add_argument("--config", type=Path, default=None)
    parser.add_argument("--data-dir", type=Path, default=None)
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("grid", help="print the analysis grid and AOI cell counts").set_defaults(func=cmd_grid)
    sub.add_parser("cells", help="build silver/cells.parquet from WorldCover").set_defaults(func=cmd_cells)
    sub.add_parser("weather", help="build silver/weather.parquet").set_defaults(func=cmd_weather)
    sub.add_parser("scl", help="print the SCL class shares and the class 2/7 gate").set_defaults(func=cmd_scl)
    sub.add_parser("stable-target", help="survey water/desert candidates for the cross-era test").set_defaults(
        func=cmd_stable
    )

    for name, help_text in (
        ("date", "process one date"),
        ("month", "process one month"),
        ("year", "process one season year"),
        ("all", "process every date in the ledger"),
    ):
        cmd = sub.add_parser(name, help=help_text)
        if name == "date":
            cmd.add_argument("--date", required=True)
        if name in ("month", "year"):
            cmd.add_argument("--year", type=int, required=True)
        if name == "month":
            cmd.add_argument("--month", type=int, required=True)
        cmd.add_argument("--no-verify-checksum", action="store_true", help="skip SHA-256 on Bronze reads")
        cmd.add_argument("--force", action="store_true", help="rebuild dates even if output exists")
        cmd.set_defaults(func=cmd_build)

    status = sub.add_parser("status", help="count Silver files against ledger dates")
    status.add_argument("--year", type=int, default=None)
    status.set_defaults(func=cmd_status)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = load_config(args.config)
    root = Path(args.data_dir) if args.data_dir else data_dir()
    return args.func(args, config, root)


if __name__ == "__main__":
    raise SystemExit(main())

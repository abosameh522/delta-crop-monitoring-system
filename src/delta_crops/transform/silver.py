"""Bronze -> Silver: NDVI per 100 m cell, per acquisition date.

One date at a time: read the AOI windows of the available tiles, mask with SCL,
mosaic into a global 10 m canvas (first valid pixel wins, tile priority from
config), aggregate 10x10 blocks to 100 m cells, write one Parquet file per date.
Every kept cell is written for every date, so rows = dates x kept cells.
"""

from __future__ import annotations

import os
import resource
import time
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Iterable

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from delta_crops.config import Config
from delta_crops.transform import bronze_reader
from delta_crops.transform.grid import PIXELS_PER_CELL, Grid, cell_ids
from delta_crops.transform.reflectance import ndvi_with_mask

NDVI_SCHEMA = pa.schema(
    [
        ("cell_id", pa.int32()),
        ("date", pa.date32()),
        ("ndvi", pa.float32()),
        ("valid_pixels", pa.uint8()),
        ("total_pixels", pa.uint8()),
        ("masked_pct", pa.float32()),
    ]
)

REPORT_SCHEMA = pa.schema(
    [
        ("date", pa.date32()),
        ("tiles", pa.string()),
        ("rows", pa.int32()),
        ("seconds", pa.float32()),
        ("peak_rss_mb", pa.float32()),
        ("ndvi_p05", pa.float32()),
        ("ndvi_p50", pa.float32()),
        ("ndvi_p95", pa.float32()),
        ("null_pct", pa.float32()),
        ("masked_p50", pa.float32()),
        ("masked_p90", pa.float32()),
        ("masked_max", pa.float32()),
        ("overlap_median_abs_ndvi", pa.float32()),
        ("overlap_pixels", pa.int64()),
    ]
)


@dataclass
class DateOutcome:
    table: pa.Table
    stats: dict[str, Any]
    scl_counts: np.ndarray = field(default_factory=lambda: np.zeros(256, dtype=np.int64))


@dataclass
class MergeResult:
    """What a tile contributed to the canvas."""

    written: int
    overlap_pixels: int
    overlap_median: float | None


def ndvi_path(root: Path, day: date) -> Path:
    return root / "silver" / "ndvi" / f"year={day.year}" / f"month={day.month:02d}" / f"date={day.isoformat()}.parquet"


def reports_dir(root: Path) -> Path:
    return root / "silver" / "_reports"


def block_slice(canvas: np.ndarray, block: np.ndarray, row_off: int, col_off: int) -> tuple[slice, slice, slice, slice]:
    """Slices of (canvas, block) for placing `block` at (row_off, col_off)."""
    height, width = canvas.shape
    src_r0 = max(0, -row_off)
    src_c0 = max(0, -col_off)
    dst_r0 = max(0, row_off)
    dst_c0 = max(0, col_off)
    rows = min(block.shape[0] - src_r0, height - dst_r0)
    cols = min(block.shape[1] - src_c0, width - dst_c0)
    rows = max(rows, 0)
    cols = max(cols, 0)
    return (
        slice(dst_r0, dst_r0 + rows),
        slice(dst_c0, dst_c0 + cols),
        slice(src_r0, src_r0 + rows),
        slice(src_c0, src_c0 + cols),
    )


def merge_block(
    canvas: np.ndarray,
    claimed: np.ndarray,
    ndvi: np.ndarray,
    valid: np.ndarray,
    keep: np.ndarray,
    row_off: int,
    col_off: int,
) -> MergeResult:
    """Write `ndvi` into the canvas where it is valid, unclaimed and inside the AOI.

    Also measures the disagreement with the pixels already claimed by a
    higher-priority tile (used for the 36RTV/36RUV overlap audit).
    """
    rs, cs, bs, bcs = block_slice(canvas, ndvi, row_off, col_off)
    if rs.stop == rs.start or cs.stop == cs.start:
        return MergeResult(0, 0, None)
    ndvi_sub = ndvi[bs, bcs]
    valid_sub = valid[bs, bcs]
    claimed_region = claimed[rs, cs]
    keep_sub = keep[rs, cs]

    overlap_pixels = 0
    overlap_median: float | None = None
    both = valid_sub & claimed_region & keep_sub
    if both.any():
        overlap_pixels = int(both.sum())
        if overlap_pixels > 10_000:
            overlap_median = float(np.median(np.abs(ndvi_sub[both] - canvas[rs, cs][both])))

    write = valid_sub & ~claimed_region & keep_sub
    region = canvas[rs, cs]
    region[write] = ndvi_sub[write]
    claimed_region |= write
    return MergeResult(int(write.sum()), overlap_pixels, overlap_median)


def aggregate_canvas(
    canvas: np.ndarray,
    claimed: np.ndarray,
    total_px: np.ndarray,
    keep_cells: np.ndarray,
    min_valid_fraction: float,
    grid: Grid,
) -> dict[str, np.ndarray]:
    """10x10 block means to 100 m cells, flattened to kept cells."""
    shape = grid.nrows, PIXELS_PER_CELL, grid.ncols, PIXELS_PER_CELL
    counts = claimed.reshape(shape).sum(axis=(1, 3), dtype=np.uint16)
    sums = np.where(claimed, canvas, 0.0).reshape(shape).sum(axis=(1, 3), dtype=np.float64)
    mean = np.divide(sums, counts, out=np.full(sums.shape, np.nan, dtype="float64"), where=counts > 0)

    masked_pct = np.where(total_px > 0, 100.0 * (1.0 - counts / np.maximum(total_px, 1)), 100.0)
    ndvi_cell = np.where(counts >= total_px * min_valid_fraction, mean, np.nan).astype("float32")

    keep_flat = keep_cells.ravel()
    return {
        "ndvi": ndvi_cell.ravel()[keep_flat],
        "valid_pixels": counts.ravel()[keep_flat].astype("uint8"),
        "total_pixels": total_px.ravel()[keep_flat].astype("uint8"),
        "masked_pct": np.clip(masked_pct.ravel()[keep_flat], 0.0, 100.0).astype("float32"),
    }


def process_date(
    day: date,
    tiles: dict[str, dict[str, bronze_reader.Asset]],
    grid: Grid,
    keep_cells: np.ndarray,
    pixel_mask: np.ndarray,
    total_px: np.ndarray,
    config: Config,
) -> DateOutcome:
    """Compute one date's 100 m NDVI rows (kept cells only)."""
    masked_classes = config.silver.masked_scl_classes
    priority = list(config.silver.tile_priority)
    order = [t for t in priority if t in tiles] + sorted(t for t in tiles if t not in priority)

    canvas = np.full(grid.pixel_shape, np.nan, dtype="float32")
    claimed = np.zeros(grid.pixel_shape, dtype=bool)
    seen = np.zeros(grid.pixel_shape, dtype=bool)  # SCL counted once per ground pixel
    scl_counts = np.zeros(256, dtype=np.int64)
    overlap_pixels = 0
    overlap_medians: list[float] = []

    for tile in order:
        bands = tiles[tile]
        red, red_transform = bronze_reader.read_band(bands["B04"], config.bbox)
        nir, _ = bronze_reader.read_band(bands["B08"], config.bbox)
        scl = bronze_reader.read_scl_on_grid(bands["SCL"], config.bbox, red_transform, red.shape)
        if scl.shape != red.shape:
            raise bronze_reader.LedgerError(f"SCL shape {scl.shape} != red shape {red.shape}")

        ndvi, valid = ndvi_with_mask(red, nir, scl, masked_classes)
        del red, nir
        row_off, col_off = bronze_reader.tile_offsets(red_transform, grid.x0, grid.y_top)
        merged = merge_block(canvas, claimed, ndvi, valid, pixel_mask, row_off, col_off)
        overlap_pixels += merged.overlap_pixels
        if merged.overlap_median is not None:
            overlap_medians.append(merged.overlap_median)

        rs, cs, bs, bcs = block_slice(np.empty(grid.pixel_shape, dtype=np.uint8), scl, row_off, col_off)
        if rs.stop > rs.start and cs.stop > cs.start:
            fresh = pixel_mask[rs, cs] & ~seen[rs, cs]
            values = scl[bs, bcs][fresh]
            if values.size:
                scl_counts += np.bincount(values.ravel(), minlength=256)
            seen[rs, cs] |= pixel_mask[rs, cs]
        del scl, ndvi, valid

    arrays = aggregate_canvas(canvas, claimed, total_px, keep_cells, config.silver.min_valid_fraction, grid)
    del canvas, claimed
    # NaN means "no value" for a cell; store it as a real Parquet NULL so SQL
    # `is null` works and downstream aggregates skip it naturally.
    ndvi_array = pc.if_else(
        pc.is_nan(pa.array(arrays["ndvi"], type=pa.float32())),
        pa.scalar(None, pa.float32()),
        pa.array(arrays["ndvi"], type=pa.float32()),
    )
    table = pa.Table.from_arrays(
        [
            pa.array(cell_ids(grid)[keep_cells.ravel()], type=pa.int32()),
            pa.array([day] * len(arrays["ndvi"]), type=pa.date32()),
            ndvi_array,
            pa.array(arrays["valid_pixels"], type=pa.uint8()),
            pa.array(arrays["total_pixels"], type=pa.uint8()),
            pa.array(arrays["masked_pct"], type=pa.float32()),
        ],
        schema=NDVI_SCHEMA,
    )

    finite = arrays["ndvi"][np.isfinite(arrays["ndvi"])]
    stats = {
        "date": day,
        "tiles": ",".join(order),
        "rows": int(len(arrays["ndvi"])),
        "ndvi_p05": float(np.percentile(finite, 5)) if finite.size else None,
        "ndvi_p50": float(np.percentile(finite, 50)) if finite.size else None,
        "ndvi_p95": float(np.percentile(finite, 95)) if finite.size else None,
        "null_pct": float(100.0 * (1 - finite.size / max(len(arrays["ndvi"]), 1))),
        "masked_p50": float(np.percentile(arrays["masked_pct"], 50)),
        "masked_p90": float(np.percentile(arrays["masked_pct"], 90)),
        "masked_max": float(arrays["masked_pct"].max()) if arrays["masked_pct"].size else None,
        "overlap_median_abs_ndvi": float(np.median(overlap_medians)) if overlap_medians else None,
        "overlap_pixels": overlap_pixels,
    }
    return DateOutcome(table=table, stats=stats, scl_counts=scl_counts)


def write_parquet_atomic(table: pa.Table, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(".parquet.tmp")
    try:
        pq.write_table(table, tmp, compression="zstd")
        os.replace(tmp, target)
    finally:
        tmp.unlink(missing_ok=True)


def is_valid_output(path: Path, expected_rows: int) -> bool:
    if not path.exists():
        return False
    try:
        return pq.ParquetFile(path).metadata.num_rows == expected_rows
    except Exception:  # noqa: BLE001 - a corrupt file is simply rebuilt
        return False


def upsert_report(path: Path, rows: list[dict[str, Any]], schema: pa.Schema) -> None:
    """Merge new per-date rows into an existing report, keyed by date."""
    if not rows:
        return
    table = pa.Table.from_pylist(rows, schema=schema)
    if path.exists():
        existing = pq.read_table(path).to_pylist()
        merged = {row["date"]: row for row in existing}
        merged.update({row["date"]: row for row in table.to_pylist()})
        table = pa.Table.from_pylist(sorted(merged.values(), key=lambda r: r["date"]), schema=schema)
    write_parquet_atomic(table, path)


def build(
    dates: Iterable[date],
    config: Config,
    data_root: Path,
    keep_cells: np.ndarray,
    pixel_mask: np.ndarray,
    total_px: np.ndarray,
    grid: Grid,
    verify_checksum: bool = True,
    force: bool = False,
) -> dict[str, Any]:
    """Process dates, skipping valid existing outputs. Returns a run summary."""
    from delta_crops.errors import SilverDateFailure, TransientFailure

    ledger = bronze_reader.load_ledger(data_root)
    expected_rows = int(keep_cells.sum())
    scl_by_year: dict[int, np.ndarray] = {}
    report_rows: list[dict[str, Any]] = []
    overlap_rows: list[dict[str, Any]] = []
    summary: dict[str, Any] = {"processed": 0, "skipped": 0, "failed": 0, "errors": []}
    started_all = time.monotonic()
    dates = list(dates)

    for index, day in enumerate(dates, start=1):
        target = ndvi_path(data_root, day)
        if not force and is_valid_output(target, expected_rows):
            summary["skipped"] += 1
            if index % 25 == 0:
                print(f"[{index}/{len(dates)}] {day} skip (valid output exists)", flush=True)
            continue

        started = time.monotonic()
        try:
            tiles, problems = bronze_reader.assets_for_date(ledger, data_root, day, verify_checksum=verify_checksum)
            if problems:
                raise bronze_reader.LedgerError("; ".join(problems))
            if not tiles:
                raise bronze_reader.LedgerError("no assets in ledger")
            outcome = process_date(day, tiles, grid, keep_cells, pixel_mask, total_px, config)
        except bronze_reader.LedgerError as exc:
            summary["failed"] += 1
            summary["errors"].append(f"{day}: LedgerError: {exc}")
            print(f"[{index}/{len(dates)}] {day} FAILED LedgerError: {exc}", flush=True)
            continue
        except TransientFailure:
            raise
        except duckdb.IOException as exc:
            # Real DuckDB file-lock conflict verified as _duckdb.IOException
            # ("Could not set lock on file ...") in duckdb 2.0.0.
            raise TransientFailure(f"DuckDB I/O failure on {day}") from exc
        except Exception as exc:  # noqa: BLE001 - any other per-date failure is a failed date
            exc_name = type(exc).__name__
            summary["failed"] += 1
            summary["errors"].append(f"{day}: {exc_name}: {exc}")
            print(f"[{index}/{len(dates)}] {day} FAILED {exc_name}: {exc}", flush=True)
            continue

        write_parquet_atomic(outcome.table, target)
        elapsed = time.monotonic() - started
        summary["processed"] += 1

        stats = dict(outcome.stats)
        stats["seconds"] = elapsed
        stats["peak_rss_mb"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0
        report_rows.append(stats)
        scl_by_year.setdefault(day.year, np.zeros(256, dtype=np.int64))
        scl_by_year[day.year] += outcome.scl_counts
        if stats["overlap_median_abs_ndvi"] is not None:
            overlap_rows.append(
                {
                    "date": day,
                    "median_abs_ndvi_diff": stats["overlap_median_abs_ndvi"],
                    "pixels": stats["overlap_pixels"],
                }
            )
        print(
            f"[{index}/{len(dates)}] {day} tiles={stats['tiles']} rows={stats['rows']} {elapsed:.2f}s "
            f"ndvi_p50={stats['ndvi_p50'] if stats['ndvi_p50'] is None else round(stats['ndvi_p50'], 3)} "
            f"null={stats['null_pct']:.1f}% masked_p50={stats['masked_p50']:.1f}%",
            flush=True,
        )

    if report_rows:
        upsert_report(reports_dir(data_root) / "dates.parquet", report_rows, REPORT_SCHEMA)
    if overlap_rows:
        upsert_report(
            reports_dir(data_root) / "tile_overlap.parquet",
            overlap_rows,
            pa.schema([("date", pa.date32()), ("median_abs_ndvi_diff", pa.float32()), ("pixels", pa.int64())]),
        )
    if scl_by_year:
        _write_scl_shares(data_root, scl_by_year)

    summary["wall_seconds"] = time.monotonic() - started_all
    summary["peak_rss_mb"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0
    return summary


def _write_scl_shares(data_root: Path, scl_by_year: dict[int, np.ndarray]) -> None:
    """Per-year SCL class shares over in-AOI pixels, merged with earlier years."""
    path = reports_dir(data_root) / "scl_shares.parquet"
    schema = pa.schema(
        [("year", pa.int16()), ("scl_class", pa.int8()), ("pixels", pa.int64()), ("pct_of_aoi_pixels", pa.float32())]
    )
    rows: dict[tuple[int, int], dict[str, Any]] = {}
    if path.exists():
        for row in pq.read_table(path).to_pylist():
            rows[(row["year"], row["scl_class"])] = row
    for year, counts in scl_by_year.items():
        total = int(counts.sum())
        for scl_class in range(12):
            rows[(year, scl_class)] = {
                "year": year,
                "scl_class": scl_class,
                "pixels": int(counts[scl_class]),
                "pct_of_aoi_pixels": float(100.0 * counts[scl_class] / total) if total else 0.0,
            }
    table = pa.Table.from_pylist([rows[key] for key in sorted(rows)], schema=schema)
    write_parquet_atomic(table, path)

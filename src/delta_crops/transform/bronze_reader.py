"""Ledger-verified Bronze reads.

Silver never globs `data/bronze`: it asks the ingestion ledger which assets
belong to a date, then verifies that each file is still the one that was
logged (size, and optionally SHA-256). Bronze is treated as read-only.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import rasterio
from rasterio.enums import Resampling
from rasterio.warp import reproject
from rasterio.warp import reproject

from delta_crops.config import BBox
from delta_crops.ingestion.bronze import read_records
from delta_crops.ingestion.earth_search import baseline_key
from delta_crops.ingestion.raster import GDAL_ENV, window_for_bbox

BANDS = ("B04", "B08", "SCL")


@dataclass(frozen=True)
class Asset:
    band: str
    tile: str
    path: Path
    item_id: str
    sha256: str | None


class LedgerError(RuntimeError):
    """Raised when the ledger and the files on disk disagree."""


def sha256_file(path: Path, chunk_size: int = 1 << 20) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_ledger(data_root: Path) -> dict[str, dict[str, Any]]:
    """Latest record per asset_key, read from the append-only JSONL."""
    records = read_records(data_root / "bronze" / "_logs" / "ingestion_log.jsonl")
    index: dict[str, dict[str, Any]] = {}
    for record in records:
        index[record["asset_key"]] = record
    return index


def imagery_dates(ledger: dict[str, dict[str, Any]]) -> list[date]:
    """Sorted acquisition dates that have any Sentinel-2 asset in the ledger."""
    days = {
        row["acquired_at"][:10]
        for row in ledger.values()
        if row.get("source") == "earth-search" and row.get("acquired_at")
    }
    return [date.fromisoformat(day) for day in sorted(days)]


def assets_for_date(
    ledger: dict[str, dict[str, Any]],
    data_root: Path,
    day: date,
    verify_checksum: bool = True,
) -> tuple[dict[str, dict[str, Asset]], list[str]]:
    """Return {tile: {band: Asset}} for one date plus a list of problems.

    A problem (missing band, missing file, size or checksum mismatch) means the
    date must be skipped rather than partially processed.
    """
    tiles: dict[str, dict[str, Asset]] = {}
    problems: list[str] = []
    selected: dict[tuple[str, str], dict[str, Any]] = {}
    for record in ledger.values():
        if record.get("source") != "earth-search" or not str(record.get("acquired_at") or "").startswith(day.isoformat()):
            continue
        key = (record.get("tile", ""), record.get("band", ""))
        current = selected.get(key)
        if current is None or (baseline_key(record.get("processing_baseline")), record.get("item_id", "")) > (baseline_key(current.get("processing_baseline")), current.get("item_id", "")):
            selected[key] = record
    for record in selected.values():
        if record.get("source") != "earth-search":
            continue
        if not str(record.get("acquired_at") or "").startswith(day.isoformat()):
            continue
        band = record.get("band")
        if band not in BANDS:
            problems.append(f"unexpected band {band!r} for {record.get('asset_key')}")
            continue
        path = data_root / record["path"]
        if not path.exists():
            problems.append(f"missing file {record['path']}")
            continue
        if path.stat().st_size != record.get("bytes"):
            problems.append(f"size mismatch {record['path']}")
            continue
        if verify_checksum and record.get("sha256") and sha256_file(path) != record["sha256"]:
            problems.append(f"checksum mismatch {record['path']}")
            continue
        tiles.setdefault(record["tile"], {})[band] = Asset(
            band=band, tile=record["tile"], path=path, item_id=record["item_id"], sha256=record.get("sha256")
        )

    for tile, bands in list(tiles.items()):
        missing = [band for band in BANDS if band not in bands]
        if missing:
            problems.append(f"tile {tile} missing {missing}")
            del tiles[tile]
    return tiles, problems


def read_band(
    asset: Asset,
    bbox: BBox,
    target_shape: tuple[int, int] | None = None,
    resampling: Resampling = Resampling.nearest,
) -> tuple[np.ndarray, rasterio.Affine]:
    """Read the AOI window of one Bronze COG.

    `target_shape` is used for SCL: reading the 20 m band at the 10 m band's
    shape with nearest resampling replicates each class into a 2x2 block instead
    of interpolating between classes.
    """
    with rasterio.Env(**GDAL_ENV):
        with rasterio.open(asset.path) as src:
            window = window_for_bbox(src, bbox)
            array = src.read(1, window=window, out_shape=target_shape, resampling=resampling)
            return array, src.window_transform(window)


def read_scl_on_grid(
    asset: Asset,
    bbox: BBox,
    transform: rasterio.Affine,
    shape: tuple[int, int],
) -> np.ndarray:
    """Resample the 20 m SCL band onto an existing 10 m band grid (nearest).

    Sentinel-2 SCL is a 20 m product whose origin can sit half a 10 m pixel away
    from the B04/B08 grid, so class codes are mapped by nearest neighbour onto
    the exact band grid rather than by block replication.
    """
    with rasterio.Env(**GDAL_ENV):
        with rasterio.open(asset.path) as src:
            window = window_for_bbox(src, bbox)
            source = src.read(1, window=window)
            source_transform = src.window_transform(window)
            destination = np.zeros(shape, dtype="uint8")
            reproject(
                source,
                destination,
                src_transform=source_transform,
                src_crs=src.crs,
                dst_transform=transform,
                dst_crs=src.crs,
                resampling=Resampling.nearest,
                src_nodata=0,
                dst_nodata=0,
            )
            return destination


def tile_offsets(transform: rasterio.Affine, grid_x0: float, grid_y_top: float, pixel_m: int = 10) -> tuple[int, int]:
    """Integer (row_off, col_off) of a tile window inside the global 10 m canvas.

    Bronze tiles are 10 m UTM with origins on the 10 m lattice, so the tile maps
    onto the analysis grid by pure slicing. If an origin were off-lattice we
    refuse rather than silently resample.
    """
    dx = transform.c - grid_x0
    dy = grid_y_top - transform.f
    col = dx / pixel_m
    row = dy / pixel_m
    if abs(col - round(col)) > 1e-3 or abs(row - round(row)) > 1e-3:
        raise LedgerError(f"tile origin is not on the 10 m lattice: dx={dx} dy={dy}")
    return int(round(row)), int(round(col))


__all__ = ["Asset", "BANDS", "LedgerError", "assets_for_date", "imagery_dates", "load_ledger", "read_band", "tile_offsets"]

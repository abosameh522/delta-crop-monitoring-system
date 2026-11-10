"""Static Silver tables: the 100 m cell catalogue from ESA WorldCover.

The WorldCover COG is EPSG:4326 at 10 m. It is reprojected with **nearest
neighbour** onto the exact same 10 m UTM lattice used for NDVI, so a WorldCover
cell and an NDVI cell cover the same ground; class counts are then summed over
each 10x10 block, over in-AOI pixels only.
"""

from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import rasterio
from rasterio.enums import Resampling
from rasterio.transform import from_origin
from rasterio.warp import reproject, transform_bounds
from rasterio.windows import from_bounds

from delta_crops.config import BBox
from delta_crops.ingestion.raster import GDAL_ENV
from delta_crops.transform.grid import PIXELS_PER_CELL, Grid, cell_ids, centroid_lonlat

WORLDCOVER_URL = (
    "https://esa-worldcover.s3.eu-central-1.amazonaws.com/v200/2021/map/ESA_WorldCover_10m_2021_v200_N30E030_Map.tif"
)
CROPLAND_CLASS = 40
CROPLAND_THRESHOLD_PCT = 50.0
#: Class codes present in WorldCover v200; order matters for the tie-break.
WORLDCOVER_CLASSES = (10, 20, 30, 40, 50, 60, 70, 80, 90, 95, 100)

CELLS_SCHEMA = pa.schema(
    [
        ("cell_id", pa.int32()),
        ("cell_row", pa.uint16()),
        ("cell_col", pa.uint16()),
        ("centroid_lon", pa.float64()),
        ("centroid_lat", pa.float64()),
        ("cropland_pct", pa.float32()),
        ("is_cropland", pa.bool_()),
        ("worldcover_class", pa.uint8()),
    ]
)


def worldcover_on_grid(grid: Grid, bbox: BBox, cache_dir: Path) -> np.ndarray:
    """Reproject WorldCover to the analysis 10 m lattice (nearest), cached."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache = cache_dir / f"worldcover_{int(grid.x0)}_{int(grid.y0)}_{grid.ncols}x{grid.nrows}.npy"
    if cache.exists():
        return np.load(cache)

    target_transform = from_origin(grid.x0, grid.y_top, grid.pixel_size_m, grid.pixel_size_m)
    with rasterio.Env(**GDAL_ENV):
        with rasterio.open(WORLDCOVER_URL) as src:
            west, south, east, north = transform_bounds("EPSG:32636", src.crs, grid.x0, grid.y0, grid.x1, grid.y_top)
            window = from_bounds(west, south, east, north, src.transform).round()
            window = window.intersection(rasterio.windows.Window(0, 0, src.width, src.height))
            source = src.read(1, window=window)
            source_transform = src.window_transform(window)
            destination = np.zeros(grid.pixel_shape, dtype="uint8")
            reproject(
                source,
                destination,
                src_transform=source_transform,
                src_crs=src.crs,
                dst_transform=target_transform,
                dst_crs="EPSG:32636",
                resampling=Resampling.nearest,
                src_nodata=0,
                dst_nodata=0,
            )
    np.save(cache, destination)
    return destination


def build_cells(
    grid: Grid,
    bbox: BBox,
    keep_cells: np.ndarray,
    pixel_mask: np.ndarray,
    data_root: Path,
) -> int:
    """Write silver/cells.parquet and return the number of kept cells."""
    classes = worldcover_on_grid(grid, bbox, data_root / "silver" / "_cache")
    keep_flat = keep_cells.ravel()
    in_aoi = pixel_mask

    blocks = classes.reshape(grid.nrows, PIXELS_PER_CELL, grid.ncols, PIXELS_PER_CELL)
    mask_blocks = in_aoi.reshape(grid.nrows, PIXELS_PER_CELL, grid.ncols, PIXELS_PER_CELL)
    total = mask_blocks.sum(axis=(1, 3)).astype(np.int32)
    counts = np.stack(
        [((blocks == code) & mask_blocks).sum(axis=(1, 3)) for code in WORLDCOVER_CLASSES]
    ).astype(np.int32)  # (nclasses, nrows, ncols), in-AOI pixels only

    cropland = counts[WORLDCOVER_CLASSES.index(CROPLAND_CLASS)]
    cropland_pct = np.where(total > 0, 100.0 * cropland / np.maximum(total, 1), np.nan)

    # argmax returns the first maximum, and WORLDCOVER_CLASSES is ascending, so
    # ties resolve to the lowest class code as documented.
    majority = np.asarray(WORLDCOVER_CLASSES, dtype="uint8")[np.argmax(counts, axis=0)]
    majority = np.where(total > 0, majority, 0).astype("uint8")

    lon, lat = centroid_lonlat(grid)
    lon = lon.reshape(grid.shape).ravel()[keep_flat]
    lat = lat.reshape(grid.shape).ravel()[keep_flat]
    rows = np.repeat(np.arange(grid.nrows, dtype=np.uint16), grid.ncols)[keep_flat]
    cols = np.tile(np.arange(grid.ncols, dtype=np.uint16), grid.nrows)[keep_flat]
    cpct_out = cropland_pct.ravel()[keep_flat].astype("float32")
    table = pa.Table.from_arrays(
        [
            pa.array(cell_ids(grid)[keep_flat], type=pa.int32()),
            pa.array(rows, type=pa.uint16()),
            pa.array(cols, type=pa.uint16()),
            pa.array(lon, type=pa.float64()),
            pa.array(lat, type=pa.float64()),
            pa.array(cpct_out, type=pa.float32()),
            pa.array(np.where(np.isnan(cpct_out), None, cpct_out >= CROPLAND_THRESHOLD_PCT), type=pa.bool_()),
            pa.array(majority.ravel()[keep_flat], type=pa.uint8()),
        ],
        schema=CELLS_SCHEMA,
    )
    target = data_root / "silver" / "cells.parquet"
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(".parquet.tmp")
    try:
        pq.write_table(table, tmp, compression="zstd")
        os.replace(tmp, target)
    finally:
        tmp.unlink(missing_ok=True)
    return len(table)

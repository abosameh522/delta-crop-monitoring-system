"""The 100 m analysis grid in EPSG:32636 and its AOI masks.

The grid covers the UTM bounding box of the WGS84 AOI rectangle. Because the
rectangle rotates in UTM, that envelope also contains corner triangles outside
the AOI; every downstream table keeps only cells that intersect the AOI polygon.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import rasterio
from rasterio.features import rasterize
from rasterio.transform import from_origin
from rasterio.warp import transform as warp_points
from rasterio.warp import transform_bounds

from delta_crops.config import BBox

TARGET_CRS = "EPSG:32636"
CELL_SIZE_M = 100
PIXEL_SIZE_M = 10
PIXELS_PER_CELL = CELL_SIZE_M // PIXEL_SIZE_M


@dataclass(frozen=True)
class Grid:
    """North-up grid: row 0 is the northernmost row, column 0 the westernmost."""

    x0: float  # west edge, EPSG:32636
    y0: float  # south edge, EPSG:32636
    ncols: int
    nrows: int
    cell_size_m: int = CELL_SIZE_M

    @property
    def y_top(self) -> float:
        return self.y0 + self.nrows * self.cell_size_m

    @property
    def x1(self) -> float:
        return self.x0 + self.ncols * self.cell_size_m

    @property
    def pixel_size_m(self) -> int:
        return PIXEL_SIZE_M if self.cell_size_m == CELL_SIZE_M else self.cell_size_m // PIXELS_PER_CELL

    @property
    def shape(self) -> tuple[int, int]:
        return self.nrows, self.ncols

    @property
    def pixel_shape(self) -> tuple[int, int]:
        return self.nrows * PIXELS_PER_CELL, self.ncols * PIXELS_PER_CELL

    @property
    def cell_transform(self) -> rasterio.Affine:
        return from_origin(self.x0, self.y_top, self.cell_size_m, self.cell_size_m)

    @property
    def pixel_transform(self) -> rasterio.Affine:
        return from_origin(self.x0, self.y_top, self.pixel_size_m, self.pixel_size_m)


def utm_bounds(bbox: BBox, crs: str = TARGET_CRS) -> tuple[float, float, float, float]:
    return transform_bounds("EPSG:4326", crs, *bbox.as_list)


def make_grid(bbox: BBox, cell_size_m: int = CELL_SIZE_M) -> Grid:
    """Snap the grid outward so the AOI envelope is fully covered by whole cells."""
    west, south, east, north = utm_bounds(bbox)
    x0 = float(np.floor(west / cell_size_m) * cell_size_m)
    y0 = float(np.floor(south / cell_size_m) * cell_size_m)
    ncols = int(np.ceil((east - x0) / cell_size_m))
    nrows = int(np.ceil((north - y0) / cell_size_m))
    return Grid(x0=x0, y0=y0, ncols=ncols, nrows=nrows, cell_size_m=cell_size_m)


def aoi_polygon(bbox: BBox, crs: str = TARGET_CRS, densify_km: float = 1.0) -> dict:
    """Project the WGS84 AOI rectangle to `crs` as a densified polygon.

    Densifying keeps the projected edges smooth (a UTM edge is not exactly
    straight), so rasterisation does not over- or under-cover by a pixel.
    """
    steps_x = max(2, int(round((bbox.east - bbox.west) * 111 * np.cos(np.radians(bbox.south)) / densify_km)))
    steps_y = max(2, int(round((bbox.north - bbox.south) * 111 / densify_km)))
    lons, lats = [], []
    for i in range(steps_x + 1):
        lons.append(bbox.west + (bbox.east - bbox.west) * i / steps_x)
        lats.append(bbox.south)
    for i in range(1, steps_y + 1):
        lons.append(bbox.east)
        lats.append(bbox.south + (bbox.north - bbox.south) * i / steps_y)
    for i in range(1, steps_x + 1):
        lons.append(bbox.east - (bbox.east - bbox.west) * i / steps_x)
        lats.append(bbox.north)
    for i in range(1, steps_y):
        lons.append(bbox.west)
        lats.append(bbox.north - (bbox.north - bbox.south) * i / steps_y)
    xs, ys = warp_points("EPSG:4326", crs, lons, lats)
    return {"type": "Polygon", "coordinates": [[(float(x), float(y)) for x, y in zip(xs, ys)]]}


def cell_mask(grid: Grid, polygon: dict) -> np.ndarray:
    """True for cells that intersect the AOI polygon (all_touched)."""
    return rasterize(
        [polygon],
        out_shape=grid.shape,
        transform=grid.cell_transform,
        all_touched=True,
        fill=0,
        default_value=1,
        dtype="uint8",
    ).astype(bool)


def pixel_mask(grid: Grid, polygon: dict) -> np.ndarray:
    """True for 10 m pixels that intersect the AOI polygon (all_touched)."""
    return rasterize(
        [polygon],
        out_shape=grid.pixel_shape,
        transform=grid.pixel_transform,
        all_touched=True,
        fill=0,
        default_value=1,
        dtype="uint8",
    ).astype(bool)


def cell_ids(grid: Grid) -> np.ndarray:
    """cell_id = row * 1000 + col, over the kept cells."""
    rows = np.repeat(np.arange(grid.nrows, dtype=np.int32), grid.ncols)
    cols = np.tile(np.arange(grid.ncols, dtype=np.int32), grid.nrows)
    return rows * 1000 + cols


def pixel_cell_index(grid: Grid) -> np.ndarray:
    """Index of each 10 m pixel's cell in the flattened (row-major) cell array."""
    rows = np.repeat(np.arange(grid.nrows * PIXELS_PER_CELL, dtype=np.int32), grid.ncols * PIXELS_PER_CELL)
    cols = np.tile(np.arange(grid.ncols * PIXELS_PER_CELL, dtype=np.int32), grid.nrows * PIXELS_PER_CELL)
    return (rows // PIXELS_PER_CELL) * grid.ncols + (cols // PIXELS_PER_CELL)


def centroid_lonlat(grid: Grid) -> tuple[np.ndarray, np.ndarray]:
    """WGS84 centroids of every cell, as flattened (row-major) arrays."""
    xs = grid.x0 + (np.arange(grid.ncols) + 0.5) * grid.cell_size_m
    ys = grid.y_top - (np.arange(grid.nrows) + 0.5) * grid.cell_size_m
    gx, gy = np.meshgrid(xs, ys)
    lon, lat = warp_points(TARGET_CRS, "EPSG:4326", gx.ravel().tolist(), gy.ravel().tolist())
    return np.asarray(lon), np.asarray(lat)


def grid_fingerprint(bbox: BBox, grid: Grid) -> str:
    payload = json.dumps(
        {"bbox": bbox.as_list, "x0": grid.x0, "y0": grid.y0, "ncols": grid.ncols, "nrows": grid.nrows,
         "cell": grid.cell_size_m},
        sort_keys=True,
    )
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:12]


def load_or_build(bbox: BBox, cache_dir: Path) -> tuple[Grid, np.ndarray, np.ndarray]:
    """Return (grid, cell_mask, pixel_mask), cached on disk by grid fingerprint."""
    grid = make_grid(bbox)
    cache_dir.mkdir(parents=True, exist_ok=True)
    stem = cache_dir / f"grid_{grid_fingerprint(bbox, grid)}"
    if (stem.with_suffix(".cells.npy")).exists() and (stem.with_suffix(".pixels.npy")).exists():
        return grid, np.load(stem.with_suffix(".cells.npy")), np.load(stem.with_suffix(".pixels.npy"))
    polygon = aoi_polygon(bbox)
    keep = cell_mask(grid, polygon)
    pixels = pixel_mask(grid, polygon)
    np.save(stem.with_suffix(".cells.npy"), keep)
    np.save(stem.with_suffix(".pixels.npy"), pixels)
    return grid, keep, pixels

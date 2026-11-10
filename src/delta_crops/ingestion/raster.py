"""Windowed COG reads and Bronze raster writes."""

from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import rasterio
from rasterio.warp import transform_bounds
from rasterio.windows import Window, from_bounds

from delta_crops.config import BBox

#: GDAL/vsicurl settings used for every remote read. See PROJECT.md §3.4.
GDAL_ENV: dict[str, str] = {
    # Never list a "directory" over HTTP; the COG URL is a single object.
    "GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR",
    # Only .tif is a valid remote extension; avoids HEAD probes for sidecars.
    "CPL_VSIL_CURL_ALLOWED_EXTENSIONS": ".tif",
    # Retry transient 429/5xx at the libcurl level.
    "GDAL_HTTP_MAX_RETRY": "3",
    "GDAL_HTTP_RETRY_DELAY": "1",
    "GDAL_HTTP_TIMEOUT": "60",
    "GDAL_HTTP_CONNECTTIMEOUT": "10",
    # Let GDAL coalesce adjacent range reads into one multirange request.
    "GDAL_HTTP_MULTIRANGE": "YES",
    # Keep recently read blocks in memory across band reads of the same scene.
    "VSI_CACHE": "TRUE",
    "VSI_CACHE_SIZE": "50000000",
    # Keep responses compressed; GDAL inflates the deflate blocks itself.
    "GDAL_HTTP_HEADERS": "Accept-Encoding: identity",
}


def window_for_bbox(src: rasterio.DatasetReader, bbox: BBox) -> Window:
    """Return the pixel window covering the AOI, clipped to the dataset."""
    west, south, east, north = transform_bounds("EPSG:4326", src.crs, *bbox.as_list)
    window = from_bounds(west, south, east, north, src.transform).round()
    col_off = max(int(window.col_off), 0)
    row_off = max(int(window.row_off), 0)
    width = min(int(window.width), src.width - col_off)
    height = min(int(window.height), src.height - row_off)
    return Window(col_off, row_off, max(width, 0), max(height, 0))


def read_window(href: str, bbox: BBox) -> tuple[np.ndarray, rasterio.Affine, object, float | None]:
    """Read only the AOI window from a remote COG."""
    with rasterio.Env(**GDAL_ENV):
        with rasterio.open(href) as src:
            window = window_for_bbox(src, bbox)
            if window.width == 0 or window.height == 0:
                raise ValueError(f"AOI does not overlap {href}")
            array = src.read(1, window=window)
            return array, src.window_transform(window), src.crs, src.nodata


def nodata_pct(array: np.ndarray, nodata: float | None) -> float:
    """Percentage of nodata pixels inside the read window."""
    if nodata is None:
        return 0.0
    return float(100.0 * np.count_nonzero(array == nodata) / array.size)


def write_cog(path: Path, array: np.ndarray, transform: rasterio.Affine, crs: object, nodata: float | None) -> None:
    """Write a deflate-compressed COG atomically.

    No overviews: Silver reads 10 m windows at native resolution, so pyramids only
    cost space (measured: 3.9 MB of overviews on a 10.9 MB band).
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    profile = {
        "driver": "COG",
        "width": array.shape[1],
        "height": array.shape[0],
        "count": 1,
        "dtype": array.dtype.name,
        "crs": crs,
        "transform": transform,
        "compress": "DEFLATE",
        "predictor": 2 if array.dtype.kind == "u" else 1,
        "zlevel": 6,
        "blocksize": 512,
        "overviews": "NONE",
        "nodata": nodata,
    }
    try:
        with rasterio.open(tmp_path, "w", **profile) as dst:
            dst.write(array, 1)
        os.replace(tmp_path, path)
    finally:
        tmp_path.unlink(missing_ok=True)


def band_resolution_m(href: str) -> float:
    """Native resolution of one asset, in metres (used for logging)."""
    with rasterio.Env(**GDAL_ENV):
        with rasterio.open(href) as src:
            return float(abs(src.res[0]))

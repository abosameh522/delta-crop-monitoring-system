"""Offline unit tests for the Bronze -> Silver rules (Phase 3).

Every test here runs on tiny synthetic arrays: no network, no Bronze data.
"""

from __future__ import annotations

import numpy as np
import pytest

from delta_crops.config import load_config
from delta_crops.transform import silver
from delta_crops.transform.grid import Grid
from delta_crops.transform.reflectance import (
    REFLECTANCE_SCALE,
    ndvi_from_reflectance,
    ndvi_with_mask,
    to_reflectance,
    valid_pixel_mask,
)


# --------------------------------------------------------------------------- #
# Reflectance and NDVI
# --------------------------------------------------------------------------- #
def test_to_reflectance_is_scale_only():
    dn = np.array([[0, 1000, 10000]], dtype="uint16")
    assert np.allclose(to_reflectance(dn), dn.astype("float32") * REFLECTANCE_SCALE)
    # no -0.1 offset, no clipping
    assert to_reflectance(np.array([0], dtype="uint16"))[0] == pytest.approx(0.0)
    assert to_reflectance(np.array([20000], dtype="uint16"))[0] == pytest.approx(2.0)


def test_ndvi_handles_zero_denominator_and_is_bounded():
    red = np.array([0, 1000, 5000], dtype="float32")
    nir = np.array([0, 3000, 5000], dtype="float32")
    ndvi = ndvi_from_reflectance(red, nir)
    assert np.isnan(ndvi[0])  # 0/0 must not be 0 or inf
    assert ndvi[1] == pytest.approx(0.5)
    assert ndvi[2] == pytest.approx(0.0)
    assert np.all(np.isnan(ndvi) | ((ndvi >= -1) & (ndvi <= 1)))


def test_valid_pixel_mask_drops_zeros_and_masked_scl_classes():
    red = np.array([[1000, 0, 1000, 1000]])
    nir = np.array([[3000, 3000, 0, 3000]])
    scl = np.array([[4, 4, 4, 3]])  # last pixel is cloud shadow
    valid = valid_pixel_mask(red, nir, scl, (0, 1, 2, 3, 8, 9, 10, 11))
    assert valid.tolist() == [[True, False, False, False]]


def test_ndvi_with_mask_keeps_kept_classes_only():
    red = np.full((1, 4), 1000, dtype="uint16")
    nir = np.full((1, 4), 3000, dtype="uint16")
    scl = np.array([[4, 6, 7, 9]])  # vegetation, water, unclassified, cloud
    ndvi, valid = ndvi_with_mask(red, nir, scl, (0, 1, 2, 3, 8, 9, 10, 11))
    assert valid.tolist() == [[True, True, True, False]]
    assert np.isnan(ndvi[0, 3])
    assert np.allclose(ndvi[0, :3], 0.5)


# --------------------------------------------------------------------------- #
# Cell aggregation
# --------------------------------------------------------------------------- #
def _tiny_grid() -> Grid:
    """2x2 cells of 100 m -> 20x20 pixels at 10 m."""
    return Grid(x0=0.0, y0=0.0, ncols=2, nrows=2, cell_size_m=100)


def test_aggregate_canvas_uses_valid_pixels_as_denominator():
    grid = _tiny_grid()
    canvas = np.full(grid.pixel_shape, np.nan, dtype="float32")
    claimed = np.zeros(grid.pixel_shape, dtype=bool)
    # Cell (0,0): 50 of 100 pixels valid with NDVI 0.4 (exactly the threshold).
    canvas[0:5, 0:10] = 0.4
    claimed[0:5, 0:10] = True
    # Cell (0,1): 40 pixels valid -> below 50 %, NDVI must be NULL.
    canvas[0:4, 10:20] = 0.6
    claimed[0:4, 10:20] = True

    total_px = np.full(grid.shape, 100, dtype=np.int32)
    keep = np.ones(grid.shape, dtype=bool)
    out = silver.aggregate_canvas(canvas, claimed, total_px, keep, 0.5, grid)

    assert out["valid_pixels"][0] == 50
    assert out["ndvi"][0] == pytest.approx(0.4)
    assert out["masked_pct"][0] == pytest.approx(50.0)
    assert out["valid_pixels"][1] == 40
    assert np.isnan(out["ndvi"][1])
    assert out["masked_pct"][1] == pytest.approx(60.0)


def test_aggregate_canvas_counts_only_in_aoi_pixels_in_masked_pct():
    grid = _tiny_grid()
    canvas = np.full(grid.pixel_shape, np.nan, dtype="float32")
    claimed = np.zeros(grid.pixel_shape, dtype=bool)
    # Full valid cell, but the cell only has 60 in-AOI pixels (edge cell).
    canvas[0:10, 0:10] = 0.2
    claimed[0:10, 0:10] = True

    total_px = np.full(grid.shape, 100, dtype=np.int32)
    total_px[0, 0] = 60
    keep = np.ones(grid.shape, dtype=bool)
    out = silver.aggregate_canvas(canvas, claimed, total_px, keep, 0.5, grid)
    assert out["masked_pct"][0] == pytest.approx(0.0)


# --------------------------------------------------------------------------- #
# Tile merge
# --------------------------------------------------------------------------- #
def test_merge_block_first_valid_pixel_wins():
    canvas = np.full((10, 10), np.nan, dtype="float32")
    claimed = np.zeros((10, 10), dtype=bool)
    keep = np.ones((10, 10), dtype=bool)

    high = np.full((10, 10), 0.1, dtype="float32")
    valid = np.ones((10, 10), dtype=bool)
    assert silver.merge_block(canvas, claimed, high, valid, keep, 0, 0).written == 100
    assert np.allclose(canvas, 0.1)

    low = np.full((10, 10), 0.9, dtype="float32")
    result = silver.merge_block(canvas, claimed, low, valid, keep, 0, 0)
    assert result.written == 0
    assert np.allclose(canvas, 0.1)  # the first tile keeps the pixel


def test_merge_block_respects_placed_offset_and_aoi_mask():
    canvas = np.full((10, 10), np.nan, dtype="float32")
    claimed = np.zeros((10, 10), dtype=bool)
    keep = np.zeros((10, 10), dtype=bool)
    keep[0:5, 0:5] = True
    block = np.full((10, 10), 0.3, dtype="float32")
    valid = np.ones((10, 10), dtype=bool)
    result = silver.merge_block(canvas, claimed, block, valid, keep, 0, 0)
    assert result.written == 25
    assert np.isnan(canvas[5:, :]).all()


# --------------------------------------------------------------------------- #
# Config and file layout
# --------------------------------------------------------------------------- #
def test_config_mask_set_is_the_documented_one():
    config = load_config()
    assert config.silver.min_valid_fraction == 0.5
    assert set(config.silver.masked_scl_classes) == {0, 1, 2, 3, 8, 9, 10, 11}
    assert config.silver.tile_priority == ("36RTV", "36RUV")


def test_ndvi_path_uses_hive_partitions(tmp_path):
    from datetime import date

    path = silver.ndvi_path(tmp_path, date(2023, 7, 1))
    assert path.relative_to(tmp_path).as_posix() == "silver/ndvi/year=2023/month=07/date=2023-07-01.parquet"


def test_is_valid_output_rejects_wrong_row_count(tmp_path):
    import pyarrow as pa

    table = pa.table({"cell_id": pa.array([1, 2, 3], type=pa.int32())})
    target = tmp_path / "x.parquet"
    silver.write_parquet_atomic(table, target)
    assert silver.is_valid_output(target, 3)
    assert not silver.is_valid_output(target, 4)
    assert not silver.is_valid_output(tmp_path / "missing.parquet", 3)

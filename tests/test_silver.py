"""Offline unit tests for the Bronze -> Silver rules (Phase 3).

Every test here runs on tiny synthetic arrays: no network, no Bronze data.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import numpy as np
import pytest

from delta_crops.config import Config, data_dir, load_config
from delta_crops.transform import silver
from delta_crops.transform.grid import Grid, load_or_build
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


# --------------------------------------------------------------------------- #
# Cross-era stable target (Test 7)
# --------------------------------------------------------------------------- #
def _eroded_water_mask(grid: Grid, keep: np.ndarray, pix_mask: np.ndarray, erosion_px: int) -> np.ndarray:
    """Eroded WorldCover water (class 80) pixels inside the AOI, kept cells only."""
    from scipy import ndimage

    cache = Path("data/silver/_cache")
    wc = np.load(cache / "worldcover_295100_3403000_487x508.npy")
    keep_px = np.repeat(np.repeat(keep.reshape(grid.nrows, grid.ncols), 10, axis=0), 10, axis=1)
    water_raw = (wc == 80) & pix_mask & keep_px
    return ndimage.binary_erosion(water_raw, iterations=erosion_px)


def _valid_observations_for_year(
    year: int,
    dates: list[str],
    water_eroded: np.ndarray,
    config: Config,
    root: Path,
) -> dict[str, object]:
    """Return per-date and pooled validity + reflectance/NDVI values for one year.

    Pooling rule (from silver.stable_target.pooling): a pixel counts for the year
    if it is valid (SCL-unmasked AND DN > 0 in both bands) on at least one date
    in the year's 1-7 July window.
    """
    from datetime import date

    from delta_crops.transform import bronze_reader
    from delta_crops.transform.reflectance import ndvi_from_reflectance, to_reflectance

    ledger = bronze_reader.load_ledger(root)
    grid, keep, pix_mask = load_or_build(config.bbox, root / "silver" / "_cache")

    per_date: list[dict[str, object]] = []
    yearly_valid = np.zeros(water_eroded.shape, dtype=bool)
    red_values: list[float] = []
    ndvi_values: list[float] = []

    for d in dates:
        day = date.fromisoformat(d)
        tiles, problems = bronze_reader.assets_for_date(ledger, root, day, verify_checksum=False)
        if problems:
            raise RuntimeError(f"{d}: Bronze problems {problems}")
        tile_names = sorted(tiles)

        h, w = water_eroded.shape
        canvas_valid = np.zeros((h, w), dtype=bool)
        canvas_red = np.zeros((h, w), dtype="float32")
        canvas_ndvi = np.full((h, w), np.nan, dtype="float32")

        for tile_name in tile_names:
            bands = tiles[tile_name]
            red_dn, rt = bronze_reader.read_band(bands["B04"], config.bbox)
            nir_dn, _ = bronze_reader.read_band(bands["B08"], config.bbox)
            scl = bronze_reader.read_scl_on_grid(bands["SCL"], config.bbox, rt, red_dn.shape)
            row_off, col_off = bronze_reader.tile_offsets(rt, grid.x0, grid.y_top)
            H, W = canvas_valid.shape
            r0, c0 = max(0, row_off), max(0, col_off)
            r1, c1 = min(H, row_off + red_dn.shape[0]), min(W, col_off + red_dn.shape[1])
            sr0, sc0 = max(0, -row_off), max(0, -col_off)
            sub_r, sub_c = slice(sr0, sr0 + (r1 - r0)), slice(sc0, sc0 + (c1 - c0))
            dn_zero = (red_dn[sub_r, sub_c] <= 0) | (nir_dn[sub_r, sub_c] <= 0)
            scl_m = np.isin(scl[sub_r, sub_c], list(config.silver.masked_scl_classes))
            tile_valid = (~dn_zero) & (~scl_m)
            red_ref = to_reflectance(red_dn[sub_r, sub_c])
            ndvi = ndvi_from_reflectance(red_ref, to_reflectance(nir_dn[sub_r, sub_c]))

            rv = canvas_valid[r0:r1, c0:c1]
            rr = canvas_red[r0:r1, c0:c1]
            rn = canvas_ndvi[r0:r1, c0:c1]
            newly = tile_valid & ~rv
            rv[newly] = True
            rr[newly] = red_ref[newly]
            rn[newly] = ndvi[newly]
            del red_dn, nir_dn, scl, dn_zero, scl_m, tile_valid, red_ref, ndvi
            del rv, rr, rn, newly

        valid_water = canvas_valid & water_eroded
        count_today = int(valid_water.sum())
        yearly_valid |= valid_water
        yy, xx = np.nonzero(valid_water)
        red_values.extend(canvas_red[yy, xx].tolist())
        ndvi_values.extend(canvas_ndvi[yy, xx].tolist())

        per_date.append(
            {
                "date": d,
                "tiles": ",".join(tile_names),
                "valid_pixels": count_today,
            }
        )
        del canvas_valid, canvas_red, canvas_ndvi, valid_water, yy, xx

    pooled = int(yearly_valid.sum())
    red_arr = np.asarray(red_values, dtype="float64")
    ndvi_arr = np.asarray(ndvi_values, dtype="float64")
    return {
        "per_date": per_date,
        "pooled_valid_pixels": pooled,
        "median_red_reflectance": float(np.median(red_arr)) if red_arr.size else None,
        "median_ndvi": float(np.median(ndvi_arr)) if ndvi_arr.size else None,
        "n_observations": int(red_arr.size),
    }


def test_cross_era_stable_target_runs_and_is_within_tolerance():
    """Test 7: cross-era stable target on eroded pure-water pixels.

    Reads silver.stable_target from config. Skips only while it is null
    (not the case after the Option B config commit). Computes median red
    reflectance and median NDVI for eroded water pixels valid in each year's
    1-7 July window, then checks |delta| against the configured tolerances.
    No tolerance, pixel set, or window is changed if the test fails.
    """
    config = load_config()
    st = config.silver.stable_target
    assert st is not None, "stable_target must be frozen before test 7 runs"
    assert st["class"] == 80
    assert st["erosion_px"] == 3
    assert st["window"] == ["07-01", "07-07"]
    assert st["min_valid_pixels"] == 1000
    assert st["pooling"] == "at_least_one_date"
    assert st["max_delta_median_red"] == 0.02
    assert st["max_delta_median_ndvi"] == 0.05

    data_root = data_dir()
    silver_root = Path("data/silver")
    grid, keep, pix_mask = load_or_build(config.bbox, silver_root / "_cache")
    water_eroded = _eroded_water_mask(grid, keep, pix_mask, st["erosion_px"])
    total_eroded = int(water_eroded.sum())

    year_results: dict[int, dict[str, object]] = {}
    for year in (2019, 2023):
        dates = [f"{year}-07-01", f"{year}-07-03", f"{year}-07-06"] if year == 2019 else [
            "2023-07-02",
            "2023-07-05",
            "2023-07-07",
        ]
        result = _valid_observations_for_year(year, dates, water_eroded, config, data_root)
        year_results[year] = result
        print(
            f"\n  {year}: pooled_valid={result['pooled_valid_pixels']:,}/{total_eroded:,} "
            f"obs={result['n_observations']:,} "
            f"median_red={result['median_red_reflectance']:.4f} "
            f"median_ndvi={result['median_ndvi']:.4f}"
        )
        for pd in result["per_date"]:
            print(f"    {pd['date']}: tiles={pd['tiles']} valid={pd['valid_pixels']:,}")
        assert result["pooled_valid_pixels"] >= st["min_valid_pixels"]

    r19 = year_results[2019]
    r23 = year_results[2023]
    delta_red = abs(r19["median_red_reflectance"] - r23["median_red_reflectance"])
    delta_ndvi = abs(r19["median_ndvi"] - r23["median_ndvi"])
    print(
        f"\n  delta median red = {delta_red:.4f} (tolerance {st['max_delta_median_red']})"
    )
    print(
        f"  delta median NDVI = {delta_ndvi:.4f} (tolerance {st['max_delta_median_ndvi']})"
    )
    assert delta_red <= st["max_delta_median_red"], f"red delta {delta_red:.4f} > {st['max_delta_median_red']}"
    assert delta_ndvi <= st["max_delta_median_ndvi"], f"NDVI delta {delta_ndvi:.4f} > {st['max_delta_median_ndvi']}"

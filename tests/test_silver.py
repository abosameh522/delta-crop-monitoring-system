"""Offline unit tests for the Bronze -> Silver rules (Phase 3).

Every test here runs on tiny synthetic arrays: no network, no Bronze data.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import numpy as np
import pytest

from delta_crops.config import Config, data_dir, load_config
from delta_crops.transform import bronze_reader, silver
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
# Cross-era stable target (Test 7) — Option C: built-up (class 50)
# --------------------------------------------------------------------------- #
def _builtup_set(grid: Grid, keep: np.ndarray, pix_mask: np.ndarray, erosion_px: int, min_component_px: int) -> np.ndarray:
    """Eroded WorldCover built-up (class 50) pixels, keeping only components >= min_component_px."""
    from scipy import ndimage

    cache = Path("data/silver/_cache")
    wc = np.load(cache / "worldcover_295100_3403000_487x508.npy")
    keep_px = np.repeat(np.repeat(keep.reshape(grid.nrows, grid.ncols), 10, axis=0), 10, axis=1)
    built_raw = (wc == 50) & pix_mask & keep_px
    built_eroded = ndimage.binary_erosion(built_raw, iterations=erosion_px)
    labels, n_comp = ndimage.label(built_eroded, structure=np.ones((3, 3), dtype=int))
    if n_comp > 0:
        unique, counts = np.unique(labels, return_counts=True)
        valid = (unique > 0) & (counts >= min_component_px)
        big_labels = unique[valid]
        built_final = np.isin(labels, big_labels)
    else:
        built_final = np.zeros_like(built_eroded)
    return built_final


def _year_medians(
    year: int,
    dates: list[str],
    built_set: np.ndarray,
    config: Config,
    root: Path,
) -> dict[str, object]:
    """Compute per-date medians of red reflectance and NDVI on the built-up set,
    then return the median of the date-medians (per-date medians, then median of those).

    Qualifying dates: valid pixels >= min_valid_fraction of the set, with
    validity = SCL-unmasked AND DN > 0 in both bands AND (if require_dn_gt_one) DN > 1 in both bands.
    """
    from datetime import date

    from delta_crops.transform import bronze_reader
    from delta_crops.transform.reflectance import ndvi_from_reflectance, to_reflectance

    st = config.silver.stable_target
    ledger = bronze_reader.load_ledger(root)
    grid, keep, pix_mask = load_or_build(config.bbox, root / "silver" / "_cache")

    date_medians: list[dict[str, object]] = []
    qualifying_dates = 0

    for d in dates:
        day = date.fromisoformat(d)
        tiles, problems = bronze_reader.assets_for_date(ledger, root, day, verify_checksum=False)
        if problems:
            raise RuntimeError(f"{d}: Bronze problems {problems}")
        tile_names = sorted(tiles)

        h, w = built_set.shape
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

            red_sub = red_dn[sub_r, sub_c]
            nir_sub = nir_dn[sub_r, sub_c]
            scl_sub = scl[sub_r, sub_c]

            dn_zero = (red_sub <= 0) | (nir_sub <= 0)
            dn_one = (red_sub <= 1) | (nir_sub <= 1)
            scl_m = np.isin(scl_sub, list(config.silver.masked_scl_classes))

            if st.get("require_dn_gt_one", False):
                tile_valid = (~dn_zero) & (~dn_one) & (~scl_m)
            else:
                tile_valid = (~dn_zero) & (~scl_m)

            red_ref = to_reflectance(red_sub)
            ndvi = ndvi_from_reflectance(red_ref, to_reflectance(nir_sub))

            rv = canvas_valid[r0:r1, c0:c1]
            rr = canvas_red[r0:r1, c0:c1]
            rn = canvas_ndvi[r0:r1, c0:c1]
            newly = tile_valid & ~rv
            rv[newly] = True
            rr[newly] = red_ref[newly]
            rn[newly] = ndvi[newly]
            del red_dn, nir_dn, scl, dn_zero, dn_one, scl_m, tile_valid, red_ref, ndvi
            del rv, rr, rn, newly

        valid_built = canvas_valid & built_set
        count_today = int(valid_built.sum())
        total_built = int(built_set.sum())
        valid_fraction = count_today / total_built if total_built > 0 else 0.0

        if valid_fraction >= st["min_valid_fraction"]:
            qualifying_dates += 1
            yy, xx = np.nonzero(valid_built)
            red_vals = canvas_red[yy, xx]
            ndvi_vals = canvas_ndvi[yy, xx]
            date_medians.append(
                {
                    "date": d,
                    "tiles": ",".join(tile_names),
                    "valid_pixels": count_today,
                    "valid_fraction": valid_fraction,
                    "median_red": float(np.median(red_vals)) if red_vals.size else None,
                    "median_ndvi": float(np.median(ndvi_vals)) if ndvi_vals.size else None,
                    "n_obs": int(red_vals.size),
                }
            )
            print(
                f"    {d}: tiles={tile_names} valid={count_today:,}/{total_built:,} "
                f"frac={valid_fraction:.3f} QUALIFIES "
                f"median_red={date_medians[-1]['median_red']:.4f} "
                f"median_ndvi={date_medians[-1]['median_ndvi']:.4f}"
            )
        else:
            print(
                f"    {d}: tiles={tile_names} valid={count_today:,}/{total_built:,} "
                f"frac={valid_fraction:.3f} BELOW THRESHOLD ({st['min_valid_fraction']})"
            )

        del canvas_valid, canvas_red, canvas_ndvi, valid_built

    assert qualifying_dates >= st["min_qualifying_dates"], (
        f"Year {year}: only {qualifying_dates} qualifying dates, need >= {st['min_qualifying_dates']}"
    )

    # Median of date-medians
    red_medians = [m["median_red"] for m in date_medians if m["median_red"] is not None]
    ndvi_medians = [m["median_ndvi"] for m in date_medians if m["median_ndvi"] is not None]
    return {
        "per_date": date_medians,
        "qualifying_dates": qualifying_dates,
        "median_red": float(np.median(red_medians)) if red_medians else None,
        "median_ndvi": float(np.median(ndvi_medians)) if ndvi_medians else None,
    }


def test_cross_era_stable_target_runs_and_is_within_tolerance():
    """Test 7: cross-era stable target on built-up (class 50) pixels.

    Option C: WorldCover class 50 (built-up), erosion 3 px (30 m), connected
    components >= 1 km2 (10,000 px). Qualifying dates in 1-14 July window with
    valid fraction >= 0.5, requiring DN > 1 in both bands (excludes B08 DN==1 floor).
    Per-date medians are computed, then the median of date-medians per year.
    Tolerances: |delta median red| <= 0.02, |delta median NDVI| <= 0.05.
    No tolerance, pixel set, window, or rule is changed if the test fails.
    """
    config = load_config()
    st = config.silver.stable_target
    assert st is not None, "stable_target must be frozen before test 7 runs"
    assert st["class"] == 50
    assert st["erosion_px"] == 3
    assert st["min_component_px"] == 10000
    assert st["window"] == ["07-01", "07-14"]
    assert st["min_valid_fraction"] == 0.5
    assert st["min_qualifying_dates"] == 2
    assert st["require_dn_gt_one"] is True
    assert st["pooling"] == "median_of_date_medians"
    assert st["max_delta_median_red"] == 0.02
    assert st["max_delta_median_ndvi"] == 0.05

    data_root = data_dir()
    silver_root = Path("data/silver")
    grid, keep, pix_mask = load_or_build(config.bbox, silver_root / "_cache")
    built_set = _builtup_set(grid, keep, pix_mask, st["erosion_px"], st["min_component_px"])
    total_built = int(built_set.sum())
    print(f"\n  Total built-up set pixels: {total_built:,}")

    # Get all dates in the window for both years
    ledger = bronze_reader.load_ledger(data_root)
    all_dates = bronze_reader.imagery_dates(ledger)
    window_start, window_end = st["window"]
    window_start_day = int(window_start.split("-")[1])
    window_end_day = int(window_end.split("-")[1])
    dates_2019 = [d for d in all_dates if d.year == 2019 and d.month == 7 and window_start_day <= d.day <= window_end_day]
    dates_2023 = [d for d in all_dates if d.year == 2023 and d.month == 7 and window_start_day <= d.day <= window_end_day]
    dates_2019_str = [d.isoformat() for d in dates_2019]
    dates_2023_str = [d.isoformat() for d in dates_2023]
    print(f"  2019 dates in window: {dates_2019_str}")
    print(f"  2023 dates in window: {dates_2023_str}")

    year_results: dict[int, dict[str, object]] = {}
    for year, dates in ((2019, dates_2019_str), (2023, dates_2023_str)):
        result = _year_medians(year, dates, built_set, config, data_root)
        year_results[year] = result
        print(
            f"\n  {year}: qualifying_dates={result['qualifying_dates']} "
            f"median_red={result['median_red']:.4f} "
            f"median_ndvi={result['median_ndvi']:.4f}"
        )
        for pd in result["per_date"]:
            print(f"    {pd['date']}: tiles={pd['tiles']} valid={pd['valid_pixels']:,} "
                  f"median_red={pd['median_red']:.4f} median_ndvi={pd['median_ndvi']:.4f}")

    r19 = year_results[2019]
    r23 = year_results[2023]
    delta_red = abs(r19["median_red"] - r23["median_red"])
    delta_ndvi = abs(r19["median_ndvi"] - r23["median_ndvi"])
    print(
        f"\n  delta median red = {delta_red:.4f} (tolerance {st['max_delta_median_red']})"
    )
    print(
        f"  delta median NDVI = {delta_ndvi:.4f} (tolerance {st['max_delta_median_ndvi']})"
    )
    assert delta_red <= st["max_delta_median_red"], f"red delta {delta_red:.4f} > {st['max_delta_median_red']}"
    assert delta_ndvi <= st["max_delta_median_ndvi"], f"NDVI delta {delta_ndvi:.4f} > {st['max_delta_median_ndvi']}"

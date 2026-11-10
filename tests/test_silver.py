"""Offline unit tests for the Bronze -> Silver rules (Phase 3).

Unit tests use tiny arrays; test 7 requires the real local stable-target report.
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
# Cross-era stable target (Test 7): frozen within-orbit rule (D-035)
# --------------------------------------------------------------------------- #
def _qualifies(row: dict, rule: dict) -> bool:
    """Require the configured valid fraction and an allowed tile combination."""
    tiles = set(row["tile_mix"].split("+"))
    return row["option_c_valid_fraction"] >= rule["min_valid_fraction"] and any(
        tiles == set(allowed) for allowed in rule["allowed_tile_mixes"]
    )


def _orbit_results(rows: list[dict], rule: dict) -> list[dict]:
    """Summarize all configured groups before deciding whether the test passes."""
    results = []
    for orbit in rule["groups"]:
        years = {}
        for year in rule["years"]:
            selected = [r for r in rows if r["year"] == year and
                        r["relative_orbit"] == int(orbit[1:]) and
                        rule["window"][0] <= r["date"].strftime("%m-%d") <= rule["window"][1] and
                        _qualifies(r, rule)]
            years[year] = {
                "dates": sorted(str(r["date"]) for r in selected),
                "red": float(np.median([r["median_red"] for r in selected])) if selected else None,
                "ndvi": float(np.median([r["median_ndvi"] for r in selected])) if selected else None,
            }
        eligible = all(len(y["dates"]) >= rule["min_qualifying_dates"] for y in years.values())
        result = {"orbit": orbit, "years": years, "eligible": eligible}
        if eligible:
            old, new = (years[y] for y in rule["years"])
            result["delta_red"] = abs(old["red"] - new["red"])
            result["delta_ndvi"] = abs(old["ndvi"] - new["ndvi"])
            result["passes"] = (result["delta_red"] <= rule["max_delta_median_red"] and
                                result["delta_ndvi"] <= rule["max_delta_median_ndvi"])
        results.append(result)
    return results


def test_cross_era_stable_target_runs_and_is_within_tolerance():
    """Test 7 runs on real persisted measurements; missing data fails, never skips."""
    import json
    import re
    import pyarrow.parquet as pq

    rule = load_config().silver.stable_target
    assert rule is not None
    assert (rule["class"], rule["erosion_px"], rule["min_component_px"]) == (50, 3, 10000)
    assert rule["require_dn_gt_one"] is True
    assert rule["pooling"] == "median_of_date_medians"
    assert rule["years"] == [2019, 2023]
    assert rule["window"] == ["06-01", "08-31"]
    assert rule["grouping"] == "relative_orbit" and rule["orbit_source"] == "s2:product_uri"
    assert rule["eligibility"] == "min_qualifying_dates_in_each_year"
    assert rule["verdict"] == "all_eligible_groups_pass_both_tolerances"
    assert rule["groups"] == {"R021": {"sensing_time_utc_minute": "08:41"},
                              "R064": {"sensing_time_utc_minute": "08:51"}}
    assert rule["allowed_tile_mixes"] == [["36RTV", "36RUV"], ["36RUV"]]
    assert (rule["min_valid_fraction"], rule["min_qualifying_dates"]) == (0.5, 2)
    assert (rule["max_delta_median_red"], rule["max_delta_median_ndvi"]) == (0.02, 0.05)

    path = data_dir() / rule["report_path"]
    assert path.is_file(), (
        f"Required stable-target measurements missing: {path}. "
        "Restore/recreate stable_target_dates.parquet with the frozen Option C DN>1 "
        "measurements and s2:product_uri orbit metadata; this test cannot skip."
    )
    table = pq.read_table(path)
    metadata = table.schema.metadata or {}
    assert metadata.get(b'option_c') == b'WorldCover50; erosion3; components>=10000 pixels; same mask as existing test7'
    assert metadata.get(b'validity') == b'both B04 and B08 DN > 1; SCL excluded 0,1,2,3,8,9,10,11; sorted-tile first valid mosaic'
    rows = table.to_pylist()
    assert rows, "Stable-target measurement report is empty"
    assert len({r["date"] for r in rows}) == len(rows), "Duplicate stable-target dates"
    for row in rows:
        assert row["date"].year == row["year"]
        assert 0 <= row["option_c_valid_fraction"] <= 1
        assert np.isfinite(row["median_red"]) and np.isfinite(row["median_ndvi"])
        items = json.loads(row["source_items"])
        assert items, f"{row['date']}: missing source product metadata"
        derived = set()
        for props in items.values():
            match = re.search(r"_R(\d{3})_", props["s2:product_uri"])
            assert match, f"{row['date']}: product URI lacks relative orbit"
            derived.add(int(match.group(1)))
            assert props["datetime"][:10] == str(row["date"])
            assert props["datetime"][11:16] == row["sensing_time_utc_minute"]
        assert derived == {row["relative_orbit"]}, f"{row['date']}: orbit metadata mismatch"
        group = f"R{row['relative_orbit']:03d}"
        assert group in rule["groups"], f"Unexpected orbit {group}; never silently drop it"
        assert row["sensing_time_utc_minute"] == rule["groups"][group]["sensing_time_utc_minute"]

    results = _orbit_results(rows, rule)
    eligible = [r for r in results if r["eligible"]]
    assert eligible, "No orbit has enough qualifying dates in EACH year"
    for result in results:
        print(f"\n{result['orbit']} eligible={result['eligible']}")
        for year, summary in result["years"].items():
            print(f"  {year}: n={len(summary['dates'])} red={summary['red']} NDVI={summary['ndvi']}")
            print(f"  dates={summary['dates']}")
        if result["eligible"]:
            print(f"  delta_red={result['delta_red']:.8f} <= {rule['max_delta_median_red']}; "
                  f"delta_NDVI={result['delta_ndvi']:.8f} <= {rule['max_delta_median_ndvi']}; "
                  f"{'PASS' if result['passes'] else 'FAIL'}")
    assert all(r["passes"] for r in eligible), f"Eligible orbit failure(s): {eligible}"


def test_stable_target_qualifying_tiles_and_fraction():
    rule = load_config().silver.stable_target
    assert _qualifies({"tile_mix": "36RUV", "option_c_valid_fraction": 0.5}, rule)
    assert _qualifies({"tile_mix": "36RTV+36RUV", "option_c_valid_fraction": 0.5}, rule)
    assert not _qualifies({"tile_mix": "36RTV", "option_c_valid_fraction": 1.0}, rule)
    assert not _qualifies({"tile_mix": "36RUV", "option_c_valid_fraction": 0.499}, rule)


def _measurement_rows() -> list[dict]:
    """Tiny synthetic rows to test pooling and eligibility, not satellite evidence."""
    return [dict(date=date(year, 6, day), year=year, relative_orbit=orbit,
                 tile_mix="36RUV", option_c_valid_fraction=0.5,
                 median_red=red, median_ndvi=0.1)
            for year in (2019, 2023) for orbit in (21, 64)
            for day, red in ((1, 0.1), (6, 0.2), (11, 0.9))]


def test_stable_target_pools_date_medians_and_checks_both_years():
    rule = load_config().silver.stable_target
    rows = _measurement_rows()
    results = _orbit_results(rows, rule)
    assert all(r["eligible"] and r["years"][2019]["red"] == 0.2 for r in results)
    rows = [r for r in rows if not (r["year"] == 2023 and r["relative_orbit"] == 64 and r["date"].day != 1)]
    results = _orbit_results(rows, rule)
    assert results[0]["eligible"] and not results[1]["eligible"]


def test_stable_target_keeps_failing_eligible_orbit():
    rows = _measurement_rows()
    for row in rows:
        if row["year"] == 2023 and row["relative_orbit"] == 64:
            row["median_red"] += 0.03
    results = _orbit_results(rows, load_config().silver.stable_target)
    assert len(results) == 2 and all(r["eligible"] for r in results)
    assert results[0]["passes"] and not results[1]["passes"]

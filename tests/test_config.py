"""Scope and threshold checks for config/aoi.yaml."""

from __future__ import annotations

import math
from datetime import date
from pathlib import Path
from typing import Any

import pytest
import yaml

AOI_PATH = Path(__file__).resolve().parents[1] / "config" / "aoi.yaml"

KM_PER_DEG_LAT = 110.574
KM_PER_DEG_LON_AT_EQUATOR = 111.320

# Tanta and El-Mahalla El-Kubra must both sit comfortably inside the AOI.
TANTA = (30.7865, 31.0004)
MAHALLA = (30.9706, 31.1667)


@pytest.fixture(scope="module")
def aoi() -> dict[str, Any]:
    with AOI_PATH.open(encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def bbox_size_km(bbox: dict[str, float]) -> tuple[float, float]:
    """Return (width_km, height_km) of a WGS84 bbox.

    Uses a local flat approximation around the bbox centre, which is accurate to
    well under 1% for a 50 km box.
    """
    mid_lat = math.radians((bbox["south"] + bbox["north"]) / 2)
    width = (bbox["east"] - bbox["west"]) * KM_PER_DEG_LON_AT_EQUATOR * math.cos(mid_lat)
    height = (bbox["north"] - bbox["south"]) * KM_PER_DEG_LAT
    return width, height


def season_dates(aoi: dict[str, Any], year: int) -> tuple[date, date]:
    """Expand the MM-DD window for one study year into concrete dates."""
    window = aoi["seasons"]["window"]
    return (
        date.fromisoformat(f"{year}-{window['start']}"),
        date.fromisoformat(f"{year}-{window['end']}"),
    )


def test_bbox_is_roughly_50_by_50_km(aoi: dict[str, Any]) -> None:
    width, height = bbox_size_km(aoi["bbox"])
    assert 45 <= width <= 55, f"width is {width:.1f} km"
    assert 45 <= height <= 55, f"height is {height:.1f} km"


def test_bbox_is_ordered_and_inside_the_delta(aoi: dict[str, Any]) -> None:
    bbox = aoi["bbox"]
    assert bbox["west"] < bbox["east"]
    assert bbox["south"] < bbox["north"]
    assert 29.5 < bbox["west"] and bbox["east"] < 32.5
    assert 30.0 < bbox["south"] and bbox["north"] < 31.7
    assert aoi["crs"] == "EPSG:4326"


def test_bbox_contains_tanta_and_mahalla(aoi: dict[str, Any]) -> None:
    bbox = aoi["bbox"]
    for name, (lat, lon) in {"Tanta": TANTA, "El-Mahalla": MAHALLA}.items():
        assert bbox["south"] < lat < bbox["north"], f"{name} latitude outside AOI"
        assert bbox["west"] < lon < bbox["east"], f"{name} longitude outside AOI"


def test_seasons_are_contiguous_from_2019_with_2023_as_focus(aoi: dict[str, Any]) -> None:
    seasons = aoi["seasons"]
    years = seasons["years"]
    assert years[0] == 2019, "the baseline starts at 2019"
    assert years == list(range(2019, years[-1] + 1)), "baseline years must be contiguous"
    assert len(set(years)) == len(years), "baseline years must be unique"
    assert seasons["focus"] == 2023
    assert seasons["focus"] in years


def test_every_season_is_the_same_may_to_september_window(aoi: dict[str, Any]) -> None:
    for year in aoi["seasons"]["years"]:
        start, end = season_dates(aoi, year)
        assert (start.month, start.day) == (5, 1)
        assert (end.month, end.day) == (9, 30)
        assert start.year == end.year == year
        assert start < end


def test_grid_is_100m_in_utm_zone_36n(aoi: dict[str, Any]) -> None:
    grid = aoi["grid"]
    assert grid["cell_size_m"] == 100
    assert grid["crs"] == "EPSG:32636"


def test_quality_thresholds_match_the_architecture(aoi: dict[str, Any]) -> None:
    quality = aoi["quality"]
    assert quality["max_scene_cloud_pct"] == 40
    assert quality["min_weekly_observations"] == 2
    assert 0 < quality["max_weekly_masked_pct"] <= 100
    assert aoi["sources"]["imagery"] == "earth-search"

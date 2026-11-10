"""Earth Search STAC search and scene selection."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterable, Mapping

from pystac import Item
from pystac_client import Client

EARTH_SEARCH_URL = "https://earth-search.aws.element84.com/v1"
COLLECTION = "sentinel-2-l2a"

#: Pipeline band name -> Earth Search asset name.
BAND_ASSETS: dict[str, str] = {"B04": "red", "B08": "nir", "SCL": "scl"}


@dataclass(frozen=True)
class Scene:
    """The parts of a STAC item the ingestion needs."""

    item_id: str
    tile: str
    acquired: datetime
    baseline: tuple[int, ...]
    cloud_cover: float
    hrefs: Mapping[str, str]
    boa_offset_applied: bool | None = None

    @property
    def day(self) -> date:
        return self.acquired.date()


def _parse_baseline(value: object) -> tuple[int, ...]:
    try:
        return tuple(int(part) for part in str(value).split("."))
    except (TypeError, ValueError):
        return (0,)


def baseline_key(value: object) -> tuple[int, ...]:
    """Normalize processing baseline for numeric, deterministic ordering."""
    if isinstance(value, tuple):
        return value
    return _parse_baseline(value)


def _parse_tile(item_id: str) -> str:
    parts = item_id.split("_")
    return parts[1] if len(parts) > 1 else "unknown"


def scene_from_item(item: Item) -> Scene | None:
    """Convert a STAC item to a Scene, or None if a required asset is missing."""
    hrefs: dict[str, str] = {}
    for band, asset_name in BAND_ASSETS.items():
        asset = item.assets.get(asset_name)
        if asset is None or not asset.href:
            return None
        hrefs[band] = asset.href

    acquired = item.datetime
    if acquired is None:
        return None
    return Scene(
        item_id=item.id,
        tile=_parse_tile(item.id),
        acquired=acquired,
        baseline=_parse_baseline(item.properties.get("s2:processing_baseline")),
        cloud_cover=float(item.properties.get("eo:cloud_cover") or 0.0),
        hrefs=hrefs,
        boa_offset_applied=item.properties.get("earthsearch:boa_offset_applied"),
    )


def select_best(scenes: Iterable[Scene]) -> list[Scene]:
    """Keep one scene per (tile, acquisition date): the highest processing baseline.

    A single tile-date can carry several items (e.g. 2019-05-07 has two 02.12 items and
    one 05.00), so the newest baseline wins deterministically. Measured 10 m for both
    05.00 (2019) and 05.09 (2023) on the real smoke assets; no resolution assumption.
    """
    best: dict[tuple[str, date], Scene] = {}
    for scene in scenes:
        key = (scene.tile, scene.day)
        current = best.get(key)
        if current is None or (baseline_key(scene.baseline), scene.item_id) > (baseline_key(current.baseline), current.item_id):
            best[key] = scene
    return sorted(best.values(), key=lambda scene: (scene.day, scene.tile))


def build_search_params(
    bbox: list[float],
    start: date,
    end: date,
    max_cloud_pct: float,
    collection: str = COLLECTION,
) -> dict[str, object]:
    """Build the STAC search request body (pure function, no network).

    Raises ValueError for an inverted date range or a malformed bbox so a bad CLI
    call fails before it reaches the API.
    """
    if len(bbox) != 4:
        raise ValueError(f"bbox must have 4 values, got {len(bbox)}")
    west, south, east, north = bbox
    if west >= east or south >= north:
        raise ValueError(f"bbox is degenerate: {bbox}")
    if end < start:
        raise ValueError(f"date range is inverted: {start} > {end}")
    return {
        "bbox": [west, south, east, north],
        "datetime": f"{start.isoformat()}/{end.isoformat()}",
        "collections": [collection],
        "query": {"eo:cloud_cover": {"lt": max_cloud_pct}},
        "max_items": None,
    }


def search_scenes(
    bbox: list[float],
    start: date,
    end: date,
    max_cloud_pct: float,
    url: str = EARTH_SEARCH_URL,
) -> list[Scene]:
    """Query Earth Search for L2A scenes overlapping the bbox, deduplicated."""
    client = Client.open(url)
    search = client.search(**build_search_params(bbox, start, end, max_cloud_pct))
    scenes = [scene for item in search.items() if (scene := scene_from_item(item)) is not None]
    return select_best(scenes)


def expected_assets(scenes: Iterable[Scene]) -> set[str]:
    """Asset keys the ingestion must produce for a set of scenes."""
    return {f"{scene.item_id}:{band}" for scene in scenes for band in BAND_ASSETS}

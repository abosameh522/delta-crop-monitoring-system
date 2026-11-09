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

    Baseline 04.00+ is 10 m and harmonized; older baselines are often 20 m and not
    harmonized, so the newer version must win when both exist.
    """
    best: dict[tuple[str, date], Scene] = {}
    for scene in scenes:
        key = (scene.tile, scene.day)
        current = best.get(key)
        if current is None or (scene.baseline, scene.item_id) > (current.baseline, current.item_id):
            best[key] = scene
    return sorted(best.values(), key=lambda scene: (scene.day, scene.tile))


def search_scenes(
    bbox: list[float],
    start: date,
    end: date,
    max_cloud_pct: float,
    url: str = EARTH_SEARCH_URL,
) -> list[Scene]:
    """Query Earth Search for L2A scenes overlapping the bbox, deduplicated."""
    client = Client.open(url)
    search = client.search(
        bbox=bbox,
        **{"datetime": f"{start.isoformat()}/{end.isoformat()}"},
        collections=[COLLECTION],
        query={"eo:cloud_cover": {"lt": max_cloud_pct}},
        max_items=None,
    )
    scenes = [scene for item in search.items() if (scene := scene_from_item(item)) is not None]
    return select_best(scenes)


def expected_assets(scenes: Iterable[Scene]) -> set[str]:
    """Asset keys the ingestion must produce for a set of scenes."""
    return {f"{scene.item_id}:{band}" for scene in scenes for band in BAND_ASSETS}

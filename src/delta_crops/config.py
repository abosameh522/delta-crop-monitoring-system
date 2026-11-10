"""Typed loader for config/aoi.yaml."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONFIG_PATH = Path("config/aoi.yaml")


@dataclass(frozen=True)
class BBox:
    west: float
    south: float
    east: float
    north: float

    @property
    def as_list(self) -> list[float]:
        return [self.west, self.south, self.east, self.north]

    @property
    def center(self) -> tuple[float, float]:
        """Return (lat, lon) of the bbox centre."""
        return ((self.south + self.north) / 2, (self.west + self.east) / 2)


@dataclass(frozen=True)
class Seasons:
    focus: int
    years: tuple[int, ...]
    window_start: str
    window_end: str
    label: str

    def bounds(self, year: int) -> tuple[date, date]:
        """Expand the MM-DD window for one study year into concrete dates."""
        return (
            date.fromisoformat(f"{year}-{self.window_start}"),
            date.fromisoformat(f"{year}-{self.window_end}"),
        )


@dataclass(frozen=True)
class Grid:
    cell_size_m: int
    crs: str


@dataclass(frozen=True)
class Sources:
    imagery: str
    imagery_fallback: str


@dataclass(frozen=True)
class Quality:
    max_scene_cloud_pct: float
    min_weekly_observations: int
    max_weekly_masked_pct: float


@dataclass(frozen=True)
class Silver:
    """Rules that turn Bronze rasters into Silver tables (Phase 3)."""

    min_valid_fraction: float
    masked_scl_classes: tuple[int, ...]
    tile_priority: tuple[str, ...]
    max_scl_dark_pct: float
    max_scl_unclassified_pct: float
    stable_target: dict[str, Any] | None


@dataclass(frozen=True)
class Config:
    name: str
    bbox: BBox
    crs: str
    seasons: Seasons
    grid: Grid
    sources: Sources
    quality: Quality
    silver: Silver


def config_path(path: str | Path | None = None) -> Path:
    """Resolve the config path from an argument, the environment, or the default."""
    if path is not None:
        return Path(path)
    return Path(os.environ.get("DELTA_CROPS_CONFIG", DEFAULT_CONFIG_PATH))


def load_config(path: str | Path | None = None) -> Config:
    """Read and validate the AOI config file."""
    with config_path(path).open(encoding="utf-8") as handle:
        raw: dict[str, Any] = yaml.safe_load(handle)

    bbox = raw["bbox"]
    seasons = raw["seasons"]
    return Config(
        name=raw["name"],
        bbox=BBox(
            west=float(bbox["west"]),
            south=float(bbox["south"]),
            east=float(bbox["east"]),
            north=float(bbox["north"]),
        ),
        crs=raw["crs"],
        seasons=Seasons(
            focus=int(seasons["focus"]),
            years=tuple(int(year) for year in seasons["years"]),
            window_start=str(seasons["window"]["start"]),
            window_end=str(seasons["window"]["end"]),
            label=str(seasons["label"]),
        ),
        grid=Grid(cell_size_m=int(raw["grid"]["cell_size_m"]), crs=str(raw["grid"]["crs"])),
        sources=Sources(
            imagery=str(raw["sources"]["imagery"]),
            imagery_fallback=str(raw["sources"]["imagery_fallback"]),
        ),
        quality=Quality(
            max_scene_cloud_pct=float(raw["quality"]["max_scene_cloud_pct"]),
            min_weekly_observations=int(raw["quality"]["min_weekly_observations"]),
            max_weekly_masked_pct=float(raw["quality"]["max_weekly_masked_pct"]),
        ),
        silver=Silver(
            min_valid_fraction=float(raw["silver"]["min_valid_fraction"]),
            masked_scl_classes=tuple(int(c) for c in raw["silver"]["masked_scl_classes"]),
            tile_priority=tuple(str(t) for t in raw["silver"]["tile_priority"]),
            max_scl_dark_pct=float(raw["silver"]["max_scl_dark_pct"]),
            max_scl_unclassified_pct=float(raw["silver"]["max_scl_unclassified_pct"]),
            stable_target=raw["silver"].get("stable_target"),
        ),
    )


def data_dir() -> Path:
    """Root of the local data lake (gitignored)."""
    return Path(os.environ.get("DELTA_CROPS_DATA_DIR", "data"))

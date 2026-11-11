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
class Ingestion:
    """Asset download retry policy (Phase 6a step 7)."""

    asset_attempts: int = 3
    retry_base_delay_s: float = 1.0


@dataclass(frozen=True)
class Heatwave:
    threshold_c: float
    min_consecutive_days: int


@dataclass(frozen=True)
class Gold:
    """Rules that turn Silver into the weekly Gold tables (Phase 5)."""

    week_first: int
    week_last: int
    min_observations: int
    max_masked_pct: float
    min_baseline_years: int
    heatwave: Heatwave
    max_gate_null_pct: float
    orbit_cache: str
    duckdb_threads: int
    duckdb_memory_limit: str


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
    data_quality: dict[str, Any] | None = None
    gold: Gold | None = None
    ingestion: Ingestion = Ingestion()


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
        data_quality=raw.get("data_quality"),
        silver=Silver(
            min_valid_fraction=float(raw["silver"]["min_valid_fraction"]),
            masked_scl_classes=tuple(int(c) for c in raw["silver"]["masked_scl_classes"]),
            tile_priority=tuple(str(t) for t in raw["silver"]["tile_priority"]),
            max_scl_dark_pct=float(raw["silver"]["max_scl_dark_pct"]),
            max_scl_unclassified_pct=float(raw["silver"]["max_scl_unclassified_pct"]),
            stable_target=raw["silver"].get("stable_target"),
        ),
        gold=_load_gold(raw.get("gold")),
        ingestion=_load_ingestion(raw.get("ingestion")),
    )


def _load_ingestion(raw: dict[str, Any] | None) -> Ingestion:
    """Parse the optional retry block; defaults are the documented policy."""
    if raw is None:
        return Ingestion()
    return Ingestion(
        asset_attempts=int(raw.get("asset_attempts", 3)),
        retry_base_delay_s=float(raw.get("retry_base_delay_s", 1.0)),
    )


def _load_gold(raw: dict[str, Any] | None) -> Gold | None:
    """Parse the Phase 5 block; absent means Gold is not configured yet."""
    if raw is None:
        return None
    heatwave = raw["heatwave"]
    return Gold(
        week_first=int(raw["weeks"]["first"]),
        week_last=int(raw["weeks"]["last"]),
        min_observations=int(raw["min_observations"]),
        max_masked_pct=float(raw["max_masked_pct"]),
        min_baseline_years=int(raw["min_baseline_years"]),
        heatwave=Heatwave(
            threshold_c=float(heatwave["threshold_c"]),
            min_consecutive_days=int(heatwave["min_consecutive_days"]),
        ),
        max_gate_null_pct=float(raw["max_gate_null_pct"]),
        orbit_cache=str(raw["orbit_cache"]),
        duckdb_threads=int(raw["duckdb_threads"]),
        duckdb_memory_limit=str(raw["duckdb_memory_limit"]),
    )


def require_gold(config: Config) -> Gold:
    """Return the Gold rules or fail loudly: thresholds have no implicit defaults."""
    if config.gold is None:
        raise ValueError("config is missing the 'gold' block (Phase 5 thresholds)")
    return config.gold


def data_dir() -> Path:
    """Root of the local data lake (gitignored)."""
    return Path(os.environ.get("DELTA_CROPS_DATA_DIR", "data"))

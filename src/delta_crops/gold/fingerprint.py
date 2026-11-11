"""Fingerprints for every Gold output.

A fingerprint is written next to its Parquet file as
`<name>.parquet.fingerprint.json`. It records the inputs that determine that
output, so a run can tell the difference between "nothing changed" and "the
inputs changed but the row count happens to be unchanged".

Silver partition identity uses the relative file name, byte size and
`mtime_ns`. Silver writes one file per date atomically and never rewrites a date
in place, so this is enough to detect added, removed or replaced partitions
without re-hashing ~750 MB of pixel data on every build. Daily weather is one
small file, so its full SHA-256 is cheap and is used instead.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any

from delta_crops.config import Config, require_gold
from delta_crops.gold.as_of import season_state

#: Bump when the meaning of any Gold output changes.
GOLD_FINGERPRINT_VERSION = "gold_v1"

TABLE_FILES = {
    "weekly_ndvi": "weekly_ndvi.parquet",
    "ndvi_anomaly": "ndvi_anomaly.parquet",
    "weekly_weather": "weekly_weather.parquet",
}


def output_path(root: Path, table: str, year: int) -> Path:
    """Path of one Gold table year partition."""
    return root / "gold" / table / f"year={year}" / TABLE_FILES[table]


def fingerprint_path(target: Path) -> Path:
    """Fingerprint file stored next to its Parquet output."""
    return target.with_name(target.name + ".fingerprint.json")


def read_fingerprint(path: Path) -> dict[str, Any] | None:
    """Return the stored fingerprint, or None when the file is absent."""
    if not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def write_fingerprint(path: Path, payload: dict[str, Any]) -> None:
    """Atomically replace a fingerprint file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    try:
        tmp.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def fingerprints_match(stored: dict[str, Any] | None, current: dict[str, Any] | None) -> bool:
    """Exact structural equality; None never matches a real fingerprint."""
    return stored is not None and current is not None and stored == current


def canonical_hash(payload: Any) -> str:
    """Stable SHA-256 of a JSON-serialisable payload."""
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_file(path: Path) -> str:
    """Streaming SHA-256 of a file."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def weather_file_sha256(root: Path) -> str | None:
    """SHA-256 of the single Silver daily weather file, if present."""
    path = root / "silver" / "weather.parquet"
    return sha256_file(path) if path.is_file() else None


def gold_config_hash(config: Config) -> str:
    """Hash of every configured rule that can change a Gold output."""
    return canonical_hash(
        {
            "seasons": {
                "window": [config.seasons.window_start, config.seasons.window_end],
                "years": list(config.seasons.years),
            },
            "gold": asdict(require_gold(config)),
        }
    )


def silver_ndvi_identity(root: Path, year: int) -> list[dict[str, Any]]:
    """Name, size and mtime of every Silver NDVI partition for one year."""
    directory = root / "silver" / "ndvi" / f"year={year}"
    if not directory.is_dir():
        return []
    entries = []
    for path in sorted(directory.glob("*/date=*.parquet")):
        stat = path.stat()
        entries.append(
            {
                "path": str(path.relative_to(directory)),
                "bytes": stat.st_size,
                "mtime_ns": stat.st_mtime_ns,
            }
        )
    return entries


def _base(table: str, year: int, as_of: date | None, config: Config) -> dict[str, Any]:
    return {
        "version": GOLD_FINGERPRINT_VERSION,
        "table": table,
        "year": year,
        "as_of": as_of.isoformat() if as_of is not None else None,
        "config_hash": gold_config_hash(config),
    }


def weekly_fingerprint(root: Path, config: Config, year: int) -> dict[str, Any]:
    """Inputs of `gold/weekly_ndvi/year=Y`."""
    state = season_state(root, config, year)
    payload = _base("weekly_ndvi", year, state.as_of, config)
    payload["weather_sha256"] = weather_file_sha256(root)
    payload["silver_files"] = silver_ndvi_identity(root, year)
    return payload


def weather_fingerprint(root: Path, config: Config, year: int) -> dict[str, Any]:
    """Inputs of `gold/weekly_weather/year=Y`."""
    state = season_state(root, config, year)
    payload = _base("weekly_weather", year, state.as_of, config)
    payload["weather_sha256"] = weather_file_sha256(root)
    return payload


def stored_weekly_fingerprint(root: Path, year: int) -> dict[str, Any] | None:
    """The weekly fingerprint currently stored next to the output."""
    return read_fingerprint(fingerprint_path(output_path(root, "weekly_ndvi", year)))


def anomaly_fingerprint(
    root: Path,
    config: Config,
    year: int,
    complete: list[int] | tuple[int, ...],
) -> dict[str, Any]:
    """Inputs of `gold/ndvi_anomaly/year=Y`.

    The baseline pool is the complete seasons other than `year`; their *stored*
    weekly fingerprints identify the weekly files actually read by the build.
    The current year's stored fingerprint is included too because the anomaly
    row carries the current weekly NDVI.
    """
    state = season_state(root, config, year)
    complete_sorted = sorted(int(item) for item in complete)
    pool = [item for item in complete_sorted if item != year]
    payload = _base("ndvi_anomaly", year, state.as_of, config)
    payload["complete_years"] = complete_sorted
    payload["baseline_years"] = pool
    payload["baseline_weekly"] = {
        str(item): stored_weekly_fingerprint(root, item) for item in pool
    }
    payload["current_weekly"] = stored_weekly_fingerprint(root, year)
    return payload


def verify_weekly_inputs(root: Path, year: int) -> str | None:
    """Reason adoption of a weekly output cannot be verified, or None."""
    if not silver_ndvi_identity(root, year):
        return f"{year}: no Silver NDVI partitions"
    if weather_file_sha256(root) is None:
        return f"{year}: silver/weather.parquet is missing"
    return None


def verify_weather_inputs(root: Path, config: Config, year: int) -> str | None:
    """Reason adoption of a weekly-weather output cannot be verified, or None."""
    if weather_file_sha256(root) is None:
        return f"{year}: silver/weather.parquet is missing"
    state = season_state(root, config, year)
    if state.as_of is None:
        return f"{year}: no as_of (weather and acquisition dates are incomplete)"
    return None


def verify_anomaly_inputs(root: Path, year: int, complete: list[int] | tuple[int, ...]) -> str | None:
    """Reason adoption of an anomaly output cannot be verified, or None."""
    pool = [item for item in sorted(int(value) for value in complete) if item != year]
    for dependency in [year, *pool]:
        if stored_weekly_fingerprint(root, dependency) is None:
            return f"{year}: weekly fingerprint missing for {dependency}"
    return None

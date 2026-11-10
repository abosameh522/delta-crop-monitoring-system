"""Silver weather table from the Bronze Open-Meteo JSON.

One row per local day for every configured season year; a missing day is a hard
error because reanalysis is complete, so a gap would mean a broken download.
"""

from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from delta_crops.config import Config

COLUMNS = {
    "temperature_2m_max": "temp_max_c",
    "temperature_2m_min": "temp_min_c",
    "temperature_2m_mean": "temp_mean_c",
    "precipitation_sum": "precip_mm",
    "shortwave_radiation_sum": "solar_mj_m2",
}

WEATHER_SCHEMA = pa.schema(
    [
        ("date", pa.date32()),
        ("temp_max_c", pa.float32()),
        ("temp_min_c", pa.float32()),
        ("temp_mean_c", pa.float32()),
        ("precip_mm", pa.float32()),
        ("solar_mj_m2", pa.float32()),
    ]
)


def build_weather(config: Config, data_root: Path) -> int:
    """Read all season years from Bronze and write silver/weather.parquet."""
    rows: list[dict[str, object]] = []
    seen: set[date] = set()
    for year in config.seasons.years:
        path = data_root / "bronze" / "weather" / f"year={year}" / "open_meteo_daily.json"
        if not path.exists():
            raise FileNotFoundError(f"missing Bronze weather for {year}: {path}")
        payload = json.loads(path.read_text(encoding="utf-8"))
        daily = payload["daily"]
        for index, day in enumerate(daily["time"]):
            parsed = date.fromisoformat(day)
            if parsed in seen:
                raise ValueError(f"duplicate weather date {parsed}")
            seen.add(parsed)
            row: dict[str, object] = {"date": parsed}
            for source_name, column in COLUMNS.items():
                value = daily[source_name][index]
                if value is None:
                    raise ValueError(f"null {source_name} on {parsed}")
                row[column] = float(value)
            rows.append(row)

    rows.sort(key=lambda row: row["date"])
    expected = sum((config.seasons.bounds(year)[1] - config.seasons.bounds(year)[0]).days + 1 for year in config.seasons.years)
    if len(rows) != expected:
        raise ValueError(f"weather has {len(rows)} days, expected {expected}")

    table = pa.Table.from_pylist(rows, schema=WEATHER_SCHEMA)
    target = data_root / "silver" / "weather.parquet"
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(".parquet.tmp")
    try:
        pq.write_table(table, tmp, compression="zstd")
        os.replace(tmp, target)
    finally:
        tmp.unlink(missing_ok=True)
    return len(rows)

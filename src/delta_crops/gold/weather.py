"""Weekly weather and the heatwave definition baked into Gold.

A heatwave day is a day whose daily maximum temperature is at or above the
configured threshold and that lies inside a run of at least the configured
number of consecutive such days. Runs are detected on the full 1 May - 30 Sep
daily series, then counted per ISO week, so a run crossing a week boundary is
split by day, not restarted.
"""

from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from delta_crops.config import Config, require_gold
from delta_crops.gold.weekly import season_weeks, weather_path

WEEKLY_WEATHER_SCHEMA = pa.schema(
    [
        ("week_start", pa.date32()),
        ("temp_max_mean_c", pa.float32()),
        ("temp_max_max_c", pa.float32()),
        ("temp_min_mean_c", pa.float32()),
        ("precip_sum_mm", pa.float32()),
        ("solar_sum_mj_m2", pa.float32()),
        ("heatwave_days", pa.uint8()),
        ("n_days", pa.uint8()),
    ]
)


def heatwave_day_flags(
    temp_max_c: list[float], threshold_c: float, min_consecutive_days: int
) -> list[bool]:
    """Mark every day of a qualifying hot run; shorter runs stay unflagged."""
    if min_consecutive_days < 1:
        raise ValueError("min_consecutive_days must be >= 1")
    flags = [False] * len(temp_max_c)
    index = 0
    while index < len(temp_max_c):
        if temp_max_c[index] < threshold_c:
            index += 1
            continue
        end = index
        while end + 1 < len(temp_max_c) and temp_max_c[end + 1] >= threshold_c:
            end += 1
        if end - index + 1 >= min_consecutive_days:
            for position in range(index, end + 1):
                flags[position] = True
        index = end + 1
    return flags


def build_weekly_weather(root: Path, config: Config, force: bool = False) -> dict:
    """Write `gold/weekly_weather/year=Y/` from the Silver daily weather table."""
    gold = require_gold(config)
    source = root / "silver" / "weather.parquet"
    table = pq.read_table(source, columns=["date", "temp_max_c", "temp_min_c", "precip_mm", "solar_mj_m2"])
    daily: dict[int, list[dict]] = {}
    for row in table.to_pylist():
        daily.setdefault(row["date"].year, []).append(row)
    summary: dict = {"years": []}
    for year in config.seasons.years:
        rows = sorted(daily.get(year, []), key=lambda row: row["date"])
        weeks = season_weeks(year, gold.week_first, gold.week_last)
        expected_days = (config.seasons.bounds(year)[1] - config.seasons.bounds(year)[0]).days + 1
        if len(rows) != expected_days:
            raise ValueError(f"{year}: {len(rows)} weather days, expected {expected_days}")
        by_date = {row["date"]: row for row in rows}
        flags = heatwave_day_flags(
            [row["temp_max_c"] for row in rows],
            gold.heatwave.threshold_c,
            gold.heatwave.min_consecutive_days,
        )
        hot_days = {row["date"] for row, flag in zip(rows, flags) if flag}
        output = []
        for week_start in weeks:
            days = [by_date[week_start + timedelta(days=offset)] for offset in range(7)]
            output.append({
                "week_start": week_start,
                "temp_max_mean_c": sum(day["temp_max_c"] for day in days) / 7,
                "temp_max_max_c": max(day["temp_max_c"] for day in days),
                "temp_min_mean_c": sum(day["temp_min_c"] for day in days) / 7,
                "precip_sum_mm": sum(day["precip_mm"] for day in days),
                "solar_sum_mj_m2": sum(day["solar_mj_m2"] for day in days),
                "heatwave_days": sum(day["date"] in hot_days for day in days),
                "n_days": 7,
            })
        target = weather_path(root, year)
        target.parent.mkdir(parents=True, exist_ok=True)
        if not force and target.is_file() and pq.ParquetFile(target).metadata.num_rows == len(weeks):
            summary["years"].append(dict(year=year, rows=len(weeks), skipped=True))
            continue
        tmp = target.with_suffix(".parquet.tmp")
        try:
            pq.write_table(pa.Table.from_pylist(output, schema=WEEKLY_WEATHER_SCHEMA), tmp, compression="zstd")
            os.replace(tmp, target)
        finally:
            tmp.unlink(missing_ok=True)
        summary["years"].append(dict(year=year, rows=len(weeks), skipped=False,
                                     heatwave_days=len(hot_days)))
    return summary

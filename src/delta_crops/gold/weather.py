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
from delta_crops.gold.as_of import season_states
from delta_crops.gold.fingerprint import (
    fingerprint_path,
    fingerprints_match,
    read_fingerprint,
    verify_weather_inputs,
    weather_fingerprint,
    write_fingerprint,
)
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


def build_weekly_weather(root: Path, config: Config, force: bool = False, adopt: bool = False) -> dict:
    """Write `gold/weekly_weather/year=Y/` and its fingerprint; skip verified output."""
    gold = require_gold(config)
    source = root / "silver" / "weather.parquet"
    table = pq.read_table(source, columns=["date", "temp_max_c", "temp_min_c", "precip_mm", "solar_mj_m2"])
    daily: dict[int, list[dict]] = {}
    for row in table.to_pylist():
        daily.setdefault(row["date"].year, []).append(row)
    summary: dict = {
        "table": "weekly_weather", "years": [], "errors": [],
        "built": 0, "skipped": 0, "rebuilt_due_to_input_change": 0,
        "unfingerprinted": 0, "adopted": 0, "failed": 0,
    }
    states = season_states(root, config, config.seasons.years)
    for year in config.seasons.years:
        state = states[year]
        rows = sorted(daily.get(year, []), key=lambda row: row["date"])
        weeks = season_weeks(year, gold.week_first, gold.week_last)
        if state.as_of is not None:
            weeks = [week for week in weeks
                     if week + timedelta(days=6) <= state.as_of]
        else:
            weeks = []
        by_date = {row["date"]: row for row in rows}
        start, end = config.seasons.bounds(year)
        if state.complete:
            required_end: date | None = end
        elif weeks:
            required_end = weeks[-1] + timedelta(days=6)
        else:
            required_end = None
        if required_end is not None:
            missing = [
                start + timedelta(days=offset)
                for offset in range((required_end - start).days + 1)
                if start + timedelta(days=offset) not in by_date
            ]
            if missing:
                raise ValueError(
                    f"{year}: {len(missing)} missing weather days before {required_end}"
                )
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
        stored = read_fingerprint(fingerprint_path(target))
        build_reason = None
        if not force and target.is_file():
            if stored is None and not adopt:
                summary["unfingerprinted"] += 1
                print(f"{year} weather skip (unfingerprinted)", flush=True)
                summary["years"].append(dict(year=year, rows=len(weeks), status="unfingerprinted",
                                             skipped=True, as_of=state.as_of, complete=state.complete))
                continue
            if stored is None and adopt:
                problem = verify_weather_inputs(root, config, year)
                if problem:
                    summary["failed"] += 1
                    summary["errors"].append(problem)
                    summary["years"].append(dict(year=year, rows=0, status="failed",
                                                 skipped=True, as_of=state.as_of,
                                                 complete=state.complete))
                    continue
                write_fingerprint(fingerprint_path(target), weather_fingerprint(root, config, year))
                summary["adopted"] += 1
                print(f"{year} weather adopt (verified inputs)", flush=True)
                summary["years"].append(dict(year=year, rows=len(weeks), status="adopted",
                                             skipped=True, as_of=state.as_of, complete=state.complete))
                continue
            current = weather_fingerprint(root, config, year)
            valid = pq.ParquetFile(target).metadata.num_rows == len(weeks)
            if fingerprints_match(stored, current) and valid:
                summary["skipped"] += 1
                summary["years"].append(dict(year=year, rows=len(weeks), status="skipped",
                                             skipped=True, as_of=state.as_of, complete=state.complete))
                continue
            build_reason = ("input changed" if not fingerprints_match(stored, current)
                            else "invalid output")
        tmp = target.with_suffix(".parquet.tmp")
        try:
            pq.write_table(pa.Table.from_pylist(output, schema=WEEKLY_WEATHER_SCHEMA), tmp, compression="zstd")
            os.replace(tmp, target)
        finally:
            tmp.unlink(missing_ok=True)
        write_fingerprint(fingerprint_path(target), weather_fingerprint(root, config, year))
        if build_reason == "input changed":
            summary["rebuilt_due_to_input_change"] += 1
            status = "rebuilt_due_to_input_change"
        else:
            summary["built"] += 1
            status = "built"
        summary["years"].append(dict(year=year, rows=len(weeks), skipped=False,
                                     heatwave_days=len(hot_days), status=status,
                                     as_of=state.as_of, complete=state.complete))
    return summary

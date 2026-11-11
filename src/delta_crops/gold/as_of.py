"""Per-year observation cut-off (`as_of`) for Gold.

Gold never consults the wall clock. A season is *complete* when every configured
weather day from season start to season end is present in
`silver/weather.parquet` (the season end has therefore passed). A complete
season is observed through its configured season end, whatever the date of the
last image. A season that is still in progress is observed through the earlier
of its last Silver acquisition date and its last weather date.

Only complete seasons may contribute to another year's anomaly baseline.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from delta_crops.config import Config


@dataclass(frozen=True)
class SeasonState:
    """Observation cut-off and completeness for one season year."""

    year: int
    season_end: date
    complete: bool
    as_of: date | None
    latest_acquisition: date | None
    latest_weather: date | None


def season_end(config: Config, year: int) -> date:
    """Configured last day of the season (never a hard-coded date)."""
    return config.seasons.bounds(year)[1]


def expected_weather_days(config: Config, year: int) -> int:
    """Number of daily weather records a complete season must contain."""
    start, end = config.seasons.bounds(year)
    return (end - start).days + 1


def weather_dates_by_year(root: Path) -> dict[int, set[date]]:
    """Distinct weather dates per year from the Silver weather table."""
    path = root / "silver" / "weather.parquet"
    if not path.is_file():
        return {}
    dates = pq.read_table(path, columns=["date"]).column("date").to_pylist()
    by_year: dict[int, set[date]] = {}
    for day in dates:
        by_year.setdefault(day.year, set()).add(day)
    return by_year


def latest_acquisition_by_year(root: Path) -> dict[int, date]:
    """Latest date with Silver NDVI rows per year (the last processed image)."""
    directory = root / "silver" / "ndvi"
    if not directory.is_dir() or not any(directory.glob("year=*/*/*.parquet")):
        return {}
    glob = str(directory / "year=*" / "*" / "*.parquet")
    con = duckdb.connect()
    try:
        rows = con.execute(
            "SELECT year(date) AS yr, max(date) AS latest"
            " FROM read_parquet(?) GROUP BY 1",
            [glob],
        ).fetchall()
    finally:
        con.close()
    return {int(year): latest for year, latest in rows if year is not None and latest is not None}


def season_states(root: Path, config: Config, years: list[int] | tuple[int, ...]) -> dict[int, SeasonState]:
    """Compute the observation cut-off for every requested year in one pass."""
    weather = weather_dates_by_year(root)
    acquisitions = latest_acquisition_by_year(root)
    states: dict[int, SeasonState] = {}
    for year in years:
        end = season_end(config, year)
        dates = weather.get(year, set())
        complete = end in dates and len(dates) == expected_weather_days(config, year)
        latest_weather = max(dates) if dates else None
        latest_acq = acquisitions.get(year)
        if complete:
            as_of: date | None = min(end, latest_weather or end)
        elif latest_acq is not None and latest_weather is not None:
            as_of = min(latest_acq, latest_weather, end)
        else:
            as_of = None
        states[year] = SeasonState(
            year=year,
            season_end=end,
            complete=complete,
            as_of=as_of,
            latest_acquisition=latest_acq,
            latest_weather=latest_weather,
        )
    return states


def season_state(root: Path, config: Config, year: int) -> SeasonState:
    """Observation cut-off for one year."""
    return season_states(root, config, [year])[year]


def complete_years(root: Path, config: Config, years: list[int] | tuple[int, ...]) -> tuple[int, ...]:
    """Years that satisfy the complete-season rule, sorted."""
    states = season_states(root, config, years)
    return tuple(sorted(year for year, state in states.items() if state.complete))


def week_is_observed(week_start: date, as_of: date | None) -> bool:
    """A week is evaluated only when its Sunday is on or before ``as_of``."""
    return as_of is not None and week_start + timedelta(days=6) <= as_of

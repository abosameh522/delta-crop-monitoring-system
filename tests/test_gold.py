"""Phase 5 Gold tests: heatwave rule, week keys, statuses, anomaly, build shape."""

from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from delta_crops.config import Config, load_config
from delta_crops.gold import weather as gold_weather
from delta_crops.gold.anomaly import build_anomaly
from delta_crops.gold.as_of import (
    complete_years,
    season_end,
    season_state,
    week_is_observed,
)
from delta_crops.gold.queries import sample_query
from delta_crops.gold.weekly import (
    STATUS_CASE,
    build_weekly_ndvi,
    iso_week_start,
    season_weeks,
    weekly_path,
)
from delta_crops.gold.weather import build_weekly_weather, heatwave_day_flags

YEARS = (2021, 2022, 2023, 2024)
CROPLAND_VALUES = {2020: 0.10, 2021: 0.20, 2022: 0.30, 2023: 0.40, 2024: 0.50}
NDVI_SCHEMA = pa.schema([
    ("cell_id", pa.int32()), ("date", pa.date32()), ("ndvi", pa.float32()),
    ("valid_pixels", pa.uint8()), ("total_pixels", pa.uint8()), ("masked_pct", pa.float32()),
])


def test_heatwave_flags_only_complete_runs_including_the_threshold_day() -> None:
    temps = [39.0, 40.0, 41.0, 42.0, 39.0, 40.0, 40.0, 40.0, 39.0, 40.0, 41.0, 39.0]
    flags = heatwave_day_flags(temps, threshold_c=40.0, min_consecutive_days=3)
    assert flags == [False, True, True, True, False, True, True, True, False, False, False, False]


def test_season_weeks_are_iso_mondays_19_to_38() -> None:
    weeks = season_weeks(2023, 19, 38)
    assert len(weeks) == 20
    assert weeks[0] == date(2023, 5, 8) and weeks[0].isocalendar().week == 19
    assert weeks[-1] == date(2023, 9, 18) and weeks[-1].isocalendar().week == 38
    assert all(week.weekday() == 0 and week.isocalendar().week in range(19, 39) for week in weeks)
    assert iso_week_start(date(2023, 5, 14)) == date(2023, 5, 8)  # Sunday belongs to Monday's week
    assert season_weeks(2019, 19, 38)[0] == date(2019, 5, 6)


def _week19_monday(year: int) -> date:
    day = date(year, 5, 1)
    while day.isocalendar().week != 19:
        day += timedelta(days=1)
    return iso_week_start(day)


def test_weekly_status_rule() -> None:
    con = duckdb.connect()
    sql = STATUS_CASE.format(n_obs="n_obs", min_obs=2, masked="masked", max_masked=50.0)
    rows = con.execute(
        f"SELECT n_obs, masked, {sql} FROM (VALUES"
        " (0, NULL), (1, 10.0), (2, 50.0), (2, 50.01), (3, NULL)) AS t(n_obs, masked)"
    ).fetchall()
    assert [row[2] for row in rows] == [
        "insufficient_observations", "insufficient_observations", "ok", "too_cloudy", "ok",
    ]


@pytest.fixture()
def gold_env(tmp_path: Path) -> tuple[Path, Config]:
    """Four cells, four years, and one crafted week per year."""
    return _make_gold_env(tmp_path, YEARS)


def _make_gold_env(root: Path, years: tuple[int, ...]) -> tuple[Path, Config]:
    """Build a self-contained Silver fixture with a crafted week per year."""
    base = load_config()
    config = replace(
        base,
        seasons=replace(base.seasons, focus=max(years), years=years),
        gold=replace(base.gold, orbit_cache="silver/_cache/orbits.parquet"),
    )
    cells = pa.table({
        "cell_id": pa.array([0, 1, 2, 3], pa.int32()),
        "is_cropland": pa.array([True, True, False, False], pa.bool_()),
    })
    (root / "silver").mkdir(parents=True)
    pq.write_table(cells, root / "silver" / "cells.parquet")

    weather_rows = []
    for year in years:
        start, end = config.seasons.bounds(year)
        for offset in range((end - start).days + 1):
            day = start + timedelta(days=offset)
            temp_max = 35.0
            if year == 2023 and date(year, 7, 14) <= day <= date(year, 7, 16):
                temp_max = 40.0 + (day - date(year, 7, 14)).days
            if year == 2023 and date(year, 8, 1) <= day <= date(year, 8, 2):
                temp_max = 40.0
            if day in {date(year, 7, 13), date(year, 7, 17), date(year, 7, 31), date(year, 8, 3)}:
                temp_max = 39.0
            weather_rows.append(dict(
                date=day, temp_max_c=temp_max, temp_min_c=25.0,
                temp_mean_c=30.0, precip_mm=0.0, solar_mj_m2=25.0,
            ))
    pq.write_table(pa.Table.from_pylist(weather_rows), root / "silver" / "weather.parquet")

    orbit_rows = []
    for year in years:
        monday = _week19_monday(year)
        for offset in (0, 1, 2, 3, 4, 7, 9):
            day = monday + timedelta(days=offset)
            orbit_rows.append(dict(date=day, relative_orbit=21, tile="36RTV"))
    cache = root / "silver" / "_cache" / "orbits.parquet"
    cache.parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist(orbit_rows), cache)

    for year in years:
        monday = _week19_monday(year)
        days = [monday + timedelta(days=offset) for offset in (0, 1, 2, 3, 4, 7, 9)]
        for day in days:
            rows = []
            for cell_id in (0, 1, 2, 3):
                value = _synthetic_ndvi(year, cell_id, day, monday)
                rows.append(dict(
                    cell_id=cell_id, date=day, ndvi=value,
                    valid_pixels=100 if value is not None else 0,
                    total_pixels=100,
                    masked_pct=0.0 if value is not None else 100.0,
                ))
            target = root / "silver" / "ndvi" / f"year={year}" / f"month={day.month:02d}" / f"date={day}.parquet"
            target.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(pa.Table.from_pylist(rows, schema=NDVI_SCHEMA), target)
    return root, config


def _synthetic_ndvi(year: int, cell_id: int, day: date, monday: date) -> float | None:
    offset = (day - monday).days
    if offset >= 7:  # week 20 is empty everywhere
        return None
    if cell_id == 0:
        return CROPLAND_VALUES[year]
    if cell_id == 1:
        return 0.25 if offset == 0 else None
    if cell_id == 2:
        return 0.35 if year == 2024 else None
    return 0.40 if offset in (0, 1) else None


def test_weekly_build_contract_and_anomaly(gold_env: tuple[Path, Config]) -> None:
    root, config = gold_env
    summary = build_weekly_ndvi(root, config, force=True)
    assert all(item["rows"] == 4 * 20 for item in summary["years"])

    table = pq.read_table(weekly_path(root, 2024)).to_pylist()
    week19 = _week19_monday(2024)
    assert week19 == date(2024, 5, 6)
    by_cell = {(row["cell_id"], row["week_start"]): row for row in table}
    assert by_cell[(0, week19)]["status"] == "ok"
    assert by_cell[(0, week19)]["n_obs"] == 5 and by_cell[(0, week19)]["n_orbits"] == 1
    assert by_cell[(0, week19)]["orbit_mix"] == "R021"
    assert by_cell[(1, week19)]["status"] == "insufficient_observations"
    assert by_cell[(1, week19)]["ndvi_mean"] is None  # never a one-date value
    assert by_cell[(3, week19)]["status"] == "too_cloudy"
    assert by_cell[(3, week19)]["ndvi_mean"] is None and by_cell[(3, week19)]["n_obs"] == 2
    assert by_cell[(3, week19)]["masked_pct_mean"] == pytest.approx(60.0)
    assert by_cell[(0, week19 + timedelta(days=7))]["status"] == "insufficient_observations"

    build_anomaly(root, config, force=True)
    anomaly = pq.read_table(
        root / "gold" / "ndvi_anomaly" / "year=2024" / "ndvi_anomaly.parquet"
    ).to_pylist()
    by_key = {(row["cell_id"], row["week_start"]): row for row in anomaly}
    ok = by_key[(0, week19)]
    assert ok["status"] == "ok" and ok["n_baseline_years"] == 3
    assert ok["baseline_mean"] == pytest.approx(0.30, abs=1e-6)  # 2024 itself is excluded
    assert ok["anomaly"] == pytest.approx(0.20, abs=1e-6)
    assert ok["zscore"] == pytest.approx(2.0, abs=1e-6)
    # Current-week status wins over no_baseline and over a usable baseline.
    assert by_key[(1, week19)]["status"] == "insufficient_observations"
    assert by_key[(3, week19)]["status"] == "too_cloudy"
    assert by_key[(2, week19)]["status"] == "no_baseline"
    assert by_key[(2, week19)]["baseline_mean"] is None
    assert by_key[(0, week19 + timedelta(days=7))]["status"] == "insufficient_observations"


def test_weekly_build_is_idempotent(gold_env: tuple[Path, Config]) -> None:
    root, config = gold_env
    build_weekly_ndvi(root, config, force=True)
    target = weekly_path(root, 2023)
    first = hashlib.sha256(target.read_bytes()).hexdigest()
    skipped = build_weekly_ndvi(root, config)
    assert skipped["years"][2]["skipped"] is True
    assert hashlib.sha256(target.read_bytes()).hexdigest() == first
    build_weekly_ndvi(root, config, years=[2023], force=True)
    assert hashlib.sha256(target.read_bytes()).hexdigest() == first


def test_weekly_weather_heatwave_days(gold_env: tuple[Path, Config]) -> None:
    root, config = gold_env
    summary = build_weekly_weather(root, config, force=True)
    assert all(item["rows"] == 20 for item in summary["years"])
    rows = {row["week_start"]: row for row in
            pq.read_table(root / "gold" / "weekly_weather" / "year=2023" / "weekly_weather.parquet").to_pylist()}
    july_week = iso_week_start(date(2023, 7, 14))
    assert rows[july_week]["heatwave_days"] == 3  # 40/41/42, all inside one 3-day run
    assert rows[iso_week_start(date(2023, 8, 1))]["heatwave_days"] == 0  # 2-day run does not count
    assert all(row["n_days"] == 7 for row in rows.values())


def test_sample_query_returns_focus_season_with_coverage(gold_env: tuple[Path, Config]) -> None:
    root, config = gold_env
    build_weekly_ndvi(root, config, force=True)
    build_anomaly(root, config, force=True)
    build_weekly_weather(root, config, force=True)
    con = duckdb.connect()
    try:
        frame = con.execute(sample_query(root, 2024)).df()
    finally:
        con.close()
    assert len(frame) == 20
    assert list(frame.columns)[:3] == ["week_start", "coverage_pct", "ndvi"]
    first = frame.iloc[0]
    assert first["coverage_pct"] == pytest.approx(50.0)  # one of two cropland cells is usable
    assert first["heatwave_days"] == 0


# ─── as_of helper: complete vs in-progress seasons, no wall clock ───


def _trim_weather_after(root: Path, year: int, last_day: date) -> None:
    """Keep every other year intact but drop one year's weather after a date."""
    path = root / "silver" / "weather.parquet"
    table = pq.read_table(path)
    rows = [
        row for row in table.to_pylist()
        if not (row["date"].year == year and row["date"] > last_day)
    ]
    pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), path)


def _add_ndvi_date(root: Path, year: int, day: date, value: float = 0.30) -> None:
    """Append one synthetic acquisition for every fixture cell."""
    rows = [
        dict(cell_id=cell_id, date=day, ndvi=value, valid_pixels=100,
             total_pixels=100, masked_pct=0.0)
        for cell_id in (0, 1, 2, 3)
    ]
    target = root / "silver" / "ndvi" / f"year={year}" / f"month={day.month:02d}" / f"date={day}.parquet"
    target.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=NDVI_SCHEMA), target)


def test_complete_season_as_of_is_the_configured_season_end(
    gold_env: tuple[Path, Config],
) -> None:
    root, config = gold_env
    state = season_state(root, config, 2023)
    assert state.complete is True
    assert state.as_of == date(2023, 9, 30)
    assert state.as_of == season_end(config, 2023)  # comes from config, not a constant
    assert complete_years(root, config, YEARS) == tuple(sorted(YEARS))


def test_in_progress_as_of_is_min_of_last_acquisition_and_last_weather(
    gold_env: tuple[Path, Config],
) -> None:
    root, config = gold_env
    as_of = date(2024, 7, 3)
    _trim_weather_after(root, 2024, as_of)
    _add_ndvi_date(root, 2024, as_of - timedelta(days=2))  # imagery lags weather
    state = season_state(root, config, 2024)
    assert state.complete is False
    assert state.latest_weather == as_of
    assert state.latest_acquisition == as_of - timedelta(days=2)
    assert state.as_of == as_of - timedelta(days=2)


def test_incomplete_weather_keeps_a_season_out_of_the_complete_pool(
    gold_env: tuple[Path, Config],
) -> None:
    root, config = gold_env
    # Season end is present, but one mid-season day is missing.
    path = root / "silver" / "weather.parquet"
    table = pq.read_table(path)
    dropped = date(2024, 7, 15)
    rows = [row for row in table.to_pylist() if row["date"] != dropped]
    pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), path)
    _add_ndvi_date(root, 2024, date(2024, 9, 30))
    assert season_state(root, config, 2024).complete is False
    assert 2024 not in complete_years(root, config, YEARS)


def test_week_is_observed_only_once_its_sunday_has_passed() -> None:
    assert week_is_observed(date(2023, 9, 18), date(2023, 9, 24)) is True
    assert week_is_observed(date(2023, 9, 25), date(2023, 9, 27)) is False
    assert week_is_observed(date(2023, 9, 25), None) is False


# ─── as_of wired into the real build: complete, in-progress and late-image cases ───


def test_complete_season_never_reports_not_yet_observed(
    gold_env: tuple[Path, Config],
) -> None:
    root, config = gold_env
    build_weekly_ndvi(root, config, force=True)
    for year in YEARS:
        statuses = set(
            pq.read_table(weekly_path(root, year), columns=["status"])
            .column("status").to_pylist()
        )
        assert "not_yet_observed" not in statuses


def test_in_progress_weeks_after_as_of_are_not_yet_observed(
    gold_env: tuple[Path, Config],
) -> None:
    root, config = gold_env
    as_of = date(2024, 7, 3)
    _trim_weather_after(root, 2024, as_of)
    _add_ndvi_date(root, 2024, as_of)
    build_weekly_ndvi(root, config, years=[2024], force=True)
    rows = {
        (row["cell_id"], row["week_start"]): row
        for row in pq.read_table(weekly_path(root, 2024)).to_pylist()
    }
    containing = iso_week_start(as_of)
    assert containing == date(2024, 7, 1)
    # The week containing as_of is not yet observed even though it has one image.
    assert rows[(0, containing)]["status"] == "not_yet_observed"
    assert rows[(0, containing)]["ndvi_mean"] is None
    # The previous week (Sunday 30 Jun) is evaluated, just too sparse.
    assert rows[(0, date(2024, 6, 24))]["status"] == "insufficient_observations"
    for week in season_weeks(2024, 19, 38):
        if week + timedelta(days=6) > as_of:
            assert rows[(0, week)]["status"] == "not_yet_observed"
        else:
            assert rows[(0, week)]["status"] != "not_yet_observed"


def test_late_last_acquisition_in_complete_season_keeps_week_38_evaluated(
    gold_env: tuple[Path, Config],
) -> None:
    root, config = gold_env
    # Last image is Friday 22 Sep, before week 38 ends on Sunday 24 Sep.
    _add_ndvi_date(root, 2023, date(2023, 9, 22))
    state = season_state(root, config, 2023)
    assert state.complete is True
    assert state.latest_acquisition == date(2023, 9, 22)
    assert state.as_of == date(2023, 9, 30)  # config season end, not the last image
    build_weekly_ndvi(root, config, years=[2023], force=True)
    week38 = season_weeks(2023, 19, 38)[-1]
    assert week38 == date(2023, 9, 18)
    rows = {
        (row["cell_id"], row["week_start"]): row
        for row in pq.read_table(weekly_path(root, 2023)).to_pylist()
    }
    assert rows[(0, week38)]["status"] != "not_yet_observed"


# ─── anomaly: not_yet_observed propagation and complete-season baseline pool ───


def test_anomaly_propagates_not_yet_observed(gold_env: tuple[Path, Config]) -> None:
    root, config = gold_env
    as_of = date(2024, 7, 3)
    _trim_weather_after(root, 2024, as_of)
    _add_ndvi_date(root, 2024, as_of)
    build_weekly_ndvi(root, config, years=[2024], force=True)
    build_anomaly(root, config, years=[2024], force=True)
    week = iso_week_start(as_of)
    weekly = {
        (row["cell_id"], row["week_start"]): row
        for row in pq.read_table(weekly_path(root, 2024)).to_pylist()
    }
    anomaly = {
        (row["cell_id"], row["week_start"]): row
        for row in pq.read_table(
            root / "gold" / "ndvi_anomaly" / "year=2024" / "ndvi_anomaly.parquet"
        ).to_pylist()
    }
    assert weekly[(0, week)]["status"] == "not_yet_observed"
    assert anomaly[(0, week)]["status"] == "not_yet_observed"
    assert anomaly[(0, week)]["anomaly"] is None
    assert anomaly[(0, week)]["ndvi_mean"] is None


def test_partial_season_is_excluded_from_another_years_baseline(tmp_path: Path) -> None:
    years = (2020, 2021, 2022, 2023, 2024)
    root, config = _make_gold_env(tmp_path / "five", years)
    as_of = date(2024, 7, 3)
    _trim_weather_after(root, 2024, as_of)
    _add_ndvi_date(root, 2024, as_of)
    build_weekly_ndvi(root, config, force=True)
    build_anomaly(root, config, force=True)
    week19 = _week19_monday(2023)
    row = next(
        row for row in pq.read_table(
            root / "gold" / "ndvi_anomaly" / "year=2023" / "ndvi_anomaly.parquet"
        ).to_pylist()
        if row["cell_id"] == 0 and row["week_start"] == week19
    )
    # 2024 is partial and must not enter 2023's baseline, even though its week 19 is ok.
    assert row["status"] == "ok"
    assert row["n_baseline_years"] == 3
    assert row["baseline_mean"] == pytest.approx(0.20, abs=1e-6)


def test_baseline_with_fewer_than_three_complete_years_gives_no_baseline(
    tmp_path: Path,
) -> None:
    root, config = _make_gold_env(tmp_path / "four", YEARS)
    as_of = date(2024, 7, 3)
    _trim_weather_after(root, 2024, as_of)
    _add_ndvi_date(root, 2024, as_of)
    build_weekly_ndvi(root, config, force=True)
    build_anomaly(root, config, force=True)
    week19 = _week19_monday(2023)
    row = next(
        row for row in pq.read_table(
            root / "gold" / "ndvi_anomaly" / "year=2023" / "ndvi_anomaly.parquet"
        ).to_pylist()
        if row["cell_id"] == 0 and row["week_start"] == week19
    )
    assert row["status"] == "no_baseline"
    assert row["n_baseline_years"] == 2
    assert row["anomaly"] is None

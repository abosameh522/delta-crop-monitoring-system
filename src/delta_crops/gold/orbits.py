"""Relative-orbit metadata cache and the cropland orbit diagnostic.

The orbit id comes from `s2:product_uri` in the STAC item, because Earth Search
does not expose `sat:relative_orbit`. The cache lives under Silver's `_cache`
(Bronze is read-only since Phase 3) and is used only to label weekly rows and to
report whether orbit mix could bias them.
"""

from __future__ import annotations

import json
import os
import re
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from delta_crops.config import Config
from delta_crops.ingestion.earth_search import (
    COLLECTION,
    EARTH_SEARCH_URL,
    build_search_params,
)

PRODUCT_ORBIT = re.compile(r"_R(\d{3})_")

ORBIT_SCHEMA = pa.schema(
    [
        ("date", pa.date32()),
        ("tile", pa.string()),
        ("relative_orbit", pa.int16()),
        ("sensing_time_utc", pa.string()),
        ("view_incidence_angle", pa.float32()),
        ("view_azimuth", pa.float32()),
        ("item_id", pa.string()),
        ("product_uri", pa.string()),
        ("cloud_cover", pa.float32()),
        ("processing_baseline", pa.string()),
        ("fetched_at_utc", pa.timestamp("us", tz="UTC")),
    ]
)


def _baseline(value: object) -> tuple[int, ...]:
    try:
        return tuple(int(part) for part in str(value).split("."))
    except (TypeError, ValueError):
        return (0,)


def _float_or_none(value: object) -> float | None:
    return None if value is None else float(value)


def fetch_orbit_rows(config: Config, years: list[int] | None = None) -> list[dict]:
    """One STAC metadata query per season year; no imagery is read or written."""
    from pystac_client import Client

    client = Client.open(EARTH_SEARCH_URL)
    fetched = datetime.now(timezone.utc)
    rows: list[dict] = []
    for year in years or config.seasons.years:
        start, end = config.seasons.bounds(year)
        search = client.search(
            **build_search_params(config.bbox.as_list, start, end,
                                  config.quality.max_scene_cloud_pct, COLLECTION)
        )
        for item in search.items():
            properties = item.properties
            match = PRODUCT_ORBIT.search(str(properties.get("s2:product_uri", "")))
            if item.datetime is None or match is None:
                continue
            rows.append(dict(
                date=item.datetime.date(),
                tile=item.id.split("_")[1] if "_" in item.id else "unknown",
                relative_orbit=int(match.group(1)),
                sensing_time_utc=item.datetime.strftime("%H:%M"),
                view_incidence_angle=_float_or_none(properties.get("view:incidence_angle")),
                view_azimuth=_float_or_none(properties.get("view:azimuth")),
                item_id=item.id,
                product_uri=str(properties["s2:product_uri"]),
                cloud_cover=_float_or_none(properties.get("eo:cloud_cover")),
                processing_baseline=str(properties.get("s2:processing_baseline")),
                fetched_at_utc=fetched,
            ))
    return rows


def _dedupe(rows: list[dict]) -> list[dict]:
    """Keep the newest processing baseline per (date, tile), like ingestion does."""
    best: dict[tuple[date, str], dict] = {}
    for row in rows:
        key = (row["date"], row["tile"])
        current = best.get(key)
        if current is None or (_baseline(row["processing_baseline"]), row["item_id"]) > (
            _baseline(current["processing_baseline"]), current["item_id"]
        ):
            best[key] = row
    return [best[key] for key in sorted(best)]


def build_orbit_cache(
    root: Path, config: Config, years: list[int] | None = None, force: bool = False
) -> Path:
    """Write the orbit cache under Silver's cache directory."""
    target = root / config.gold.orbit_cache if config.gold else None
    if target is None:
        raise ValueError("config is missing the 'gold' block")
    if target.is_file() and not force:
        return target
    rows = _dedupe(fetch_orbit_rows(config, years))
    if not rows:
        raise ValueError("STAC returned no items with s2:product_uri")
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(".parquet.tmp")
    try:
        pq.write_table(pa.Table.from_pylist(rows, schema=ORBIT_SCHEMA), tmp, compression="zstd")
        os.replace(tmp, target)
    finally:
        tmp.unlink(missing_ok=True)
    return target


def cropland_orbit_diagnostic(root: Path, config: Config, cache: Path) -> dict:
    """Per-date cropland NDVI by orbit, plus 5-day paired differences per year.

    Pairing adjacent dates exactly 5 days apart removes most seasonality; pairs
    are then grouped by the orbit combination so a same-orbit pair is not read as
    an orbit effect.
    """
    import duckdb
    import numpy as np

    gold = config.gold
    con = duckdb.connect()
    con.execute("SET threads = ?", [gold.duckdb_threads]).fetchall()
    con.execute("SET memory_limit = ?", [gold.duckdb_memory_limit]).fetchall()
    try:
        frame = con.execute(
            "SELECT o.date AS day, d.relative_orbit AS orbit, avg(o.ndvi) AS mean_ndvi,"
            " count(*) AS cells "
            "FROM read_parquet(?) o "
            "JOIN read_parquet(?) c USING (cell_id) "
            "JOIN (SELECT date, max(relative_orbit) AS relative_orbit"
            "      FROM read_parquet(?) GROUP BY date) d USING (date) "
            "WHERE c.is_cropland AND o.ndvi IS NOT NULL "
            "GROUP BY 1, 2 ORDER BY 1",
            [
                str(root / "silver" / "ndvi" / "year=*" / "**" / "*.parquet"),
                str(root / "silver" / "cells.parquet"),
                str(cache),
            ],
        ).df()
    finally:
        con.close()
    frame["day"] = frame["day"].astype("datetime64[ns]")
    frame["year"] = frame["day"].dt.year

    per_orbit = []
    for year, group in frame.groupby("year"):
        for orbit, orbit_rows in group.groupby("orbit"):
            per_orbit.append(dict(
                year=int(year), orbit=f"R{int(orbit):03d}", dates=int(len(orbit_rows)),
                mean_of_date_means=round(float(orbit_rows["mean_ndvi"].mean()), 6),
                std_of_date_means=round(float(orbit_rows["mean_ndvi"].std(ddof=1)), 6)
                if len(orbit_rows) > 1 else None,
            ))

    # Adjacent acquisition pairs. The requested window is exactly 5 days, but in
    # this series a 5-day pair is the SAME relative orbit seen by the other
    # platform, so cross-orbit pairs (usually 2-3 days apart) are reported too.
    pairs = []
    for year, group in frame.groupby("year"):
        group = group.sort_values("day")
        dates = list(group["day"])
        for left, right in zip(dates, dates[1:]):
            gap = (right - left).days
            if gap > 7:
                continue
            first = group[group["day"] == left].iloc[0]
            second = group[group["day"] == right].iloc[0]
            pairs.append(dict(
                year=int(year), gap_days=gap,
                date_from=left.date().isoformat(), date_to=right.date().isoformat(),
                orbit_from=f"R{int(first['orbit']):03d}", orbit_to=f"R{int(second['orbit']):03d}",
                ndvi_from=round(float(first["mean_ndvi"]), 6),
                ndvi_to=round(float(second["mean_ndvi"]), 6),
                difference=round(float(second["mean_ndvi"] - first["mean_ndvi"]), 6),
            ))

    grouped: dict[tuple[int, int, str, str], list[float]] = {}
    for pair in pairs:
        grouped.setdefault(
            (pair["year"], pair["gap_days"], pair["orbit_from"], pair["orbit_to"]), []
        ).append(pair["difference"])
    pair_summary = []
    for (year, gap, orbit_from, orbit_to), values in sorted(grouped.items()):
        series = np.asarray(values, dtype="float64")
        pair_summary.append(dict(
            year=year, gap_days=gap, orbit_from=orbit_from, orbit_to=orbit_to, pairs=len(values),
            same_orbit=orbit_from == orbit_to,
            mean_difference=round(float(series.mean()), 6),
            std_difference=round(float(series.std(ddof=1)), 6) if len(values) > 1 else None,
        ))

    def summarize(selected: list[dict]) -> dict:
        series = np.asarray([pair["difference"] for pair in selected], dtype="float64")
        return dict(
            pairs=len(selected),
            same_orbit_pairs=sum(1 for pair in selected if pair["orbit_from"] == pair["orbit_to"]),
            cross_orbit_pairs=sum(1 for pair in selected if pair["orbit_from"] != pair["orbit_to"]),
            mean_difference=round(float(series.mean()), 6) if len(series) else None,
            std_difference=round(float(series.std(ddof=1)), 6) if len(series) > 1 else None,
        )

    years = [dict(year=year, **summarize([p for p in pairs if p["year"] == year and p["gap_days"] == 5]))
             for year in sorted({pair["year"] for pair in pairs})]
    by_gap = []
    for year in sorted({pair["year"] for pair in pairs}):
        for gap in sorted({pair["gap_days"] for pair in pairs if pair["year"] == year}):
            by_gap.append(dict(year=year, gap_days=gap,
                               **summarize([p for p in pairs if p["year"] == year and p["gap_days"] == gap])))
    return dict(per_orbit=per_orbit, pair_summary=pair_summary, years=years, by_gap=by_gap,
                per_date=frame.assign(day=frame["day"].dt.date.astype(str)).to_dict(orient="records"))


def format_orbit_diagnostic(result: dict) -> str:
    lines = ["cropland mean NDVI by orbit (per date, then within-year summary):"]
    for row in result["per_orbit"]:
        lines.append(
            f"  {row['year']} {row['orbit']} dates={row['dates']:>2}"
            f" mean={row['mean_of_date_means']:.4f} std={row['std_of_date_means']}"
        )
    lines.append("")
    lines.append("5-day paired differences (same year, adjacent dates):")
    for row in result["years"]:
        lines.append(
            f"  {row['year']} pairs={row['pairs']:>2} same_orbit={row['same_orbit_pairs']:>2}"
            f" mean={row['mean_difference']:+.4f} std={row['std_difference']}"
        )
    lines.append("all adjacent pairs by gap (cross-orbit pairs isolate the orbit mix):")
    for row in result["by_gap"]:
        lines.append(
            f"  {row['year']} gap={row['gap_days']}d n={row['pairs']:>2}"
            f" same_orbit={row['same_orbit_pairs']:>2} cross_orbit={row['cross_orbit_pairs']:>2}"
            f" mean={row['mean_difference']:+.4f} std={row['std_difference']}"
        )
    lines.append("")
    lines.append("pair detail by orbit combination:")
    for row in result["pair_summary"]:
        lines.append(
            f"  {row['year']} gap={row['gap_days']}d {row['orbit_from']}->{row['orbit_to']}"
            f" n={row['pairs']:>2} mean={row['mean_difference']:+.4f} std={row['std_difference']}"
        )
    return "\n".join(lines)

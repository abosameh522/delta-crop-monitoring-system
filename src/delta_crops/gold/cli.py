"""Command line entry point for the Gold layer.

    python -m delta_crops.gold.cli gate        # NULL decomposition; stops if > cap
    python -m delta_crops.gold.cli orbits      # STAC metadata cache (no imagery)
    python -m delta_crops.gold.cli weather     # weekly_weather
    python -m delta_crops.gold.cli weekly      # weekly_ndvi
    python -m delta_crops.gold.cli anomaly     # ndvi_anomaly
    python -m delta_crops.gold.cli build       # gate + weather + weekly + anomaly
    python -m delta_crops.gold.cli verify      # row counts, ranges, sample query
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import duckdb

from delta_crops.config import Config, data_dir, load_config, require_gold
from delta_crops.gold import anomaly, gate, orbits, weather, weekly
from delta_crops.gold.queries import sample_query
from delta_crops.errors import to_exit_code, TransientFailure, SilverDateFailure


def _years(args: argparse.Namespace, config: Config) -> list[int]:
    return [args.year] if getattr(args, "year", None) else list(config.seasons.years)


def cmd_gate(args: argparse.Namespace, config: Config, root: Path) -> int:
    report = gate.weekly_gate(root, config)
    print(gate.format_gate_table(report))
    print(f"\nreport: {report['report_path']}")
    for row in report["seasons"]:
        print(f"{row['year']} NULL cell-dates: {row['null_cell_dates']}")
    return 0


def cmd_orbits(args: argparse.Namespace, config: Config, root: Path) -> int:
    path = orbits.build_orbit_cache(root, config, _years(args, config), force=args.refresh)
    print(f"orbit cache: {path}")
    if args.diagnose:
        result = orbits.cropland_orbit_diagnostic(root, config, path)
        print(orbits.format_orbit_diagnostic(result))
        directory = root / "gold" / "_reports"
        directory.mkdir(parents=True, exist_ok=True)
        target = directory / "orbit_diagnostic.json"
        target.write_text(json.dumps(result, indent=2), encoding="utf-8")
        print(f"\ndiagnostic: {target}")
    return 0


def cmd_weekly(args: argparse.Namespace, config: Config, root: Path) -> int:
    try:
        summary = weekly.build_weekly_ndvi(root, config, _years(args, config), force=args.force, adopt=args.adopt)
    except TransientFailure:
        raise
    except SilverDateFailure:
        return 4
    except Exception as exc:  # noqa: BLE001
        return to_exit_code(exc)
    print(json.dumps(summary, indent=2))
    return 0


def cmd_anomaly(args: argparse.Namespace, config: Config, root: Path) -> int:
    try:
        summary = anomaly.build_anomaly(root, config, _years(args, config), force=args.force, adopt=args.adopt)
    except TransientFailure:
        raise
    except SilverDateFailure:
        return 4
    except Exception as exc:  # noqa: BLE001
        return to_exit_code(exc)
    print(json.dumps(summary, indent=2))
    return 0


def cmd_weather(args: argparse.Namespace, config: Config, root: Path) -> int:
    try:
        summary = weather.build_weekly_weather(root, config, force=args.force, adopt=args.adopt)
    except TransientFailure:
        raise
    except SilverDateFailure:
        return 4
    except Exception as exc:  # noqa: BLE001
        return to_exit_code(exc)
    print(json.dumps(summary, indent=2))
    return 0


def cmd_build(args: argparse.Namespace, config: Config, root: Path) -> int:
    gold = require_gold(config)
    try:
        report = gate.weekly_gate(root, config)
    except TransientFailure:
        raise
    except gate.WeeklyGateFailure as error:
        print(str(error))
        return 11
    print(gate.format_gate_table(report))
    cache = root / gold.orbit_cache
    if not cache.is_file():
        print(f"orbit cache missing, building: {orbits.build_orbit_cache(root, config)}")
    try:
        print(json.dumps(weather.build_weekly_weather(root, config, force=args.force, adopt=args.adopt), indent=2))
        print(json.dumps(weekly.build_weekly_ndvi(root, config, _years(args, config), force=args.force, adopt=args.adopt), indent=2))
        print(json.dumps(anomaly.build_anomaly(root, config, _years(args, config), force=args.force, adopt=args.adopt), indent=2))
    except TransientFailure:
        raise
    except SilverDateFailure:
        return 4
    except Exception as exc:  # noqa: BLE001
        return to_exit_code(exc)
    return 0


def cmd_verify(args: argparse.Namespace, config: Config, root: Path) -> int:
    gold = require_gold(config)
    con = duckdb.connect()
    con.execute("SET threads = ?", [gold.duckdb_threads]).fetchall()
    con.execute("SET memory_limit = ?", [gold.duckdb_memory_limit]).fetchall()
    try:
        for year in config.seasons.years:
            weekly_glob = str(root / "gold" / "weekly_ndvi" / f"year={year}" / "*.parquet")
            anomaly_glob = str(root / "gold" / "ndvi_anomaly" / f"year={year}" / "*.parquet")
            weather_glob = str(root / "gold" / "weekly_weather" / f"year={year}" / "*.parquet")
            row = con.execute(
                "SELECT count(*), count(DISTINCT (cell_id, week_start)),"
                " min(ndvi_mean), max(ndvi_mean),"
                " count(*) FILTER (WHERE status = 'ok'),"
                " count(*) FILTER (WHERE status = 'insufficient_observations'),"
                " count(*) FILTER (WHERE status = 'too_cloudy'),"
                " count(DISTINCT week_start),"
                " count(*) FILTER (WHERE status <> 'ok' AND ndvi_mean IS NOT NULL),"
                " count(*) FILTER (WHERE status = 'ok' AND ndvi_mean IS NULL),"
                " count(*) FILTER (WHERE n_obs < 2 AND status <> 'insufficient_observations')"
                f" FROM read_parquet('{weekly_glob}')"
            ).fetchall()[0]
            anomaly_row = con.execute(
                "SELECT count(*), count(DISTINCT (cell_id, week_start)),"
                " min(baseline_mean), max(baseline_mean), min(anomaly), max(anomaly),"
                " count(*) FILTER (WHERE status = 'no_baseline'),"
                " count(*) FILTER (WHERE status = 'ok' AND (anomaly IS NULL OR zscore IS NULL"
                "   AND baseline_std > 0))"
                f" FROM read_parquet('{anomaly_glob}')"
            ).fetchall()[0]
            weather_rows = con.execute(
                f"SELECT count(*), sum(heatwave_days) FROM read_parquet('{weather_glob}')"
            ).fetchall()[0]
            print(
                f"{year} weekly: rows={row[0]:,} keys={row[1]:,} ndvi=[{row[2]}, {row[3]}]"
                f" ok={row[4]:,} insuff={row[5]:,} cloudy={row[6]:,} weeks={row[7]}"
                f" bad_null={row[8]} bad_ok={row[9]} bad_insuff={row[10]}"
            )
            print(
                f"{year} anomaly: rows={anomaly_row[0]:,} keys={anomaly_row[1]:,}"
                f" baseline=[{anomaly_row[2]}, {anomaly_row[3]}]"
                f" anomaly=[{anomaly_row[4]}, {anomaly_row[5]}] no_baseline={anomaly_row[6]:,}"
                f" ok_without_anomaly={anomaly_row[7]}"
            )
            print(f"{year} weather: rows={weather_rows[0]} heatwave_days={weather_rows[1]}")
        started = time.perf_counter()
        frame = con.execute(sample_query(root, config.seasons.focus)).df()
        seconds = time.perf_counter() - started
        print(f"\nsample query: {len(frame)} rows in {seconds:.3f} s")
        print(frame.to_string(index=False))
    finally:
        con.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    from delta_crops.errors import WeeklyGateFailure
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=None)
    parser.add_argument("--config", type=Path, default=None)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("gate", "orbits", "weather", "weekly", "anomaly", "build", "verify"):
        command = sub.add_parser(name)
        command.add_argument("--year", type=int, default=None)
        command.add_argument("--force", action="store_true")
        command.add_argument("--adopt", action="store_true")
        if name == "orbits":
            command.add_argument("--refresh", action="store_true")
            command.add_argument("--diagnose", action="store_true")
    args = parser.parse_args(argv)
    root = args.data_dir or data_dir()
    config = load_config(args.config)
    handler = {
        "gate": cmd_gate, "orbits": cmd_orbits, "weather": cmd_weather,
        "weekly": cmd_weekly, "anomaly": cmd_anomaly, "build": cmd_build,
        "verify": cmd_verify,
    }[args.command]
    try:
        return handler(args, config, root)
    except WeeklyGateFailure as error:
        print(str(error))
        return 11
    except TransientFailure as error:
        print(f"Transient failure: {error}")
        return 3
    except SilverDateFailure:
        return 4
    except Exception as exc:  # noqa: BLE001
        return to_exit_code(exc)


if __name__ == "__main__":
    raise SystemExit(main())

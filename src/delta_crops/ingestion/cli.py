"""Command line entry point for Bronze ingestion."""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date
from pathlib import Path

import requests

from delta_crops.config import Config, data_dir, load_config
from delta_crops.ingestion import bronze
from delta_crops.ingestion.earth_search import expected_assets, search_scenes
from delta_crops.errors import AssetFailure, to_exit_code, TransientFailure


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _write_summary(path: Path | None, payload: dict) -> None:
    """Write a machine-readable CLI summary atomically; the flow reads it."""
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    tmp.replace(path)


def _years_between(start: date, end: date) -> list[int]:
    return list(range(start.year, end.year + 1))


def cmd_ingest(args: argparse.Namespace, config: Config) -> int:
    """Download imagery for a window, then ensure weather for every year it spans."""
    if args.year:
        start, end = config.seasons.bounds(args.year)
    elif args.start and args.end:
        start, end = _parse_date(args.start), _parse_date(args.end)
    else:
        print("error: give --year or both --start and --end", file=sys.stderr)
        return 2

    root = Path(args.data_dir) if args.data_dir else data_dir()
    summary: dict = {
        "status": "running",
        "new": 0,
        "skipped": 0,
        "bytes": 0,
        "failed": 0,
        "weather": {},
    }
    started = time.monotonic()
    try:
        print(f"Searching Earth Search {start} .. {end} (cloud < {config.quality.max_scene_cloud_pct}%)")
        try:
            scenes = search_scenes(
                bbox=config.bbox.as_list,
                start=start,
                end=end,
                max_cloud_pct=config.quality.max_scene_cloud_pct,
            )
        except requests.RequestException as exc:
            # Only transport/HTTP failures are retryable; anything else is a bug.
            print(f"STAC search failed: {type(exc).__name__}: {exc}")
            raise TransientFailure("STAC search failed") from exc

        tiles = sorted({scene.tile for scene in scenes})
        print(f"Selected {len(scenes)} scenes over tiles {tiles}")

        try:
            stats = bronze.ingest_scenes(
                scenes=scenes,
                bbox=config.bbox,
                config=config,
                data_root=root,
                verify_checksum=not args.no_checksum,
                workers=args.workers,
                attempts=args.asset_retries,
                retry_base_delay_s=args.retry_delay,
            )
        except AssetFailure as exc:
            print(f"Ingestion asset failure: {exc}")
            summary["status"] = "failed"
            return 4
        except TransientFailure:
            summary["status"] = "failed"
            raise
        except Exception as exc:  # noqa: BLE001 - unexpected
            print(f"Unexpected ingestion error: {type(exc).__name__}: {exc}")
            summary["status"] = "failed"
            return 1
        summary.update(
            new=stats["downloaded"],
            skipped=stats["skipped"],
            bytes=stats["bytes"],
            failed=stats["failed"],
        )

        partial_years = set(getattr(args, "allow_partial_weather_year", None) or [])
        for year in _years_between(start, end):
            try:
                result = bronze.ingest_weather(
                    config, year, root, allow_partial=year in partial_years
                )
            except TransientFailure:
                summary["status"] = "failed"
                raise
            except Exception as exc:  # noqa: BLE001
                print(f"Weather {year}: FAILED {type(exc).__name__}: {exc}")
                summary["status"] = "failed"
                return 1
            summary["weather"][str(year)] = result
            print(f"Weather {year}: {result}")

        print(
            f"Done in {time.monotonic() - started:.1f}s: "
            f"{stats['downloaded']} downloaded, {stats['skipped']} skipped, {stats['failed']} failed"
        )
        print(f"Log: {bronze.log_path(root)}")
        if stats["failed"] > 0:
            summary["status"] = "failed"
            raise AssetFailure(f"{stats['failed']} asset(s) failed")
        summary["status"] = "ok"
        return 0
    finally:
        _write_summary(getattr(args, "summary_json", None), summary)


def cmd_weather(args: argparse.Namespace, config: Config) -> int:
    """Fetch Open-Meteo daily data for one year or for every configured season year."""
    root = Path(args.data_dir) if args.data_dir else data_dir()
    years = [args.year] if args.year else list(config.seasons.years)
    for year in years:
        try:
            result = bronze.ingest_weather(config, year, root)
        except TransientFailure:
            raise
        except Exception as exc:  # noqa: BLE001
            print(f"Weather {year}: FAILED {type(exc).__name__}: {exc}")
            return 1
        print(f"Weather {year}: {result}")
    return 0


def cmd_compact(args: argparse.Namespace, _: Config) -> int:
    root = Path(args.data_dir) if args.data_dir else data_dir()
    rows = bronze.compact_log(root)
    print(f"Wrote {rows} rows to {bronze.parquet_path(root)}")
    return 0


def cmd_status(args: argparse.Namespace, _: Config) -> int:
    root = Path(args.data_dir) if args.data_dir else data_dir()
    records = bronze.latest_records(bronze.read_records(bronze.log_path(root)))
    if not records:
        print("No ingestion records yet.")
        return 0
    by_year: dict[str, list[dict]] = {}
    for record in records.values():
        year = (record.get("acquired_at") or record["asset_key"])[:4]
        by_year.setdefault(year, []).append(record)
    for year in sorted(by_year):
        rows = by_year[year]
        if args.year and year != str(args.year):
            continue
        total_bytes = sum(row["bytes"] for row in rows)
        nodata = [row["nodata_pct"] for row in rows if row.get("nodata_pct") is not None]
        nodata_text = f", mean nodata {sum(nodata) / len(nodata):.2f}%" if nodata else ""
        print(f"{year}: {len(rows)} assets, {total_bytes / 1e9:.2f} GB{nodata_text}")
    return 0


def cmd_verify(args: argparse.Namespace, config: Config) -> int:
    root = Path(args.data_dir) if args.data_dir else data_dir()
    start, end = config.seasons.bounds(args.year)
    print(f"Re-querying STAC for {start} .. {end} ...")
    scenes = search_scenes(
        bbox=config.bbox.as_list,
        start=start,
        end=end,
        max_cloud_pct=config.quality.max_scene_cloud_pct,
    )
    expected = expected_assets(scenes)
    records = bronze.latest_records(bronze.read_records(bronze.log_path(root)))
    missing: list[str] = []
    for asset_key in sorted(expected):
        record = records.get(asset_key)
        path = root / record["path"] if record else None
        if record is None or path is None or not bronze.is_complete(record, path):
            missing.append(asset_key)
    print(f"Expected {len(expected)} assets from {len(scenes)} scenes; complete {len(expected) - len(missing)}")
    if missing:
        print(f"Missing {len(missing)} (first 10):")
        for asset_key in missing[:10]:
            print(f"  {asset_key}")
        return 1
    print("Bronze is complete for this year.")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="delta-crops-ingest")
    parser.add_argument("--config", type=Path, default=None)
    parser.add_argument("--data-dir", type=Path, default=None)
    sub = parser.add_subparsers(dest="command", required=True)

    ingest = sub.add_parser("ingest", help="download missing AOI windows")
    ingest.add_argument("--year", type=int)
    ingest.add_argument("--start", type=str)
    ingest.add_argument("--end", type=str)
    ingest.add_argument("--no-checksum", action="store_true", help="skip SHA-256 on existing files")
    ingest.add_argument("--workers", type=int, default=4, help="parallel asset downloads")
    ingest.add_argument("--asset-retries", type=int, default=None,
                        help="attempts per asset (default: config ingestion.asset_attempts)")
    ingest.add_argument("--retry-delay", type=float, default=None,
                        help="retry backoff base in seconds (default: config value)")
    ingest.add_argument("--summary-json", type=Path, default=None,
                        help="write a machine-readable ingestion summary here")
    ingest.add_argument("--allow-partial-weather-year", type=int, action="append", default=None,
                        help="year whose Open-Meteo response may be short (in-progress season)")
    ingest.set_defaults(func=cmd_ingest)

    weather_cmd = sub.add_parser("weather", help="fetch Open-Meteo daily data (no imagery)")
    weather_cmd.add_argument("--year", type=int, default=None)
    weather_cmd.set_defaults(func=cmd_weather)

    compact = sub.add_parser("compact", help="rewrite JSONL log as deduplicated Parquet")
    compact.set_defaults(func=cmd_compact)

    status = sub.add_parser("status", help="show logged progress from JSONL")
    status.add_argument("--year", type=int)
    status.set_defaults(func=cmd_status)

    verify = sub.add_parser("verify", help="compare STAC expectations with Bronze files")
    verify.add_argument("--year", type=int, required=True)
    verify.set_defaults(func=cmd_verify)

    return parser


def main(argv: list[str] | None = None) -> int:
    from delta_crops.errors import AssetFailure, TransientFailure
    try:
        args = build_parser().parse_args(argv)
    except SystemExit as exc:
        return exc.code
    config = load_config(args.config)
    try:
        return args.func(args, config)
    except AssetFailure as exc:
        print(f"Asset failure: {exc}")
        return 4
    except TransientFailure as exc:
        print(f"Transient failure: {exc}")
        return 3
    except Exception as exc:  # noqa: BLE001 - contract: unexpected errors exit 1
        print(f"Unexpected failure: {type(exc).__name__}: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

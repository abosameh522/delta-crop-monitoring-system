"""Bronze ingestion: crash-safe JSONL log, COG downloads, compaction."""

from __future__ import annotations

import hashlib
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pyarrow as pa
import pyarrow.parquet as pq
from rasterio.errors import RasterioIOError

from delta_crops.config import BBox, Config
from delta_crops.ingestion import weather
from delta_crops.ingestion.earth_search import BAND_ASSETS, Scene
from delta_crops.ingestion.raster import nodata_pct, read_window, write_cog

LOG_FILENAME = "ingestion_log.jsonl"
PARQUET_FILENAME = "ingestion_log.parquet"

PA_SCHEMA = pa.schema(
    [
        ("asset_key", pa.string()),
        ("ingested_at_utc", pa.timestamp("us", tz="UTC")),
        ("source", pa.string()),
        ("collection", pa.string()),
        ("item_id", pa.string()),
        ("tile", pa.string()),
        ("acquired_at", pa.timestamp("us", tz="UTC")),
        ("band", pa.string()),
        ("path", pa.string()),
        ("bytes", pa.int64()),
        ("sha256", pa.string()),
        ("cloud_cover", pa.float32()),
        ("processing_baseline", pa.string()),
        ("boa_offset_applied", pa.bool_()),
        ("stac_href", pa.string()),
        ("nodata_pct", pa.float32()),
        ("resolution_m", pa.float32()),
        ("duration_s", pa.float32()),
        ("http_requests", pa.int32()),
    ]
)


def log_path(data_root: Path) -> Path:
    return data_root / "bronze" / "_logs" / LOG_FILENAME


def parquet_path(data_root: Path) -> Path:
    return data_root / "bronze" / "_logs" / PARQUET_FILENAME


def sha256_file(path: Path, chunk_size: int = 1 << 20) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_records(path: Path) -> list[dict[str, Any]]:
    """Read complete JSONL records; a torn final line from a crash is ignored."""
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return records


def latest_records(records: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Index records by asset_key; later records win."""
    index: dict[str, dict[str, Any]] = {}
    for record in records:
        index[record["asset_key"]] = record
    return index


def append_record(path: Path, record: dict[str, Any]) -> None:
    """Append one JSON line and fsync it immediately.

    If a previous crash left a line without a trailing newline, terminate that line
    first so the new record is not glued to the partial one.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    needs_newline = False
    if path.exists() and path.stat().st_size > 0:
        with path.open("rb") as handle:
            handle.seek(-1, os.SEEK_END)
            needs_newline = handle.read(1) != b"\n"
    with path.open("a", encoding="utf-8") as handle:
        if needs_newline:
            handle.write("\n")
        handle.write(json.dumps(record, sort_keys=True, default=str) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def is_complete(record: dict[str, Any] | None, absolute_path: Path, verify_checksum: bool = True) -> bool:
    """True when the logged asset exists with the logged size and checksum."""
    if record is None or not absolute_path.exists():
        return False
    if absolute_path.stat().st_size != record.get("bytes"):
        return False
    if verify_checksum and sha256_file(absolute_path) != record.get("sha256"):
        return False
    return True


def _bronze_raster_path(data_root: Path, scene: Scene, band: str) -> Path:
    return (
        data_root
        / "bronze"
        / "sentinel2"
        / f"tile={scene.tile}"
        / f"year={scene.day.year}"
        / f"date={scene.day.isoformat()}"
        / f"{band}.tif"
    )


def _record_for_asset(
    scene: Scene,
    band: str,
    data_root: Path,
    absolute_path: Path,
    array_nodata_pct: float,
    resolution_m: float,
    duration_s: float,
) -> dict[str, Any]:
    return {
        "asset_key": f"{scene.item_id}:{band}",
        "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": "earth-search",
        "collection": "sentinel-2-l2a",
        "item_id": scene.item_id,
        "tile": scene.tile,
        "acquired_at": scene.acquired.isoformat(),
        "band": band,
        "path": absolute_path.relative_to(data_root).as_posix(),
        "bytes": absolute_path.stat().st_size,
        "sha256": sha256_file(absolute_path),
        "cloud_cover": scene.cloud_cover,
        "processing_baseline": ".".join(str(part) for part in scene.baseline),
        "boa_offset_applied": scene.boa_offset_applied,
        "stac_href": scene.hrefs[band],
        "nodata_pct": array_nodata_pct,
        "resolution_m": resolution_m,
        "duration_s": round(duration_s, 3),
        "http_requests": None,
    }


class _AssetDownloadError(RuntimeError):
    """One asset failed after its own retries; recorded and skipped."""


def ingest_scenes(
    scenes: Iterable[Scene],
    bbox: BBox,
    config: Config,
    data_root: Path,
    verify_checksum: bool = True,
    workers: int = 4,
    attempts: int | None = None,
    retry_base_delay_s: float | None = None,
) -> dict[str, Any]:
    """Download missing AOI windows for the given scenes. Safe to re-run.

    Only ``rasterio.errors.RasterioIOError`` is retried, ``attempts`` times per
    asset with exponential backoff (base delay configurable, zero in tests).
    Every other exception is an asset failure and is not retried; remaining
    assets still finish before ``AssetFailure`` (exit 4) is raised.
    """
    import threading
    from concurrent.futures import ThreadPoolExecutor, as_completed

    from delta_crops.errors import AssetFailure

    policy = config.ingestion
    attempts = int(policy.asset_attempts if attempts is None else attempts)
    if attempts < 1:
        raise ValueError("attempts must be >= 1")
    base_delay = float(policy.retry_base_delay_s if retry_base_delay_s is None
                       else retry_base_delay_s)

    path = log_path(data_root)
    index = latest_records(read_records(path))
    stats: dict[str, Any] = {"downloaded": 0, "skipped": 0, "failed": 0, "bytes": 0, "failed_assets": []}
    lock = threading.Lock()

    pending: list[tuple[Scene, str]] = []
    for scene in scenes:
        for band in BAND_ASSETS:
            asset_key = f"{scene.item_id}:{band}"
            target = _bronze_raster_path(data_root, scene, band)
            existing = index.get(asset_key)
            if is_complete(existing, target, verify_checksum):
                stats["skipped"] += 1
                continue
            pending.append((scene, band))

    def read_with_retry(href: str, asset_key: str):
        last: RasterioIOError | None = None
        for attempt in range(1, attempts + 1):
            try:
                return read_window(href, bbox)
            except RasterioIOError as exc:
                last = exc
                if attempt >= attempts:
                    break
                delay = base_delay * (2 ** (attempt - 1))
                if delay > 0:
                    time.sleep(delay)
        raise _AssetDownloadError(
            f"{asset_key}: RasterioIOError after {attempts} attempt(s): {last}"
        )

    def download(scene: Scene, band: str) -> None:
        asset_key = f"{scene.item_id}:{band}"
        target = _bronze_raster_path(data_root, scene, band)
        started = time.monotonic()
        array, transform, crs, nodata = read_with_retry(scene.hrefs[band], asset_key)
        tmp_path = target.with_suffix(target.suffix + ".tmp")
        try:
            write_cog(target, array, transform, crs, nodata)
        except Exception as exc:  # noqa: BLE001 - any asset-scoped write error
            # write_cog is atomic, but a mocked or interrupted writer must not
            # leave target.tif.tmp behind either.
            tmp_path.unlink(missing_ok=True)
            raise _AssetDownloadError(
                f"{asset_key}: write_cog failed: {type(exc).__name__}: {exc}"
            ) from exc
        # The ledger record is derived from the final file only: the size and
        # SHA-256 are computed after os.replace has made the output visible.
        record = _record_for_asset(
            scene=scene,
            band=band,
            data_root=data_root,
            absolute_path=target,
            array_nodata_pct=nodata_pct(array, nodata),
            resolution_m=float(abs(transform.a)),
            duration_s=time.monotonic() - started,
        )
        with lock:
            append_record(path, record)
            index[asset_key] = record
            stats["downloaded"] += 1
            stats["bytes"] += int(record["bytes"])
            print(
                f"  {asset_key}  {record['bytes'] / 1e6:.1f} MB  "
                f"{record['duration_s']:.1f}s  nodata {record['nodata_pct']:.2f}%"
            )

    with ThreadPoolExecutor(max_workers=max(workers, 1)) as pool:
        futures = {pool.submit(download, scene, band): (scene, band) for scene, band in pending}
        for future in as_completed(futures):
            scene, band = futures[future]
            asset_key = f"{scene.item_id}:{band}"
            try:
                future.result()
            except _AssetDownloadError as exc:
                with lock:
                    stats["failed"] += 1
                    stats["failed_assets"].append({"asset_key": asset_key, "error": str(exc)})
            except Exception as exc:  # noqa: BLE001 - asset-scoped and non-retryable
                with lock:
                    stats["failed"] += 1
                    stats["failed_assets"].append(
                        {"asset_key": asset_key, "error": f"{type(exc).__name__}: {exc}"}
                    )

    if stats["failed"] > 0:
        print(f"Failed assets ({stats['failed']}):")
        for item in stats["failed_assets"]:
            print(f"  {item['asset_key']}: {item['error']}")
        keys = ", ".join(item["asset_key"] for item in stats["failed_assets"])
        raise AssetFailure(f"{stats['failed']} asset(s) failed: {keys}")
    return stats


def ingest_weather(config: Config, year: int, data_root: Path, allow_partial: bool = False) -> str:
    """Fetch and store one year of daily weather.

    Returns ``"downloaded"``, ``"skipped"`` or ``"partial"``. ``allow_partial``
    is only for a season still in progress: a short Open-Meteo response is then
    stored with ``weather_status=partial`` and is non-blocking. A complete year
    must return every configured day.
    """
    path = log_path(data_root)
    index = latest_records(read_records(path))
    asset_key = f"weather:{year}"
    target = data_root / "bronze" / "weather" / f"year={year}" / "open_meteo_daily.json"
    existing = index.get(asset_key)
    # A partial file must be refreshed on the next run so later days arrive.
    if existing and existing.get("weather_status", "complete") != "partial" and is_complete(existing, target):
        return "skipped"

    start, end = config.seasons.bounds(year)
    lat, lon = config.bbox.center
    started = time.monotonic()
    payload = weather.fetch_daily(lat, lon, start, end, allow_partial=allow_partial)
    status = weather.validate_payload(payload, start, end, allow_partial=allow_partial)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    os.replace(tmp, target)
    append_record(
        path,
        {
            "asset_key": asset_key,
            "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
            "source": "open-meteo",
            "collection": "open-meteo-archive",
            "item_id": None,
            "tile": None,
            "acquired_at": None,
            "band": None,
            "path": target.relative_to(data_root).as_posix(),
            "bytes": target.stat().st_size,
            "sha256": sha256_file(target),
            "cloud_cover": None,
            "processing_baseline": None,
            "boa_offset_applied": None,
            "stac_href": weather.ARCHIVE_URL,
            "nodata_pct": None,
            "resolution_m": None,
            "weather_status": status,
            "duration_s": round(time.monotonic() - started, 3),
            "http_requests": 1,
        },
    )
    return "partial" if status == "partial" else "downloaded"


def _parquet_rows(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for record in records:
        row = dict(record)
        for key in ("ingested_at_utc", "acquired_at"):
            value = row.get(key)
            row[key] = datetime.fromisoformat(value) if value else None
        rows.append(row)
    return rows


def compact_log(data_root: Path) -> int:
    """Deduplicate JSONL by asset_key and write the Parquet log atomically."""
    records = latest_records(read_records(log_path(data_root)))
    rows = sorted(_parquet_rows(records.values()), key=lambda row: row["asset_key"])
    table = pa.Table.from_pylist(rows, schema=PA_SCHEMA)
    target = parquet_path(data_root)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(".parquet.tmp")
    pq.write_table(table, tmp, compression="zstd")
    os.replace(tmp, target)
    return len(rows)

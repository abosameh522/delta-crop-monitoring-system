"""DuckDB scans real Silver; Pandera validates small per-file aggregates."""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import duckdb
import numpy as np
import pandas as pd
import pandera.pandas as pda
import pyarrow as pa
import pyarrow.parquet as pq

from delta_crops.config import Config
from delta_crops.transform.bronze_reader import imagery_dates, load_ledger
from delta_crops.transform.silver import ndvi_path
from delta_crops.quality.gaps import season_gaps
from delta_crops.quality.report import QualityFailure, write_report

# Deliberately declared, never inferred from an input file or imported from its writer.
EXPECTED_SCHEMA = pa.schema([
    ("cell_id", pa.int32()), ("date", pa.date32()), ("ndvi", pa.float32()),
    ("valid_pixels", pa.uint8()), ("total_pixels", pa.uint8()), ("masked_pct", pa.float32()),
])
REQUIRED_COLUMNS = ("cell_id", "date", "valid_pixels", "total_pixels", "masked_pct")
COUNT_CHECKS = {"ndvi_invalid": "ndvi_range", "duplicates": "duplicate_keys",
    "bad_pixels": "pixel_counts", "required_nulls": "required_values",
    "foreign_cells": "cell_keys", "wrong_dates": "date_partition", "row_error": "row_count"}


def null_rate_context(dates: list[dict], rules: dict) -> list[dict]:
    """Leave-one-out dual-tile seasonal median/MAD; single-tile is context only."""
    output = []
    for row in dates:
        item = dict(row)
        item.update(null_alert=False, null_reasons=[], peer_median=None, peer_mad=None,
                    robust_threshold=None)
        if len(row["tiles"]) == 2:
            if row["null_pct"] > rules["dual_tile_null_alert_pct"]:
                item["null_reasons"].append("absolute_threshold")
            peers = [r["null_pct"] for r in dates if r["date"] != row["date"]
                     and r["date"][:4] == row["date"][:4] and len(r["tiles"]) == 2]
            if len(peers) >= rules["null_mad_min_other_dates"]:
                median = float(np.median(peers))
                mad = float(np.median(np.abs(np.asarray(peers) - median)))
                threshold = median + rules["null_mad_multiplier"] * mad
                item.update(peer_median=median, peer_mad=mad, robust_threshold=threshold)
                if row["null_pct"] > threshold:
                    item["null_reasons"].append("median_plus_raw_mad")
            item["null_alert"] = bool(item["null_reasons"])
        output.append(item)
    return output


def _summary_schema() -> pda.DataFrameSchema:
    columns = {"date": pda.Column(str)}
    columns.update({key: pda.Column("int64", pda.Check.eq(0)) for key in COUNT_CHECKS})
    return pda.DataFrameSchema(columns, strict=True, coerce=False)


def run_quality(root: Path, config: Config) -> Path:
    """Write a per-run report; raise after persistence when critical checks fail."""
    started = time.perf_counter()
    rules = config.data_quality
    if rules is None:
        raise ValueError("data_quality config is required; thresholds have no implicit defaults")
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8]
    directory = root / "silver/_reports/quality"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"quality_{run_id}.json"
    report = dict(run_id=run_id, status="running", config=rules, checks=[], dates=[], seasons=[],
        schema={f.name: str(f.type) for f in EXPECTED_SCHEMA},
        nullable_columns=["ndvi"],
        gap_definition="Consecutive calendar days without NDVI IS NOT NULL, including season edges; "
            "13-day violations use >=; info share uses >10; no gap is filled. "
            "No-ledger days are separated from NULL acquisition dates.", timings={})

    def add(name: str, count: int, message: str, examples: list | None = None) -> None:
        severity = rules["severity"][name]
        report["checks"].append(dict(check=name, severity=severity, count=int(count),
            status=({"critical": "fail", "alert": "alert", "info": "info"}[severity] if count else "pass"), message=message,
            examples=(examples or [])[:rules["max_failure_examples"]]))

    def has_critical() -> bool:
        return any(c["severity"] == "critical" and c["count"] for c in report["checks"])

    con = duckdb.connect()
    try:
        con.execute("SET threads = ?", [rules["duckdb_threads"]]).fetchall()
        con.execute("SET memory_limit = ?", [rules["duckdb_memory_limit"]]).fetchall()
        ledger = load_ledger(root)
        days = [d for d in imagery_dates(ledger) if d.year in config.seasons.years
                and config.seasons.bounds(d.year)[0] <= d <= config.seasons.bounds(d.year)[1]]
        if not days:
            raise ValueError("No expected acquisition dates in ingestion ledger")
        report["expected_ledger_dates"] = len(days)
        expected = {str(ndvi_path(root, d).resolve()): d for d in days}
        missing, bad_schemas, readable = [], [], []
        for filename, day in expected.items():
            if not Path(filename).is_file():
                missing.append(dict(date=str(day), file=filename))
                continue
            try:
                schema = pq.ParquetFile(filename).schema_arrow
                if not schema.equals(EXPECTED_SCHEMA, check_metadata=False):
                    bad_schemas.append(dict(date=str(day), file=filename, actual=str(schema)))
                else:
                    readable.append(filename)
            except Exception as error:
                bad_schemas.append(dict(date=str(day), file=filename, error=str(error)))
        add("missing_partition", len(missing), "Expected outputs are enumerated from ledger dates", missing)
        add("schema", len(bad_schemas), "Exact columns and Arrow dtypes, including physical date column", bad_schemas)
        report["schema_valid_files"] = len(readable)
        cells = pq.read_table(root / "silver/cells.parquet", columns=["cell_id", "is_cropland"])
        con.register("cells", cells)
        kept_count = len(cells)
        if not kept_count or len(set(cells["cell_id"].to_pylist())) != kept_count:
            raise ValueError("cells.parquet must contain a nonempty unique kept-cell universe")
        report["kept_cells"] = kept_count
        tiles_by_date = {}
        for rec in ledger.values():
            if rec.get("source") == "earth-search" and rec.get("acquired_at"):
                tiles_by_date.setdefault(rec["acquired_at"][:10], set()).add(rec["tile"])
        scan_started = time.perf_counter()
        if readable:
            con.read_parquet(readable, hive_partitioning=False, filename=True).create_view("observations")
            scan_sql = """SELECT filename,
                count(*) AS rows, count(*) FILTER (WHERE ndvi IS NULL) AS null_rows,
                count(*) FILTER (WHERE ndvi IS NOT NULL AND
                    (NOT isfinite(ndvi) OR ndvi < ? OR ndvi > ?)) AS ndvi_invalid,
                count(*) - count(DISTINCT (o.cell_id, date)) AS duplicates,
                count(*) FILTER (WHERE valid_pixels > total_pixels) AS bad_pixels,
                count(*) FILTER (WHERE o.cell_id IS NULL OR date IS NULL OR valid_pixels IS NULL
                    OR total_pixels IS NULL OR masked_pct IS NULL) AS required_nulls,
                count(*) FILTER (WHERE c.cell_id IS NULL) AS foreign_cells,
                count(DISTINCT date) AS distinct_dates, min(date) AS min_date, max(date) AS max_date,
                count(*) FILTER (WHERE masked_pct > ?) AS masked_alert_rows,
                min(ndvi) AS ndvi_min, max(ndvi) AS ndvi_max
                FROM quality_scan o LEFT JOIN cells c ON o.cell_id = c.cell_id GROUP BY filename"""
            raw = []
            # Bound DISTINCT-key state to one date (~239k rows), not 70M keys.
            for filename in readable:
                con.read_parquet(filename, hive_partitioning=False, filename=True).create_view("quality_scan", replace=True)
                aggregated = con.execute(scan_sql,
                    [rules["ndvi_min"], rules["ndvi_max"], rules["masked_pct_alert_threshold"]])
                names = [c[0] for c in aggregated.description]
                raw.extend(dict(zip(names, row)) for row in aggregated.fetchall())
            summaries = []
            for row in raw:
                filename = row.pop("filename")
                day = expected[filename]
                row["wrong_dates"] = int(row.pop("distinct_dates") != 1 or
                    row.pop("min_date") != day or row.pop("max_date") != day)
                row["row_error"] = int(row["rows"] != kept_count)
                row.update(date=str(day), file=filename, tiles=sorted(tiles_by_date[str(day)]),
                           null_pct=100 * row["null_rows"] / row["rows"] if row["rows"] else 100.0)
                summaries.append(row)
            present = {r["file"] for r in summaries}
            # An empty parquet produces no GROUP BY row and is not a valid output.
            for filename in set(readable) - present:
                summaries.append(dict(date=str(expected[filename]), file=filename,
                    tiles=sorted(tiles_by_date[str(expected[filename])]), rows=0, null_rows=0,
                    null_pct=100.0, masked_alert_rows=0, **{k:int(k == "row_error") for k in COUNT_CHECKS}))
            summaries.sort(key=lambda r: r["date"])
            frame = pd.DataFrame([{k:r[k] for k in ("date", *COUNT_CHECKS)} for r in summaries])
            for key in COUNT_CHECKS:
                frame[key] = frame[key].astype("int64")
            try:
                _summary_schema().validate(frame, lazy=True)
                report["pandera_aggregate_validation"] = "passed"
            except pda.errors.SchemaErrors as error:
                report["pandera_aggregate_validation"] = "failed"
                report["pandera_failure_examples"] = json.loads(
                    error.failure_cases.head(rules["max_failure_examples"]).to_json(orient="records"))
            for key, check in COUNT_CHECKS.items():
                bad = [dict(date=r["date"], file=r["file"], count=r[key]) for r in summaries if r[key]]
                add(check, sum(r[key] for r in summaries), f"DuckDB {key}; Pandera requires zero", bad)
            masked = sum(r["masked_alert_rows"] for r in summaries)
            add("masked_pct", masked, f"Rows above {rules['masked_pct_alert_threshold']}% masking; alert only",
                [dict(date=r["date"], rows=r["masked_alert_rows"]) for r in summaries if r["masked_alert_rows"]])
            report["dates"] = null_rate_context(summaries, rules)
            null_alerts = [dict(date=r["date"], null_pct=r["null_pct"], reasons=r["null_reasons"])
                           for r in report["dates"] if r["null_alert"]]
            add("null_rate", len(null_alerts), "Dual-tile dates only; absolute OR leave-one-out median+raw MAD", null_alerts)
            report["single_tile_dates"] = [r["date"] for r in summaries if len(r["tiles"]) == 1]
            report["total_rows"] = sum(r["rows"] for r in summaries)
        report["timings"]["scan_seconds"] = time.perf_counter() - scan_started
        # Structural errors invalidate temporal summaries: do not turn lost partitions into data gaps.
        if not has_critical() and readable:
            gap_started = time.perf_counter()
            for year in config.seasons.years:
                start, end = config.seasons.bounds(year)
                result = season_gaps(con, start, end, [d for d in days if start <= d <= end], rules,
                    directory / f"gap_longest_{year}_{run_id}.parquet")
                result["year"] = year
                report["seasons"].append(result)
            for population in ("all", "cropland"):
                counts = [dict(year=s["year"], **s["statistics"][population]) for s in report["seasons"]]
                add("gaps", sum(r["violation_count"] for r in counts),
                    f"{population}: longest gap >= {rules['gap_alert_days']} days; never filled", counts)
                over = [dict(year=r["year"], violation_pct=r["violation_pct"]) for r in counts
                        if r["violation_pct"] is not None and r["violation_pct"] > rules["gap_season_alert_pct"]]
                add("gap_season", len(over), f"{population}: >{rules['gap_season_alert_pct']}% gap violations", over)
            add("acquisition_gaps", sum(s["aoi_no_acquisition_days"] for s in report["seasons"]),
                "Days without ledger acquisition: information only, not a pipeline failure")
            report["timings"]["gap_seconds"] = time.perf_counter() - gap_started
    except Exception as error:
        report["checks"].append(dict(check="execution", severity="critical", status="fail", count=1,
            message=f"{type(error).__name__}: {error}", examples=[]))
    finally:
        con.close()
    report["status"] = "failed" if has_critical() else "passed"
    report["timings"]["total_seconds"] = time.perf_counter() - started
    write_report(path, report)
    if report["status"] == "failed":
        failures = [c["check"] for c in report["checks"] if c["severity"] == "critical" and c["count"]]
        raise QualityFailure(f"Critical quality failure: {', '.join(failures)}. Report: {path}")
    return path

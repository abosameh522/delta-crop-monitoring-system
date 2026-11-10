"""Quality checks on disposable, deliberately tiny Silver fixtures."""
from __future__ import annotations

import json
import subprocess
import sys
from copy import deepcopy
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from delta_crops.config import load_config
from delta_crops.quality.checks import EXPECTED_SCHEMA, null_rate_context, run_quality
from delta_crops.quality.dn_floor import compare_cells
from delta_crops.quality.gaps import season_gaps
from delta_crops.quality.report import QualityFailure
from delta_crops.transform.silver import ndvi_path


@pytest.fixture
def tiny_silver(tmp_path):
    config = load_config()
    config = replace(config, seasons=replace(config.seasons, years=(2023,),
        window_start="05-01", window_end="05-15"), data_quality=deepcopy(config.data_quality))
    root = tmp_path / "lake"
    (root / "silver").mkdir(parents=True)
    pq.write_table(pa.table({"cell_id": pa.array([1, 2], type=pa.int32()),
        "is_cropland": [True, False]}), root / "silver/cells.parquet")
    log = root / "bronze/_logs/ingestion_log.jsonl"
    log.parent.mkdir(parents=True)
    records = []
    for day in (date(2023, 5, 1), date(2023, 5, 15)):
        for tile in ("36RTV", "36RUV"):
            records.append(dict(asset_key=f"{day}:{tile}", source="earth-search",
                                acquired_at=str(day) + "T08:41:00Z", tile=tile))
        table = pa.Table.from_pylist([dict(cell_id=i, date=day, ndvi=v,
            valid_pixels=50, total_pixels=100, masked_pct=50.0) for i,v in ((1,-1.0),(2,1.0))],
            schema=EXPECTED_SCHEMA)
        path = ndvi_path(root, day)
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, path)
    log.write_text("\n".join(json.dumps(r) for r in records))
    return root, config, ndvi_path(root, date(2023, 5, 1))


def saved_report(root):
    return json.loads(sorted((root / "silver/_reports/quality").glob("quality_*.json"))[-1].read_text())


def test_valid_data_and_alerts_do_not_stop(tiny_silver):
    root, config, _ = tiny_silver
    report = json.loads(run_quality(root, config).read_text())
    assert report["status"] == "passed"
    assert report["seasons"][0]["statistics"]["all"]["violation_count"] == 2
    assert any(c["check"] == "gap_season" and c["count"] for c in report["checks"])
    assert report["pandera_aggregate_validation"] == "passed"


@pytest.mark.parametrize("corruption,check", [
    ("ndvi", "ndvi_range"), ("nan", "ndvi_range"), ("duplicate", "duplicate_keys"),
    ("rows", "row_count"), ("pixels", "pixel_counts"), ("missing", "missing_partition"),
    ("dtype", "schema"), ("no_date", "schema"), ("date", "date_partition"),
    ("foreign", "cell_keys"), ("required", "required_values"), ("empty", "row_count")])
def test_corruption_fails_and_persists_report(tiny_silver, corruption, check):
    root, config, path = tiny_silver
    table = pq.ParquetFile(path).read()
    rows = table.to_pylist()
    if corruption == "missing":
        path.unlink()
    elif corruption == "dtype":
        i = table.schema.get_field_index("valid_pixels")
        pq.write_table(table.set_column(i, "valid_pixels", table["valid_pixels"].cast(pa.int16())), path)
    elif corruption == "no_date":
        pq.write_table(table.drop(["date"]), path)
    else:
        if corruption == "ndvi": rows[0]["ndvi"] = 1.01
        if corruption == "nan": rows[0]["ndvi"] = float("nan")
        if corruption == "duplicate": rows[1]["cell_id"] = 1
        if corruption == "rows": rows = rows[:1]
        if corruption == "pixels": rows[0]["valid_pixels"] = 101
        if corruption == "date": rows[0]["date"] += timedelta(days=1)
        if corruption == "foreign": rows[0]["cell_id"] = 999
        if corruption == "required": rows[0]["valid_pixels"] = None
        if corruption == "empty": rows = []
        pq.write_table(pa.Table.from_pylist(rows, schema=EXPECTED_SCHEMA), path)
    with pytest.raises(QualityFailure, match=check):
        run_quality(root, config)
    report = saved_report(root)
    assert report["status"] == "failed" and report["seasons"] == []
    assert any(c["check"] == check and c["severity"] == "critical" and c["count"] for c in report["checks"])


def test_null_rate_single_tile_and_leave_one_out_mad():
    rule = load_config().data_quality
    dates = [dict(date=f"2023-05-{day:02}", tiles=["36RTV", "36RUV"], null_pct=10.0)
             for day in range(1, 7)]
    dates += [dict(date="2023-05-07", tiles=["36RUV"], null_pct=100.0),
              dict(date="2023-05-08", tiles=["36RTV", "36RUV"], null_pct=20.0)]
    result = null_rate_context(dates, rule)
    assert not result[-2]["null_alert"]
    assert result[-1]["null_alert"] and result[-1]["peer_mad"] == 0
    assert not result[0]["null_alert"]
    limited = null_rate_context([dict(date="2023-05-01", tiles=["36RTV", "36RUV"], null_pct=51.0)], rule)
    assert limited[0]["null_reasons"] == ["absolute_threshold"]
    assert limited[0]["robust_threshold"] is None


def test_gap_edges_attribution_and_all_null_cell(tmp_path):
    rule = load_config().data_quality
    con = duckdb.connect()
    con.register("cells", pa.table({"cell_id": [1, 2, 3], "is_cropland": [True, True, False]}))
    days = [date(2020,5,d) for d in (6,8,16,18)]
    con.register("observations", pa.Table.from_pylist([
        dict(cell_id=cell, date=d, ndvi=None if cell==2 or (cell==1 and d.day in (8,16)) else 0.5)
        for cell in (1,2,3) for d in days]))
    out = tmp_path / "longest.parquet"
    summary = season_gaps(con, date(2020,5,1), date(2020,5,31), days, rule, out)
    rows = {r["cell_id"]:r for r in pq.read_table(out).to_pylist()}
    assert rows[1]["gap_days"] == 13  # trailing May 19-31 beats interior 11 days
    assert rows[1]["gap_start"] == date(2020,5,19)
    assert rows[2]["gap_days"] == 31 and rows[2]["gating_days"] == 4
    assert rows[2]["no_acquisition_days"] == 27
    assert summary["statistics"]["all"]["all_gaps_attribution"]["no_acquisition_cell_days"] == 27*3
    con.close()


def test_gap_may7_to17_is_eleven_and_threshold_inclusive(tmp_path):
    con = duckdb.connect()
    con.register("cells", pa.table({"cell_id":[1],"is_cropland":[True]}))
    days = [date(2020,5,d) for d in (6,8,16,18)]
    con.register("observations",pa.Table.from_pylist([dict(cell_id=1,date=d,
        ndvi=0.5 if d.day in (6,18) else None) for d in days]))
    out=tmp_path/"longest.parquet"
    result=season_gaps(con,date(2020,5,6),date(2020,5,18),days,load_config().data_quality,out)
    row=pq.read_table(out).to_pylist()[0]
    assert (row["gap_days"],row["gating_days"],row["no_acquisition_days"]) == (11,2,9)
    assert result["statistics"]["all"]["info_count"]==1
    assert result["statistics"]["all"]["violation_count"]==0
    con.close()


def test_thirteen_missing_days_always_contain_iso_week():
    # All seven possible starting weekdays; 12 days fails for a Tuesday start.
    def full_week(start, length):
        days={start+timedelta(days=i) for i in range(length)}
        return any(d.weekday()==0 and all(d+timedelta(days=j) in days for j in range(7)) for d in days)
    monday=date(2026,11,9)
    assert all(full_week(monday+timedelta(days=i),13) for i in range(7))
    assert not full_week(monday+timedelta(days=1),12)


def test_dn_floor_audit_reports_count_transitions():
    old=dict(ndvi=np.array([0.3,0.5,np.nan]),valid_pixels=np.array([50,70,20]))
    new=dict(ndvi=np.array([np.nan,0.52,np.nan]),valid_pixels=np.array([49,69,20]))
    result=compare_cells(old,new,np.array([True,True,False]))
    assert result["all"]["nonnull_to_null"]==1
    assert result["all"]["null_share_change_pp"]==pytest.approx(100/3)
    assert result["cropland"]["paired_cells_changed_gt_0_01"]==1


def test_cli_critical_failure_has_nonzero_exit(tiny_silver):
    root,_,path=tiny_silver
    rows=pq.ParquetFile(path).read().to_pylist();rows[0]["ndvi"]=1.25
    pq.write_table(pa.Table.from_pylist(rows,schema=EXPECTED_SCHEMA),path)
    result=subprocess.run([sys.executable,"-m","delta_crops.quality.cli","--data-dir",str(root)],
                          capture_output=True,text=True)
    assert result.returncode==1 and "ndvi_range" in result.stdout
    assert saved_report(root)["status"]=="failed"

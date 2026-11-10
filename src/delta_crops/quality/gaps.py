"""Calendar-day gaps with separate acquisition and cell-gating attribution."""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import duckdb
import pyarrow as pa


def season_gaps(
    con: duckdb.DuckDBPyConnection,
    start: date,
    end: date,
    acquisition_dates: list[date],
    rules: dict,
    output: Path | None = None,
) -> dict:
    """Read observations/cells views; usable is strictly NDVI IS NOT NULL.

    Count missing calendar days between usable dates and at season boundaries.
    Ties for longest gap choose the earliest. Split each gap into absent-ledger
    days and acquisition days with NULL cell NDVI. No gap is filled.
    """
    days = set(acquisition_dates)
    rank = 0
    calendar = []
    for offset in range(-1, (end - start).days + 2):
        day = start + timedelta(days=offset)
        present = int(day in days)
        rank += present
        calendar.append(dict(day=day, acq_rank=rank, is_acq=present))
    con.register("gap_calendar_arrow", pa.Table.from_pylist(calendar))
    con.execute("CREATE OR REPLACE TEMP TABLE gap_calendar AS SELECT * FROM gap_calendar_arrow").fetchall()
    con.execute("""CREATE OR REPLACE TEMP TABLE gap_nodes AS
        SELECT cell_id, date AS day FROM observations
        WHERE ndvi IS NOT NULL AND date BETWEEN ? AND ?
        UNION ALL SELECT cell_id, ?::DATE FROM cells
        UNION ALL SELECT cell_id, ?::DATE FROM cells""",
        [start, end, start - timedelta(days=1), end + timedelta(days=1)]).fetchall()
    con.execute("""CREATE OR REPLACE TEMP TABLE gap_intervals AS
        WITH ordered AS (
            SELECT cell_id, day AS next_day,
                lag(day) OVER (PARTITION BY cell_id ORDER BY day) AS prev_day
            FROM gap_nodes
        )
        SELECT cell_id, prev_day + 1 AS gap_start, next_day - 1 AS gap_end,
            date_diff('day', prev_day, next_day) - 1 AS gap_days,
            b.acq_rank - a.acq_rank - b.is_acq AS gating_days,
            date_diff('day', prev_day, next_day) - 1
                - (b.acq_rank - a.acq_rank - b.is_acq) AS no_acquisition_days
        FROM ordered JOIN gap_calendar a ON a.day = prev_day
                     JOIN gap_calendar b ON b.day = next_day
        WHERE date_diff('day', prev_day, next_day) > 1""").fetchall()
    con.execute("""CREATE OR REPLACE TEMP TABLE gap_longest AS
        WITH ranked AS (
            SELECT * FROM gap_intervals
            QUALIFY row_number() OVER (
                PARTITION BY cell_id ORDER BY gap_days DESC, gap_start) = 1
        )
        SELECT c.cell_id, c.is_cropland, r.gap_start, r.gap_end,
            coalesce(r.gap_days, 0) AS gap_days,
            coalesce(r.gating_days, 0) AS gating_days,
            coalesce(r.no_acquisition_days, 0) AS no_acquisition_days
        FROM cells c LEFT JOIN ranked r USING (cell_id)""").fetchall()
    summary = {}
    for label, predicate in (("all", "TRUE"), ("cropland", "is_cropland")):
        row = con.execute(f"""SELECT count(*), quantile_cont(gap_days, [0.5,0.9,0.99]),
            max(gap_days), count(*) FILTER (WHERE gap_days > ?),
            count(*) FILTER (WHERE gap_days >= ?),
            sum(no_acquisition_days), sum(gating_days)
            FROM gap_longest WHERE {predicate}""",
            [rules["gap_info_days"], rules["gap_alert_days"]]).fetchall()[0]
        n, quantiles, maximum, info, alerts, no_acq, gating = row
        thresholds = {}
        for threshold in rules["gap_statistics_days"]:
            gt, ge = con.execute(f"""SELECT count(*) FILTER (WHERE gap_days > ?),
                count(*) FILTER (WHERE gap_days >= ?)
                FROM gap_longest WHERE {predicate}""", [threshold, threshold]).fetchall()[0]
            thresholds[str(threshold)] = dict(gt_count=gt, ge_count=ge,
                gt_pct=100 * gt / n if n else None, ge_pct=100 * ge / n if n else None)
        total_gap, missing, nulls = con.execute(f"""SELECT coalesce(sum(gap_days),0),
            coalesce(sum(no_acquisition_days),0), coalesce(sum(gating_days),0)
            FROM gap_intervals JOIN cells USING (cell_id) WHERE {predicate}""").fetchall()[0]
        summary[label] = dict(cells=n, p50_p90_p99=quantiles, max_days=maximum,
            info_count=info, info_pct=100 * info / n if n else None,
            violation_count=alerts, violation_pct=100 * alerts / n if n else None,
            longest_attribution=dict(no_acquisition_cell_days=no_acq, gating_cell_days=gating),
            all_gaps_attribution=dict(total_cell_days=total_gap,
                no_acquisition_cell_days=missing, gating_cell_days=nulls),
            threshold_shares=thresholds)
    boundaries = [start - timedelta(days=1)] + sorted(days) + [end + timedelta(days=1)]
    aoi = [dict(start=str(a + timedelta(days=1)), end=str(b - timedelta(days=1)),
                days=(b - a).days - 1)
           for a, b in zip(boundaries, boundaries[1:]) if (b - a).days > 1]
    result = dict(start=str(start), end=str(end), acquisition_dates=len(days),
        aoi_no_acquisition_days=(end - start).days + 1 - len(days),
        aoi_longest_gap_days=max((r["days"] for r in aoi), default=0),
        aoi_gaps=aoi, statistics=summary)
    if output is not None:
        con.execute("COPY gap_longest TO ? (FORMAT PARQUET)", [str(output)]).fetchall()
        result["longest_gap_file"] = str(output)
    for name in ("gap_nodes", "gap_intervals", "gap_longest", "gap_calendar"):
        con.execute(f"DROP TABLE {name}").fetchall()
    return result

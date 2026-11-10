"""The one query that answers the main project question for the focus season."""

from __future__ import annotations

from pathlib import Path

SAMPLE_QUERY = """
WITH cw AS (
  SELECT w.week_start,
         count(*) FILTER (WHERE w.status = 'ok') AS cells_ok,
         count(*)                               AS cropland_cells,
         avg(w.ndvi_mean)                       AS ndvi_mean,
         avg(a.anomaly)                         AS anomaly_mean
  FROM read_parquet('{weekly}') w
  JOIN read_parquet('{cells}') c USING (cell_id)
  LEFT JOIN read_parquet('{anomaly}') a USING (cell_id, week_start)
  WHERE c.is_cropland
  GROUP BY 1
)
SELECT cw.week_start,
       round(100.0 * cw.cells_ok / cw.cropland_cells, 1) AS coverage_pct,
       round(cw.ndvi_mean, 3) AS ndvi,
       round(cw.anomaly_mean, 3) AS anomaly,
       ww.heatwave_days,
       round(ww.temp_max_mean_c, 1) AS tmax_mean,
       round(ww.precip_sum_mm, 1) AS rain_mm
FROM cw JOIN read_parquet('{weather}') ww USING (week_start)
ORDER BY 1
"""


def sample_query(root: Path, year: int) -> str:
    """Render the documented query for one year of the local Gold layout."""
    return SAMPLE_QUERY.format(
        weekly=root / "gold" / "weekly_ndvi" / f"year={year}" / "*.parquet",
        anomaly=root / "gold" / "ndvi_anomaly" / f"year={year}" / "*.parquet",
        weather=root / "gold" / "weekly_weather" / f"year={year}" / "*.parquet",
        cells=root / "silver" / "cells.parquet",
    )

"""Read-only DN>0 versus DN>1 audit; never writes/rebuilds Silver partitions."""
from __future__ import annotations

import argparse
import time
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import uuid4

import numpy as np
import pyarrow.parquet as pq

from delta_crops.config import Config, data_dir, load_config
from delta_crops.quality.report import write_report
from delta_crops.transform import bronze_reader, silver
from delta_crops.transform.grid import load_or_build
from delta_crops.transform.reflectance import ndvi_with_mask


def compare_cells(old: dict, new: dict, cropland: np.ndarray) -> dict:
    """Report NULL transitions separately from NDVI changes on paired usable cells."""
    result = {}
    for label, selection in (("all", np.ones(len(cropland), dtype=bool)), ("cropland", cropland)):
        a, b = old["ndvi"][selection], new["ndvi"][selection]
        usable_a, usable_b = np.isfinite(a), np.isfinite(b)
        both = usable_a & usable_b
        result[label] = dict(cells=len(a),
            nonnull_dn_gt0=int(usable_a.sum()), nonnull_dn_gt1=int(usable_b.sum()),
            null_dn_gt0=int((~usable_a).sum()), null_dn_gt1=int((~usable_b).sum()),
            nonnull_to_null=int((usable_a & ~usable_b).sum()),
            null_to_nonnull=int((~usable_a & usable_b).sum()),
            null_share_change_pp=float(100 * (usable_a.sum() - usable_b.sum()) / len(a)) if len(a) else None,
            mean_ndvi_dn_gt0=float(a[usable_a].mean(dtype=np.float64)) if usable_a.any() else None,
            mean_ndvi_dn_gt1=float(b[usable_b].mean(dtype=np.float64)) if usable_b.any() else None,
            paired_mean_ndvi_difference=float((b[both] - a[both]).mean(dtype=np.float64)) if both.any() else None,
            paired_max_abs_ndvi_difference=float(np.max(np.abs(b[both] - a[both]))) if both.any() else None,
            paired_cells_changed_gt_0_01=int((np.abs(b[both] - a[both]) > 0.01).sum()),
            valid_pixels_dn_gt0=int(old["valid_pixels"][selection].sum(dtype=np.int64)),
            valid_pixels_dn_gt1=int(new["valid_pixels"][selection].sum(dtype=np.int64)))
    return result


def audit_dn_floor(root: Path, config: Config) -> Path:
    """Read each fixed date once, replay both validity rules in memory, report effects."""
    started = time.perf_counter()
    grid, keep, pixels = load_or_build(config.bbox, root / "silver/_cache")
    cells = pq.read_table(root / "silver/cells.parquet", columns=["is_cropland"]).to_pydict()
    cropland = np.array(cells["is_cropland"], dtype=bool)
    total = pixels.reshape(grid.nrows, 10, grid.ncols, 10).sum((1, 3))
    ledger = bronze_reader.load_ledger(root)
    report = dict(kind="DN_floor_measurement_only", rule_current="DN>0", rule_audited="DN>1",
        rebuild=False, dates=[], dates_fixed_in_config=config.data_quality["dn_floor_audit_dates"])
    for day_text in config.data_quality["dn_floor_audit_dates"]:
        day = date.fromisoformat(day_text)
        tiles, problems = bronze_reader.assets_for_date(ledger, root, day, verify_checksum=False)
        if problems:
            raise ValueError(f"{day}: {problems}")
        canvases = [np.full(grid.pixel_shape, np.nan, dtype=np.float32) for _ in range(2)]
        claimed = [np.zeros(grid.pixel_shape, dtype=bool) for _ in range(2)]
        baseline_floor_pixels = 0
        excluded_by_tile = {}
        order = [t for t in config.silver.tile_priority if t in tiles]
        order += sorted(set(tiles) - set(order))
        for tile in order:
            bands = tiles[tile]
            red, transform = bronze_reader.read_band(bands["B04"], config.bbox)
            nir, _ = bronze_reader.read_band(bands["B08"], config.bbox)
            scl = bronze_reader.read_scl_on_grid(bands["SCL"], config.bbox, transform, red.shape)
            ndvi, valid = ndvi_with_mask(red, nir, scl, config.silver.masked_scl_classes)
            stricter = valid & (red > 1) & (nir > 1)
            ro, co = bronze_reader.tile_offsets(transform, grid.x0, grid.y_top)
            rs, cs, bs, ss = silver.block_slice(canvases[0], red, ro, co)
            floor_here = valid[bs, ss] & ~stricter[bs, ss] & pixels[rs, cs]
            excluded_by_tile[tile] = int(floor_here.sum())
            baseline_floor_pixels += int((floor_here & ~claimed[0][rs, cs]).sum())
            for index, mask in enumerate((valid, stricter)):
                fresh = mask[bs, ss] & pixels[rs, cs] & ~claimed[index][rs, cs]
                target = canvases[index][rs, cs]
                target[fresh] = ndvi[bs, ss][fresh]
                claimed[index][rs, cs] |= fresh
            del red, nir, scl, ndvi, valid, stricter
        arrays = [silver.aggregate_canvas(c, v, total, keep, config.silver.min_valid_fraction, grid)
                  for c, v in zip(canvases, claimed)]
        stored = pq.read_table(silver.ndvi_path(root, day), columns=["valid_pixels", "ndvi"])
        if not np.array_equal(arrays[0]["valid_pixels"], stored["valid_pixels"].to_numpy()):
            raise ValueError(f"{day}: DN>0 audit replay does not reproduce Silver valid counts")
        if not np.allclose(arrays[0]["ndvi"], stored["ndvi"].to_numpy(), equal_nan=True, atol=1e-7):
            raise ValueError(f"{day}: DN>0 audit replay does not reproduce Silver NDVI")
        result = compare_cells(arrays[0], arrays[1], cropland)
        report["dates"].append(dict(date=day_text, tiles=order,
            baseline_mosaic_dn1_pixels=baseline_floor_pixels,
            excluded_valid_pixel_observations_by_tile=excluded_by_tile,
            net_mosaic_pixels_excluded=int(claimed[0].sum() - claimed[1].sum()), statistics=result))
        print(f"{day}: excluded mosaic pixels={report['dates'][-1]['net_mosaic_pixels_excluded']:,}; "
              f"cropland non-NULL -> NULL={result['cropland']['nonnull_to_null']:,}", flush=True)
        del canvases, claimed, arrays
    report["seconds"] = time.perf_counter() - started
    run = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8]
    path = root / "silver/_reports/quality" / f"dn_floor_{run}.json"
    write_report(path, report)
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path)
    parser.add_argument("--config", type=Path)
    args = parser.parse_args()
    print(f"Report: {audit_dn_floor(args.data_dir or data_dir(), load_config(args.config))}")


if __name__ == "__main__":
    main()

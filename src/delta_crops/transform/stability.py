"""Candidate cross-era stable targets (water / bare desert) inside the AOI.

The Phase 3 plan asks for a 1 km box that is >= 90 % one class. The Nile Delta
may not contain one, so this module only *measures* candidates; the box is frozen
in config/aoi.yaml by a human decision, never chosen silently by the code.
"""

from __future__ import annotations

import numpy as np


def survey(classes: np.ndarray, pixel_mask: np.ndarray, class_code: int, box_px: int) -> tuple[float, int, int, int]:
    """Best in-AOI share of `class_code` in a box_px square.

    Returns (best_fraction, row_px, col_px, in_aoi_pixels_in_box).
    """
    target = ((classes == class_code) & pixel_mask).astype(np.float32)
    counts = np.pad(target, ((1, 0), (1, 0))).cumsum(0).cumsum(1)
    if box_px <= 0 or box_px >= min(target.shape):
        raise ValueError(f"box_px {box_px} outside array shape {target.shape}")
    box = (
        counts[box_px:, box_px:]
        - counts[:-box_px, box_px:]
        - counts[box_px:, :-box_px]
        + counts[:-box_px, :-box_px]
    )
    share = box / float(box_px * box_px)
    row, col = np.unravel_index(int(np.argmax(share)), share.shape)
    in_aoi = int(pixel_mask[row : row + box_px, col : col + box_px].sum())
    return float(share[row, col]), int(row), int(col), in_aoi


def box_bounds_utm(
    row_px: int, col_px: int, box_px: int, grid_x0: float, grid_y_top: float, pixel_m: int = 10
) -> dict[str, float]:
    """UTM bounds of a candidate box (for freezing it in config)."""
    west = grid_x0 + col_px * pixel_m
    north = grid_y_top - row_px * pixel_m
    return {"west": west, "south": north - box_px * pixel_m, "east": west + box_px * pixel_m, "north": north}

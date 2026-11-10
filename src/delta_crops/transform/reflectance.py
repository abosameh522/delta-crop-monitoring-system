"""Pure raster maths for Bronze -> Silver: reflectance, NDVI and the SCL mask.

No I/O here, so every rule is unit-testable on tiny arrays.
"""

from __future__ import annotations

import numpy as np

#: Earth Search L2A DN -> reflectance. Scale only: the advertised -0.1 offset is
#: already applied in the pixels of this collection (PROJECT.md §3.3).
REFLECTANCE_SCALE = 0.0001


def to_reflectance(dn: np.ndarray) -> np.ndarray:
    """DN -> BOA reflectance as float32. Never subtracts an offset, never clips."""
    return dn.astype("float32") * REFLECTANCE_SCALE


def ndvi_from_reflectance(red: np.ndarray, nir: np.ndarray) -> np.ndarray:
    """(nir - red) / (nir + red) with NaN where the denominator is not positive.

    Returning NaN (rather than 0, inf or a clip) keeps invalid pixels out of the
    cell mean instead of dragging it towards an arbitrary value.
    """
    denominator = red + nir
    out = np.full(red.shape, np.nan, dtype="float32")
    np.divide(nir - red, denominator, out=out, where=denominator > 0)
    return out


def scl_lookup(masked_classes: tuple[int, ...] | list[int]) -> np.ndarray:
    """256-entry boolean LUT: True where the SCL class must be masked."""
    lut = np.zeros(256, dtype=bool)
    for value in masked_classes:
        lut[int(value)] = True
    return lut


def valid_pixel_mask(
    red_dn: np.ndarray,
    nir_dn: np.ndarray,
    scl: np.ndarray,
    masked_classes: tuple[int, ...] | list[int],
) -> np.ndarray:
    """Pixel is usable when both bands are present, the denominator is positive
    and the SCL class is not masked."""
    usable = (red_dn > 0) & (nir_dn > 0)  # both present -> denominator > 0
    return usable & ~scl_lookup(masked_classes)[scl]


def ndvi_with_mask(
    red_dn: np.ndarray,
    nir_dn: np.ndarray,
    scl: np.ndarray,
    masked_classes: tuple[int, ...] | list[int],
) -> tuple[np.ndarray, np.ndarray]:
    """Return (ndvi float32 with NaN, valid bool) for one tile window."""
    valid = valid_pixel_mask(red_dn, nir_dn, scl, masked_classes)
    ndvi = ndvi_from_reflectance(to_reflectance(red_dn), to_reflectance(nir_dn))
    ndvi[~valid] = np.nan
    return ndvi, valid

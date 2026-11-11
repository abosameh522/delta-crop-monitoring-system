"""Persist quality evidence before returning or raising a critical failure."""
from __future__ import annotations

import json
import math
import os
from pathlib import Path

#: Re-exported contract exception: there is exactly one definition, in errors.py.
from delta_crops.errors import QualityFailure  # noqa: F401


def _json_safe(value):
    """Keep non-finite corrupt measurements visible without emitting invalid JSON."""
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def write_report(path: Path, report: dict) -> None:
    """Write one JSON report atomically; never expose a partial report."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(_json_safe(report), indent=2, allow_nan=False, default=str) + "\n")
    os.replace(temporary, path)

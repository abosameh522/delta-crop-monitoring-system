"""Open-Meteo daily archive ingestion (raw JSON)."""

from __future__ import annotations

from datetime import date
from typing import Any

import requests

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
DAILY_VARIABLES = (
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "shortwave_radiation_sum",
)
TIMEZONE = "Africa/Cairo"


def build_params(lat: float, lon: float, start: date, end: date) -> dict[str, Any]:
    return {
        "latitude": round(lat, 4),
        "longitude": round(lon, 4),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "daily": ",".join(DAILY_VARIABLES),
        "timezone": TIMEZONE,
    }


def fetch_daily(lat: float, lon: float, start: date, end: date, timeout: int = 60, allow_partial: bool = False) -> dict[str, Any]:
    response = requests.get(ARCHIVE_URL, params=build_params(lat, lon, start, end), timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    validate_payload(payload, start, end, allow_partial=allow_partial)
    return payload


def validate_payload(payload: dict[str, Any], start: date, end: date, allow_partial: bool = False) -> str:
    """Fail loudly on a partial or malformed Open-Meteo response."""
    if "daily" not in payload or "time" not in payload["daily"]:
        raise ValueError("Open-Meteo response has no daily.time")
    days = payload["daily"]["time"]
    expected = (end - start).days + 1
    if len(days) != expected and not allow_partial:
        raise ValueError(f"Open-Meteo returned {len(days)} days, expected {expected}")
    if not days or days[0] != start.isoformat() or days[-1] > end.isoformat():
        raise ValueError(f"Open-Meteo range mismatch: {days[0] if days else None}..{days[-1] if days else None}")
    for variable in DAILY_VARIABLES:
        values = payload["daily"].get(variable)
        if values is None or len(values) != len(days):
            raise ValueError(f"Open-Meteo variable {variable} missing or wrong length")
    return "complete" if len(days) == expected else "partial"

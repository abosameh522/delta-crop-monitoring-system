"""Shared rendering helpers for the dashboard views."""

from __future__ import annotations

from typing import Sequence

import pandas as pd
import pydeck as pdk
import streamlit as st

from delta_crops.dashboard import data

Ramp = Sequence[tuple[float, tuple[int, int, int]]]


def lerp_color(value: float, vmin: float, vmax: float, ramp: Ramp) -> list[int]:
    """Interpolate an RGBA colour from a (position, rgb) ramp."""
    span = vmax - vmin
    position = 0.0 if span <= 0 else min(max((value - vmin) / span, 0.0), 1.0)
    for (left, first), (right, second) in zip(ramp, ramp[1:]):
        if position <= right:
            share = 0.0 if right == left else (position - left) / (right - left)
            return [
                int(round(first[0] + (second[0] - first[0]) * share)),
                int(round(first[1] + (second[1] - first[1]) * share)),
                int(round(first[2] + (second[2] - first[2]) * share)),
                190,
            ]
    return [*ramp[-1][1], 190]


NDVI_RAMP: Ramp = ((0.0, (120, 78, 40)), (0.45, (196, 190, 90)), (1.0, (20, 120, 45)))
ANOMALY_RAMP: Ramp = ((0.0, (170, 25, 25)), (0.5, (245, 245, 240)), (1.0, (25, 80, 170)))
PROBABILITY_RAMP: Ramp = ((0.0, (245, 245, 240)), (0.5, (250, 190, 60)), (1.0, (170, 20, 20)))


def map_deck(frame: pd.DataFrame, value_label: str, vmin: float, vmax: float,
             ramp: Ramp, formatter: str = "{value:.3f}", zoom: float = 10.2) -> pdk.Deck:
    """A pydeck chart of the 1 km blocks, coloured by value."""
    collection = data.geojson(frame)
    for feature in collection["features"]:
        feature["properties"]["color"] = lerp_color(
            feature["properties"]["value"], vmin, vmax, ramp
        )
    layer = pdk.Layer(
        "GeoJsonLayer",
        collection,
        get_fill_color="properties.color",
        get_line_color=[255, 255, 255, 60],
        get_line_width=1,
        stroked=True,
        filled=True,
        pickable=True,
    )
    center_lat = float(frame["lat0"].mean() + (frame["lat1"].mean() - frame["lat0"].mean()) / 2)
    center_lon = float(frame["lon0"].mean() + (frame["lon1"].mean() - frame["lon0"].mean()) / 2)
    tooltip = {"html": f"<b>{value_label}</b> {formatter}<br/>1 km block: {{cells}} cells"}
    return pdk.Deck(
        layers=[layer],
        initial_view_state=pdk.ViewState(
            latitude=center_lat, longitude=center_lon, zoom=zoom, pitch=0
        ),
        tooltip=tooltip,
        map_style="light",
    )


def legend(low: str, middle: str, high: str) -> None:
    st.caption(f"{low}  ·  {middle}  ·  {high}")

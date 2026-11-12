"""Map view: one season week of NDVI or anomaly, coloured on 1 km blocks."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from delta_crops.dashboard import common, data


def render(root: str) -> None:
    st.title("Nile Delta crop map")
    st.caption(
        "Gold `weekly_ndvi` / `ndvi_anomaly`, aggregated to 1 km blocks "
        "(10 × 10 cells of the 100 m grid)."
    )

    years = data.available_years(root)
    if not years:
        st.error("No Gold seasons found under `data/gold`.")
        return

    controls = st.columns([1, 2, 1, 1])
    year = controls[0].selectbox("Season", years, index=len(years) - 1)
    weeks = data.season_weeks(root, int(year))
    week = controls[1].select_slider("Week (ISO week start)", weeks, value=weeks[len(weeks) // 2])
    layer_label = controls[2].radio("Layer", ("NDVI", "Anomaly"))
    cropland_only = controls[3].toggle("Cropland only", value=True)

    layer = "ndvi" if layer_label == "NDVI" else "anomaly"
    frame = data.map_layer(root, int(year), week, layer, cropland_only)
    if frame.empty:
        st.warning("No usable cells for this week. Try another week or turn off the cropland filter.")
        return

    if layer == "ndvi":
        deck = common.map_deck(frame, "NDVI", 0.0, 0.85, common.NDVI_RAMP)
        common.legend("0.0 bare soil", "0.45 mixed", "0.85 dense canopy")
    else:
        deck = common.map_deck(frame, "Anomaly", -0.30, 0.30, common.ANOMALY_RAMP)
        common.legend("−0.30 below baseline", "0.00 at baseline", "+0.30 above baseline")

    st.pydeck_chart(deck, width="stretch")

    lowest, highest = float(frame["value"].min()), float(frame["value"].max())
    st.caption(
        f"{len(frame):,} blocks · {int(frame['n_cells'].sum()):,} cells · "
        f"min {lowest:.3f}, max {highest:.3f} · week starting {week}"
    )
    with st.expander("What the numbers mean"):
        st.markdown(
            "NDVI is the weekly mean over the block's `ok` cells; anomaly is the raw "
            "difference from the same cell's training-season climatology (never the "
            "z-score, see D-046). Weeks marked `too_cloudy` or "
            "`insufficient_observations` are excluded, so an empty block means no "
            "usable observation that week, not zero vegetation."
        )

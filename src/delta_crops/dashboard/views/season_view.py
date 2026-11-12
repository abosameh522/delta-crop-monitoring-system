"""Season view: NDVI against temperature for the whole AOI."""

from __future__ import annotations

import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from delta_crops.dashboard import data


def render(root: str) -> None:
    st.title("Season: NDVI vs temperature")
    st.caption(
        "Cropland-mean NDVI from Gold `weekly_ndvi`, temperature from Gold "
        "`weekly_weather` (a single grid point for the AOI, about 120 independent "
        "weeks in six seasons)."
    )

    years = data.available_years(root)
    if not years:
        st.error("No Gold seasons found under `data/gold`.")
        return
    chosen = st.multiselect("Seasons", years, default=years)
    if not chosen:
        st.info("Pick at least one season.")
        return

    frame = data.season_frame(root, tuple(int(year) for year in chosen))
    if frame.empty:
        st.warning("No weekly rows for the chosen seasons.")
        return

    figure = make_subplots(specs=[[{"secondary_y": True}]])
    figure.add_trace(
        go.Scatter(
            x=frame["week_start"], y=frame["ndvi_cropland"], name="NDVI (cropland)",
            mode="lines+markers", line={"color": "#1a7f37", "width": 3},
        ),
        secondary_y=False,
    )
    figure.add_trace(
        go.Scatter(
            x=frame["week_start"], y=frame["temp_max_mean_c"],
            name="Tmax weekly mean (°C)", mode="lines",
            line={"color": "#c2410c", "width": 2, "dash": "dot"},
        ),
        secondary_y=True,
    )
    figure.update_yaxes(title_text="NDVI", range=[0, 0.9], secondary_y=False)
    figure.update_yaxes(title_text="Tmax (°C)", secondary_y=True)
    figure.update_layout(
        height=420, margin={"l": 10, "r": 10, "t": 30, "b": 10},
        legend={"orientation": "h", "y": 1.08}, hovermode="x unified",
    )
    st.plotly_chart(figure, width="stretch")

    weather = frame.dropna(subset=["temp_max_mean_c"])
    if not weather.empty:
        bars = go.Figure()
        bars.add_trace(go.Bar(
            x=weather["week_start"], y=weather["heatwave_days"],
            name="heatwave days (Tmax ≥ 40 °C, ≥ 3-day run)", marker_color="#b91c1c",
        ))
        bars.add_trace(go.Bar(
            x=weather["week_start"], y=weather["precip_sum_mm"],
            name="precipitation (mm)", marker_color="#1d4ed8", yaxis="y2",
        ))
        bars.update_layout(
            height=240, margin={"l": 10, "r": 10, "t": 10, "b": 10},
            barmode="group", hovermode="x unified",
            yaxis={"title": "heatwave days"},
            yaxis2={"title": "mm", "overlaying": "y", "side": "right"},
            legend={"orientation": "h", "y": 1.12},
        )
        st.plotly_chart(bars, width="stretch")

    st.caption(
        "NDVI values are means over cells whose week is `ok` "
        f"({int(frame['n_cropland'].min()):,}–{int(frame['n_cropland'].max()):,} cropland cells "
        "per week). Rainfall in this AOI is effectively zero (0.0–2.1 mm per season), "
        "so the precipitation bars stay near the axis."
    )

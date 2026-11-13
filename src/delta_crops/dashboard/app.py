"""Streamlit entry point for the Nile Delta crop dashboard.

Reads only Gold tables (``data/gold``), the published pipeline health summary
and the frozen Phase 7 artifacts. Nothing here writes to the lake.

Paths come from the environment so the same code runs from a fresh clone:

| Variable | Default | Meaning |
| --- | --- | --- |
| ``DELTA_CROPS_DATA`` | ``data`` | lake root (``gold/`` and ``silver/cells.parquet``) |
| ``DELTA_CROPS_PREDICTIONS`` | ``artifacts/phase8/predictions/temporal_2024.parquet`` | S3 dump |
| ``DELTA_CROPS_ARTIFACTS`` | ``artifacts/phase7`` | metric JSONs |

Run it with ``delta-crops-dashboard`` from the repository root.

A committed demo sample of about 5 x 5 km for 2024 lives in ``demo_data/``.
``scripts/run_demo.sh`` points the three variables at it and the app then shows
a "Demo sample" banner, so a subset is never mistaken for the full lake.
"""

from __future__ import annotations

import os
from pathlib import Path

import streamlit as st

from delta_crops.dashboard.views import (
    health_view,
    map_view,
    predictions_view,
    season_view,
)

DEFAULT_DATA = "data"
DEFAULT_PREDICTIONS = "artifacts/phase8/predictions/temporal_2024.parquet"
DEFAULT_ARTIFACTS = "artifacts/phase7"


def main() -> None:
    st.set_page_config(
        page_title="Nile Delta crop monitor",
        page_icon="🌾",
        layout="wide",
    )

    root = os.environ.get("DELTA_CROPS_DATA", DEFAULT_DATA)
    predictions = os.environ.get("DELTA_CROPS_PREDICTIONS", DEFAULT_PREDICTIONS)
    artifacts = os.environ.get("DELTA_CROPS_ARTIFACTS", DEFAULT_ARTIFACTS)

    st.sidebar.title("🌾 Nile Delta crops")
    st.sidebar.caption("Sentinel-2 NDVI · Open-Meteo weather · 2019–2024")
    page = st.sidebar.radio("Page", ("Map", "Season", "Predictions", "Pipeline Health"))

    if (Path(root) / "DEMO_SAMPLE.json").is_file():
        st.info("Demo sample: subset of cells, 2024 only")

    if page == "Map":
        map_view.render(root)
    elif page == "Season":
        season_view.render(root)
    elif page == "Predictions":
        predictions_view.render(root, predictions, artifacts)
    else:
        health_view.render(root, artifacts)

    st.sidebar.divider()
    st.sidebar.caption(
        "Predictions cover 17 of the 20 Gold weeks (see the Predictions page). "
        "S5 calibration is a documented gap (see Pipeline Health)."
    )


if __name__ == "__main__":
    main()

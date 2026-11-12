"""Predictions view: the frozen S3 model's 2024 risk score, week by week."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from delta_crops.dashboard import common, data

NOTE = (
    "**Predictions cover 17 weeks (2024-05-13 → 2024-09-02); the Map page slider "
    "covers all 20 Gold weeks (2024-05-06 → 2024-09-16).** The model only scores a "
    "cell-week when the label is defined: the mean raw NDVI anomaly over *t+1* and "
    "*t+2* must be observable with `status = 'ok'`. `2024-05-06` has no anomaly in "
    "Gold (no baseline years for that ISO week, 0 of 239,304 cells), and `2024-09-09` "
    "and `2024-09-16` have no *t+2* week inside the season. Gold keeps 20 weeks; the "
    "model can honestly score 17."
)


def render(root: str, predictions_path: str, artifacts: str) -> None:
    st.title("Model predictions")
    st.caption(
        "S3 temporal campaign: trained 2019–2023, scored once on 2024. "
        "Probability that the mean raw NDVI anomaly over the next two weeks is ≤ −0.08."
    )

    try:
        scoreboard = data.s3_scoreboard(artifacts)
        weeks = data.prediction_weeks(predictions_path, "full")
    except FileNotFoundError as error:
        st.error(f"Missing model artifact: {error}")
        return
    except ValueError as error:
        st.error(f"The prediction file failed validation: {error}")
        return

    st.info(NOTE)

    controls = st.columns([1, 2, 1, 1])
    variant = controls[0].radio("Variant", ("full", "ndvi_only"))
    week = controls[1].select_slider("Week", weeks, value=weeks[len(weeks) // 2])
    threshold = controls[2].slider("Alert threshold", 0.0, 1.0, 0.5, 0.05)
    cropland_only = controls[3].toggle("Cropland only", value=True)

    try:
        frame = data.prediction_map(root, predictions_path, variant, week, cropland_only)
    except ValueError as error:
        st.error(f"The prediction file failed validation: {error}")
        return
    if frame.empty:
        st.warning("No cells with a prediction for this week and filter.")
        return

    st.pydeck_chart(
        common.map_deck(frame, "P(stress)", 0.0, 1.0, common.PROBABILITY_RAMP,
                        formatter="{value:.3f}", zoom=10.2),
        width="stretch",
    )
    common.legend("0.0 no risk", "0.5", "1.0 near-certain")

    above = float((frame["value"] >= threshold).mean() * 100.0)
    st.caption(
        f"{len(frame):,} blocks · {int(frame['n_cells'].sum()):,} cells · "
        f"mean predicted probability {frame['value'].mean():.3f} · "
        f"{above:.1f} % of blocks at or above the {threshold:.2f} threshold · week {week}"
    )

    st.subheader("S3 scoreboard (cropland / pooled, 2024)")
    rows = []
    for name, entry in scoreboard["variants"].items():
        metrics = entry["metrics"]
        rows.append({
            "variant": name,
            "rounds": entry["rounds"],
            "PR-AUC": round(metrics["pr_auc"], 7),
            "PR-AUC bar": round(scoreboard["bars"]["pr_auc"], 7),
            "P@R.5": round(metrics["precision_at_recall_0_5"], 7),
            "P@R.5 bar": round(scoreboard["bars"]["precision_at_recall_0_5"], 7),
            "Brier": round(metrics["brier"], 7),
            "Brier bar": round(scoreboard["bars"]["brier"], 7),
            "verdict": "pass" if all(entry["passed"].values()) else "FAIL",
        })
    st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")
    st.caption(
        f"{scoreboard['rows']:,} cropland cell-weeks; bars are the Phase 7 rule "
        "(B3 + 0.05 PR-AUC, strictly better P@R.5, strictly lower Brier). Both variants "
        "miss PR-AUC; `ndvi_only` beats `full`, so no weather effect is claimed."
    )

    reliability_source = data.load_predictions(predictions_path, variant)
    if cropland_only:
        reliability_source = reliability_source[reliability_source["is_cropland"]]
    table = data.reliability(reliability_source)
    display = table.copy()
    display["rows"] = display["rows"].map(lambda value: f"{value:,}")
    for column_name in ("mean_probability", "observed_rate"):
        display[column_name] = display[column_name].map(
            lambda value: "—" if pd.isna(value) else f"{value:.3f}"
        )
    st.subheader(f"Calibration on the map population ({variant})")
    st.dataframe(display, hide_index=True, width="stretch")
    st.caption(
        "Rebuilt from the dumped predictions with the Phase 7 bin edges. "
        "S5 has no equivalent table: predictions were never persisted there "
        "(see Pipeline Health)."
    )

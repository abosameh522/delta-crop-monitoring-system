"""Pipeline Health: the latest quality report, the Gold gate and the model gaps."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from delta_crops.dashboard import data


def _badge(status: str) -> str:
    return "🟢 passed" if status == "passed" else "🔴 failed"


def render(root: str, artifacts: str) -> None:
    st.title("Pipeline Health")
    st.caption(
        "Published deterministically from the newest Phase 4 quality report and the "
        "newest Gold weekly gate into `data/gold/_reports/pipeline_health.json` "
        "(`python -m delta_crops.dashboard.health`)."
    )

    try:
        health = data.load_health(root)
    except (FileNotFoundError, ValueError) as error:
        st.error(str(error))
        return

    quality = health["quality"]
    gate = health.get("gate") or {}
    st.subheader("Quality report")
    columns = st.columns(4)
    columns[0].metric("Status", _badge(quality["status"]))
    columns[1].metric("Rows checked", f"{int(quality['total_rows']):,}")
    columns[2].metric("Ledger dates", f"{int(quality['expected_ledger_dates']):,}")
    columns[3].metric("Kept cells", f"{int(quality['kept_cells']):,}")
    st.caption(
        f"source `{health['sources']['quality']}` · schema files "
        f"{quality['schema_valid_files']} · Pandera aggregate "
        f"{quality['pandera_aggregate_validation']} · dates {quality['dates']['n']} "
        f"({quality['dates']['with_nulls']} with NULLs, worst "
        f"{quality['dates']['max_null_date']} at {int(quality['dates']['max_null_rows']):,} rows)"
    )

    checks = pd.DataFrame(quality["checks"])
    st.dataframe(
        checks, hide_index=True, width="stretch",
        column_config={
            "status": st.column_config.TextColumn("status", width="small"),
            "count": st.column_config.NumberColumn("count", format="%d"),
        },
    )
    critical = checks[(checks["severity"] == "critical") & (checks["status"] != "pass")]
    if critical.empty:
        st.success("All critical checks passed. Alerts are shown above and are not hidden.")
    else:
        st.error(f"{len(critical)} critical checks did not pass.")

    st.subheader("Season gaps")
    seasons = pd.DataFrame(quality["seasons"])
    st.dataframe(seasons, hide_index=True, width="stretch")
    st.caption(quality["gap_definition"])

    st.subheader("Gold weekly gate")
    if gate:
        st.write(
            f"**{_badge(gate['status'])}** · source `{health['sources']['gate']}` · "
            f"max weekly NULL share {gate['gate_max_null_pct']} % · "
            f"{gate['kept_cells']:,} cells ({gate['cropland_cells']:,} cropland)"
        )
        table = []
        for season in gate["seasons"]:
            for population, values in season["populations"].items():
                table.append({
                    "year": season["year"], "population": population,
                    "structural %": values["structural_share_pct"],
                    "non-structural %": values["nonstructural_null_pct"],
                    "total NULL %": values["total_null_pct"],
                })
        st.dataframe(pd.DataFrame(table), hide_index=True, width="stretch")
    else:
        st.warning("No weekly gate report was found next to the quality report.")

    st.subheader("Model status (Phase 7)")
    try:
        scoreboard = data.s3_scoreboard(artifacts)
        spatial = data.s5_summary(artifacts)
    except FileNotFoundError as error:
        st.warning(f"Model artifacts are not available: {error}")
        return

    model_rows = []
    for name, entry in scoreboard["variants"].items():
        model_rows.append({
            "variant": name,
            "S3 PR-AUC": round(entry["metrics"]["pr_auc"], 7),
            "S3 bar": round(scoreboard["bars"]["pr_auc"], 7),
            "S3 verdict": "pass" if all(entry["passed"].values()) else "FAIL on PR-AUC",
        })
    st.dataframe(pd.DataFrame(model_rows), hide_index=True, width="stretch")

    if spatial["reliability_measured"]:
        st.success("S5 reliability table present.")
    else:
        st.warning(
            "**S5 calibration is unmeasured.** The spatial-holdout run never wrote a "
            "reliability table: `reliab` occurs 0 times in `xgb_spatial.json`, no "
            "prediction file was produced, and `run_spatial_fold` keeps probabilities "
            "in memory. This is a documented gap, not a pending task, and no S3/S4 "
            "table is substituted for it."
        )
    st.caption(
        f"S5 scored {spatial['blocks_scored']} of {spatial['blocks_declared']} declared "
        f"held-out blocks; PR-AUC on held-out blocks "
        f"{spatial['full_point']['pr_auc']:.7f} against a bar of "
        f"{spatial['full_pr_auc_bar']:.7f}; paired "
        f"full − B3 {spatial['paired']['mean']:+.4f} "
        f"[{spatial['paired']['p2_5']:+.4f}, {spatial['paired']['p97_5']:+.4f}]."
    )

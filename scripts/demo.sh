#!/usr/bin/env bash
# Two-minute demo: publish the health summary, print the shot list, serve the app.
#
#   ./scripts/demo.sh            # http://127.0.0.1:8501
#   ./scripts/demo.sh --check    # verify prerequisites only, do not serve
#
# Requires the lake, the frozen prediction dump and the venv. See docs/demo-2min.md.
set -euo pipefail

cd "$(dirname "$0")/.."

DATA_DIR="${DELTA_CROPS_DATA:-data}"
PREDICTIONS="${DELTA_CROPS_PREDICTIONS:-artifacts/phase8/predictions/temporal_2024.parquet}"
PYTHON="${PYTHON:-.venv/bin/python}"
PORT="${PORT:-8501}"

fail() {
  printf 'demo: %s\n' "$1" >&2
  exit 1
}

[ -x "$PYTHON" ] || fail "no venv at $PYTHON; run: uv venv --python 3.12 .venv && uv pip install -e '.[dev,dashboard]'"
[ -d "$DATA_DIR/gold/weekly_ndvi" ] || fail "no Gold lake under $DATA_DIR; see README 'Get a data lake'"
[ -f "$PREDICTIONS" ] || fail "no prediction dump at $PREDICTIONS; run: python -m delta_crops.models.predictions"
[ -f "artifacts/phase7/xgb_temporal.json" ] || fail "no Phase 7 metrics under artifacts/phase7; see docs/model-card.md"

echo "== publish pipeline health =="
"$PYTHON" -m delta_crops.dashboard.health

cat <<'SHOTS'

== two-minute shot list (docs/demo-2min.md) ==
0:00  README architecture      "Sentinel-2 NDVI + Open-Meteo, six seasons, Bronze/Silver/Gold."
0:15  Map, 2024-07-15          "2,468 cropland blocks; empty blocks mean no observation."
0:40  Season, 2019-2024        "NDVI peaks in July-August; no visible heatwave response over six seasons."
1:05  Predictions              "Misses PR-AUC by 0.0008; ndvi_only beats full; 17 of 20 weeks."
1:40  Pipeline Health          "Quality and Gold gate green; S5 calibration is a documented gap."
1:55  Close                    "One command, tests, limitations written down."
SHOTS

echo "Open: http://127.0.0.1:${PORT}  (Ctrl-C to stop)"

if [ "${1:-}" = "--check" ]; then
  echo "demo: prerequisites OK (not serving)"
  exit 0
fi

exec "$PYTHON" -m delta_crops.dashboard --server.port "$PORT" --server.address 127.0.0.1

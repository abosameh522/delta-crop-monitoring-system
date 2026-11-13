#!/usr/bin/env bash
# Run the dashboard on the committed demo sample (about 5 x 5 km, 2024 only).
#
#   scripts/run_demo.sh                 # http://localhost:8501
#   scripts/run_demo.sh --server.port 8600
#
# Everything comes from demo_data/, so no local lake or model run is needed.
set -euo pipefail

cd "$(dirname "$0")/.."

export DELTA_CROPS_DATA="demo_data"
export DELTA_CROPS_PREDICTIONS="demo_data/artifacts/phase8/predictions/temporal_2024.parquet"
export DELTA_CROPS_ARTIFACTS="demo_data/artifacts/phase7"

PYTHON="${PYTHON:-.venv/bin/python}"
exec "$PYTHON" -m delta_crops.dashboard "$@"

#!/usr/bin/env bash
# Sequential Bronze backfill: one year at a time, logged, resumable.
#
#   tmux new-session -d -s bronze -c "$PWD" \
#     'systemd-inhibit --what=sleep:idle --why="Bronze backfill" \
#        bash scripts/backfill.sh 2>&1 | tee data/bronze/_logs/backfill.log'
#
# Re-running is safe: every completed asset is skipped by size + SHA-256.
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1
mkdir -p data/bronze/_logs

if [ "$#" -gt 0 ]; then
  years=("$@")
else
  years=(2019 2020 2021 2022 2023)
fi

failed=0
for y in "${years[@]}"; do
  echo "=== ingest $y $(date -Is) ==="
  if ! .venv/bin/python -m delta_crops.ingestion.cli ingest --year "$y" --workers 4 \
      2>&1 | tee "data/bronze/_logs/ingest_${y}.log"; then
    echo "FAILED: $y (re-run the same command to resume)"
    failed=1
  fi
done

.venv/bin/python -m delta_crops.ingestion.cli compact
echo "=== backfill finished $(date -Is) (failed=$failed) ==="
exit "$failed"

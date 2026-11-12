# Two-minute demo

A shot list for a screen recording, not a separate build. Everything shown is the real app reading
the real lake and the frozen Phase 7 artifacts. Numbers below are the ones on screen; if a number
changes, change it here only after re-reading the source artifact.

## Before recording (one terminal)

```bash
./scripts/demo.sh --check     # verifies lake, prediction dump, artifacts; publishes health
./scripts/demo.sh             # serves http://127.0.0.1:8501
```

Browser at 1440 × 900, zoom 100 %, devtools closed. Pre-load the app once so the first paint is warm.

## Shot list

| Time | Page / action | What is on screen | Narration |
| --- | --- | --- | --- |
| 0:00–0:15 | README architecture section | Mermaid data flow | “Sentinel-2 NDVI and Open-Meteo weather for six seasons in the Nile Delta, stored as Bronze, Silver and Gold Parquet, with quality gates between the layers.” |
| 0:15–0:40 | **Map** → NDVI, season 2024, week **2024-07-15**, cropland only | Green 1 km blocks | “2,468 cropland blocks, 1 km each. Values are means over `ok` cells only, so an empty block means no usable observation that week, not dead crops. The slider has all 20 Gold weeks.” |
| 0:40–1:05 | **Season** → all seasons | NDVI vs Tmax lines, heatwave bars | “NDVI peaks in July–August and every season tracks its temperature band. Over six seasons there is no visible heatwave response in this AOI — that is a description, not proof that heat has no effect.” |
| 1:05–1:40 | **Predictions** | Probability map, scoreboard, calibration | “Trained on 2019–2023, scored once on 2024. The headline model misses the pre-registered PR-AUC bar by 0.0008, and the NDVI-only variant beats the model with weather, so I claim no weather skill. It scores 17 weeks, not 20: the two-week target needs a future week.” |
| 1:40–1:55 | **Pipeline Health** | Quality checks, gate table, S5 warning | “Quality and the Gold gate are green. S5 calibration is unmeasured and shown as a documented gap, not hidden.” |
| 1:55–2:00 | Repo tree or terminal | `git log --oneline -5` | “One command to run, a test suite that pins the README numbers to the artifacts, and every limitation written down.” |

## Do not say

- “Predicts crop failure” — the model predicts an NDVI-anomaly threshold.
- “Detects heatwave damage” — no effect is demonstrated.
- “Production ready”, “scheduled end-to-end”, “runs itself” — the schedule has never run on real data.
- “Validated against yields” — there is no yield data in the project.

## What to show if asked for depth

- `docs/limitations.md` — every open item, with evidence.
- `docs/model-card.md` — calibration, S5, the near-miss.
- `scripts/readme_results.py --check` — the README table is generated from artifacts.
- `pytest -q` — the suite that enforces the above.

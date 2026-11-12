#!/usr/bin/env python3
"""Render the README results table from the Phase 7 artifacts.

The README must never carry a hand-typed metric. This script reads
``xgb_temporal.json``, ``xgb_loso.json``, ``xgb_spatial.json`` and the B3
reference in ``baselines_metrics.json``, and writes the table between two HTML
markers in README.md. ``--check`` exits non-zero if the committed block differs,
so a stale number fails a test instead of shipping.

    python scripts/readme_results.py            # rewrite the block
    python scripts/readme_results.py --check    # verify, non-zero on drift
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

MARKER_START = "<!-- results:start -->"
MARKER_END = "<!-- results:end -->"
DEFAULT_ARTIFACTS = Path("artifacts/phase7")
DEFAULT_README = Path("README.md")
PR_AUC_MARGIN = 0.05
METRICS = ("pr_auc", "precision_at_recall_0_5", "brier")
HEADERS = ("Campaign", "Variant", "PR-AUC", "PR-AUC bar", "P@R.5", "P@R.5 bar",
           "Brier", "Brier bar", "Verdict")


@dataclass(frozen=True)
class Row:
    """One campaign/variant result exactly as the artifact stores it."""

    campaign: str
    variant: str
    metrics: dict[str, float]
    bars: dict[str, float]

    def passed(self, metric: str) -> bool:
        if metric == "pr_auc":
            return self.metrics[metric] >= self.bars[metric]
        if metric == "precision_at_recall_0_5":
            return self.metrics[metric] > self.bars[metric]
        return self.metrics[metric] < self.bars[metric]

    @property
    def verdict(self) -> str:
        failed = [name for name in METRICS if not self.passed(name)]
        if not failed:
            return "**pass**"
        if failed == ["pr_auc"]:
            return "fail PR-AUC only"
        return "fail " + ", ".join(failed)


def _load(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(f"missing artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _bars(b3: dict[str, float]) -> dict[str, float]:
    """The pre-registered rule: B3 + 0.05 PR-AUC, strictly better, strictly lower."""
    return {
        "pr_auc": b3["pr_auc"] + PR_AUC_MARGIN,
        "precision_at_recall_0_5": b3["precision_at_recall_0_5"],
        "brier": b3["brier"],
    }


def load_results(artifacts: Path = DEFAULT_ARTIFACTS) -> dict[str, Any]:
    """Read the three campaigns plus the reference needed for S3's bars."""
    temporal = _load(artifacts / "xgb_temporal.json")
    loso = _load(artifacts / "xgb_loso.json")
    spatial = _load(artifacts / "xgb_spatial.json")
    baselines = _load(artifacts / "baselines_metrics.json")

    b3 = baselines["reports"][0]["groups"]["cropland/pooled"]["models"]["B3"]
    s3_bars = _bars(b3)
    rows: list[Row] = []

    for variant in ("full", "ndvi_only"):
        entry = temporal["variants"][variant]
        group = entry["groups"]["cropland/pooled"]
        model = group["models"]["M"]
        rows.append(Row(
            campaign="S3 temporal (2024)",
            variant=variant,
            metrics={name: float(model[name]) for name in METRICS},
            bars=s3_bars,
        ))

    loso_variants = loso["summary"]["verdict"]["variants"]
    for variant in ("full", "ndvi_only"):
        entry = loso_variants[variant]
        rows.append(Row(
            campaign="S4 leave-one-season-out",
            variant=variant,
            metrics={name: float(entry[name]) for name in METRICS},
            bars={
                "pr_auc": float(entry["pr_auc_bar"]),
                "precision_at_recall_0_5": float(entry["p_at_r_5_bar"]),
                "brier": float(entry["brier_bar"]),
            },
        ))

    spatial_variants = spatial["summary"]["variants"]
    for variant in ("full", "ndvi_only"):
        entry = spatial_variants[variant]
        rows.append(Row(
            campaign="S5 held-out 5 km blocks",
            variant=variant,
            metrics={name: float(entry["point"][name]) for name in METRICS},
            bars={
                "pr_auc": float(entry["pr_auc_bar"]),
                "precision_at_recall_0_5": float(entry["p_at_r_5_bar"]),
                "brier": float(entry["brier_bar"]),
            },
        ))

    return {
        "rows": rows,
        "folds": len(loso["summary"]["folds"]),
        "s3_rows": int(temporal["variants"]["full"]["groups"]["cropland/pooled"]["rows"]),
        "s3_positives": int(temporal["variants"]["full"]["groups"]["cropland/pooled"]["positives"]),
        "blocks_scored": len(spatial["per_block_cropland_pooled"]),
        "blocks_declared": int(spatial["partition"]["n_heldout_blocks"]),
        "source": "S3 and S4 PR-AUC bars are the campaign's B3 + 0.05; P@R.5 and Brier "
                  "must be strictly better.",
    }


def render_table(results: dict[str, Any]) -> str:
    """Markdown table plus the population lines, all derived from artifacts."""
    lines = ["| " + " | ".join(HEADERS) + " |",
             "| " + " | ".join("---" for _ in HEADERS) + " |"]
    for row in results["rows"]:
        values = [row.campaign, f"`{row.variant}`"]
        for metric in METRICS:
            values.append(f"{row.metrics[metric]:.7f}")
            values.append(f"{row.bars[metric]:.7f}")
        values.append(row.verdict)
        lines.append("| " + " | ".join(values) + " |")
    lines.append("")
    lines.append(
        f"Populations: S3 scores {results['s3_rows']:,} cropland cell-weeks "
        f"({results['s3_positives']:,} positives) once on 2024; S4 averages "
        f"{results['folds']} held-out seasons; S5 scores {results['blocks_scored']} of "
        f"{results['blocks_declared']} declared held-out 5 km blocks. "
        + results["source"]
    )
    return "\n".join(lines)


def render_block(artifacts: Path = DEFAULT_ARTIFACTS) -> str:
    """The full marked block, byte-stable for identical artifacts."""
    table = render_table(load_results(artifacts))
    return f"{MARKER_START}\n{table}\n{MARKER_END}"


def extract_block(readme: str) -> str:
    """The committed block, or raise if the markers are missing or duplicated."""
    if readme.count(MARKER_START) != 1 or readme.count(MARKER_END) != 1:
        raise ValueError("README must contain exactly one results marker pair")
    start = readme.index(MARKER_START)
    end = readme.index(MARKER_END) + len(MARKER_END)
    return readme[start:end]


def update_readme(readme_path: Path, artifacts: Path) -> bool:
    """Rewrite the block in place; return whether the file changed."""
    readme = readme_path.read_text(encoding="utf-8")
    block = render_block(artifacts)
    if extract_block(readme) == block:
        return False
    updated = readme.replace(extract_block(readme), block)
    readme_path.write_text(updated, encoding="utf-8")
    return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifacts", type=Path, default=DEFAULT_ARTIFACTS)
    parser.add_argument("--readme", type=Path, default=DEFAULT_README)
    parser.add_argument("--check", action="store_true",
                        help="verify instead of writing; non-zero when stale")
    args = parser.parse_args(argv)

    if args.check:
        committed = extract_block(args.readme.read_text(encoding="utf-8"))
        expected = render_block(args.artifacts)
        if committed != expected:
            print("README results block is stale; run scripts/readme_results.py",
                  file=sys.stderr)
            return 1
        print("README results block matches the artifacts")
        return 0

    changed = update_readme(args.readme, args.artifacts)
    print(f"{args.readme}: {'updated' if changed else 'already current'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

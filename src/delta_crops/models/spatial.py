"""S5: spatial holdout on 5 km blocks and block-bootstrap intervals.

The 5 km block is ``(cell_row // 50) * 10000 + (cell_col // 50)``, which the
dataset builder already emits as ``block_5km``. A held-out block contributes
**no rows of any season** to fitting, early stopping or refit, so the model is
scored on locations it never saw. Everything here is frozen in
``docs/phase7-preregistration.md`` (section 5.5).

Per-cell climatology still comes from each held-out cell's own history: this
tests new locations *with known history*, not a location with no history.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

import numpy as np

#: Seeded permutation of the sorted block ids, frozen before any S5 fit.
SPATIAL_SEED = 20260702
#: Seeded bootstrap over the held-out blocks, frozen before any S5 fit.
BOOTSTRAP_SEED = 20260703
#: 28 of the 110 blocks (25.45 %) are held out.
HELDOUT_FRACTION = 0.25
#: Frozen number of block-bootstrap resamples.
BOOTSTRAP_DRAWS = 1000
#: Frozen percentile interval.
INTERVAL = (2.5, 97.5)
TARGET_RECALL = 0.5


@dataclass(frozen=True)
class BlockPartition:
    """The frozen train/held-out split of the 5 km block ids."""

    train_blocks: tuple[int, ...]
    held_out_blocks: tuple[int, ...]

    def __post_init__(self) -> None:
        if not self.train_blocks or not self.held_out_blocks:
            raise ValueError("the spatial partition needs both sides non-empty")
        overlap = set(self.train_blocks) & set(self.held_out_blocks)
        if overlap:
            raise ValueError(f"blocks on both sides of the partition: {sorted(overlap)}")

    @property
    def all_blocks(self) -> tuple[int, ...]:
        return tuple(sorted((*self.train_blocks, *self.held_out_blocks)))

    def as_dict(self) -> dict[str, Any]:
        return {
            "seed": SPATIAL_SEED,
            "heldout_fraction": HELDOUT_FRACTION,
            "n_train_blocks": len(self.train_blocks),
            "n_heldout_blocks": len(self.held_out_blocks),
            "heldout_share": len(self.held_out_blocks) / len(self.all_blocks),
            "train_blocks": list(self.train_blocks),
            "heldout_blocks": list(self.held_out_blocks),
        }


def all_block_ids(root: Path, cells_path: Path | None = None) -> list[int]:
    """Every 5 km block in the AOI, whether or not it has model rows.

    The frozen partition is defined over the AOI's block lattice (110 blocks),
    not over whichever blocks happen to have rows in one split, so the held-out
    set cannot shift with the data. Reads only ``silver/cells.parquet``.
    """
    import duckdb

    path = cells_path or (root / "silver" / "cells.parquet")
    con = duckdb.connect()
    try:
        rows = con.sql(
            "SELECT DISTINCT (CAST(cell_row AS INTEGER) // 50) * 10000 "
            "+ (CAST(cell_col AS INTEGER) // 50) AS block FROM read_parquet(?)",
            params=[str(path)],
        ).fetchall()
    finally:
        con.close()
    return sorted(int(row[0]) for row in rows)


def partition_blocks(block_ids: Sequence[int], seed: int = SPATIAL_SEED) -> BlockPartition:
    """Hold out the first ``ceil(0.25 * n)`` blocks of a seeded permutation.

    Pure function of the id set and the seed: no result can influence which
    blocks are held out, and the partition is reproducible from the ids alone.
    """
    unique = tuple(sorted({int(block) for block in block_ids}))
    if len(unique) < 4:
        raise ValueError("need at least four 5 km blocks to hold any out")
    order = np.random.default_rng(seed).permutation(len(unique))
    n_held = int(np.ceil(HELDOUT_FRACTION * len(unique)))
    held = {unique[index] for index in order[:n_held]}
    train = tuple(block for block in unique if block not in held)
    held_out = tuple(block for block in unique if block in held)
    if set(train) & set(held_out):
        raise AssertionError("a block appears on both sides of the spatial split")
    if not train or not held_out:
        raise AssertionError("the spatial split left one side empty")
    return BlockPartition(train_blocks=train, held_out_blocks=held_out)


def spatial_frames(all_rows, partition: BlockPartition, test_year: int):
    """Filter the built rows to the S5 population and verify the split is clean.

    Training rows are the training-block rows of the training seasons; test rows
    are the held-out-block rows of ``test_year``. The partition invariant is
    checked before filtering (a block on both sides is exactly the spatial leak
    this run exists to rule out), and the resulting frames are re-checked after.
    """
    held = set(partition.held_out_blocks)
    train_blocks = set(partition.train_blocks)
    if held & train_blocks:
        raise ValueError(
            f"blocks on both sides of the partition: {sorted(held & train_blocks)}")
    if not held or not train_blocks:
        raise ValueError("the spatial partition left one side empty")

    is_test_season = all_rows["year"].to_numpy() == int(test_year)
    block = all_rows["block_5km"].to_numpy()
    in_held = np.isin(block, list(held))

    train = all_rows[~is_test_season & ~in_held].reset_index(drop=True)
    test = all_rows[is_test_season & in_held].reset_index(drop=True)
    if not len(train) or not len(test):
        raise ValueError("the spatial split left one side empty")

    leaked_train = set(train["block_5km"].unique()) & held
    if leaked_train:
        raise ValueError(f"held-out blocks on the training side: {sorted(leaked_train)}")
    leaked_test = set(test["block_5km"].unique()) & train_blocks
    if leaked_test:
        raise ValueError(f"training blocks on the test side: {sorted(leaked_test)}")
    if test_year in set(train["year"].unique()):
        raise ValueError(f"test season {test_year} appears in the training rows")
    if set(test["year"].unique()) != {int(test_year)}:
        raise ValueError(f"test rows are not exactly {test_year}")
    return train, test


def _pr_metrics(labels: np.ndarray, scores: np.ndarray) -> tuple[float | None, float | None]:
    """PR-AUC and P@R.5 from a single sort of the scores."""
    y = np.asarray(labels, dtype=np.int64)
    if y.sum() == 0:
        return None, None
    scores = np.asarray(scores, dtype=np.float64)
    order = np.argsort(-scores, kind="mergesort")
    y = y[order]
    sorted_scores = scores[order]
    cumulative = np.cumsum(y)
    last = np.r_[np.flatnonzero(np.diff(sorted_scores)), len(y) - 1]
    true_positive = cumulative[last]
    seen = last + 1
    precision = true_positive / np.maximum(seen, 1)
    total_positives = int(true_positive[-1])
    recall = true_positive / total_positives
    ap = float(np.sum(np.diff(np.r_[0.0, recall]) * precision))
    index = int(np.searchsorted(recall, TARGET_RECALL, side="left"))
    p_at_recall = float(precision[index]) if index < len(precision) else None
    return ap, p_at_recall


def per_block_metrics(test, prediction: np.ndarray, mask: np.ndarray) -> list[dict[str, Any]]:
    """PR-AUC inside each held-out block; unscorable blocks are listed, not dropped."""
    rows: list[dict[str, Any]] = []
    block = test["block_5km"].to_numpy()
    label = test["label"].to_numpy(dtype=np.int64)
    for block_id in sorted({int(value) for value in block[mask]}):
        inside = mask & (block == block_id)
        y = label[inside]
        positives = int(y.sum())
        entry: dict[str, Any] = {
            "block": block_id,
            "rows": int(len(y)),
            "positives": positives,
            "prevalence": float(y.mean()) if len(y) else None,
        }
        if 0 < positives < len(y):
            ap, p_at_recall = _pr_metrics(y, prediction[inside])
            entry["pr_auc"] = ap
            entry["precision_at_recall_0_5"] = p_at_recall
            entry["scorable"] = True
        else:
            entry["pr_auc"] = None
            entry["precision_at_recall_0_5"] = None
            entry["scorable"] = False
        rows.append(entry)
    return rows


def _interval(values: list[float]) -> dict[str, float | None] | None:
    clean = [value for value in values if value is not None]
    if not clean:
        return None
    low, high = np.percentile(clean, INTERVAL)
    return {
        "mean": float(np.mean(clean)),
        "std": float(np.std(clean, ddof=1)) if len(clean) > 1 else 0.0,
        "p2_5": float(low),
        "p97_5": float(high),
        "draws": len(clean),
    }


def block_bootstrap(test, predictions: dict[str, np.ndarray], mask: np.ndarray,
                    draws: int = BOOTSTRAP_DRAWS, seed: int = BOOTSTRAP_SEED,
                    blocks: Sequence[int] | None = None) -> dict[str, Any]:
    """Resample whole blocks and recompute the frozen metrics per draw.

    Rows inside a 5 km block are strongly correlated, so the resampling unit is
    the block, never the row. Every drawn set scores all models, so the
    ``M - B3`` difference is paired.
    """
    block = test["block_5km"].to_numpy()
    label = test["label"].to_numpy(dtype=np.int64)
    selected = mask.copy()
    usable_blocks = sorted({int(value) for value in block[selected]})
    if blocks is not None:
        usable_blocks = [int(b) for b in blocks]
    if len(usable_blocks) < 2:
        raise ValueError("block bootstrap needs at least two held-out blocks")

    # Precompute each block's row indices once; a draw is then a concatenation.
    index_by_block = {b: np.flatnonzero(selected & (block == b)) for b in usable_blocks}
    rng = np.random.default_rng(seed)
    collected: dict[str, dict[str, list[float]]] = {
        name: {"pr_auc": [], "precision_at_recall_0_5": [], "brier": []}
        for name in predictions
    }
    differences: dict[str, list[float]] = {"pr_auc": [], "precision_at_recall_0_5": [],
                                           "brier": []}
    skipped = 0
    for _ in range(draws):
        chosen = rng.choice(len(usable_blocks), size=len(usable_blocks), replace=True)
        pieces = [index_by_block[usable_blocks[i]] for i in chosen]
        index = np.concatenate(pieces)
        y = label[index]
        if y.sum() == 0 or y.sum() == len(y):
            skipped += 1
            continue
        draw: dict[str, dict[str, float | None]] = {}
        for name, prediction in predictions.items():
            p = prediction[index]
            ap, p_at_recall = _pr_metrics(y, p)
            brier = float(np.mean((y - p) ** 2))
            draw[name] = {"pr_auc": ap, "precision_at_recall_0_5": p_at_recall,
                          "brier": brier}
            if ap is not None:
                collected[name]["pr_auc"].append(ap)
            if p_at_recall is not None:
                collected[name]["precision_at_recall_0_5"].append(p_at_recall)
            collected[name]["brier"].append(brier)
        if "full" in draw and "B3" in draw:
            for metric in differences:
                left, right = draw["full"][metric], draw["B3"][metric]
                if left is not None and right is not None:
                    differences[metric].append(float(left - right))
    return {
        "draws_requested": int(draws),
        "draws_usable": int(draws - skipped),
        "draws_skipped_single_class": int(skipped),
        "blocks": usable_blocks,
        "metrics": {name: {metric: _interval(values) for metric, values in by_metric.items()}
                    for name, by_metric in collected.items()},
        "paired_difference_full_minus_B3": {
            metric: _interval(values) for metric, values in differences.items()
        },
    }

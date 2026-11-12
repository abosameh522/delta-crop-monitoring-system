"""S5 tests: the 5 km block partition, the spatial filter and the block bootstrap."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from delta_crops.models.spatial import (
    BOOTSTRAP_DRAWS,
    HELDOUT_FRACTION,
    BlockPartition,
    block_bootstrap,
    partition_blocks,
    spatial_frames,
)


def _blocks(n: int = 110) -> list[int]:
    return [(row * 10000) + col for row in range(n // 10) for col in range(n % 10 or 10)]


def test_partition_holds_out_a_quarter_and_is_a_pure_function_of_the_ids() -> None:
    blocks = _blocks(110)
    partition = partition_blocks(blocks)
    assert len(partition.all_blocks) == 110
    assert len(partition.held_out_blocks) == 28
    assert len(partition.train_blocks) == 82
    assert abs(len(partition.held_out_blocks) / 110 - HELDOUT_FRACTION) < 0.01

    # Same ids in any order and any repetition -> identical partition.
    shuffled = list(reversed(blocks + blocks))
    again = partition_blocks(shuffled)
    assert again == partition
    # A different seed draws a different set, but the same sizes.
    other = partition_blocks(blocks, seed=1)
    assert other.held_out_blocks != partition.held_out_blocks
    assert len(other.held_out_blocks) == len(partition.held_out_blocks)


def test_partition_refuses_a_set_too_small_to_hold_anything_out() -> None:
    with pytest.raises(ValueError):
        partition_blocks([1, 2, 3])


def _frame(years: list[int], blocks: list[int]) -> pd.DataFrame:
    return pd.DataFrame({
        "year": years,
        "block_5km": blocks,
        "label": [0, 1] * (len(years) // 2),
    })


def test_spatial_frames_put_every_held_out_block_row_on_the_test_side() -> None:
    partition = BlockPartition(train_blocks=(2, 4), held_out_blocks=(1, 3))
    rows = _frame(
        years=[2019] * 4 + [2020] * 4 + [2024] * 4,
        blocks=[1, 2, 3, 4] * 3,
    )
    train, test = spatial_frames(rows, partition, 2024)
    assert set(train["year"]) == {2019, 2020}
    assert set(test["year"]) == {2024}
    assert set(train["block_5km"]) == {2, 4}
    assert set(test["block_5km"]) == {1, 3}
    # No held-out block appears in training for any season.
    assert not set(train["block_5km"]) & {1, 3}


def test_spatial_frames_drop_a_held_out_block_from_every_training_season() -> None:
    partition = BlockPartition(train_blocks=(2, 4), held_out_blocks=(1, 3))
    # Held-out block 1 is present in 2019 too; it must not reach the training side.
    rows = pd.DataFrame({
        "year": [2019, 2019, 2020, 2024, 2024],
        "block_5km": [1, 2, 1, 1, 3],
        "label": [0, 1, 0, 1, 0],
    })
    train, test = spatial_frames(rows, partition, 2024)
    assert set(train["block_5km"]) == {2}
    assert set(test["block_5km"]) == {1, 3}
    assert 1 not in set(train["block_5km"])


def test_a_partition_cannot_put_a_block_on_both_sides() -> None:
    with pytest.raises(ValueError, match="both sides"):
        BlockPartition(train_blocks=(1, 2), held_out_blocks=(2, 3))
    with pytest.raises(ValueError):
        BlockPartition(train_blocks=(), held_out_blocks=(1,))


def test_spatial_frames_refuse_a_test_side_that_is_not_the_held_out_season() -> None:
    partition = BlockPartition(train_blocks=(2, 4), held_out_blocks=(1, 3))
    rows = pd.DataFrame({
        "year": [2019, 2024, 2023],
        "block_5km": [2, 1, 3],
        "label": [0, 1, 0],
    })
    # 2023 is a held-out block but not the test season -> it must not be scored.
    train, test = spatial_frames(rows, partition, 2024)
    assert set(test["year"]) == {2024}
    assert set(train["year"]) == {2019}


def _bootstrap_frame(n_blocks: int = 6, rows: int = 40) -> pd.DataFrame:
    rng = np.random.default_rng(0)
    block = np.repeat(np.arange(n_blocks), rows)
    signal = (block % 3).astype(float) / 3.0
    label = (rng.random(len(block)) < 0.2 + 0.5 * signal).astype(np.int64)
    return pd.DataFrame({"block_5km": block, "label": label})


def test_block_bootstrap_resamples_whole_blocks_and_reports_paired_differences() -> None:
    frame = _bootstrap_frame()
    truth = frame["label"].to_numpy(dtype=float)
    rng_noise = np.random.default_rng(11).random(len(frame))
    # M ranks on the real signal, B3 on block id only: the paired gain is real.
    prediction = {"full": truth + rng_noise * 0.05,
                  "B3": (frame["block_5km"].to_numpy(dtype=float) % 3)}
    mask = np.ones(len(frame), dtype=bool)
    result = block_bootstrap(frame, prediction, mask, draws=50, seed=7)
    assert result["draws_requested"] == 50
    assert result["draws_usable"] + result["draws_skipped_single_class"] == 50
    assert result["blocks"] == list(range(6))
    for name in ("full", "B3"):
        interval = result["metrics"][name]["pr_auc"]
        assert interval is not None
        assert interval["p2_5"] <= interval["mean"] <= interval["p97_5"]
    paired = result["paired_difference_full_minus_B3"]["pr_auc"]
    assert paired is not None
    # M uses the signal far more strongly than B3, so the paired gain is positive.
    assert paired["mean"] > 0


def test_block_bootstrap_uses_a_frozen_draw_count_and_resamples_blocks_not_rows() -> None:
    frame = _bootstrap_frame(n_blocks=4, rows=10)
    prediction = {"full": frame["label"].to_numpy(dtype=float),
                  "B3": frame["label"].to_numpy(dtype=float) * 0.5}
    mask = np.ones(len(frame), dtype=bool)
    result = block_bootstrap(frame, prediction, mask)
    assert result["draws_requested"] == BOOTSTRAP_DRAWS
    # Each draw repeats each of the 4 blocks once on average, so a draw has
    # roughly the full row count -- never a single block's worth.
    assert len(result["blocks"]) == 4
    with pytest.raises(ValueError):
        block_bootstrap(frame.head(5), {"full": np.zeros(5)}, mask[:5])

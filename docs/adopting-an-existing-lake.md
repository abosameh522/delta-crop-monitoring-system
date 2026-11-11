# Adopting an existing lake

`--adopt` writes fingerprint sidecars next to Silver and Gold outputs that were
produced before fingerprints existed. It does not rebuild or rewrite any
Parquet. This page records the procedure and the measured result on the real
lake (2026-11-11).

## What adoption claims

- Silver: every Bronze asset for the date is present in the ledger, has the
  logged size, and matches its ledger SHA-256. Verification is forced even with
  `--no-verify-checksum`.
- Gold: the Silver partitions and the weather file that feed the output are the
  current ones (Silver file name, size and `mtime_ns`; weather SHA-256).
- It does **not** prove that the legacy output was originally produced from
  those inputs. Gold adoption also does not check the content (row count) of the
  existing Parquet; `scripts/gold_regression.py` is that independent check and
  must run before adopting.

## Measured result (real lake, 2026-11-11)

Before touching anything, Gold regression on the existing lake:

```
15/15 OK, 0 diffs
weekly_ndvi and ndvi_anomaly: 4,786,080 rows per year
weekly_weather: 20 rows per year
total 44.93 s
```

A `/tmp/adopt_rehearsal` copy was adopted first (Silver 2019–2023 copied, real
Bronze symlinked, real Gold copied). At that point the real lake had 0
fingerprint sidecars and the ledger had 1727 lines.

Silver, real lake:

```bash
python -m delta_crops.transform.cli --config config/aoi.yaml --data-dir data all --adopt
```

```
0 built, 0 skipped, 0 rebuilt (input change), 0 unfingerprinted, 293 adopted, 0 failed in 23.3s (peak RSS 183 MB)
```

Exit 0. Per-year sidecars: 2019: 59, 2020: 59, 2021: 60, 2022: 59, 2023: 56.
The 23.3 s was measured on a warm page cache; there is no cold-disk timing.

Gold, real lake, in this order, no `--year`, no `--force`:

```bash
python -m delta_crops.gold.cli --config config/aoi.yaml --data-dir data weather --adopt
python -m delta_crops.gold.cli --config config/aoi.yaml --data-dir data weekly --adopt
python -m delta_crops.gold.cli --config config/aoi.yaml --data-dir data anomaly --adopt
```

Each printed summary: `errors []`, `built 0`, `skipped 0`, `unfingerprinted 0`,
`adopted 5`, `failed 0`. Total Gold sidecars: 15.

After the run, a SHA-256 over the sorted list of all **non-sidecar** files was
identical before and after for `data/silver` and for `data/gold`. Sidecars: 293
Silver, 15 Gold. The ledger was still 1727 lines and `git status` was clean.

## Procedure

1. Run the independent check first: `python scripts/gold_regression.py`
   (expect `15/15 OK`, 0 diffs).
2. Rehearse on a copy in `/tmp` (Silver copied, real Bronze symlinked read-only,
   Gold copied). Never adopt straight onto the real lake first.
3. Silver: `transform.cli ... all --adopt` (or `year --year Y --adopt`).
4. Gold: `weather`, then `weekly`, then `anomaly`. No `--year`, no `--force`,
   and do not use `gold.cli build --adopt` (it also runs the gate and may build
   the orbit cache).
5. Read the printed summary counts, not the exit code. Expected: `built 0`,
   `adopted 293` (Silver) and `adopted 5` per Gold table, `failed 0`.
6. Verify: counts of `*.fingerprint.json` (293 Silver, 15 Gold) and a
   before/after SHA-256 of the non-sidecar files.

## Undo

Only sidecars are new:

```bash
find data/silver -name '*.fingerprint.json' -delete
find data/gold   -name '*.fingerprint.json' -delete
```

Nothing else changes; every output then reads as "unfingerprinted" again.

## Known limitations (not fixed yet)

- `gold.cli weather` ignores `--year`: `cmd_weather` never reads `args.year`
  (`src/delta_crops/gold/cli.py:98-100`) and `build_weekly_weather` loops over
  `config.seasons.years` (`src/delta_crops/gold/weather.py:67,81`). In the
  rehearsal, `weather --year 2019 --adopt` **built** the four missing years
  2020–2023 (byte-identical to the real ones) instead of adopting them. On a
  lake where an output file is missing, `weather --adopt` builds it.
- `--summary-json` writes a file only in `gold.cli build`; `weather`, `weekly`
  and `anomaly` accept the flag but write nothing. Read the printed JSON.
- Gold commands exit 0 even when `failed > 0`; read the summary counts.
- `--adopt` with `--force` rebuilds instead of adopting — never combine them.
  `gold.cli build --adopt` additionally writes a weekly gate report and may
  build the orbit cache (network) if `silver/_cache/orbits.parquet` is missing.
- Gold fingerprints depend on Silver file mtime and size. Copying, touching or
  rewriting `data/silver` after adoption makes Gold look like "input changed".
  Do not copy `data/silver` in place; use a temp copy for experiments.
- Silver adoption hashes every Bronze asset it fingerprints. Keep it forced
  even with `--no-verify-checksum` (already the behaviour).

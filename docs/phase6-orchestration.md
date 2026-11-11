# Phase 6a orchestration

Phase 6a adds deterministic orchestration contracts without Docker packaging.
The scheduled flow processes only an explicitly configured in-progress year; with no in-progress year it records a logged no-op. Complete years require an explicit manual reprocess flag. Exit codes are 0 success, 1 unexpected, 2 argparse usage, 3 transient, 4 asset/date, 10 critical quality, and 11 Gold weekly-gate failure. Only code 3 retries with exponential backoff.

Gold uses a per-year `as_of`: season end for complete seasons, otherwise the minimum of latest ingested acquisition and latest weather date. Weeks whose week end is after `as_of` are `not_yet_observed`.

Regression: `.venv/bin/python scripts/gold_regression.py` runs DuckDB EXCEPT in both directions and row counts against `data/_regression/gold_before`. Slow pytest: `.venv/bin/pytest -q -m slow`.

Docker packaging, Docker Compose, and container memory measurements remain Phase 6b.


## Verification correction — 2026-11-11

Phase 6a is NOT DONE. The former completion claim is withdrawn. The existing
flow contains only scheduled_noop and scheduled_flow, not four pipeline tasks.
Gold fingerprints, dynamic per-year as_of, partial-weather pipeline integration,
asset retry/atomic raster output, and try/finally pipeline failure logging are NOT DONE.
Three strict xfails explicitly identify missing Gold fingerprint/retry work;
helper tests are not evidence of wired pipeline behavior.

Silver fingerprints are wired to build and CLI. An empty-assets placeholder was
causing every comparison to differ; fixed to use the chosen ledger assets.
Temporary fixture evidence: first build two dates -> built=2; unchanged date
skips, changed date -> rebuilt_due_to_input_change=1, skipped=1. Six related tests
pass. Legacy files are still reported unfingerprinted unless explicitly adopted;
current-input adoption does not establish original production provenance.

Default pytest: 93 passed, 1 deselected, 3 xfailed in 4.87s. All collected: 97.
Slow: `.venv/bin/python -m pytest -q -m slow -o addopts='' -s` ->
1 passed, 96 deselected in 49.41s. Gold rebuilt in 41.67s; comparisons total
48.91s. Every table/year matched row counts and both EXCEPT directions (0).
The script now roots all rebuilt outputs in TemporaryDirectory with read-only
Silver input access. Anomalies read the rebuilt weekly tables, not existing Gold.

Local concurrency proof used no Docker and no project data. Commands:

```bash
PREFECT_HOME=/tmp/delta_phase6a_prefect prefect server start --host 127.0.0.1 --port 8420
PREFECT_HOME=/tmp/delta_phase6a_prefect PREFECT_API_URL=http://127.0.0.1:8420/api python spike.py
```

The one-task `spike` flow is served using
`spike.serve(name='local-proof', global_limit=1, limit=2)`; two API run creation
calls were submitted with asyncio.gather. API deployment metadata confirmed
`global_concurrency_limit.limit=1`. The first serve attempt preceded server
readiness and failed with `httpx.ConnectError: All connection attempts failed`;
it succeeded after the server became ready. The queued second run was initially
rescheduled by Prefect, then completed normally. UTC timestamps:

```text
A START 2026-11-11T02:14:44.456091+00:00
A END 2026-11-11T02:14:47.456425+00:00
B START 2026-11-11T02:15:14.854044+00:00
B END 2026-11-11T02:15:17.854678+00:00
```

This proves local deployment serialization only, not a working product pipeline.
Server and runner are stopped after verification; Docker remains Phase 6b.


## Step 4 acceptance evidence

Normal builds with a valid existing output and missing fingerprint report
`unfingerprinted=1, built=0, skipped=0, rebuilt_due_to_input_change=0` and return
before reading Bronze assets. `skipped` counts matching-fingerprint skips;
`unfingerprinted` is a separate skip reason. `--force` explicitly rebuilds.

`--adopt` verifies selected asset presence, size and SHA-256; it requires every
asset to have a ledger SHA-256 and enforces verification even if the checksum
bypass flag was supplied. Only the sidecar fingerprint is written. Temporary
fixture SHA-256 before/after:
`c2e2f78eb869ae87008c8ea0a76ae653017d048a0cb501d4de12e0fa4841309e`.
A same-size corrupt input is rejected without a sidecar or Parquet modification.
This verifies current inputs, not proof that legacy output was built from them.
No adoption or verification ran on real Bronze/Silver in this step.


## Step 7 — asset retry and atomic Bronze writes

Only `rasterio.errors.RasterioIOError` is retried. Each asset gets
`ingestion.asset_attempts` attempts (default 3) with exponential backoff based
on `ingestion.retry_base_delay_s` (default 1.0 s; tests pass 0). The CLI exposes
`--asset-retries` and `--retry-delay`.

`CRSError`, `OSError` from a write, and every other asset-scoped exception are
NOT retried and are NOT `TransientFailure`. They are recorded as a failed asset
and the run finishes the remaining assets, then raises `AssetFailure` (exit 4).
Errors outside the asset boundary (for example a ledger write failure) propagate
as unexpected (exit 1). STAC search failures remain `requests.RequestException`
-> `TransientFailure` (exit 3).

Nested retry budget: an asset can be attempted at most 3 (asset) x 3 (task) = 9
times. `AssetFailure` (exit 4) is itself NEVER retried by the task.

Bronze writes go to `<band>.tif.tmp` with an explicit GDAL driver (`COG`, a
GTiff container, as decided in Phase 2), then `os.replace` to the final path.
The tmp file is deleted on any exception. The ledger line is appended only
after `os.replace` succeeds, and its byte size and SHA-256 are computed from the
final file. A crash therefore cannot leave a visible partial raster or a ledger
line without a file.

Scheduled idempotency reads the JSONL ledger and checks the final file size
(`verify_checksum=False`), so an already-logged asset causes zero downloads and
zero re-hashing. `--no-checksum` selects this mode; the default still verifies
SHA-256.

Evidence tests (mock `read_window`/`write_cog`, tmp fixtures only):
RasterioIOError twice then success -> 1 asset, 3 read calls, 1 ledger line;
always RasterioIOError -> 3 calls, no final/tmp/ledger, failed summary printed;
one failed asset does not stop others and CLI `main()` returns 4; write
exception leaves no tmp/ledger; CRSError -> 1 call; scheduled re-run -> 0 reads
and 0 downloads.

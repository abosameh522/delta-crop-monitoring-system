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


## Step 8 — the real four-task flow

`delta_crops.orchestration.flow` holds the Prefect 3 flow and the retry
contract; `delta_crops.orchestration.pipeline` holds the four tasks and the
scheduled entry logic. Tasks call each CLI `main()` **in-process**. In-process
calls keep the stable integer exit contract, avoid a Python interpreter start per
task, and let tests monkeypatch the CLI mains directly.

### Retry mapping

| CLI exit | Meaning | Task behaviour |
| --- | --- | --- |
| 0 | success | continue |
| 3 | transient (`TransientFailure`) | raise `TransientError`; Prefect retries |
| 4 | asset / Silver date failure | `PipelineError`; no retry |
| 10 | critical quality failure | `PipelineError`; no retry |
| 11 | Gold weekly gate failure | `PipelineError`; no retry |
| 1 / other | unexpected | `PipelineError`; no retry |

`retry_codes()` returns exactly `{3}`. Retries use Prefect 3.9.2's verified
`retry_condition_fn(task, task_run, state)`: the condition checks
`state.data` for `TransientError`. Defaults are 3 retries with 1/2/4 s
exponential delays (`ingestion`-style backoff is separate); tests pass
`retry_delays=[0, 0, 0]`. The quality task is `retries=0` unconditionally.

Task order is ingest -> Silver -> quality -> Gold. A raised task stops the flow,
so Silver failure skips quality and Gold, and quality failure skips Gold.

### Scheduled selection and no-op

`in_progress_years()` reads `gold.as_of.season_states` for every configured
season. A season is complete only when every configured weather day is present
and the season end has passed. Scheduled mode processes **only** incomplete
years. With none (today: 2019–2023 are complete) the run is an explicit logged
no-op with `status=no_op`; adding a future season such as 2027 to
`seasons.years` makes it the scheduled year automatically. Complete years are
touched only with the explicit `reprocess_year` flow parameter (CLI:
`python -m delta_crops.orchestration --reprocess-year 2021`).

### Partial weather

The flow computes the in-progress years and passes
`--allow-partial-weather-year YEAR` to ingestion for exactly those years.
`bronze.ingest_weather(..., allow_partial=True)` stores a short Open-Meteo
response with `weather_status=partial` and returns `"partial"`; that is
non-blocking and appears as `ingest.weather[year] = "partial"` in the run log.
A complete year is always fetched with `allow_partial=False`, so a short
response fails the ingest task. A partial weather file is refreshed on the next
scheduled run instead of being skipped.

### Run log

Every run appends one record to `<data-dir>/_logs/pipeline_runs.jsonl` in a
`try/finally`, including failures (`status=failed` plus an ERROR log line).
Fields: `run_id`, `start_utc`, `end_utc`, `status`, `years`, `as_of` per year,
`tasks` (status + exit code per task), `ingest` (new / skipped / bytes /
weather), `silver` (built / skipped / rebuilt_due_to_input_change /
unfingerprinted / failed), `quality` (status and failed checks), `gold` (per
table counts), `gold_verify`, `gold_verify_summary`, `bronze_verify`,
`bronze_verify_years`, `duration_s`. `gold_verify` is the Gold verification
status; `bronze_verify` belongs to the separate weekly checksum flow. Both are
`not_run` when that step did not execute in the record's run.

The CLI writes a small JSON summary (`--summary-json`) next to the run log
(`_logs/task_summaries/`); the flow reads it back. If a summary is missing (for
example a mocked CLI in a test) the task still records its exit code.

### Scheduled checksums vs weekly verification

Scheduled ingestion passes `--no-checksum` and Silver passes
`--no-verify-checksum`: both trust the ledger plus file size, so a re-run does
no re-hashing. The separate `weekly_verify_flow` runs the ingestion `verify`
command for every configured year with full SHA-256 checking and writes its own
run-log record (`bronze_verify=ok|failed`, `bronze_verify_years`). In the main
pipeline record, `gold_verify` is the Gold verification status and
`gold_verify_summary` holds its summary; `bronze_verify` stays `not_run` because
that flow did not run. This keeps the fast scheduled path fast while a weekly
job still proves Bronze integrity.

### Operational notes

- **Multiple incomplete years:** `run_pipeline` processes *every* configured
  year whose season is not complete, not just the first one. Today none are
  incomplete, so the scheduled run is a no-op. If two seasons are open at once,
  both are ingested/Silver-built in the same run.
- **Disk-full writes:** an `OSError` from a Bronze write (for example a full
  disk) is a failed asset with exit 4, not a transient error. The task does not
  retry it; free space and re-run the ingest. The same applies to any asset
  write that fails after retries.
- **Silver identity is metadata-based:** Silver fingerprints use partition
  names, file sizes and `mtime_ns`, not content hashes. Copying `data/silver` to
  another machine (or restoring it) changes mtimes, so Gold will consider its
  inputs changed and rebuild unless the fingerprints are copied too or adopted
  with the `--adopt` path. Moving files with preserved mtimes avoids the rebuild.

### Failure caveat from Step 7

An `OSError` from a Bronze write (for example a full disk) is a failed asset
(exit 4), not transient. It requires manual intervention (free space, then
re-run); the task will not retry it.

### Evidence tests

`tests/test_orchestration_flow.py` uses `prefect_test_harness` with every CLI
main monkeypatched and a temporary `--data-dir`: all-zero success and run log;
ingest 3, 3, 0 retried success; ingest 4 not retried with Silver/quality/Gold
never called; quality 10 not retried with Gold never called; Silver 3 always
exhausts 1 + 3 attempts; no in-progress year logs a no-op; failure run log
fields; explicit reprocess year; weekly verify flow; and a real ingestion CLI
summary consumed by the flow (including `partial` weather). `tests/test_ingestion.py`
adds a real partial-weather store/refresh test and the CLI flag wiring test.
`tests/test_orchestration_integration.py` goes further: it runs the **real** four
CLI mains on a tiny temporary lake (1 tile, 3 dates, mini ledger, `cells.parquet`,
weather, orbit cache) and mocks only the three network boundaries (STAC search,
remote raster reads, Open-Meteo). It asserts the flow's flags, real Silver/Gold
outputs and run-log counts, then re-runs to prove 0 imagery downloads, Silver
skips and Gold skips. No test touches real data or a live Prefect server.

### Test runtime and the `flow` marker

The quick suite used to run in ~3–6 s. It is now ~28 s because every Prefect flow
test starts the in-process harness server and every flow/task run pays the
Prefect state-transition cost. `pytest --durations=10` shows ~4 s of harness
setup per flow-test module and 1–2 s per flow run; the pipeline logic itself is
milliseconds. The flow and integration tests are marked `flow` and remain in the
default CI run. For a fast local loop use:

```bash
pytest -q -m "not flow and not slow"
```

CI/acceptance keeps running `pytest -q` (includes `flow`) and
`pytest -q -m slow`.

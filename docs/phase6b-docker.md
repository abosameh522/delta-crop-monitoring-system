# Phase 6b — Docker packaging

One container serves one Prefect deployment. `flow.serve` starts the Prefect
API (SQLite inside `PREFECT_HOME`) in-process, so there is no separate server
container and no worker to poll a work pool.

## Why this layout

The Phase 6a spike ran `flow.serve(global_limit=1, limit=2)` on Prefect 3.9.2
with two concurrently submitted runs and observed no overlap
(02:14:44–02:14:47 and 02:15:14–02:15:17 UTC on 2026-11-11). Running the same
`serve` call inside a container preserves exactly that behaviour with one
process and one volume.

| Option | Verdict |
| --- | --- |
| `flow.serve(global_limit=1)` in one container | Chosen: one process, no DB bootstrap step, no worker polling, same code path as the spike |
| `prefect server start` + `prefect worker start` | Rejected as the default: two containers and a migration/ordering step for no capability the project needs |
| Deployment + cron worker | Rejected: adds a work pool and a third long-lived process |

Trade-off: run history and schedule state live in the SQLite file under
`PREFECT_HOME`. Losing that directory loses Prefect history, not data: Bronze,
Silver and Gold are on the `data/` volume.

## Dependencies

Everything the CLIs import is a **main** dependency, so `uv sync --frozen
--no-dev` produces a working runtime image:

- `prefect`, `duckdb`, `rasterio`, `pyarrow` — declared in
  `[project].dependencies`;
- `pyyaml` (config), `pystac-client` (STAC search), `requests` (Open-Meteo);
- `pandera[pandas]` pulls `pandas` and `numpy` for the quality layer.

`[project.optional-dependencies].dev` contains only `pytest`, and it is
installed solely in the `test` build stage.

## Images

`Dockerfile` has two targets:

| Stage | Contents | Purpose |
| --- | --- | --- |
| `runtime` (default) | `python:3.12-slim`, uv from a pinned image, `.venv` from `uv.lock`, `src/`, `config/` | the shipped image |
| `test` | `runtime` + `uv sync --frozen --extra dev` | container-side test run |

Both run as a non-root `app` user whose uid/gid come from the build args
`UID`/`GID` (default 1000:1000, overridable through `HOST_UID`/`HOST_GID`).
`data/` is never copied into an image; `.dockerignore` excludes it along with
`.venv/`, `.git/`, `.prefect/`, caches, `.env`, `tests/` and `docs/`.

## Compose

```bash
mkdir -p .prefect .prefect-test
HOST_UID="$(id -u)" HOST_GID="$(id -g)" docker compose build
docker compose up -d
docker compose ps
```

- `./data:/app/data` (read-write) and `./config:/app/config:ro`.
- `./.prefect:/var/lib/prefect` with `PREFECT_HOME=/var/lib/prefect`, so Prefect
  state is never inside `data/`. The directory is created by you as uid 1000 so
  the container user can write it; a named volume would be root-owned and need
  an extra `chown`.
- `DELTA_CROPS_DATA_DIR=/app/data` and `DELTA_CROPS_CONFIG=/app/config/aoi.yaml`
  are set explicitly: `config.data_dir()` reads `DELTA_CROPS_DATA_DIR` and falls
  back to `./data`, and `config.config_path()` reads `DELTA_CROPS_CONFIG` and
  falls back to `config/aoi.yaml`.
- `restart: unless-stopped`, `init: true`, `stop_grace_period: 30s`.
- No published ports: the API stays on `127.0.0.1:4200` inside the container, so
  nothing unauthenticated is exposed on the host. Use `docker compose exec` for
  `prefect` commands.
- Healthcheck: `python -m delta_crops.orchestration.healthcheck` (every 60 s,
  3 retries, 60 s start period). It requires `PREFECT_HOME` to be writable and
  `GET $PREFECT_API_URL/health` to return `200 true`.

## The five-day schedule

`python -m delta_crops.orchestration.serve` registers the deployment
`delta-crops-scheduled` with:

```python
scheduled_flow.serve(
    name="delta-crops-scheduled",
    interval=timedelta(days=5),
    global_limit=1,
    pause_on_shutdown=True,
    parameters={"data_dir": ..., "config_path": ...},
)
```

- **Concurrency:** `global_limit=1` means a second run queues until the first
  finishes. `init: true` reaps child processes; all retry logic is Phase 6a's.
- **Manual run of a complete year:**
  `docker compose run --rm orchestrator python -m delta_crops.orchestration --reprocess-year 2021`.
  A one-off process sharing the same volumes and `PREFECT_HOME`; it writes its
  own `_logs/pipeline_runs.jsonl` record.
- **Schedule anchor:** the interval is anchored when the deployment is
  registered, which happens on every container start. A restart therefore
  re-anchors the clock, so the next run is at most five days after the last
  start (never sooner than five days after a run that just completed). This is
  accepted because every run is idempotent: a completed season is a logged
  `no_op`, an in-progress season skips existing Bronze assets and verified
  Silver/Gold partitions. What you observe after a restart is a fresh
  deployment registration line in the log and the next run scheduled five days
  out; no duplicate work. A fixed anchor would require creating the schedule
  explicitly with `IntervalSchedule(anchor_date=...)`, which is future work.
- **`pause_on_shutdown=True`** (Prefect's default, stated explicitly): the
  schedule is paused while the API is down, so `docker compose stop` pauses the
  deployment instead of accumulating missed runs; `docker compose up -d`
  resumes it on the next start. Setting it to `False` would keep the schedule
  active during downtime and fire catch-up runs on restart — idempotent, but
  noisier and unnecessary for a five-day cadence.

## Container-side tests

`tests/` is excluded from the image, so the `tests` service mounts it read-only
and uses the `test` stage for pytest (the runtime image cannot import pytest):

```bash
docker compose --profile test run --rm \
  tests pytest -q -m "not slow and not flow"
```

The real `data/` volume is mounted **read-only** in this service, so a test that
tries to write the lake fails instead of corrupting it. The flow/integration
tests can be run explicitly with `-m flow` (they still mock the network).

## Unverified assumptions

1. `python:3.12-slim` plus the `libexpat1` / `libgomp1` runtime packages are
   enough for the rasterio, pyarrow and duckdb wheels; confirmed by the smoke
   import in the acceptance run.
2. The uv image tag `ghcr.io/astral-sh/uv:0.9` exists and is current enough.
3. The ephemeral API is reachable at `http://127.0.0.1:4200/api/health` from
   inside the container.
4. `prefect flow-run ls` timestamps are fine-grained enough to show that two
   rapid runs did not overlap.
5. Container memory is measured by `docker stats --no-stream`; this phase makes
   no RAM claim.

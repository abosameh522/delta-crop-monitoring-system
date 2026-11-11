# Phase 6b — Docker packaging

One `server` container runs a real Prefect server (SQLite state, scheduler
enabled) and one `orchestrator` container runs a runner that serves the
five-day deployment against it.

## Why not a single container with an ephemeral API

The first compose version ran only `flow.serve()` and set
`PREFECT_API_URL=http://127.0.0.1:4200/api`, expecting an in-process ephemeral
API. It crash-looped with `httpx.ConnectError` on every restart, and no SQLite
file was ever created. Reading the installed Prefect 3.9.2 source explains why,
and also kills the "just remove the variable" fix:

| Question | Evidence |
| --- | --- |
| When does an ephemeral server start? | `prefect/client/orchestration/__init__.py:245-258` (`get_client`): `api = PREFECT_API_URL.value()`; `if not api and PREFECT_SERVER_EPHEMERAL_ENABLED:` → `SubprocessASGIServer().start()`. Only when the API URL is empty. |
| What if `PREFECT_API_URL` is set? | Same lines: the branch is skipped, the client connects to that URL, and nothing is started. With no server there, `httpx.ConnectError` — exactly the crash loop. |
| Is the ephemeral port fixed? | No. `prefect/server/api/server.py:1123-1125`: `SubprocessASGIServer._port_range = range(8000, 9000)`, and `find_available_port()` (1147-1155) picks a random free port. `get_client` calls `SubprocessASGIServer()` with no port, so no setting can pin it. A fixed-port healthcheck cannot match it. |
| Would an ephemeral API run the schedule? | **No.** `prefect/server/services/scheduler.py:212-215` registers the scheduler as a perpetual service **without** `run_in_ephemeral=True`, and `prefect/server/services/perpetual_services.py:78-92` filters exactly on that flag. Ephemeral servers run only `run_in_ephemeral` services. `prefect/runner/runner.py:383-386` warns: "Cannot schedule flows on an ephemeral server; run `prefect server start` to start the scheduler." |

So `PREFECT_API_URL` was not the real bug — the missing **server process** was.
Removing the variable would have replaced a visible crash loop with a silently
dead schedule.

## Layout

| Service | Process | Why |
| --- | --- | --- |
| `server` | `prefect server start --host 0.0.0.0 --port 4200 --no-ui` | Runs the scheduler, owns the SQLite state; one job per process |
| `orchestrator` | `python -m delta_crops.orchestration.serve` | Runner + deployment registration, `PREFECT_API_URL=http://server:4200/api` |
| `tests` (`--profile test`) | `pytest -q -m "not slow and not flow"` | Container-side suite from the `test` build stage |

Alternative considered: one container whose entrypoint starts the server in the
background and then `exec`s the runner. It keeps one container but needs a shell
entrypoint, a readiness wait loop and signal handling for two processes, and the
healthcheck would have to describe both. Two services let Compose express the
startup order (`depends_on: server: condition: service_healthy`) with no bespoke
script.

## Healthchecks: what they actually prove

| Service | Check | Proves | Does **not** prove |
| --- | --- | --- | --- |
| `server` | `GET http://127.0.0.1:4200/api/health` returns `200 true`, plus `PREFECT_HOME` writable | the API process is up, serving, and can persist state | that the scheduler loop is enabled (it is asserted separately by `PREFECT_SERVER_SERVICES_SCHEDULER_ENABLED=true`) |
| `orchestrator` | `GET http://server:4200/api/health` and `GET /api/deployments/name/delta-crops-phase6a/delta-crops-scheduled` returns `paused: false` | the runner connected to the same API and registered an active deployment | that the runner is making progress — this is **not** a heartbeat; a wedged runner with a registered deployment still reports healthy |

A real heartbeat check would need a `last_polled`/`next_scheduled_start_time`
assertion; that is future work, not claimed here.

## Images and permissions

`Dockerfile` has two targets: `runtime` (default: `python:3.12-slim`, uv from a
pinned image, `uv sync --frozen --no-dev`, non-root `app` user, no `data/`) and
`test` (`+ uv sync --frozen --extra dev`).

`ARG UID=1000` / `ARG GID=1000`, and Compose passes
`UID: "${HOST_UID:-1000}"` / `GID: "${HOST_GID:-1000}"`. **Build with the
literal defaults.** The first build picked up gid 966 because the shell's
primary group had changed (`newgrp docker`); deriving the build args from
`id -g` reproduces that bug and the container user stops matching the owner of
`data/`. If a host really needs other ids, pass them explicitly:

```bash
HOST_UID=1000 HOST_GID=1000 docker compose build
```

## Running

```bash
mkdir -p .prefect

# build with the documented literal ids, never `id -g`
HOST_UID=1000 HOST_GID=1000 docker compose build
docker compose up -d
docker compose ps            # server healthy, then orchestrator healthy
docker compose logs --tail=40 server orchestrator

# memory: measured, never claimed
docker stats --no-stream

# the runner must reach the server container, not loopback
docker compose exec orchestrator prefect deployment ls
docker compose exec orchestrator prefect deployment inspect \
  'delta-crops-phase6a/delta-crops-scheduled'

# two rapid runs: global_limit=1 means the second waits
docker compose exec orchestrator prefect deployment run \
  'delta-crops-phase6a/delta-crops-scheduled'
docker compose exec orchestrator prefect deployment run \
  'delta-crops-phase6a/delta-crops-scheduled'
docker compose exec orchestrator prefect flow-run ls --limit 5

# manual run of a complete year
docker compose run --rm orchestrator \
  python -m delta_crops.orchestration --reprocess-year 2021

# container-side tests (tests/ mounted read-only; pytest from the test stage)
docker compose --profile test run --rm tests
docker compose --profile test run --rm tests pytest -q tests/test_orchestration_integration.py

# failure logging: expect non-zero exit and an ERROR naming the bad config
docker compose run --rm orchestrator \
  python -m delta_crops.orchestration --data-dir /tmp/dc_smoke \
  --config /app/config/does_not_exist.yaml; echo "exit=$?"

# smoke on a tmp fixture: never the real lake
mkdir -p /tmp/dc_smoke
docker compose run --rm -v /tmp/dc_smoke:/tmp/dc_smoke orchestrator \
  python -m delta_crops.orchestration --data-dir /tmp/dc_smoke \
  --config /app/config/aoi.yaml
cat /tmp/dc_smoke/_logs/pipeline_runs.jsonl

docker compose down
```

`DELTA_CROPS_DATA_DIR=/app/data` and `DELTA_CROPS_CONFIG=/app/config/aoi.yaml`
are set explicitly: `config.data_dir()` reads `DELTA_CROPS_DATA_DIR` (fallback
`./data`) and `config.config_path()` reads `DELTA_CROPS_CONFIG` (fallback
`config/aoi.yaml`).

Durable Prefect state is `./.prefect` mounted at `/var/lib/prefect` on the
`server` service only. The runner uses `PREFECT_HOME=/tmp/prefect` (client
scratch, nothing durable) so two processes never share one SQLite file. No
ports are published; use `docker compose exec` for Prefect CLI work.

## The five-day schedule

`serve.py` registers `delta-crops-scheduled` with `interval=timedelta(days=5)`,
`global_limit=1`, `pause_on_shutdown=True`. It refuses to start without
`PREFECT_API_URL`, because an ephemeral API has no scheduler.

- **Anchor:** registration happens on every start, so the interval re-anchors
  there. The next run is at most five days after the last start. Accepted
  because runs are idempotent (complete season → `no_op`; in-progress → skips
  existing assets and verified partitions). A fixed anchor needs an explicit
  `IntervalSchedule(anchor_date=...)`; future work.
- **`pause_on_shutdown=True`:** the schedule is paused when the runner stops,
  so `docker compose stop`/`restart` does not accumulate missed runs; the next
  start re-registers and resumes.
- **Concurrency:** `global_limit=1` is enforced by the server, so a second run
  queues behind the first.

## Tests that pin this

`tests/test_phase6b.py` parses `docker-compose.yml` instead of matching text, so
the three failure modes above are test failures, not surprises:

- `test_compose_runs_a_real_prefect_server_for_the_scheduler`
- `test_compose_healthcheck_probes_the_api_each_service_uses`
- `test_compose_runner_does_not_use_a_loopback_api_url`
- `test_serve_refuses_to_start_without_an_api_url`

The first three fail on the previous compose file; the fourth fails on the
previous `serve.py`. Verified by restoring both files from git and re-running.

## Unverified

1. The uv image tag `ghcr.io/astral-sh/uv:0.9` builds as written (the earlier
   import check ran from a built image, so this is close to proven).
2. `prefect server start` with `PREFECT_HOME=./.prefect` creates its SQLite file
   there and reaches healthy within the 40 s start period on Ahmed's machine.
3. The runner reaches `http://server:4200/api` over the compose network.
4. `prefect flow-run ls` timestamps can show the two queued runs did not
   overlap.
5. Container memory is whatever `docker stats --no-stream` says; no claim.

## Container-side tests: what they can and cannot cover

- **Host-only tests skip inside the image.** Five (now six) tests parse
  `Dockerfile`, `.dockerignore` and `docker-compose.yml`, and those files are
  deliberately not in the image (`.dockerignore` drops them). The `test` stage
  sets `DELTA_CROPS_IN_TEST_IMAGE=1` and those tests carry one `host_only`
  marker with the reason "reads repo files … verified on the host". The marker
  is never applied because a file is missing: on the host the same tests run and
  fail for real. `test_only_the_docker_file_tests_are_host_only` pins the exact
  set of marked tests, and
  `test_dockerfile_sets_the_test_image_marker_only_in_the_test_stage` pins that
  the variable exists only in the `test` stage.
- **Rebuild the test image after any `src/` change.**
  `HOST_UID=1000 HOST_GID=1000 docker compose --profile test build tests`
  before `docker compose --profile test run --rm tests`. The image copies the
  source at build time; running a stale `delta-crops:6b-test` executes old code.
  This happened once: a test image built before the compose fix reported 9
  failures that the current tree did not have.
- **A green container suite is not proof of the compose wiring.** It proves the
  Python package, the CLIs and the Prefect client work inside the image. It does
  not prove that `server` and `orchestrator` start, reach each other, register
  the deployment or stay healthy — that is what `docker compose ps`, the
  healthchecks, `prefect deployment inspect` and `flow-run ls` are for.

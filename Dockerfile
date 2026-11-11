# Two targets: `runtime` (default, shipped) and `test` (adds pytest).
# The runtime image never carries test code or dev dependencies.
# syntax=docker/dockerfile:1

# ---------------------------------------------------------------- base ----
FROM python:3.12-slim AS base

# Host uid/gid: the container user owns the bind-mounted data/ and .prefect/.
ARG UID=1000
ARG GID=1000

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv

# Runtime libraries the manylinux wheels of rasterio / pyarrow / duckdb need.
# ca-certificates is for the STAC and Open-Meteo HTTPS calls.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates libexpat1 libgomp1 \
 && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.9 /uv /uvx /bin/

RUN groupadd --gid "${GID}" app \
 && useradd --uid "${UID}" --gid "${GID}" --create-home --shell /usr/sbin/nologin app

WORKDIR /app

# ------------------------------------------------------------- runtime ----
FROM base AS runtime

# Dependency layer first, so source edits do not re-resolve the lockfile.
# --frozen: the lockfile is the only resolution input; never re-lock here.
# --no-dev: no dev dependency-group in the shipped image.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src ./src
COPY config ./config

RUN uv sync --frozen --no-dev \
 && mkdir -p /app/data /var/lib/prefect \
 && chown -R app:app /app /var/lib/prefect

ENV PATH=/app/.venv/bin:$PATH

USER app

# No ENTRYPOINT: `docker compose run ... python -m ...` must reach the venv
# python directly. The compose service supplies the default command.
CMD ["python", "-m", "delta_crops.orchestration.serve"]

# ---------------------------------------------------------------- test ----
# `docker compose --profile test run --rm tests` or `docker build --target test`.
# Dev dependencies live only in this stage: the shipped image cannot import pytest.
FROM runtime AS test

# Tells the suite it is inside the image: the tests that parse Dockerfile,
# .dockerignore and docker-compose.yml skip here (those files are not copied on
# purpose) and still run on the host. It is NOT set in the runtime stage.
ENV DELTA_CROPS_IN_TEST_IMAGE=1

USER root
RUN uv sync --frozen --extra dev \
 && chown -R app:app /app
USER app

CMD ["pytest", "-q", "-m", "not slow and not flow"]

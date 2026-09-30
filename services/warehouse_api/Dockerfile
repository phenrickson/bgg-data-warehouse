# Warehouse read API image. Built from the repo root:
#   docker build -f services/warehouse_api/Dockerfile -t warehouse-api .
FROM python:3.12-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN pip install uv

# Dependencies from the monorepo root pyproject + the `api` extra (fastapi/uvicorn), minus
# the packages only the scrapers and pipelines use. Cold-start time is mostly app import
# (logged at startup), and the API never touches these — but google-cloud-bigquery imports
# bigquery_storage (and through it pandas/numpy) whenever it is installed. Leaving them out
# cuts installed packages from ~430MB to ~67MB and the app import ~3x. bigquery treats the
# storage client as optional (try/except ImportError), so the client works without it.
#
# UV_COMPILE_BYTECODE: uv does not compile .pyc by default, so every cold start was
# compiling each imported module from source. --locked: fail the build on a stale uv.lock.
ENV UV_COMPILE_BYTECODE=1
COPY . .
RUN uv sync --locked --extra api \
    --no-install-package pandas \
    --no-install-package pandas-gbq \
    --no-install-package polars \
    --no-install-package pyarrow \
    --no-install-package numpy \
    --no-install-package db-dtypes \
    --no-install-package google-cloud-bigquery-storage \
    --no-install-package google-cloud-bigquery-datatransfer \
    --no-install-package playwright \
    --no-install-package playwright-stealth

# UV_NO_SYNC: without it, `uv run` in the CMD sees the packages excluded above as missing
# and reinstalls them from PyPI on every container start.
ENV UV_NO_SYNC=1
ENV PYTHONUNBUFFERED=1
EXPOSE 8080

# Cloud Run sends traffic to $PORT (default 8080). Caller auth is enforced by Cloud Run
# IAM before requests reach the app.
#
# The echo is temporary startup timing (see the top of main.py): it marks the moment the
# container process begins, before `uv run`. `exec` hands PID 1 to `uv`, as an exec-form
# CMD would, so Cloud Run's SIGTERM reaches it the same way.
CMD ["sh", "-c", "echo '[startup] container process started'; exec uv run uvicorn services.warehouse_api.main:app --host 0.0.0.0 --port 8080"]

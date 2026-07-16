# Warehouse read API image. Built from the repo root:
#   docker build -f services/warehouse_api/Dockerfile -t warehouse-api .
FROM python:3.12-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN pip install uv

# Dependencies from the monorepo root pyproject + the `api` extra (fastapi/uvicorn).
COPY . .
RUN uv sync --extra api

ENV PYTHONUNBUFFERED=1
EXPOSE 8080

# Cloud Run sends traffic to $PORT (default 8080). Caller auth is enforced by Cloud Run
# IAM before requests reach the app.
CMD ["uv", "run", "uvicorn", "services.warehouse_api.main:app", "--host", "0.0.0.0", "--port", "8080"]

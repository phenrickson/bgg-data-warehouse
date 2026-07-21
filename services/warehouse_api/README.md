# Warehouse Read API (`bgg-warehouse-api`)

A modular-monolith FastAPI read API over the warehouse's materialized data. One router
per resource; ships `/health` and the `games` router today.

- **Design:** `docs/superpowers/specs/2026-07-16-warehouse-services-architecture-design.md`
- **Games slice:** `docs/superpowers/specs/2026-07-16-game-detail-api-design.md`
- **Auth/gating:** `docs/superpowers/specs/2026-07-16-service-auth-pattern-design.md`

## Layout

- `main.py` — FastAPI app; mounts routers, exposes `/health`.
- `routers/` — one `APIRouter` per resource (`games.py`).
- `auth.py` — resolves the GCP project for the BigQuery client (ADC). Inbound caller
  auth is enforced by Cloud Run IAM, not the app.
- Query logic lives in `src/warehouse/readers/` (pure, testable), not here.

## Run locally

```bash
uv run --extra api uvicorn services.warehouse_api.main:app --port 8080
curl localhost:8080/health          # {"status":"ok"}
curl localhost:8080/games/13        # full game document (needs GCP creds for BigQuery)
```

## Container

```bash
docker build -f services/warehouse_api/Dockerfile -t warehouse-api .
docker run -p 8080:8080 warehouse-api
```

## Deploy

Gated Cloud Run service (`--no-allow-unauthenticated`), deployed via
`config/cloudbuild.yaml` + `.github/workflows/deploy-warehouse-api.yml`. Access is
granted through the invoker group per the auth-pattern spec.

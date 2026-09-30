"""BGG Warehouse read API.

A modular-monolith FastAPI service over the warehouse's materialized data. One router
per resource; this build ships ``/health``, the ``games`` router, and the
``monitoring`` router. See
docs/superpowers/specs/2026-07-16-warehouse-services-architecture-design.md.
"""

# Startup timing, temporary: cold starts show 20-90s of silence between Cloud Run's
# "Starting new instance" and uvicorn's "Started server process". Together with the
# timestamp the Dockerfile CMD prints, these lines split that gap into image pull /
# `uv run` / imports. Remove once the cold-start cause is settled.
import time

_import_start = time.perf_counter()
print("[startup] main.py: beginning app imports", flush=True)

import logging  # noqa: E402

from dotenv import load_dotenv  # noqa: E402
from fastapi import FastAPI  # noqa: E402

from services.warehouse_api.routers import games, monitoring  # noqa: E402

print(f"[startup] main.py: app imports took {time.perf_counter() - _import_start:.2f}s", flush=True)

load_dotenv()
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="BGG Warehouse API", version="0.1.0")

app.include_router(games.router)
app.include_router(monitoring.router)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8080)

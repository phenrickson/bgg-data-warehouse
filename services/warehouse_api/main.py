"""BGG Warehouse read API.

A modular-monolith FastAPI service over the warehouse's materialized data. One router
per resource; this build ships ``/health`` and the ``games`` router. See
docs/superpowers/specs/2026-07-16-warehouse-services-architecture-design.md.
"""

import logging

from dotenv import load_dotenv
from fastapi import FastAPI

from services.warehouse_api.routers import games

load_dotenv()
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="BGG Warehouse API", version="0.1.0")

app.include_router(games.router)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8080)

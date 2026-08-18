"""Monitoring resource router.

Thin HTTP shell over ``src.warehouse.readers.monitoring``, same shape as
``routers.games``. Serves bgg-viewer's "what's new" page.
"""

from fastapi import APIRouter, Query

from src.warehouse.readers import monitoring as reader

router = APIRouter(tags=["monitoring"])


@router.get("/new-games")
def get_new_games(
    days: int = Query(7, ge=1, le=365),
    limit: int = Query(5000, ge=1, le=20000),
):
    """Games first fetched into the warehouse in the last `days` days, newest first.

    `limit` is a safety valve, not a practical cap — a monitoring page that silently
    truncates the thing it exists to show you is worse than no limit at all. 5000
    comfortably covers a 365-day window at current volume (~2,300 games / 90 days).
    """
    return reader.fetch_recently_added(days_back=days, limit=limit)

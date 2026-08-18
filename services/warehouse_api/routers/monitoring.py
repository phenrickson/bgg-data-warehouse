"""Monitoring resource router.

Thin HTTP shell over ``src.warehouse.readers.monitoring``, same shape as
``routers.games``. Serves bgg-viewer's "what's new" page.
"""

import time

from fastapi import APIRouter, Query

from src.warehouse.readers import monitoring as reader

router = APIRouter(tags=["monitoring"])

# `LIMIT` doesn't change what the query scans or costs — the aggregation runs over
# the same date range regardless of how many rows come back. So there is no reason
# for a "default" lower than the safety valve itself: a two-tier default/max, once
# already, quietly reintroduced the exact silent-truncation bug this replaced (200
# turned out to be a near-miss on real data; a smaller "default" than "max" is just
# a slower version of the same mistake as growth continues).
_MAX_ROWS = 20000

# In-process cache, keyed by (days, limit). New games arrive on the daily pipeline
# cadence, not sub-minute, so a short TTL trades a little staleness for skipping a
# full BigQuery scan on every page load and every day-range toggle.
_CACHE_TTL_SECONDS = 300
_cache: dict[tuple[int, int], tuple[float, list[dict]]] = {}


def _reset_cache() -> None:
    """Test seam — clears the module-level cache between tests."""
    _cache.clear()


@router.get("/new-games")
def get_new_games(
    days: int = Query(7, ge=1, le=365),
    limit: int = Query(_MAX_ROWS, ge=1, le=_MAX_ROWS),
):
    """Games first fetched into the warehouse in the last `days` days, newest first."""
    key = (days, limit)
    now = time.time()
    cached = _cache.get(key)
    if cached is not None and now - cached[0] < _CACHE_TTL_SECONDS:
        return cached[1]

    result = reader.fetch_recently_added(days_back=days, limit=limit)
    _cache[key] = (now, result)
    return result

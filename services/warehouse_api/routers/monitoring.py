"""Monitoring resource router.

Thin HTTP shell over ``src.warehouse.readers.monitoring``, same shape as
``routers.games``. Serves bgg-viewer's "what's new" page and the admin pipeline monitor.
"""

import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

from fastapi import APIRouter, HTTPException, Query

from src.monitoring import chain
from src.monitoring.github import fetch_runs
from src.warehouse.readers import monitoring as reader
from src.warehouse.readers import pipeline as pipeline_reader

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


_pipeline_cache: dict[int, tuple[float, dict]] = {}


def _reset_cache() -> None:
    """Test seam — clears the module-level caches between tests."""
    _cache.clear()
    _pipeline_cache.clear()


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


def _collect_runs(start: datetime, end: datetime, token: str) -> chain.Runs:
    """Every chain workflow's runs in the window, listed in parallel (~13 requests)."""
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {
            source: pool.submit(fetch_runs, source[0], source[1], start, end, token)
            for source in chain.SOURCES
        }
        return {source: f.result() for source, f in futures.items()}


@router.get("/monitoring/pipeline")
def get_pipeline(days: int = Query(14, ge=1, le=30)):
    """Today's chain, a ``days``-long history, table freshness and live models."""
    cached = _pipeline_cache.get(days)
    if cached is not None and time.time() - cached[0] < _CACHE_TTL_SECONDS:
        return cached[1]

    token = os.environ.get("GH_TOKEN")
    if not token:
        raise HTTPException(503, "GH_TOKEN is not set on the warehouse API")

    now = datetime.now(UTC)
    try:
        runs = _collect_runs(chain.history_start(now, days), now, token)
        tables = pipeline_reader.fetch_table_status()
        models = pipeline_reader.fetch_deployed_models()
    except Exception as exc:  # GitHub or BigQuery: report it, don't serve a partial status
        raise HTTPException(502, f"pipeline status unavailable: {exc}") from exc

    result = chain.build_report(runs, days, now) | {"tables": tables, "models": models}
    _pipeline_cache[days] = (time.time(), result)
    return result

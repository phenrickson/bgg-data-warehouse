"""Monitoring resource router.

Thin HTTP shell over ``src.warehouse.readers.monitoring``, same shape as
``routers.games``. Serves bgg-viewer's "what's new" page and the admin pipeline monitor.
"""

import os
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from datetime import UTC, datetime

from fastapi import APIRouter, HTTPException, Query
from google.api_core import exceptions as gexc

from src.monitoring import chain
from src.monitoring import lineage as lineage_mod
from src.monitoring.github import fetch_jobs, fetch_runs, iso
from src.warehouse.readers import monitoring as reader
from src.warehouse.readers import lineage as lineage_reader
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
_lineage_cache: dict[str, tuple[float, dict]] = {}
_schema_cache: dict[str, tuple[float, dict]] = {}


def _reset_cache() -> None:
    """Test seam — clears the module-level caches between tests."""
    _cache.clear()
    _pipeline_cache.clear()
    _lineage_cache.clear()
    _schema_cache.clear()


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


def _collect_jobs(run_ids: list[int], token: str) -> chain.Jobs:
    """Jobs of the ML Pipeline runs ``chain.jobs_needed`` names (usually just today's)."""
    if not run_ids:
        return {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {rid: pool.submit(fetch_jobs, chain.MODELS, rid, token) for rid in run_ids}
        return {rid: f.result() for rid, f in futures.items()}


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
        jobs = _collect_jobs(chain.jobs_needed(runs, days, now), token)
        tables = pipeline_reader.fetch_table_status()
        models = pipeline_reader.fetch_deployed_models()
    except Exception as exc:  # GitHub or BigQuery: report it, don't serve a partial status
        raise HTTPException(502, f"pipeline status unavailable: {exc}") from exc

    result = chain.build_report(runs, days, now, jobs) | {"tables": tables, "models": models}
    _pipeline_cache[days] = (time.time(), result)
    return result


def _lineage() -> dict:
    """The lineage graph with per-table metadata, cached like the pipeline report."""
    cached = _lineage_cache.get("lineage")
    if cached is not None and time.time() - cached[0] < _CACHE_TTL_SECONDS:
        return cached[1]
    try:
        summary, data = lineage_reader.fetch_compilation()
        nodes, edges = lineage_mod.parse_compilation_result(data)
        meta = lineage_reader.fetch_table_meta([n.id for n in nodes])
    except Exception as exc:  # Dataform or BigQuery: report it, don't serve a partial graph
        raise HTTPException(502, f"lineage unavailable: {exc}") from exc
    sha = summary.get("resolvedGitCommitSha") or ""
    result = {
        "generated_at": iso(datetime.now(UTC)),
        "compilation": {"name": summary.get("name"), "created": summary.get("createTime"),
                        "commit": sha[:7] or None},
        "nodes": [asdict(n) | meta.get(n.id, {}) for n in nodes],
        "edges": [list(e) for e in edges],
    }
    _lineage_cache["lineage"] = (time.time(), result)
    return result


@router.get("/monitoring/lineage")
def get_lineage():
    """Dataform lineage of the latest clean compilation of main, with table metadata."""
    return _lineage()


@router.get("/monitoring/tables/{table_id}")
def get_table_schema(table_id: str):
    """Schema of one table in the current lineage (``project.dataset.table``)."""
    cached = _schema_cache.get(table_id)
    if cached is not None and time.time() - cached[0] < _CACHE_TTL_SECONDS:
        return cached[1]
    if table_id not in {n["id"] for n in _lineage()["nodes"]}:
        raise HTTPException(400, "not a table in the current lineage")
    try:
        schema = lineage_reader.fetch_table_schema(table_id)
    except gexc.NotFound as exc:
        raise HTTPException(404, f"{table_id} not found") from exc
    except gexc.Forbidden as exc:
        raise HTTPException(403, f"the warehouse API cannot read {table_id}") from exc
    result = {"id": table_id, "schema": schema}
    _schema_cache[table_id] = (time.time(), result)
    return result

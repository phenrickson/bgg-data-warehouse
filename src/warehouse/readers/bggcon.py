"""Reader for the BGG.CON library analytics: the recorder's events and the library's copies.

Plain reads only; ``src.bggcon.analytics`` does the arithmetic. TEMPORARY with the rest of
BGG.CON 2026 (bgg-viewer docs/superpowers/specs/2026-11-17-bggcon-analytics-design.md).
"""

import json
import os
from datetime import datetime
from typing import Any

from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage

from src.bggcon.recorder import WINDOW_START
from src.warehouse.bq import dataset, get_client

LIBRARY = "bggcon/library.json"


def fetch_events(client: bigquery.Client | None = None) -> list[dict[str, Any]]:
    """Every checkout event from the recorder's window on (duplicates included).

    Manual test runs (``BGGCON_TEST_RUN=1``) write events to the same table, unmarked; their
    heartbeat is marked, and a run's events and heartbeat share its ``observed_at``.
    """
    client = client or get_client()
    sql = f"""
        SELECT copy_id, bgg_id, name, event, observed_at, lastcheckout_id
        FROM `{dataset('raw')}.bggcon_checkout_events`
        WHERE observed_at >= @start
          AND observed_at NOT IN (
            SELECT observed_at FROM `{dataset('raw')}.bggcon_runs`
            WHERE note = 'test run' AND observed_at >= @start
          )
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("start", "TIMESTAMP", WINDOW_START)]
    )
    return [dict(row) for row in client.query(sql, job_config=job_config).result()]


def _run_time(fn: str, client: bigquery.Client | None) -> datetime | None:
    client = client or get_client()
    sql = f"""
        SELECT {fn}(observed_at) AS t
        FROM `{dataset('raw')}.bggcon_runs`
        WHERE ok AND IFNULL(note, '') != 'test run' AND observed_at >= @start
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("start", "TIMESTAMP", WINDOW_START)]
    )
    [row] = list(client.query(sql, job_config=job_config).result())
    return row["t"]


def fetch_as_of(client: bigquery.Client | None = None) -> datetime | None:
    """The recorder's latest good run ("now" for the numbers); None before its first."""
    return _run_time("MAX", client)


def fetch_baseline(client: bigquery.Client | None = None) -> datetime | None:
    """The recorder's first good run: its events were copies already out, not checkouts."""
    return _run_time("MIN", client)


def fetch_copies() -> dict[int, int]:
    """Copies per BGG id from the viewer's ``bggcon/library.json``; {} if it isn't there."""
    bucket = storage.Client().bucket(
        os.environ.get("BGGCON_BUCKET", "bgg-data-warehouse-bgg-viewer-artifacts")
    )
    try:
        library = json.loads(bucket.blob(LIBRARY).download_as_text())
    except NotFound:
        return {}
    return {g["bgg_id"]: g["copies"] for g in library.get("games", []) if g.get("bgg_id")}

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
    """Every checkout event from the recorder's window on (duplicates included)."""
    client = client or get_client()
    sql = f"""
        SELECT copy_id, bgg_id, name, event, observed_at, lastcheckout_id
        FROM `{dataset('raw')}.bggcon_checkout_events`
        WHERE observed_at >= @start
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("start", "TIMESTAMP", WINDOW_START)]
    )
    return [dict(row) for row in client.query(sql, job_config=job_config).result()]


def fetch_as_of(client: bigquery.Client | None = None) -> datetime | None:
    """The recorder's latest good run ("now" for the numbers); None before its first."""
    client = client or get_client()
    sql = f"""
        SELECT MAX(observed_at) AS as_of
        FROM `{dataset('raw')}.bggcon_runs`
        WHERE ok AND IFNULL(note, '') != 'test run' AND observed_at >= @start
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("start", "TIMESTAMP", WINDOW_START)]
    )
    [row] = list(client.query(sql, job_config=job_config).result())
    return row["as_of"]


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

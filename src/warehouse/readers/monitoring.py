"""Reader for the 'what's new' monitoring view.

Games recently added to the warehouse, for bgg-viewer's /whats-new page. Separate
from ``readers.games`` because this reads across games rather than looking one up.
"""

from typing import Any, Optional

from google.cloud import bigquery

from src.warehouse.bq import dataset, get_client


def fetch_recently_added(
    days_back: int = 7,
    limit: int = 20000,
    client: Optional[bigquery.Client] = None,
) -> list[dict[str, Any]]:
    """Games first fetched in the last ``days_back`` days, newest first.

    ``first_seen`` is ``MIN(fetch_timestamp)`` per ``game_id`` from
    ``raw.fetched_responses`` — the same definition bgg-dash-viewer's
    ``/app/new-games`` page already uses. ``predicted_hurdle_prob`` is returned
    raw/unthresholded; any tiering is a display decision made by the caller.
    """
    client = client or get_client()
    sql = f"""
        WITH first_fetches AS (
            SELECT game_id, MIN(fetch_timestamp) AS first_seen
            FROM `{dataset('raw')}.fetched_responses`
            WHERE fetch_status = 'success'
            GROUP BY game_id
        )
        SELECT g.game_id, g.name, g.year_published, g.thumbnail,
               ff.first_seen, p.predicted_hurdle_prob
        FROM first_fetches ff
        JOIN `{dataset('analytics')}.games_features` g USING (game_id)
        LEFT JOIN `{dataset('predictions')}.bgg_predictions` p USING (game_id)
        WHERE ff.first_seen > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days_back DAY)
        ORDER BY ff.first_seen DESC
        LIMIT @limit
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("days_back", "INT64", days_back),
            bigquery.ScalarQueryParameter("limit", "INT64", limit),
        ]
    )
    return [dict(row) for row in client.query(sql, job_config=job_config).result()]

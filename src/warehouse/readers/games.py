"""Reader for the ``games`` resource of the warehouse read API.

Pure query functions over BigQuery — one per block plus a ``get_game`` aggregator.
Every function accepts an optional ``client`` for dependency injection (tests pass a
fake), and parameterizes ``game_id`` via ``ScalarQueryParameter`` (never string
interpolation). Tables are resolved through ``src.warehouse.bq.dataset`` so no
project/dataset is hard-coded.
"""

from typing import Any, Optional

from google.cloud import bigquery

from src.warehouse.bq import dataset, get_client


def _rows(client: bigquery.Client, sql: str, game_id: int, extra_params=None) -> list[dict]:
    params = [bigquery.ScalarQueryParameter("game_id", "INT64", game_id)]
    if extra_params:
        params.extend(extra_params)
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    return [dict(row) for row in client.query(sql, job_config=job_config).result()]


def get_features(game_id: int, client: Optional[bigquery.Client] = None) -> Optional[dict[str, Any]]:
    """Game record + per-player-count recommendations. ``None`` if the game is unknown."""
    client = client or get_client()
    rows = _rows(
        client,
        f"SELECT * FROM `{dataset('analytics')}.games_features` "
        "WHERE game_id = @game_id LIMIT 1",
        game_id,
    )
    if not rows:
        return None
    features = rows[0]
    features["player_counts"] = _rows(
        client,
        f"SELECT * FROM `{dataset('analytics')}.player_count_recommendations` "
        "WHERE game_id = @game_id ORDER BY player_count",
        game_id,
    )
    return features


def get_predictions(game_id: int, client: Optional[bigquery.Client] = None) -> Optional[dict[str, Any]]:
    """Latest prediction row plus ``first_prediction_ts``.

    ``bgg_predictions`` holds one (latest) row per game; full time-series history would
    read ``ml_predictions_landing`` and is deferred to a later slice.
    """
    client = client or get_client()
    rows = _rows(
        client,
        f"""
        SELECT p.*, f.first_prediction_ts
        FROM `{dataset('predictions')}.bgg_predictions` p
        LEFT JOIN `{dataset('predictions')}.game_first_prediction` f USING (game_id)
        WHERE p.game_id = @game_id
        """,
        game_id,
    )
    return rows[0] if rows else None


def get_embedding(game_id: int, client: Optional[bigquery.Client] = None) -> Optional[dict[str, Any]]:
    """UMAP/PCA coordinates for the game. ``None`` if not embedded."""
    client = client or get_client()
    rows = _rows(
        client,
        f"SELECT * FROM `{dataset('predictions')}.bgg_game_coordinates` "
        "WHERE game_id = @game_id",
        game_id,
    )
    return rows[0] if rows else None


def get_similar(game_id: int, n: int = 10, client: Optional[bigquery.Client] = None) -> list[dict[str, Any]]:
    """Nearest neighbours by cosine distance over the game's embedding."""
    client = client or get_client()
    sql = f"""
    WITH target AS (
      SELECT embedding
      FROM `{dataset('analytics')}.game_similarity_search`
      WHERE game_id = @game_id
    )
    SELECT
      s.game_id,
      s.name,
      s.year_published,
      ML.DISTANCE(s.embedding, (SELECT embedding FROM target), 'COSINE') AS distance
    FROM `{dataset('analytics')}.game_similarity_search` s
    WHERE s.game_id != @game_id
      AND (SELECT embedding FROM target) IS NOT NULL
    ORDER BY distance ASC
    LIMIT @n
    """
    return _rows(
        client, sql, game_id,
        extra_params=[bigquery.ScalarQueryParameter("n", "INT64", n)],
    )


def get_provenance(game_id: int, client: Optional[bigquery.Client] = None) -> Optional[dict[str, Any]]:
    """Fetch/load metadata — when the warehouse last pulled this game from BGG."""
    client = client or get_client()
    rows = _rows(
        client,
        f"SELECT * FROM `{dataset('raw')}.fetched_responses` "
        "WHERE game_id = @game_id LIMIT 1",
        game_id,
    )
    return rows[0] if rows else None


def get_game(game_id: int, client: Optional[bigquery.Client] = None) -> Optional[dict[str, Any]]:
    """Compose the full game document. ``None`` when the game has no features row."""
    client = client or get_client()
    features = get_features(game_id, client=client)
    if features is None:
        return None
    return {
        "game_id": game_id,
        "features": features,
        "predictions": get_predictions(game_id, client=client),
        "embedding": get_embedding(game_id, client=client),
        "similar": get_similar(game_id, client=client),
        "provenance": get_provenance(game_id, client=client),
    }

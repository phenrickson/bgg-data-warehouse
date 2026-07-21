"""Reader for the ``games`` resource of the warehouse read API.

Pure query functions over BigQuery — one per block plus a ``get_game`` aggregator.
Every function accepts an optional ``client`` for dependency injection (tests pass a
fake), and parameterizes ``game_id`` via ``ScalarQueryParameter`` (never string
interpolation). Tables are resolved through ``src.warehouse.bq.dataset`` so no
project/dataset is hard-coded.
"""

from concurrent.futures import ThreadPoolExecutor
from typing import Any, Optional

from google.cloud import bigquery

from src.warehouse.bq import dataset, get_client

# Columns are listed explicitly rather than `SELECT *`. These serving tables are not
# clustered on game_id, so a query scans every row of every column it names — and a
# star-select would silently grow more expensive each time a column is added upstream.
# The lists below reproduce the tables' current shape exactly (no contract change);
# `description` stays included, since dropping it is a ~37% saving that clustering
# would make you want to reverse.
FEATURE_COLUMNS = [
    "game_id", "name", "year_published",
    "bayes_average", "average_rating", "average_weight", "users_rated",
    "hurdle", "geek_rating", "complexity", "rating", "log_users_rated", "num_weights",
    "min_players", "max_players", "min_playtime", "max_playtime", "min_age",
    "image", "thumbnail", "description",
    "categories", "mechanics", "publishers", "designers", "artists", "families",
    "load_timestamp", "last_updated",
]

PLAYER_COUNT_COLUMNS = [
    "game_id", "name", "player_count",
    "best_votes", "recommended_votes", "not_recommended_votes", "total_votes",
    "best_percentage", "recommended_percentage",
]

EMBEDDING_COLUMNS = [
    "game_id", "umap_1", "umap_2", "pca_1", "pca_2",
    "embedding_model", "embedding_version", "created_ts",
]

PROVENANCE_COLUMNS = ["record_id", "game_id", "fetch_timestamp", "fetch_status"]


def _rows(client: bigquery.Client, sql: str, game_id: int, extra_params=None) -> list[dict]:
    params = [bigquery.ScalarQueryParameter("game_id", "INT64", game_id)]
    if extra_params:
        params.extend(extra_params)
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    return [dict(row) for row in client.query(sql, job_config=job_config).result()]


def get_feature_row(game_id: int, client: Optional[bigquery.Client] = None) -> Optional[dict[str, Any]]:
    """The game's row from ``games_features`` (no player counts). ``None`` if unknown."""
    client = client or get_client()
    rows = _rows(
        client,
        f"SELECT {', '.join(FEATURE_COLUMNS)} FROM `{dataset('analytics')}.games_features` "
        "WHERE game_id = @game_id LIMIT 1",
        game_id,
    )
    return rows[0] if rows else None


def get_player_counts(game_id: int, client: Optional[bigquery.Client] = None) -> list[dict[str, Any]]:
    """Per-player-count recommendation rows.

    Split out from :func:`get_features` so ``/players`` can be served without scanning
    ``games_features`` at all.
    """
    client = client or get_client()
    return _rows(
        client,
        f"SELECT {', '.join(PLAYER_COUNT_COLUMNS)} "
        f"FROM `{dataset('analytics')}.player_count_recommendations` "
        "WHERE game_id = @game_id ORDER BY player_count",
        game_id,
    )


def get_features(game_id: int, client: Optional[bigquery.Client] = None) -> Optional[dict[str, Any]]:
    """Game record + per-player-count recommendations. ``None`` if the game is unknown."""
    client = client or get_client()
    features = get_feature_row(game_id, client=client)
    if features is None:
        return None
    features["player_counts"] = get_player_counts(game_id, client=client)
    return features


def get_predictions(game_id: int, client: Optional[bigquery.Client] = None) -> Optional[dict[str, Any]]:
    """Latest prediction row plus ``first_prediction_ts``.

    ``bgg_predictions`` holds one (latest) row per game and is year-filtered, so older
    games legitimately have no row. Full time-series history would read
    ``ml_predictions_landing`` and is deferred to a later slice.

    This is the one query that keeps a (qualified) ``p.*``: the column set is owned by
    the ML pipeline and grows when new model outputs are added, so enumerating it here
    would silently drop new predictions. At ~6 MB it is also the cheapest query, so
    there is nothing to gain by pinning it.
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
        f"SELECT {', '.join(EMBEDDING_COLUMNS)} "
        f"FROM `{dataset('predictions')}.bgg_game_coordinates` "
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
        f"SELECT {', '.join(PROVENANCE_COLUMNS)} "
        f"FROM `{dataset('raw')}.fetched_responses` "
        "WHERE game_id = @game_id LIMIT 1",
        game_id,
    )
    return rows[0] if rows else None


def get_game(game_id: int, client: Optional[bigquery.Client] = None) -> Optional[dict[str, Any]]:
    """Compose the full game document. ``None`` when the game has no features row.

    The six block queries are issued **concurrently**, so wall-clock latency is the
    slowest query rather than the sum of all six. ``bigquery.Client`` is thread-safe for
    query submission.

    Trade-off: there is no early short-circuit any more, so an unknown game costs all
    six queries instead of one. Misses are rare; the common path is what matters.
    """
    client = client or get_client()
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {
            "features": pool.submit(get_feature_row, game_id, client),
            "player_counts": pool.submit(get_player_counts, game_id, client),
            "predictions": pool.submit(get_predictions, game_id, client),
            "embedding": pool.submit(get_embedding, game_id, client),
            "similar": pool.submit(get_similar, game_id, 10, client),
            "provenance": pool.submit(get_provenance, game_id, client),
        }
        results = {key: future.result() for key, future in futures.items()}

    features = results["features"]
    if features is None:
        return None
    features["player_counts"] = results["player_counts"]
    return {
        "game_id": game_id,
        "features": features,
        "predictions": results["predictions"],
        "embedding": results["embedding"],
        "similar": results["similar"],
        "provenance": results["provenance"],
    }

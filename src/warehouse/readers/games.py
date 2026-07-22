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

    ``bgg_predictions`` already joins ``game_first_prediction`` itself, so it carries
    ``first_prediction_ts`` (and ``is_new_1d``/``is_new_7d``) — joining it again here
    both duplicated the field and referenced a second table for nothing (BigQuery bills
    a 10 MB minimum *per table*).

    This is the one query that keeps ``SELECT *``: the column set is owned by the ML
    pipeline and grows when new model outputs are added, so enumerating it would
    silently drop new predictions. At ~6 MB there is nothing to gain by pinning it.
    """
    client = client or get_client()
    rows = _rows(
        client,
        f"SELECT * FROM `{dataset('predictions')}.bgg_predictions` WHERE game_id = @game_id",
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


# ML.DISTANCE needs literals for the metric, and the embedding column name is chosen by
# `dims` — neither can be a query parameter, so both are allow-listed before being
# interpolated. Never interpolate these values unvalidated.
DISTANCE_METRICS = {"COSINE", "EUCLIDEAN", "DOT_PRODUCT"}
EMBEDDING_DIMS = {8: "embedding_8", 16: "embedding_16", 32: "embedding_32", 64: "embedding"}

# The default profile's semantics, mirrored from definitions/game_neighbors.sqlx. Used
# only to fill gaps when a caller tunes *some* parameters.
DEFAULT_MIN_RATINGS = 100
DEFAULT_COMPLEXITY_BAND = 0.75
DEFAULT_TOP_K = 10


def get_similar(
    game_id: int,
    *,
    profile: str = "default",
    n: Optional[int] = None,
    band: Optional[float] = None,
    metric: Optional[str] = None,
    min_ratings: Optional[int] = None,
    dims: Optional[int] = None,
    client: Optional[bigquery.Client] = None,
) -> list[dict[str, Any]]:
    """Nearest neighbours, filtered the way the front-end filters them.

    With no tuning parameters this reads the **precomputed** ``game_neighbors`` table
    (one partitioned lookup). Supplying any of ``n``/``band``/``metric``/
    ``min_ratings``/``dims`` falls through to the **live** query over
    ``game_similarity_search``. Both paths apply the same filters — the precomputed
    table is a materialized cache of one parameter set, not different behaviour.
    """
    client = client or get_client()
    if all(v is None for v in (n, band, metric, min_ratings, dims)):
        return _similar_precomputed(game_id, profile, client)
    return _similar_live(
        game_id,
        n=n or DEFAULT_TOP_K,
        band=DEFAULT_COMPLEXITY_BAND if band is None else band,
        metric=(metric or "COSINE").upper(),
        min_ratings=DEFAULT_MIN_RATINGS if min_ratings is None else min_ratings,
        dims=dims or 64,
        client=client,
    )


def _similar_precomputed(game_id: int, profile: str, client: bigquery.Client) -> list[dict[str, Any]]:
    rows = _rows(
        client,
        f"SELECT similar FROM `{dataset('analytics')}.game_neighbors` "
        "WHERE profile = @profile AND game_id = @game_id",
        game_id,
        extra_params=[bigquery.ScalarQueryParameter("profile", "STRING", profile)],
    )
    return [dict(s) for s in rows[0]["similar"]] if rows else []


def _similar_live(
    game_id: int, *, n: int, band: float, metric: str, min_ratings: int,
    dims: int, client: bigquery.Client,
) -> list[dict[str, Any]]:
    if metric not in DISTANCE_METRICS:
        raise ValueError(f"unsupported distance metric: {metric!r}")
    if dims not in EMBEDDING_DIMS:
        raise ValueError(f"unsupported embedding dims: {dims!r}")
    column = EMBEDDING_DIMS[dims]

    # Filter first, then rank — the candidate set is source-relative, which is exactly
    # why an unfiltered global ranking gives different (and worse) results.
    sql = f"""
    WITH src AS (
      SELECT complexity, {column} AS embedding
      FROM `{dataset('analytics')}.game_similarity_search`
      WHERE game_id = @game_id
    )
    SELECT
      s.game_id,
      s.name,
      s.year_published,
      ML.DISTANCE(s.{column}, src.embedding, '{metric}') AS distance
    FROM `{dataset('analytics')}.game_similarity_search` s, src
    WHERE s.game_id != @game_id
      AND s.users_rated >= @min_ratings
      AND s.complexity BETWEEN src.complexity - @band AND src.complexity + @band
    ORDER BY distance ASC
    LIMIT @n
    """
    return _rows(
        client, sql, game_id,
        extra_params=[
            bigquery.ScalarQueryParameter("n", "INT64", n),
            bigquery.ScalarQueryParameter("band", "FLOAT64", band),
            bigquery.ScalarQueryParameter("min_ratings", "INT64", min_ratings),
        ],
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


def _profile_row(game_id: int, client: bigquery.Client) -> Optional[dict[str, Any]]:
    # SELECT * is deliberate here: game_profile is RANGE-partitioned on game_id, so this
    # reads a single partition (~2MB) and we want the whole row. The no-star rule applies
    # to the per-block readers, which scan unclustered tables.
    rows = _rows(
        client,
        f"SELECT * FROM `{dataset('analytics')}.game_profile` WHERE game_id = @game_id",
        game_id,
    )
    return rows[0] if rows else None


def get_game(
    game_id: int,
    client: Optional[bigquery.Client] = None,
    profile: str = "default",
) -> Optional[dict[str, Any]]:
    """Compose the full game document. ``None`` when the game has no profile row.

    Reads the pre-joined ``game_profile`` row plus the precomputed neighbour list —
    two partitioned lookups (~21MB) rather than the old six-table fan-out (~361MB).
    The two run concurrently so latency stays at roughly one query.
    """
    client = client or get_client()
    with ThreadPoolExecutor(max_workers=2) as pool:
        profile_f = pool.submit(_profile_row, game_id, client)
        similar_f = pool.submit(_similar_precomputed, game_id, profile, client)
        row = profile_f.result()
        similar = similar_f.result()

    if row is None:
        return None

    features = dict(row)
    # Nested blocks live alongside the feature columns in the table; lift them out so
    # the response shape matches what the API has always returned.
    predictions = features.pop("predictions", None)
    embedding = features.pop("embedding", None)
    provenance = features.pop("provenance", None)
    features["player_counts"] = [dict(pc) for pc in (features.pop("player_counts", None) or [])]

    return {
        "game_id": game_id,
        "features": features,
        "predictions": dict(predictions) if predictions is not None else None,
        "embedding": dict(embedding) if embedding is not None else None,
        "similar": similar,
        "provenance": dict(provenance) if provenance is not None else None,
    }

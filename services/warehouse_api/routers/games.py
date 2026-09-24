"""Games resource router.

Thin HTTP shell over ``src.warehouse.readers.games``. Existence is defined by the
features row: endpoints that require the game to exist return 404 when it doesn't;
optional blocks (predictions, embedding, provenance) return 200 with a possibly-null
body since a real game may simply not have that block yet.
"""

from fastapi import APIRouter, HTTPException, Query

from src.warehouse.readers import games as reader

router = APIRouter(prefix="/games", tags=["games"])


def _require(value, game_id: int):
    if value is None:
        raise HTTPException(status_code=404, detail=f"game {game_id} not found")
    return value


@router.get("/{game_id}")
def get_game(game_id: int, profile: str = "similar"):
    """Full game document (features + predictions + embedding + similar + provenance).

    ``similar_profiles`` carries all three precomputed neighbour lists
    (``similar`` | ``recommender`` | ``sicko``) so the front-end can switch between them
    without a refetch; a profile with no list for this game is an empty array. The
    top-level ``similar`` block mirrors one of them, chosen by ``profile`` (default
    ``similar``), and is kept for back-compat. An unknown ``profile`` is a 400.
    """
    try:
        return _require(reader.get_game(game_id, profile=profile), game_id)
    except ValueError as exc:  # unknown profile — caller error, not a bug
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/{game_id}/features")
def get_features(game_id: int):
    return _require(reader.get_features(game_id), game_id)


@router.get("/{game_id}/players")
def get_players(game_id: int):
    """Per-player-count recommendations.

    Reads the player-count table directly rather than the whole features row, so this
    endpoint doesn't pay for a ``games_features`` scan it never uses. Returns an empty
    list for an unknown game.
    """
    return reader.get_player_counts(game_id)


@router.get("/{game_id}/predictions")
def get_predictions(game_id: int):
    return reader.get_predictions(game_id)


@router.get("/{game_id}/embedding")
def get_embedding(game_id: int):
    return reader.get_embedding(game_id)


@router.get("/{game_id}/similar")
def get_similar(
    game_id: int,
    profile: str = "similar",
    n: int | None = None,
    band: float | None = None,
    metric: str | None = None,
    min_ratings: int | None = None,
    dims: int | None = None,
    collection: str | None = None,
    year_min: int | None = None,
    ids: list[int] | None = Query(None),
):
    """Similar games.

    Three modes:

    - **Precomputed** (no other parameters): the named ``profile`` (``similar`` |
      ``sicko`` | ``recommender``, default ``similar``) from ``game_neighbors`` — one
      clustered lookup. Returns a list.
    - **Pooled** (any of ``collection``, ``year_min``, ``ids``): every profile's
      neighbours *within that pool*, computed live with exactly the ``game_neighbors``
      logic. Returns ``{profile: [rows]}``; ``profile`` is ignored. ~3 s.
    - **Tuned** (any of ``n``, ``band``, ``metric``, ``min_ratings``, ``dims``): a live
      distance ranking with a complexity band and ratings floor only — it does **not**
      apply the profile's rating blend, percentile filters or product-line cap. Returns
      a list.

    Pool and tuning parameters can't be combined (400).
    """
    pooled = collection is not None or year_min is not None or bool(ids)
    tuned = any(v is not None for v in (n, band, metric, min_ratings, dims))
    if pooled and tuned:
        raise HTTPException(
            status_code=400,
            detail="pool parameters (collection, year_min, ids) can't be combined with tuning parameters",
        )
    try:
        if pooled:
            return reader.get_similar_pooled(
                game_id, collection=collection, year_min=year_min, ids=ids,
            )
        return reader.get_similar(
            game_id, profile=profile, n=n, band=band,
            metric=metric, min_ratings=min_ratings, dims=dims,
        )
    except ValueError as exc:  # unknown profile / unsupported metric / dims — caller error
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/{game_id}/provenance")
def get_provenance(game_id: int):
    return reader.get_provenance(game_id)

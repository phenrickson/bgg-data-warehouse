"""Games resource router.

Thin HTTP shell over ``src.warehouse.readers.games``. Existence is defined by the
features row: endpoints that require the game to exist return 404 when it doesn't;
optional blocks (predictions, embedding, provenance) return 200 with a possibly-null
body since a real game may simply not have that block yet.
"""

from fastapi import APIRouter, HTTPException

from src.warehouse.readers import games as reader

router = APIRouter(prefix="/games", tags=["games"])


def _require(value, game_id: int):
    if value is None:
        raise HTTPException(status_code=404, detail=f"game {game_id} not found")
    return value


@router.get("/{game_id}")
def get_game(game_id: int):
    """Full game document (features + predictions + embedding + similar + provenance)."""
    return _require(reader.get_game(game_id), game_id)


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
def get_similar(game_id: int, n: int = 10):
    return reader.get_similar(game_id, n=n)


@router.get("/{game_id}/provenance")
def get_provenance(game_id: int):
    return reader.get_provenance(game_id)

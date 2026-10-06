# tests/test_dataform_tags.py
"""Every Dataform action carries exactly one pipeline tag (core or publish)."""

import re
from pathlib import Path

DEFINITIONS = Path(__file__).resolve().parents[1] / "definitions"

CORE = {
    "games_active", "games_features", "game_features_hash", "best_player_counts",
    "player_count_recommendations", "game_product_line", "user_collections",
    "filter_categories", "filter_mechanics", "filter_designers", "filter_publishers",
    "filter_options_combined", "game_dropdown_options",
}
PUBLISH = {
    "bgg_description_embeddings", "bgg_complexity_predictions", "game_first_prediction",
    "bgg_predictions", "bgg_game_embeddings", "bgg_game_coordinates",
    "user_collection_predictions", "game_similarity_search", "similarity_profiles",
    "game_neighbors", "game_profile", "deployed_models",
}


def _tags(sqlx: Path) -> list[str]:
    config = re.search(r"^config\s*\{(.*?)^\}", sqlx.read_text(encoding="utf-8"), re.S | re.M).group(1)
    match = re.search(r"\btags:\s*\[([^\]]*)\]", config)
    return re.findall(r'"([^"]+)"', match.group(1)) if match else []


def test_every_action_is_listed_once():
    names = {p.stem for p in DEFINITIONS.glob("*.sqlx")}
    assert CORE.isdisjoint(PUBLISH)
    assert names == CORE | PUBLISH


def test_every_action_has_its_one_tag():
    wrong = {}
    for sqlx in DEFINITIONS.glob("*.sqlx"):
        expected = ["core"] if sqlx.stem in CORE else ["publish"]
        if _tags(sqlx) != expected:
            wrong[sqlx.stem] = _tags(sqlx)
    assert wrong == {}

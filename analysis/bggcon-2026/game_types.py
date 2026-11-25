"""Types of game, from BGG's own mechanics and categories. A game can be several types, or none."""
from checkouts import _t

_GAMES = {str(r["game_id"]): (set(r["mechanics"] or []), set(r["categories"] or [])) for r in _t.select(["game_id", "mechanics", "categories"]).to_pylist()}

# type: (mechanics, categories); any one of them counts
TYPES = {
    "Worker placement": ({"Worker Placement", "Worker Placement with Dice Workers", "Worker Placement, Different Worker Types"}, set()),
    "Deck builders": ({"Deck, Bag, and Pool Building"}, set()),
    "Co-op": ({"Cooperative Game"}, set()),
    "Economic": (set(), {"Economic"}),
    "Trains": (set(), {"Trains"}),  # see TRAIN_MECHANICS
    "Tile placement": ({"Tile Placement"}, set()),
    "Trick-taking": ({"Trick-taking"}, set()),
    "Party": (set(), {"Party Game"}),
    "Abstract": (set(), {"Abstract Strategy"}),
    "Wargames": (set(), {"Wargame"}),
}
# BGG's Trains category also covers any game set on a train; a train game has to build or run a railroad too
TRAIN_MECHANICS = {"Network and Route Building", "Connections", "Stock Holding", "Pick-up and Deliver"}


def is_type(game, name):
    mechanics, categories = _GAMES.get(game, (set(), set()))
    if name == "Trains":
        return "Trains" in categories and bool(mechanics & TRAIN_MECHANICS)
    wanted_mechanics, wanted_categories = TYPES[name]
    return bool(mechanics & wanted_mechanics or categories & wanted_categories)

"""Router tests for the warehouse API (reader mocked — no BigQuery)."""

from fastapi.testclient import TestClient

from services.warehouse_api.main import app
from services.warehouse_api.routers import games as games_router

client = TestClient(app)


def test_health():
    assert client.get("/health").json() == {"status": "ok"}


def test_get_game_ok(monkeypatch):
    monkeypatch.setattr(
        games_router.reader, "get_game",
        lambda game_id, client=None: {"game_id": game_id, "features": {"name": "Catan"}},
    )
    r = client.get("/games/13")
    assert r.status_code == 200
    assert r.json()["features"]["name"] == "Catan"


def test_get_game_missing_is_404(monkeypatch):
    monkeypatch.setattr(games_router.reader, "get_game", lambda game_id, client=None: None)
    assert client.get("/games/999999").status_code == 404


def test_predictions_sub_resource(monkeypatch):
    monkeypatch.setattr(
        games_router.reader, "get_predictions",
        lambda game_id, client=None: {"predicted_rating": 7.1},
    )
    r = client.get("/games/13/predictions")
    assert r.status_code == 200
    assert r.json()["predicted_rating"] == 7.1


def test_players_sub_resource(monkeypatch):
    """/players reads player counts directly — it must not scan games_features."""
    def _boom(*a, **k):
        raise AssertionError("/players must not read games_features")

    monkeypatch.setattr(games_router.reader, "get_features", _boom)
    monkeypatch.setattr(games_router.reader, "get_feature_row", _boom)
    monkeypatch.setattr(
        games_router.reader, "get_player_counts",
        lambda game_id, client=None: [{"player_count": "4"}],
    )
    r = client.get("/games/13/players")
    assert r.status_code == 200
    assert r.json() == [{"player_count": "4"}]


def test_similar_sub_resource(monkeypatch):
    seen = {}

    def fake(game_id, **kw):
        seen.update(kw)
        return [{"game_id": 21, "distance": 0.1}]

    monkeypatch.setattr(games_router.reader, "get_similar", fake)
    r = client.get("/games/13/similar")
    assert r.status_code == 200
    assert r.json()[0]["game_id"] == 21
    # untuned call passes no tuning params through
    assert all(seen[k] is None for k in ("n", "band", "metric", "min_ratings", "dims"))


def test_similar_passes_tuning_params(monkeypatch):
    seen = {}

    def fake(game_id, **kw):
        seen.update(kw)
        return []

    monkeypatch.setattr(games_router.reader, "get_similar", fake)
    r = client.get("/games/13/similar?n=25&band=0.5&metric=EUCLIDEAN&min_ratings=500&dims=32&profile=strict")
    assert r.status_code == 200
    assert seen["n"] == 25 and seen["band"] == 0.5 and seen["metric"] == "EUCLIDEAN"
    assert seen["min_ratings"] == 500 and seen["dims"] == 32 and seen["profile"] == "strict"


def test_similar_rejects_bad_metric(monkeypatch):
    def boom(game_id, **kw):
        raise ValueError("unsupported distance metric: 'NOPE'")

    monkeypatch.setattr(games_router.reader, "get_similar", boom)
    r = client.get("/games/13/similar?metric=NOPE")
    assert r.status_code == 400, "invalid tuning params should be 400, not 500"

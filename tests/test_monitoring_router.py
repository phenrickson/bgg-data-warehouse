"""Router tests for the monitoring resource (reader mocked — no BigQuery)."""

import pytest
from fastapi.testclient import TestClient

from services.warehouse_api.main import app
from services.warehouse_api.routers import monitoring as monitoring_router

client = TestClient(app)


@pytest.fixture(autouse=True)
def _clear_cache():
    monitoring_router._reset_cache()
    yield
    monitoring_router._reset_cache()


def test_new_games_defaults(monkeypatch):
    seen = {}

    def fake(days_back, limit):
        seen.update(days_back=days_back, limit=limit)
        return [{"game_id": 13, "name": "Catan"}]

    monkeypatch.setattr(monitoring_router.reader, "fetch_recently_added", fake)
    r = client.get("/new-games")
    assert r.status_code == 200
    assert r.json() == [{"game_id": 13, "name": "Catan"}]
    assert seen == {"days_back": 7, "limit": 20000}


def test_new_games_passes_days_and_limit(monkeypatch):
    seen = {}

    def fake(days_back, limit):
        seen.update(days_back=days_back, limit=limit)
        return []

    monkeypatch.setattr(monitoring_router.reader, "fetch_recently_added", fake)
    r = client.get("/new-games?days=30&limit=50")
    assert r.status_code == 200
    assert seen == {"days_back": 30, "limit": 50}


def test_new_games_rejects_out_of_range_days(monkeypatch):
    monkeypatch.setattr(
        monitoring_router.reader, "fetch_recently_added",
        lambda days_back, limit: [],
    )
    assert client.get("/new-games?days=0").status_code == 422
    assert client.get("/new-games?days=366").status_code == 422


def test_new_games_caches_repeat_calls_with_same_params(monkeypatch):
    calls = []

    def fake(days_back, limit):
        calls.append((days_back, limit))
        return [{"game_id": 13}]

    monkeypatch.setattr(monitoring_router.reader, "fetch_recently_added", fake)
    client.get("/new-games?days=7")
    client.get("/new-games?days=7")
    assert len(calls) == 1, "second call with identical params should hit the cache"


def test_new_games_does_not_cache_across_different_params(monkeypatch):
    calls = []

    def fake(days_back, limit):
        calls.append((days_back, limit))
        return [{"game_id": 13}]

    monkeypatch.setattr(monitoring_router.reader, "fetch_recently_added", fake)
    client.get("/new-games?days=7")
    client.get("/new-games?days=30")
    assert len(calls) == 2

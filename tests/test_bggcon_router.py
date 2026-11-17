"""Router tests for the BGG.CON analytics endpoints (reader mocked: no BigQuery, no GCS)."""

from datetime import timedelta

import pytest
from fastapi.testclient import TestClient

from services.warehouse_api.main import app
from services.warehouse_api.routers import bggcon as bggcon_router
from src.bggcon.analytics import CON_OPEN
from src.bggcon.recorder import WINDOW_START

client = TestClient(app)
AS_OF = CON_OPEN + timedelta(minutes=5)


def ev(copy, event, t):
    return {"copy_id": copy, "bgg_id": 13, "name": "Catan", "event": event, "observed_at": t,
            "lastcheckout_id": "co-1"}


@pytest.fixture(autouse=True)
def fake_reader(monkeypatch):
    bggcon_router._reset_cache()
    calls = {"events": 0}

    def events():
        calls["events"] += 1
        return [ev("c0", "out", WINDOW_START), ev("c1", "out", CON_OPEN)]

    monkeypatch.setattr(bggcon_router.reader, "fetch_events", events)
    monkeypatch.setattr(bggcon_router.reader, "fetch_as_of", lambda: AS_OF)
    monkeypatch.setattr(bggcon_router.reader, "fetch_copies", lambda: {13: 2})
    monkeypatch.setattr(bggcon_router.reader, "fetch_baseline", lambda: WINDOW_START)
    yield calls
    bggcon_router._reset_cache()


def test_activity_returns_the_buckets():
    r = client.get("/bggcon/activity")
    assert r.status_code == 200
    assert r.json()["as_of"] == AS_OF.isoformat()
    assert r.json()["buckets"][-1] == {"start": CON_OPEN.isoformat(), "checkouts": 1}


def test_games_uses_the_library_copies():
    assert client.get("/bggcon/games").json()["games"][0]["copies"] == 2


def test_games_without_a_library_file_still_answers(monkeypatch):
    monkeypatch.setattr(bggcon_router.reader, "fetch_copies", lambda: {})
    r = client.get("/bggcon/games")
    assert r.status_code == 200
    assert r.json()["games"][0]["copies"] is None


def test_results_are_cached_for_a_minute(fake_reader):
    client.get("/bggcon/games")
    client.get("/bggcon/games")
    assert fake_reader["events"] == 1


def test_the_first_runs_time_is_the_baseline_even_with_no_events_then(monkeypatch):
    # The first run found nothing out: the first event is a real checkout.
    monkeypatch.setattr(bggcon_router.reader, "fetch_events", lambda: [ev("c1", "out", CON_OPEN)])
    assert client.get("/bggcon/games").json()["games"][0]["checkouts"] == 1

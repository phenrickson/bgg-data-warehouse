"""Tests for the API-based ID probe.

Focus: the walk's stop condition, type filtering (the ID pool is shared with
RPGs and video games), and the refusal to treat a failed request as an absent
ID. See id_probe_fetcher.py.
"""

from unittest import mock

import pytest

from src.modules.id_probe_fetcher import ProbeIDFetcher


def _items(*items):
    """Build a parsed-XML response containing the given items."""
    return {"items": {"item": list(items)}}


def _item(game_id, item_type):
    return {"@id": str(game_id), "@type": item_type}


def _empty():
    """What xmltodict yields when a batch matches nothing."""
    return {"items": {"termsofuse": "https://boardgamegeek.com/xmlapi/termsofuse"}}


def _fetcher(responses, **kwargs):
    """Build a prober whose client returns the given responses in order."""
    client = mock.Mock()
    client.get_thing.side_effect = responses
    return ProbeIDFetcher(client=client, **kwargs), client


def test_probe_collects_board_game_types():
    responses = [
        _items(_item(100, "boardgame"), _item(101, "boardgameexpansion")),
        _items(_item(120, "boardgameaccessory")),
    ] + [_empty()] * 10
    fetcher, _ = _fetcher(responses, batch_size=20, stop_after_misses=40)

    found = fetcher.probe(start_id=100)

    assert found == [
        {"game_id": 100, "type": "boardgame"},
        {"game_id": 101, "type": "boardgameexpansion"},
        {"game_id": 120, "type": "boardgameaccessory"},
    ]


def test_probe_ignores_non_board_game_types():
    """The ID pool is shared with RPGs, video games and sleeves."""
    responses = [
        _items(
            _item(100, "rpgitem"),
            _item(101, "videogame"),
            _item(102, "bgsleeve"),
            _item(103, "boardgame"),
        )
    ] + [_empty()] * 10
    fetcher, _ = _fetcher(responses, batch_size=20, stop_after_misses=40)

    found = fetcher.probe(start_id=100)

    assert found == [{"game_id": 103, "type": "boardgame"}]


def test_probe_stops_after_consecutive_misses():
    responses = [_items(_item(100, "boardgame"))] + [_empty()] * 20
    fetcher, client = _fetcher(responses, batch_size=10, stop_after_misses=30)

    fetcher.probe(start_id=100)

    # one hit batch, then three empty batches (30 misses) to reach the threshold
    assert client.get_thing.call_count == 4


def test_probe_miss_streak_resets_on_a_hit():
    """A gap inside populated space must not end the walk."""
    responses = [
        _items(_item(100, "boardgame")),
        _empty(),
        _empty(),
        _items(_item(130, "boardgame")),  # gap survived
    ] + [_empty()] * 10
    fetcher, _ = _fetcher(responses, batch_size=10, stop_after_misses=30)

    found = fetcher.probe(start_id=100)

    assert [f["game_id"] for f in found] == [100, 130]


def test_probe_raises_on_failed_request():
    """A None response means the request failed, not that the IDs are absent."""
    fetcher, _ = _fetcher([None], batch_size=20, stop_after_misses=40)

    with pytest.raises(RuntimeError, match="API request failed"):
        fetcher.probe(start_id=100)


def test_probe_handles_single_item_response():
    """xmltodict collapses a lone <item> into a dict rather than a list."""
    responses = [{"items": {"item": _item(100, "boardgame")}}] + [_empty()] * 10
    fetcher, _ = _fetcher(responses, batch_size=20, stop_after_misses=40)

    found = fetcher.probe(start_id=100)

    assert found == [{"game_id": 100, "type": "boardgame"}]


def test_probe_requests_unfiltered_types():
    """A type filter would hide expansions and accessories from the probe."""
    fetcher, client = _fetcher([_empty()] * 5, batch_size=20, stop_after_misses=20)

    fetcher.probe(start_id=500)

    _, kwargs = client.get_thing.call_args
    assert kwargs["type_filter"] is None


def test_probe_respects_max_ids_cap():
    fetcher, client = _fetcher([_empty()] * 50, batch_size=10, stop_after_misses=1000)

    fetcher.probe(start_id=100, max_ids=30)

    assert client.get_thing.call_count == 3


# --- lookback: probing below the frontier -----------------------------------


def _requested_ids(client):
    """Every ID the client was asked for, in request order."""
    return [args[0] for args, _ in client.get_thing.call_args_list]


def test_probe_skips_known_ids():
    fetcher, client = _fetcher([_empty()] * 5, batch_size=5, stop_after_misses=10)

    fetcher.probe(start_id=100, frontier_id=100, skip=frozenset({101, 102, 107}))

    first, second = _requested_ids(client)[:2]
    assert first == [100, 103, 104, 105, 106]
    assert second == [108, 109, 110, 111, 112]


def test_probe_misses_below_frontier_do_not_stop():
    """Gaps below the frontier are expected; only frontier misses end the walk."""
    fetcher, client = _fetcher([_empty()] * 100, batch_size=10, stop_after_misses=30)

    fetcher.probe(start_id=1000, frontier_id=1200, max_ids=400)

    ids = _requested_ids(client)
    # 200 empty IDs below the frontier (20 batches) must not trip the 30-miss stop;
    # the walk must reach the frontier and then stop after 30 misses above it.
    assert ids[0][0] == 1000
    assert ids[-1][-1] >= 1200 + 30 - 1
    assert ids[-1][-1] < 1200 + 30 + 10


def test_probe_stops_after_misses_above_frontier():
    fetcher, client = _fetcher([_empty()] * 20, batch_size=10, stop_after_misses=30)

    fetcher.probe(start_id=100, frontier_id=100)

    assert client.get_thing.call_count == 3


def test_probe_lookback_zero_matches_current_behaviour():
    """start == frontier, no skip: the request sequence is today's frontier walk."""
    responses = [_items(_item(100, "boardgame")), _empty(), _empty(), _empty()]
    fetcher, client = _fetcher(responses, batch_size=10, stop_after_misses=30)

    found = fetcher.probe(start_id=100, frontier_id=100)

    assert _requested_ids(client) == [
        list(range(100, 110)),
        list(range(110, 120)),
        list(range(120, 130)),
        list(range(130, 140)),
    ]
    assert found == [{"game_id": 100, "type": "boardgame"}]


def test_probe_frontier_defaults_to_start():
    fetcher, client = _fetcher([_empty()] * 20, batch_size=10, stop_after_misses=30)

    fetcher.probe(start_id=100)

    assert client.get_thing.call_count == 3

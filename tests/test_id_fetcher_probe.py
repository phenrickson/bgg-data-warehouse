"""Tests for IDFetcher's probe window: how lookback becomes a start, a
frontier and a skip set before ProbeIDFetcher is asked to walk."""

from unittest import mock

import pytest

from src.modules.id_fetcher import SOURCE_PROBE, IDFetcher


@pytest.fixture
def fetcher():
    config = {"project": {"id": "test-project"}}
    with mock.patch("src.modules.id_fetcher.get_bigquery_config", return_value=config), \
         mock.patch("src.modules.id_fetcher.bigquery.Client"):
        yield IDFetcher()


def _probe_mock():
    """Patch ProbeIDFetcher so probe() records its call and returns nothing."""
    instance = mock.Mock()
    instance.probe.return_value = []
    return mock.patch(
        "src.modules.id_probe_fetcher.ProbeIDFetcher", return_value=instance
    ), instance


def test_fetch_via_probe_window(fetcher):
    patcher, probe = _probe_mock()
    with mock.patch.object(fetcher, "get_max_game_id", return_value=479570), \
         mock.patch.object(fetcher, "_known_ids_from", return_value=frozenset({479100, 479570})) as known, \
         patcher:
        fetcher._fetch_via_probe(lookback=500)

    known.assert_called_once_with(479070)
    probe.probe.assert_called_once_with(
        479070, frontier_id=479570, skip=frozenset({479100, 479570})
    )


def test_fetch_via_probe_lookback_zero_is_frontier_walk(fetcher):
    """lookback=0 must be today's call exactly: start above the max, no skip."""
    patcher, probe = _probe_mock()
    with mock.patch.object(fetcher, "get_max_game_id", return_value=479570), \
         mock.patch.object(fetcher, "_known_ids_from") as known, \
         patcher:
        fetcher._fetch_via_probe(lookback=0)

    known.assert_not_called()
    probe.probe.assert_called_once_with(479571)


def test_run_threads_lookback(fetcher):
    with mock.patch.object(fetcher, "_fetch_via_probe", return_value=[]) as via_probe:
        fetcher.run(source=SOURCE_PROBE, lookback=500)

    via_probe.assert_called_once_with(lookback=500)


def test_known_ids_from_queries_floor(fetcher):
    row = mock.Mock(); row.game_id = 479100
    fetcher.client.query.return_value.result.return_value = [row]

    ids = fetcher._known_ids_from(479070)

    assert ids == frozenset({479100})
    _, kwargs = fetcher.client.query.call_args
    params = kwargs["job_config"].query_parameters
    assert params[0].name == "floor" and params[0].value == 479070

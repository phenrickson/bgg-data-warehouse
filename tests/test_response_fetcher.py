"""Unit tests for ResponseFetcher.store_response."""

from unittest import mock

import pytest

from src.modules.response_fetcher import ResponseFetcher


def _response(*game_ids):
    """A BGG thing response in the shape store_response parses."""
    items = [{"@id": str(gid), "name": f"game {gid}"} for gid in game_ids]
    return str({"items": {"item": items}})


@pytest.fixture
def fetcher():
    with mock.patch("src.modules.response_fetcher.get_bigquery_config",
                    return_value={"project": {"id": "test-project"}}), \
         mock.patch("src.modules.response_fetcher.BGGAPIClient"), \
         mock.patch("src.modules.response_fetcher.bigquery.Client"):
        f = ResponseFetcher()
    f.bq_client = mock.Mock()
    f.bq_client.insert_rows_json.return_value = []
    return f


def _inserts(fetcher):
    """Rows passed to insert_rows_json, keyed by table id."""
    return {c.args[0]: c.args[1] for c in fetcher.bq_client.insert_rows_json.call_args_list}


def test_tracking_rows_reuse_the_raw_record_id(fetcher):
    fetcher.store_response([1, 2], _response(1, 2), no_response_ids=[3])

    inserts = _inserts(fetcher)
    raw = inserts["test-project.raw.raw_responses"]
    tracked = inserts["test-project.raw.fetched_responses"]

    raw_ids = {r["game_id"]: r["record_id"] for r in raw}
    assert set(raw_ids) == {1, 2, 3}
    assert len(set(raw_ids.values())) == 3
    assert {r["game_id"]: r["record_id"] for r in tracked} == raw_ids
    assert {r["game_id"]: r["fetch_status"] for r in tracked} == {
        1: "success", 2: "success", 3: "no_response",
    }


def test_no_per_game_record_id_lookup(fetcher):
    fetcher.store_response([1, 2], _response(1, 2))

    queries = [c.args[0] for c in fetcher.bq_client.query.call_args_list]
    assert not any("SELECT record_id" in q for q in queries)
    # Only the fetch_in_progress cleanup remains.
    assert len(queries) == 1 and "DELETE FROM" in queries[0]


def test_rows_rejected_by_raw_insert_are_not_tracked(fetcher):
    fetcher.bq_client.insert_rows_json.side_effect = [
        [{"index": 1, "errors": [{"reason": "invalid"}]}],  # raw_responses
        [],  # fetched_responses
    ]

    fetcher.store_response([1, 2], _response(1, 2))

    tracked = _inserts(fetcher)["test-project.raw.fetched_responses"]
    assert [r["game_id"] for r in tracked] == [1]

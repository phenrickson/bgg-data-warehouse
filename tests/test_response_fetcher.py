"""Unit tests for ResponseFetcher."""

import re
import sqlite3
from unittest import mock

import pandas as pd
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


# Fetch and processing history of game 1, as
# (record_id, fetched hours ago, fetch_status, [process_status, ...]),
# and whether it should be fetched.
CASES = {
    "never fetched": ([], True),
    "processed": ([("a", 240, "success", ["success"])], False),
    "fetched, not yet processed": ([("a", 240, "success", [])], False),
    "failed processing": ([("a", 240, "success", ["failed"])], True),
    "error in processing": ([("a", 240, "success", ["error"])], True),
    "failed processing recently": ([("a", 2, "success", ["failed"])], False),
    "refetched, not yet processed": (
        [("a", 240, "success", ["failed"]), ("b", 216, "success", [])], False),
    "refetched and processed": (
        [("a", 240, "success", ["failed"]), ("b", 216, "success", ["success"])], False),
    "processed, then a refresh failed": (
        [("a", 240, "success", ["success"]), ("b", 216, "success", ["failed"])], False),
    "failed processing twice": ([(r, 240, "success", ["failed"]) for r in "ab"], True),
    "failed processing three times": (
        [(r, 240, "success", ["failed"]) for r in "abc"], False),
    "one response recorded as failed many times": (
        [("a", 240, "success", ["failed"] * 6)], True),
    "unavailable twice": ([(r, 240, "no_response", []) for r in "ab"], True),
    "unavailable three times": ([(r, 240, "no_response", []) for r in "abc"], False),
}


def _selected_games(fetcher, history, game_ids):
    """Run the game selection query get_unfetched_ids sends, in SQLite."""
    fetcher.bq_client.query.return_value.to_dataframe.return_value = pd.DataFrame()
    fetcher.get_unfetched_ids(game_ids)
    # The first query is the fetch_in_progress cleanup
    sql = fetcher.bq_client.query.call_args_list[1].args[0]
    sql = re.sub(
        r"TIMESTAMP_SUB\(CURRENT_TIMESTAMP\(\), INTERVAL (\d+) (\w+)\)",
        r"datetime('now', '-\1 \2')", sql)
    sql = re.sub(r"UNNEST\(\[(\d+)\]\) AS game_id", r"(SELECT \1 AS game_id)", sql)

    db = sqlite3.connect(":memory:")
    raw = "test-project.raw"
    db.execute(f"CREATE TABLE `{raw}.thing_ids` (game_id, type)")
    db.execute(f"CREATE TABLE `{raw}.fetch_in_progress` (game_id)")
    db.execute(f"CREATE TABLE `{raw}.fetched_responses` "
               "(record_id, game_id, fetch_timestamp, fetch_status)")
    db.execute(f"CREATE TABLE `{raw}.processed_responses` (record_id, process_status)")
    db.execute(f"INSERT INTO `{raw}.thing_ids` VALUES (1, 'boardgame')")
    for record_id, hours, fetch_status, process_statuses in history:
        db.execute(f"INSERT INTO `{raw}.fetched_responses` "
                   "VALUES (?, 1, datetime('now', ?), ?)",
                   (record_id, f"-{hours} hours", fetch_status))
        for status in process_statuses:
            db.execute(f"INSERT INTO `{raw}.processed_responses` VALUES (?, ?)",
                       (record_id, status))
    return [row[0] for row in db.execute(sql.strip().rstrip(";"))]


@pytest.mark.parametrize("game_ids", [None, [1]], ids=["all", "specific"])
@pytest.mark.parametrize("history,selected", CASES.values(), ids=CASES.keys())
def test_which_games_are_fetched(fetcher, history, selected, game_ids):
    assert _selected_games(fetcher, history, game_ids) == ([1] if selected else [])

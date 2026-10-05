"""Tests for ResponseProcessor's batch selection.

Selecting a batch is split in two so the response_data blob is never scanned
table-wide: a narrow query picks the batch, then response_data is fetched for
just those records, pruned by partition (fetch_timestamp) and cluster (game_id).
"""

from datetime import datetime, UTC
from unittest import mock

import pytest

from src.modules.response_processor import ResponseProcessor


@pytest.fixture
def processor():
    config = {"project": {"id": "test-project"}}
    with mock.patch("src.modules.response_processor.get_bigquery_config", return_value=config), \
         mock.patch("src.modules.response_processor.bigquery.Client"), \
         mock.patch("src.modules.response_processor.BGGDataProcessor"), \
         mock.patch("src.modules.response_processor.BigQueryLoader"):
        yield ResponseProcessor(batch_size=1000)


def _job(rows):
    job = mock.Mock()
    job.result.return_value = rows
    return job


T1 = datetime(2026, 10, 1, 6, 0, tzinfo=UTC)
T2 = datetime(2026, 10, 2, 6, 0, tzinfo=UTC)

SELECTED = [
    {"record_id": "r-old", "game_id": 13, "fetch_timestamp": T1},
    {"record_id": "r-new", "game_id": 822, "fetch_timestamp": T2},
]
FETCHED = [
    # Returned in a different order from the selection on purpose
    {"record_id": "r-new", "response_data": "{'items': {'item': {'@id': '822'}}}"},
    {"record_id": "r-old", "response_data": '{"items": {"item": {"@id": "13"}}}'},
]


def test_selection_query_does_not_read_response_data(processor):
    processor.bq_client.query.side_effect = [_job(SELECTED), _job(FETCHED)]

    processor.get_unprocessed_responses()

    select_sql = processor.bq_client.query.call_args_list[0].args[0]
    assert "response_data" not in select_sql
    assert "LIMIT 1000" in select_sql


def test_fetch_is_pruned_to_the_selected_batch(processor):
    processor.bq_client.query.side_effect = [_job(SELECTED), _job(FETCHED)]

    processor.get_unprocessed_responses()

    fetch_call = processor.bq_client.query.call_args_list[1]
    fetch_sql = fetch_call.args[0]
    assert "fetch_timestamp BETWEEN @min_ts AND @max_ts" in fetch_sql
    assert "game_id IN UNNEST(@game_ids)" in fetch_sql
    assert "record_id IN UNNEST(@record_ids)" in fetch_sql

    params = {p.name: p for p in fetch_call.kwargs["job_config"].query_parameters}
    assert params["record_ids"].values == ["r-old", "r-new"]
    assert params["game_ids"].values == [13, 822]
    assert params["min_ts"].value == T1
    assert params["max_ts"].value == T2


def test_responses_keep_selection_order_and_parse(processor):
    processor.bq_client.query.side_effect = [_job(SELECTED), _job(FETCHED)]

    responses = processor.get_unprocessed_responses()

    assert [r["record_id"] for r in responses] == ["r-old", "r-new"]
    assert responses[0] == {
        "record_id": "r-old",
        "game_id": 13,
        "response_data": {"items": {"item": {"@id": "13"}}},
        "fetch_timestamp": T1,
    }
    # Python-repr payloads still parse via the literal_eval fallback
    assert responses[1]["response_data"] == {"items": {"item": {"@id": "822"}}}


def test_empty_selection_skips_the_fetch(processor):
    processor.bq_client.query.side_effect = [_job([])]

    assert processor.get_unprocessed_responses() == []
    assert processor.bq_client.query.call_count == 1


def test_record_missing_from_fetch_is_skipped_not_marked(processor):
    processor.bq_client.query.side_effect = [_job(SELECTED), _job(FETCHED[:1])]

    responses = processor.get_unprocessed_responses()

    assert [r["record_id"] for r in responses] == ["r-new"]
    # Not written to processed_responses, so it is picked up again next batch
    processor.bq_client.insert_rows_json.assert_not_called()


def test_empty_response_data_is_marked_no_response(processor):
    blank = [{"record_id": "r-old", "response_data": "  "}, FETCHED[0]]
    processor.bq_client.query.side_effect = [_job(SELECTED), _job(blank)]

    responses = processor.get_unprocessed_responses()

    assert [r["record_id"] for r in responses] == ["r-new"]
    row = processor.bq_client.insert_rows_json.call_args.args[1][0]
    assert row["record_id"] == "r-old"
    assert row["process_status"] == "no_response"

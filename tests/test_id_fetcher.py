"""Tests for the BGG ID fetcher."""

import os
from pathlib import Path
from unittest import mock

import pandas as pd
import pytest
from google.cloud import bigquery

from src.id_fetcher.fetcher import BGGIDFetcher


@pytest.fixture
def mock_config():
    """Create mock configuration."""
    return {
        "project": {"id": "test-project"},
        "datasets": {"raw": "test_raw"},
        "raw_tables": {
            "thing_ids": {
                "name": "thing_ids",
                "description": "Game IDs from BGG with processing status",
            }
        },
    }


@pytest.fixture
def fetcher(mock_config):
    """Create ID fetcher with mocked configuration."""
    with mock.patch("src.id_fetcher.fetcher.get_bigquery_config", return_value=mock_config):
        fetcher = BGGIDFetcher()
        yield fetcher


def test_fetch_ids(fetcher):
    """Test fetching IDs from API."""
    mock_response = {
        "items": {
            "item": [
                {"@type": "boardgame", "@id": "13"},
                {"@type": "boardgame", "@id": "14"},
                {"@type": "boardgame", "@id": "15"},
            ]
        }
    }

    with mock.patch.object(fetcher.api_client, "get_thing", return_value=mock_response):
        ids = fetcher.fetch_ids()

        assert len(ids) == 3
        assert ids[0]["game_id"] == 13
        assert ids[1]["game_id"] == 14
        assert ids[2]["game_id"] == 15
        assert all(game["type"] == "boardgame" for game in ids)


def test_fetch_ids_error(fetcher):
    """Test handling API errors when fetching IDs."""
    with mock.patch.object(fetcher.api_client, "get_thing", return_value=None):
        ids = fetcher.fetch_ids()
        assert len(ids) == 0


def test_fetch_ids_empty_response(fetcher):
    """Test handling empty API response."""
    mock_response = {"items": {"item": []}}

    with mock.patch.object(fetcher.api_client, "get_thing", return_value=mock_response):
        ids = fetcher.fetch_ids()
        assert len(ids) == 0  # Should return empty list for empty response


def test_get_existing_ids(fetcher):
    """Test fetching existing IDs from BigQuery."""
    mock_query_result = mock.Mock()
    mock_query_result.to_dataframe.return_value = pd.DataFrame(
        {"game_id": [13, 14, 15], "type": ["boardgame", "boardgame", "boardgame"]}
    )

    with mock.patch.object(fetcher.client, "query", return_value=mock_query_result):
        existing_ids = fetcher.get_existing_ids()

        assert len(existing_ids) == 3
        assert (13, "boardgame") in existing_ids
        assert (15, "boardgame") in existing_ids


def test_get_existing_ids_error(fetcher):
    """Test handling errors when fetching existing IDs."""
    with mock.patch.object(fetcher.client, "query", side_effect=Exception("Query failed")):
        existing_ids = fetcher.get_existing_ids()
        assert len(existing_ids) == 0


def test_upload_new_ids(fetcher):
    """Test uploading new IDs to BigQuery."""
    new_games = [
        {"game_id": 16, "type": "boardgame"},
        {"game_id": 17, "type": "boardgame"},
        {"game_id": 18, "type": "boardgame"},
    ]

    # Mock BigQuery operations
    mock_job = mock.Mock()
    mock_job.result = mock.Mock()
    mock_query_job = mock.Mock()
    mock_query_job.result = mock.Mock()

    with mock.patch.object(fetcher.client, "load_table_from_dataframe") as mock_load:
        with mock.patch.object(fetcher.client, "query") as mock_query:
            with mock.patch.object(fetcher.client, "delete_table") as mock_delete:
                mock_load.return_value = mock_job
                mock_query.return_value = mock_query_job
                fetcher.upload_new_ids(new_games)

                # Check that load_table_from_dataframe was called
                mock_load.assert_called_once()

                # Verify the data being uploaded
                df = mock_load.call_args[0][0]  # First positional argument
                assert len(df) == len(new_games)
                assert df["game_id"].tolist() == [16, 17, 18]
                assert all(t == "boardgame" for t in df["type"])
                assert all(not processed for processed in df["processed"])
                assert all(ts is None for ts in df["process_timestamp"])

                # Should execute merge query
                mock_query.assert_called_once()

                # Should clean up temp table
                mock_delete.assert_called_once()


def test_upload_new_ids_empty(fetcher):
    """Test uploading empty list of games."""
    with mock.patch.object(fetcher.client, "load_table_from_dataframe") as mock_load:
        fetcher.upload_new_ids([])

        # Should not attempt to upload empty data
        mock_load.assert_not_called()


def test_upload_new_ids_error(fetcher):
    """Test handling upload errors."""
    new_games = [
        {"game_id": 16, "type": "boardgame"},
        {"game_id": 17, "type": "boardgame"},
        {"game_id": 18, "type": "boardgame"},
    ]

    with mock.patch.object(
        fetcher.client, "load_table_from_dataframe", side_effect=Exception("Upload failed")
    ):
        with pytest.raises(Exception):
            fetcher.upload_new_ids(new_games)


def test_update_ids_integration(fetcher):
    """Test the complete ID update process."""
    # Mock API response
    mock_api_response = {
        "items": {
            "item": [
                {"@type": "boardgame", "@id": "13"},
                {"@type": "boardgame", "@id": "14"},
                {"@type": "boardgame", "@id": "15"},
            ]
        }
    }

    # Mock existing IDs in BigQuery
    mock_query_result = mock.Mock()
    mock_query_result.to_dataframe.return_value = pd.DataFrame(
        {"game_id": [13], "type": ["boardgame"]}
    )

    # Mock BigQuery operations
    mock_job = mock.Mock()
    mock_job.result = mock.Mock()

    with mock.patch.object(fetcher.api_client, "get_thing", return_value=mock_api_response):
        with mock.patch.object(fetcher.client, "query", return_value=mock_query_result):
            with mock.patch.object(fetcher.client, "load_table_from_dataframe") as mock_load:
                with mock.patch.object(fetcher.client, "delete_table") as mock_delete:
                    mock_load.return_value = mock_job
                    fetcher.update_ids()

                    # Should attempt to upload new IDs (14, 15)
                    mock_load.assert_called_once()
                    df = mock_load.call_args[0][0]
                    assert len(df) == 2
                    assert df["game_id"].tolist() == [14, 15]
                    assert all(t == "boardgame" for t in df["type"])

                    # Should clean up temp table
                    mock_delete.assert_called_once()


def test_update_ids_no_new_ids(fetcher):
    """Test update process when no new IDs are found."""
    # Mock API response with one game
    mock_api_response = {"items": {"item": [{"@type": "boardgame", "@id": "1"}]}}

    # Mock existing IDs in BigQuery with same game
    mock_query_result = mock.Mock()
    mock_query_result.to_dataframe.return_value = pd.DataFrame(
        {"game_id": [1], "type": ["boardgame"]}
    )

    with mock.patch.object(fetcher.api_client, "get_thing", return_value=mock_api_response):
        with mock.patch.object(fetcher.client, "query", return_value=mock_query_result):
            with mock.patch.object(fetcher.client, "load_table_from_dataframe") as mock_load:
                fetcher.update_ids()

                # Should not attempt to upload when no new IDs
                mock_load.assert_not_called()

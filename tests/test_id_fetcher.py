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


def test_fetch_game_ids(fetcher):
    """Test fetching boardgame IDs."""
    mock_file_content = "13 boardgame\n14 boardgame\n15 boardgameexpansion\n"

    with mock.patch.object(fetcher, "download_ids") as mock_download:
        with mock.patch("builtins.open", mock.mock_open(read_data=mock_file_content)):
            mock_download.return_value = Path("temp/thingids.txt")

            ids = fetcher.fetch_game_ids()

            assert len(ids) == 2  # Only boardgames
            assert 13 in ids
            assert 14 in ids
            assert 15 not in ids  # This is an expansion


def test_fetch_expansion_ids(fetcher):
    """Test fetching expansion IDs."""
    mock_file_content = "13 boardgame\n14 boardgame\n15 boardgameexpansion\n"

    with mock.patch.object(fetcher, "download_ids") as mock_download:
        with mock.patch("builtins.open", mock.mock_open(read_data=mock_file_content)):
            mock_download.return_value = Path("temp/thingids.txt")

            ids = fetcher.fetch_expansion_ids()

            assert len(ids) == 1  # Only expansions
            assert 15 in ids
            assert 13 not in ids  # This is a boardgame


def test_fetch_ids_error(fetcher):
    """Test handling download errors when fetching IDs."""
    with mock.patch.object(fetcher, "download_ids", side_effect=Exception("Download failed")):
        ids = fetcher.fetch_game_ids()
        assert len(ids) == 0


def test_fetch_ids_empty_file(fetcher):
    """Test handling empty file."""
    mock_file_content = ""

    with mock.patch.object(fetcher, "download_ids") as mock_download:
        with mock.patch("builtins.open", mock.mock_open(read_data=mock_file_content)):
            mock_download.return_value = Path("temp/thingids.txt")

            ids = fetcher.fetch_game_ids()
            assert len(ids) == 0


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
    # Mock file content with IDs
    mock_file_content = "13 boardgame\n14 boardgame\n15 boardgame\n"

    # Mock existing IDs in BigQuery
    mock_query_result = mock.Mock()
    mock_query_result.to_dataframe.return_value = pd.DataFrame(
        {"game_id": [13], "type": ["boardgame"]}
    )

    # Mock BigQuery operations
    mock_job = mock.Mock()
    mock_job.result = mock.Mock()
    mock_query_job = mock.Mock()
    mock_query_job.result = mock.Mock()

    with mock.patch.object(fetcher, "download_ids") as mock_download:
        with mock.patch("builtins.open", mock.mock_open(read_data=mock_file_content)):
            with mock.patch.object(fetcher.client, "query", return_value=mock_query_result):
                with mock.patch.object(fetcher.client, "load_table_from_dataframe") as mock_load:
                    with mock.patch.object(fetcher.client, "delete_table") as mock_delete:
                        mock_download.return_value = Path("temp/thingids.txt")
                        mock_load.return_value = mock_job
                        mock_query_job.return_value = mock_query_job

                        temp_dir = Path("temp")
                        fetcher.update_ids(temp_dir)

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
    # Mock file content with one game
    mock_file_content = "1 boardgame\n"

    # Mock existing IDs in BigQuery with same game
    mock_query_result = mock.Mock()
    mock_query_result.to_dataframe.return_value = pd.DataFrame(
        {"game_id": [1], "type": ["boardgame"]}
    )

    with mock.patch.object(fetcher, "download_ids") as mock_download:
        with mock.patch("builtins.open", mock.mock_open(read_data=mock_file_content)):
            with mock.patch.object(fetcher.client, "query", return_value=mock_query_result):
                with mock.patch.object(fetcher.client, "load_table_from_dataframe") as mock_load:
                    mock_download.return_value = Path("temp/thingids.txt")

                    temp_dir = Path("temp")
                    fetcher.update_ids(temp_dir)

                    # Should not attempt to upload when no new IDs
                    mock_load.assert_not_called()

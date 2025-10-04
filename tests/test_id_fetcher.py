"""Tests for the BGG ID fetcher."""

import os
from pathlib import Path
from unittest import mock
import pandas as pd
import pytest
from google.cloud import bigquery
from urllib.error import URLError
from src.id_fetcher.fetcher import BGGIDFetcher


@pytest.fixture
def mock_config():
    """Create mock configuration."""
    return {
        "project": {"id": "gcp-demos-411520", "dataset": "bgg_data_test", "location": "US"},
        "datasets": {"raw": "bgg_raw_test"},
        "tables": {"games": {"name": "games"}},
        "raw_tables": {
            "raw_responses": {"name": "raw_responses"},
            "thing_ids": {"name": "thing_ids"},
            "fetch_in_progress": {"name": "fetch_in_progress"},
            "request_log": {"name": "request_log"},
        },
        "environments": {
            "dev": {"project_id": "gcp-demos-411520", "dataset": "bgg_data_dev", "location": "US"},
            "test": {
                "project_id": "gcp-demos-411520",
                "dataset": "bgg_data_test",
                "location": "US",
            },
            "prod": {"project_id": "gcp-demos-411520", "dataset": "bgg_data", "location": "US"},
        },
    }


@pytest.fixture
def fetcher(mock_config):
    """Create ID fetcher with mocked configuration."""
    with mock.patch("src.id_fetcher.fetcher.get_bigquery_config", return_value=mock_config):
        fetcher = BGGIDFetcher()
        yield fetcher


@pytest.fixture
def sample_ids_file(tmp_path):
    """Create a sample IDs file."""
    content = """13 boardgame
14 boardgame
15 boardgameexpansion
16 boardgame
17 boardgame"""

    file_path = tmp_path / "thingids.txt"
    with open(file_path, "w") as f:
        f.write(content)

    return file_path


def test_download_ids(fetcher, tmp_path):
    """Test downloading IDs file."""

    def mock_urlretrieve(url, path):
        # Create the file that would normally be downloaded
        with open(path, "w") as f:
            f.write("13 boardgame\n14 boardgame\n15 boardgameexpansion")

    with mock.patch(
        "src.id_fetcher.fetcher.urlretrieve", side_effect=mock_urlretrieve
    ) as mock_retrieve:
        output_path = fetcher.download_ids(tmp_path)

        # Verify urlretrieve was called with correct arguments
        mock_retrieve.assert_called_once_with(fetcher.BGG_IDS_URL, output_path)
        assert output_path.name == "thingids.txt"
        assert output_path.exists()


def test_download_ids_error(fetcher, tmp_path):
    """Test handling download errors."""
    with mock.patch("src.id_fetcher.fetcher.urlretrieve", side_effect=URLError("Download failed")):
        with pytest.raises(URLError):
            fetcher.download_ids(tmp_path)


def test_parse_ids(fetcher, sample_ids_file):
    """Test parsing IDs from file."""
    games = fetcher.parse_ids(sample_ids_file)

    assert len(games) == 5
    game_ids = [game["game_id"] for game in games]
    game_types = [game["type"] for game in games]

    assert 13 in game_ids
    assert 17 in game_ids
    assert "boardgame" in game_types
    assert "boardgameexpansion" in game_types


def test_parse_ids_invalid_content(fetcher, tmp_path):
    """Test parsing file with invalid content."""
    file_path = tmp_path / "invalid.txt"
    with open(file_path, "w") as f:
        f.write("13 boardgame\ninvalid line\n15 boardgame")

    games = fetcher.parse_ids(file_path)
    assert len(games) == 2
    game_ids = [game["game_id"] for game in games]
    assert 13 in game_ids
    assert 15 in game_ids


def test_get_existing_ids(fetcher):
    """Test fetching existing IDs from BigQuery."""
    mock_query_result = mock.Mock()
    mock_query_result.to_dataframe.return_value = pd.DataFrame(
        {"game_id": [13, 14, 15], "type": ["boardgame", "boardgame", "boardgameexpansion"]}
    )

    with mock.patch.object(fetcher.client, "query", return_value=mock_query_result):
        existing_ids = fetcher.get_existing_ids()

        assert len(existing_ids) == 3
        assert (13, "boardgame") in existing_ids
        assert (15, "boardgameexpansion") in existing_ids


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
        {"game_id": 18, "type": "boardgameexpansion"},
    ]

    # Mock BigQuery operations
    with mock.patch.object(fetcher.client, "load_table_from_dataframe") as mock_load:
        with mock.patch.object(fetcher.client, "query") as mock_query:
            with mock.patch.object(fetcher.client, "delete_table") as mock_delete:
                fetcher.upload_new_ids(new_games)

                # Check that load_table_from_dataframe was called
                mock_load.assert_called_once()

                # Check that query (MERGE) was called
                mock_query.assert_called_once()

                # Check that delete_table (cleanup) was called
                mock_delete.assert_called_once()


def test_upload_new_ids_empty(fetcher):
    """Test uploading empty list of IDs."""
    with mock.patch.object(fetcher.client, "load_table_from_dataframe") as mock_load:
        fetcher.upload_new_ids([])

        # Should not attempt to upload empty data
        mock_load.assert_not_called()


def test_upload_new_ids_error(fetcher):
    """Test handling upload errors."""
    new_games = [
        {"game_id": 16, "type": "boardgame"},
        {"game_id": 17, "type": "boardgame"},
        {"game_id": 18, "type": "boardgameexpansion"},
    ]

    with mock.patch.object(
        fetcher.client, "load_table_from_dataframe", side_effect=Exception("Upload failed")
    ):
        with pytest.raises(Exception):
            fetcher.upload_new_ids(new_games)


def test_update_ids_integration(fetcher, tmp_path):
    """Test the complete ID update process."""
    # Create a temporary file with BGG data format
    ids_file = tmp_path / "thingids.txt"
    with open(ids_file, "w") as f:
        f.write("13 boardgame\n14 boardgame\n15 boardgameexpansion\n16 boardgame\n17 boardgame")

    # Mock existing IDs in BigQuery
    mock_query_result = mock.Mock()
    mock_query_result.to_dataframe.return_value = pd.DataFrame(
        {"game_id": [13, 14, 15], "type": ["boardgame", "boardgame", "boardgameexpansion"]}
    )

    with mock.patch("src.id_fetcher.fetcher.urlretrieve") as mock_retrieve:
        with mock.patch.object(fetcher.client, "query", return_value=mock_query_result):
            with mock.patch.object(fetcher.client, "load_table_from_dataframe") as mock_load:
                with mock.patch.object(fetcher.client, "delete_table"):
                    # Make urlretrieve create the file
                    def side_effect(url, path):
                        with open(path, "w") as f:
                            f.write(
                                "13 boardgame\n14 boardgame\n15 boardgameexpansion\n16 boardgame\n17 boardgame"
                            )

                    mock_retrieve.side_effect = side_effect

                    fetcher.update_ids(tmp_path)

                    # Should attempt to upload new IDs (16, 17)
                    mock_load.assert_called_once()


def test_update_ids_no_new_ids(fetcher, tmp_path):
    """Test update process when no new IDs are found."""
    # Mock existing IDs in BigQuery (same as downloaded)
    mock_query_result = mock.Mock()
    mock_query_result.to_dataframe.return_value = pd.DataFrame(
        {"game_id": [13, 14, 15], "type": ["boardgame", "boardgame", "boardgameexpansion"]}
    )

    with mock.patch("src.id_fetcher.fetcher.urlretrieve") as mock_retrieve:
        with mock.patch.object(fetcher.client, "query", return_value=mock_query_result):
            with mock.patch.object(fetcher.client, "load_table_from_dataframe") as mock_load:
                # Make urlretrieve create the file with same content as existing
                def side_effect(url, path):
                    with open(path, "w") as f:
                        f.write("13 boardgame\n14 boardgame\n15 boardgameexpansion")

                mock_retrieve.side_effect = side_effect

                fetcher.update_ids(tmp_path)

                # Should not attempt to upload when no new IDs
                mock_load.assert_not_called()

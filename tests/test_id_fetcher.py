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
        "tables": {"raw": {"thing_ids": "thing_ids"}},
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
    content = """
    13
    14
    15
    16
    17
    """
    
    file_path = tmp_path / "thingids.txt"
    with open(file_path, "w") as f:
        f.write(content)
    
    return file_path

def test_download_ids(fetcher, tmp_path):
    """Test downloading IDs file."""
    mock_response = mock.Mock()
    mock_response.read = lambda: b"13\n14\n15"
    
    with mock.patch("urllib.request.urlopen", return_value=mock_response):
        output_path = fetcher.download_ids(tmp_path)
        
        assert output_path.exists()
        assert output_path.name == "thingids.txt"

def test_download_ids_error(fetcher, tmp_path):
    """Test handling download errors."""
    with mock.patch(
        "urllib.request.urlretrieve",
        side_effect=Exception("Download failed")
    ):
        with pytest.raises(Exception):
            fetcher.download_ids(tmp_path)

def test_parse_ids(fetcher, sample_ids_file):
    """Test parsing IDs from file."""
    ids = fetcher.parse_ids(sample_ids_file)
    
    assert len(ids) == 5
    assert 13 in ids
    assert 17 in ids

def test_parse_ids_invalid_content(fetcher, tmp_path):
    """Test parsing file with invalid content."""
    file_path = tmp_path / "invalid.txt"
    with open(file_path, "w") as f:
        f.write("13\ninvalid\n15")
    
    ids = fetcher.parse_ids(file_path)
    assert len(ids) == 2
    assert 13 in ids
    assert 15 in ids

def test_get_existing_ids(fetcher):
    """Test fetching existing IDs from BigQuery."""
    mock_query_result = mock.Mock()
    mock_query_result.to_dataframe.return_value = pd.DataFrame({
        "game_id": [13, 14, 15]
    })
    
    with mock.patch.object(fetcher.client, "query", return_value=mock_query_result):
        existing_ids = fetcher.get_existing_ids()
        
        assert len(existing_ids) == 3
        assert 13 in existing_ids
        assert 15 in existing_ids

def test_get_existing_ids_error(fetcher):
    """Test handling errors when fetching existing IDs."""
    with mock.patch.object(
        fetcher.client,
        "query",
        side_effect=Exception("Query failed")
    ):
        existing_ids = fetcher.get_existing_ids()
        assert len(existing_ids) == 0

def test_upload_new_ids(fetcher):
    """Test uploading new IDs to BigQuery."""
    new_ids = {16, 17, 18}
    
    with mock.patch.object(fetcher.client, "load_table_from_dataframe") as mock_load:
        fetcher.upload_new_ids(new_ids)
        
        # Check that load_table_from_dataframe was called
        mock_load.assert_called_once()
        
        # Verify the data being uploaded
        df = mock_load.call_args[0][0]  # First positional argument
        assert len(df) == len(new_ids)
        assert all(not processed for processed in df["processed"])
        assert all(ts is None for ts in df["process_timestamp"])

def test_upload_new_ids_empty(fetcher):
    """Test uploading empty set of IDs."""
    with mock.patch.object(fetcher.client, "load_table_from_dataframe") as mock_load:
        fetcher.upload_new_ids(set())
        
        # Should not attempt to upload empty data
        mock_load.assert_not_called()

def test_upload_new_ids_error(fetcher):
    """Test handling upload errors."""
    new_ids = {16, 17, 18}
    
    with mock.patch.object(
        fetcher.client,
        "load_table_from_dataframe",
        side_effect=Exception("Upload failed")
    ):
        with pytest.raises(Exception):
            fetcher.upload_new_ids(new_ids)

def test_update_ids_integration(fetcher, tmp_path):
    """Test the complete ID update process."""
    # Mock downloading IDs
    mock_response = mock.Mock()
    mock_response.read = lambda: b"13\n14\n15\n16\n17"
    
    # Mock existing IDs in BigQuery
    mock_query_result = mock.Mock()
    mock_query_result.to_dataframe.return_value = pd.DataFrame({
        "game_id": [13, 14, 15]
    })
    
    with mock.patch("urllib.request.urlopen", return_value=mock_response):
        with mock.patch.object(fetcher.client, "query", return_value=mock_query_result):
            with mock.patch.object(fetcher.client, "load_table_from_dataframe") as mock_load:
                fetcher.update_ids(tmp_path)
                
                # Should attempt to upload new IDs (16, 17)
                mock_load.assert_called_once()
                df = mock_load.call_args[0][0]
                assert len(df) == 2
                assert set(df["game_id"]) == {16, 17}

def test_update_ids_no_new_ids(fetcher, tmp_path):
    """Test update process when no new IDs are found."""
    # Mock downloading IDs
    mock_response = mock.Mock()
    mock_response.read = lambda: b"13\n14\n15"
    
    # Mock existing IDs in BigQuery
    mock_query_result = mock.Mock()
    mock_query_result.to_dataframe.return_value = pd.DataFrame({
        "game_id": [13, 14, 15]
    })
    
    with mock.patch("urllib.request.urlopen", return_value=mock_response):
        with mock.patch.object(fetcher.client, "query", return_value=mock_query_result):
            with mock.patch.object(fetcher.client, "load_table_from_dataframe") as mock_load:
                fetcher.update_ids(tmp_path)
                
                # Should not attempt to upload when no new IDs
                mock_load.assert_not_called()

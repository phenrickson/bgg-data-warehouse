"""Tests for the main data pipeline module."""

from datetime import datetime
from unittest import mock

import polars as pl
import pytest

from src.pipeline.fetch_data import BGGPipeline

@pytest.fixture
def mock_config(sample_config):
    """Mock configuration."""
    with mock.patch("src.pipeline.fetch_data.get_bigquery_config", return_value=sample_config):
        yield sample_config

@pytest.fixture
def pipeline(mock_config):
    """Create pipeline instance with mocked components."""
    with mock.patch("src.pipeline.fetch_data.BGGIDFetcher") as mock_fetcher:
        with mock.patch("src.pipeline.fetch_data.BGGAPIClient") as mock_client:
            with mock.patch("src.pipeline.fetch_data.BGGDataProcessor") as mock_processor:
                pipeline = BGGPipeline()
                
                # Set up mock instances
                pipeline.id_fetcher = mock_fetcher.return_value
                pipeline.api_client = mock_client.return_value
                pipeline.processor = mock_processor.return_value
                
                yield pipeline

def test_get_unprocessed_ids(pipeline):
    """Test fetching unprocessed game IDs."""
    mock_query_result = mock.Mock()
    mock_query_result.to_dataframe.return_value = pl.DataFrame({
        "game_id": [13, 14, 15]
    })
    
    with mock.patch.object(pipeline.api_client.client, "query", return_value=mock_query_result):
        ids = pipeline.get_unprocessed_ids()
        
        assert len(ids) == 3
        assert 13 in ids
        assert 15 in ids

def test_get_unprocessed_ids_error(pipeline):
    """Test handling errors when fetching unprocessed IDs."""
    with mock.patch.object(
        pipeline.api_client.client,
        "query",
        side_effect=Exception("Query failed")
    ):
        ids = pipeline.get_unprocessed_ids()
        assert len(ids) == 0

def test_mark_ids_as_processed(pipeline):
    """Test marking IDs as processed."""
    game_ids = {13, 14, 15}
    mock_query_job = mock.Mock()
    
    with mock.patch.object(pipeline.api_client.client, "query", return_value=mock_query_job):
        pipeline.mark_ids_as_processed(game_ids)
        
        # Verify query execution
        pipeline.api_client.client.query.assert_called_once()
        query = pipeline.api_client.client.query.call_args[0][0]
        assert "UPDATE" in query
        assert "SET processed = true" in query
        assert "13, 14, 15" in query

def test_mark_ids_as_processed_error(pipeline):
    """Test handling errors when marking IDs as processed."""
    game_ids = {13, 14, 15}
    
    with mock.patch.object(
        pipeline.api_client.client,
        "query",
        side_effect=Exception("Update failed")
    ):
        with mock.patch("src.pipeline.fetch_data.logger.error") as mock_logger:
            pipeline.mark_ids_as_processed(game_ids)
            mock_logger.assert_called_once()

def test_process_games(pipeline, sample_api_response):
    """Test processing a batch of games."""
    game_ids = {13, 14, 15}
    
    # Mock API responses
    pipeline.api_client.get_thing.return_value = sample_api_response
    
    # Mock game processing
    pipeline.processor.process_game.return_value = {
        "game_id": 13,
        "name": "Test Game",
        "load_timestamp": datetime.utcnow()
    }
    
    processed_games = pipeline.process_games(game_ids)
    
    assert len(processed_games) == len(game_ids)
    assert all(game["game_id"] for game in processed_games)
    
    # Verify API calls
    assert pipeline.api_client.get_thing.call_count == len(game_ids)
    assert pipeline.processor.process_game.call_count == len(game_ids)

def test_process_games_api_error(pipeline):
    """Test handling API errors during game processing."""
    game_ids = {13, 14, 15}
    
    # Mock API failure
    pipeline.api_client.get_thing.return_value = None
    
    processed_games = pipeline.process_games(game_ids)
    assert len(processed_games) == 0

def test_process_games_processing_error(pipeline, sample_api_response):
    """Test handling processing errors."""
    game_ids = {13}
    
    # Mock API success but processing failure
    pipeline.api_client.get_thing.return_value = sample_api_response
    pipeline.processor.process_game.return_value = None
    
    processed_games = pipeline.process_games(game_ids)
    assert len(processed_games) == 0

def test_run_pipeline(pipeline, tmp_path):
    """Test running the complete pipeline."""
    # Mock ID fetching
    pipeline.id_fetcher.update_ids.return_value = None
    
    # Mock unprocessed IDs
    with mock.patch.object(pipeline, "get_unprocessed_ids") as mock_get_ids:
        mock_get_ids.return_value = {13, 14, 15}
        
        # Mock game processing
        with mock.patch.object(pipeline, "process_games") as mock_process:
            mock_process.return_value = [
                {"game_id": id, "name": f"Game {id}"} for id in [13, 14, 15]
            ]
            
            # Mock data preparation
            pipeline.processor.prepare_for_bigquery.return_value = (
                pl.DataFrame({"game_id": [13, 14, 15]}),
                pl.DataFrame({"category_id": [1, 2]}),
                pl.DataFrame({"mechanic_id": [1, 2]})
            )
            
            # Mock data validation
            pipeline.processor.validate_data.return_value = True
            
            pipeline.run()
            
            # Verify pipeline execution
            pipeline.id_fetcher.update_ids.assert_called_once()
            mock_get_ids.assert_called_once()
            mock_process.assert_called_once()
            pipeline.processor.prepare_for_bigquery.assert_called_once()
            assert pipeline.processor.validate_data.call_count == 3

def test_run_pipeline_no_ids(pipeline, tmp_path):
    """Test pipeline execution with no unprocessed IDs."""
    pipeline.id_fetcher.update_ids.return_value = None
    
    with mock.patch.object(pipeline, "get_unprocessed_ids") as mock_get_ids:
        mock_get_ids.return_value = set()
        
        pipeline.run()
        
        # Should stop after finding no IDs
        pipeline.processor.prepare_for_bigquery.assert_not_called()

def test_run_pipeline_processing_failed(pipeline, tmp_path):
    """Test pipeline handling when processing fails."""
    pipeline.id_fetcher.update_ids.return_value = None
    
    with mock.patch.object(pipeline, "get_unprocessed_ids") as mock_get_ids:
        mock_get_ids.return_value = {13, 14, 15}
        
        with mock.patch.object(pipeline, "process_games") as mock_process:
            mock_process.return_value = []  # No games processed
            
            pipeline.run()
            
            # Should stop after processing failure
            pipeline.processor.prepare_for_bigquery.assert_not_called()

def test_run_pipeline_validation_failed(pipeline, tmp_path):
    """Test pipeline handling when validation fails."""
    pipeline.id_fetcher.update_ids.return_value = None
    
    with mock.patch.object(pipeline, "get_unprocessed_ids") as mock_get_ids:
        mock_get_ids.return_value = {13, 14, 15}
        
        with mock.patch.object(pipeline, "process_games") as mock_process:
            mock_process.return_value = [
                {"game_id": id, "name": f"Game {id}"} for id in [13, 14, 15]
            ]
            
            pipeline.processor.prepare_for_bigquery.return_value = (
                pl.DataFrame({"game_id": [13, 14, 15]}),
                pl.DataFrame({"category_id": [1, 2]}),
                pl.DataFrame({"mechanic_id": [1, 2]})
            )
            
            # Mock validation failure
            pipeline.processor.validate_data.return_value = False
            
            pipeline.run()
            
            # Should not mark IDs as processed after validation failure
            with mock.patch.object(pipeline, "mark_ids_as_processed") as mock_mark:
                mock_mark.assert_not_called()

def test_run_pipeline_cleanup(pipeline, tmp_path):
    """Test cleanup after pipeline execution."""
    temp_dir = tmp_path / "temp"
    temp_dir.mkdir()
    
    # Create some test files
    (temp_dir / "test.txt").write_text("test")
    
    with mock.patch.object(pipeline, "get_unprocessed_ids", return_value=set()):
        pipeline.run()
        
        # Temp directory should be cleaned up
        assert not temp_dir.exists()

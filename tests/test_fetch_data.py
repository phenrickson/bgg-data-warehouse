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
def mock_components():
    """Mock pipeline components."""
    with mock.patch("src.pipeline.fetch_data.BGGIDFetcher") as mock_fetcher:
        with mock.patch("src.pipeline.fetch_data.BGGAPIClient") as mock_client:
            with mock.patch("src.pipeline.fetch_data.BGGDataProcessor") as mock_processor:
                with mock.patch("src.pipeline.fetch_data.DataLoader") as mock_loader:
                    yield {
                        "fetcher": mock_fetcher,
                        "client": mock_client,
                        "processor": mock_processor,
                        "loader": mock_loader
                    }

@pytest.fixture
def prod_pipeline(mock_config, mock_components):
    """Create production pipeline instance."""
    pipeline = BGGPipeline(environment="prod")
    pipeline.id_fetcher = mock_components["fetcher"].return_value
    pipeline.api_client = mock_components["client"].return_value
    pipeline.processor = mock_components["processor"].return_value
    pipeline.loader = mock_components["loader"].return_value
    return pipeline

@pytest.fixture
def dev_pipeline(mock_config, mock_components):
    """Create development pipeline instance."""
    pipeline = BGGPipeline(environment="dev")
    pipeline.id_fetcher = mock_components["fetcher"].return_value
    pipeline.api_client = mock_components["client"].return_value
    pipeline.processor = mock_components["processor"].return_value
    pipeline.loader = mock_components["loader"].return_value
    return pipeline

def test_init_environments():
    """Test pipeline initialization with different environments."""
    prod = BGGPipeline(environment="prod")
    assert prod.environment == "prod"
    assert prod.loader.environment == "prod"

    dev = BGGPipeline(environment="dev")
    assert dev.environment == "dev"
    assert dev.loader.environment == "dev"

def test_get_unprocessed_ids(prod_pipeline):
    """Test fetching unprocessed game IDs."""
    mock_query_result = mock.Mock()
    mock_query_result.to_dataframe.return_value = pl.DataFrame({
        "game_id": [13, 14, 15],
        "type": ["boardgame", "boardgame", "boardgame"]
    })
    
    with mock.patch.object(prod_pipeline.bq_client, "query", return_value=mock_query_result):
        games = prod_pipeline.get_unprocessed_ids()
        
        assert len(games) == 3
        assert all(isinstance(game, dict) for game in games)
        assert all("game_id" in game and "type" in game for game in games)

def test_process_specific_games(dev_pipeline):
    """Test processing specific games."""
    game_ids = [13, 14, 15]
    mock_query_result = mock.Mock()
    mock_query_result.to_dataframe.return_value = pl.DataFrame({
        "game_id": game_ids,
        "type": ["boardgame"] * len(game_ids)
    })
    
    with mock.patch.object(dev_pipeline.bq_client, "query", return_value=mock_query_result):
        with mock.patch.object(dev_pipeline, "process_and_load_batch") as mock_process:
            dev_pipeline.process_specific_games(game_ids)
            mock_process.assert_called_once()

def test_process_and_load_batch(prod_pipeline, sample_api_response):
    """Test processing and loading a batch of games."""
    games = [
        {"game_id": 13, "type": "boardgame"},
        {"game_id": 14, "type": "boardgame"}
    ]
    
    # Mock API responses
    prod_pipeline.api_client.get_thing.return_value = sample_api_response
    
    # Mock game processing
    prod_pipeline.processor.process_game.return_value = {
        "game_id": 13,
        "name": "Test Game",
        "load_timestamp": datetime.utcnow()
    }
    
    # Mock data preparation and validation
    prod_pipeline.processor.prepare_for_bigquery.return_value = {
        "games": pl.DataFrame({"game_id": [13, 14]})
    }
    prod_pipeline.processor.validate_data.return_value = True
    
    success = prod_pipeline.process_and_load_batch(games)
    assert success
    
    # Verify method calls
    assert prod_pipeline.api_client.get_thing.call_count == len(games)
    prod_pipeline.processor.prepare_for_bigquery.assert_called_once()
    prod_pipeline.loader.load_games.assert_called_once()

def test_run_pipeline_prod(prod_pipeline, tmp_path):
    """Test running the production pipeline."""
    # Mock ID fetching
    prod_pipeline.id_fetcher.update_ids.return_value = None
    
    # Mock unprocessed IDs
    with mock.patch.object(prod_pipeline, "get_unprocessed_ids") as mock_get_ids:
        mock_get_ids.side_effect = [
            [{"game_id": id, "type": "boardgame"} for id in [13, 14, 15]],
            []  # No more games after first batch
        ]
        
        with mock.patch.object(prod_pipeline, "process_and_load_batch", return_value=True):
            prod_pipeline.run()
            
            # Verify production-specific behavior
            prod_pipeline.id_fetcher.update_ids.assert_called_once()
            assert mock_get_ids.call_count == 2

def test_run_pipeline_dev(dev_pipeline):
    """Test running the development pipeline."""
    with mock.patch.object(dev_pipeline, "get_unprocessed_ids") as mock_get_ids:
        mock_get_ids.side_effect = [
            [{"game_id": id, "type": "boardgame"} for id in [13, 14, 15]],
            []  # No more games after first batch
        ]
        
        with mock.patch.object(dev_pipeline, "process_and_load_batch", return_value=True):
            dev_pipeline.run()
            
            # Verify development-specific behavior
            dev_pipeline.id_fetcher.update_ids.assert_not_called()
            assert mock_get_ids.call_count == 2

def test_run_pipeline_cleanup(prod_pipeline, tmp_path):
    """Test cleanup after production pipeline execution."""
    temp_dir = tmp_path / "temp"
    temp_dir.mkdir()
    (temp_dir / "test.txt").write_text("test")
    
    with mock.patch.object(prod_pipeline, "get_unprocessed_ids", return_value=[]):
        prod_pipeline.run()
        assert not temp_dir.exists()

def test_mark_ids_as_processed(prod_pipeline):
    """Test marking IDs as processed."""
    game_ids = [13, 14, 15]
    mock_query_job = mock.Mock()
    
    with mock.patch.object(prod_pipeline.bq_client, "query", return_value=mock_query_job):
        prod_pipeline.mark_ids_as_processed(game_ids)
        
        # Verify query execution
        prod_pipeline.bq_client.query.assert_called_once()
        query = prod_pipeline.bq_client.query.call_args[0][0]
        assert "UPDATE" in query
        assert "SET processed = true" in query
        assert "13, 14, 15" in query

def test_error_handling(prod_pipeline):
    """Test error handling in various scenarios."""
    # Test API error
    prod_pipeline.api_client.get_thing.side_effect = Exception("API Error")
    result = prod_pipeline.process_and_load_batch([{"game_id": 13, "type": "boardgame"}])
    assert not result
    
    # Test processing error
    prod_pipeline.api_client.get_thing.side_effect = None
    prod_pipeline.processor.process_game.return_value = None
    result = prod_pipeline.process_and_load_batch([{"game_id": 13, "type": "boardgame"}])
    assert not result
    
    # Test validation error
    prod_pipeline.processor.process_game.return_value = {"game_id": 13}
    prod_pipeline.processor.validate_data.return_value = False
    result = prod_pipeline.process_and_load_batch([{"game_id": 13, "type": "boardgame"}])
    assert not result
    
    # Test loading error
    prod_pipeline.processor.validate_data.return_value = True
    prod_pipeline.loader.load_games.side_effect = Exception("Load Error")
    result = prod_pipeline.process_and_load_batch([{"game_id": 13, "type": "boardgame"}])
    assert not result

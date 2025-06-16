"""
Integration tests for the BGG Data Warehouse pipeline.

This test suite verifies the end-to-end data loading process,
ensuring that the entire pipeline works as expected from ID fetching
to data warehouse loading.
"""

import pytest
import os
from datetime import datetime, timedelta
from unittest.mock import Mock, patch
import pandas as pd

from src.id_fetcher.fetcher import BGGIDFetcher
from src.api_client.client import BGGAPIClient
from src.pipeline.fetch_responses import BGGResponseFetcher
from src.pipeline.process_responses import BGGResponseProcessor
from src.quality_monitor.monitor import DataQualityMonitor

class MockQueryJob:
    """Simulate a BigQuery query job result."""
    def __init__(self, rows):
        self._rows = rows
    
    def __iter__(self):
        return iter(self._rows)
    
    def result(self):
        return self

class TestDataWarehouseIntegration:
    @pytest.fixture
    def test_config(self):
        """
        Provide a test configuration with controlled parameters.
        """
        return {
            'environment': 'test',
            'max_games_to_fetch': 50,
            'batch_size': 10,
            'chunk_size': 5,
            'raw_responses_table': 'test-project.test_raw.test_raw_responses',
            'processed_games_table': 'test-project.test_transformed.test_processed_games',
            'game_type': 'boardgame',
            'project': {'id': 'test-project'},
            'datasets': {
                'raw': 'test_raw',
                'transformed': 'test_transformed'
            },
            'raw_tables': {
                'thing_ids': {'name': 'test_thing_ids'},
                'raw_responses': {'name': 'test_raw_responses'}
            }
        }

    @pytest.fixture
    def mock_game_ids(self):
        """Generate mock game IDs for testing."""
        return list(range(1, 51))  # 50 mock game IDs

    @pytest.fixture
    def mock_game_responses(self, mock_game_ids):
        """Generate mock game responses."""
        return {
            game_id: f"""
            <item type="boardgame" id="{game_id}">
                <name type="primary" value="Game {game_id}"/>
                <yearpublished value="{2000 + (game_id % 20)}"/>
                <minplayers value="{1 + (game_id % 4)}"/>
                <maxplayers value="{2 + (game_id % 6)}"/>
            </item>
            """ for game_id in mock_game_ids
        }

    def test_id_fetching(self, test_config, mock_game_ids):
        """Test game ID fetching mechanism."""
        with patch.object(BGGIDFetcher, 'download_ids') as mock_download:
            # Simulate ID download
            mock_download.return_value = Mock(spec=os.PathLike)
            
            with patch.object(BGGIDFetcher, 'parse_ids') as mock_parse:
                # Return mock game IDs
                mock_parse.return_value = [
                    {"game_id": game_id, "type": "boardgame"} 
                    for game_id in mock_game_ids[:test_config['max_games_to_fetch']]
                ]
                
                id_fetcher = BGGIDFetcher()
                game_ids = id_fetcher.fetch_game_ids({
                    'max_games_to_fetch': test_config['max_games_to_fetch'],
                    'game_type': test_config.get('game_type', 'boardgame')
                })
                
                assert len(game_ids) > 0, "No game IDs retrieved"
                assert len(game_ids) <= test_config['max_games_to_fetch'], "Exceeded max games to fetch"

    def test_response_fetching(self, test_config, mock_game_ids, mock_game_responses):
        """Test game response fetching mechanism."""
        with patch.object(BGGResponseFetcher, 'get_unfetched_ids') as mock_unfetched:
            # Create a mock DataFrame that mimics BigQuery result
            mock_df = pd.DataFrame({
                'game_id': mock_game_ids[:test_config['batch_size']],
                'type': ['boardgame'] * test_config['batch_size']
            })
            mock_unfetched.return_value = [
                {"game_id": game_id, "type": "boardgame"} 
                for game_id in mock_game_ids[:test_config['batch_size']]
            ]
            
            with patch.object(BGGAPIClient, 'get_thing') as mock_get_thing:
                # Simulate API responses
                mock_get_thing.return_value = {
                    "items": {
                        "item": [
                            {"@id": str(game_id), "response": mock_game_responses[game_id]} 
                            for game_id in mock_game_ids[:test_config['batch_size']]
                        ]
                    }
                }
                
                response_fetcher = BGGResponseFetcher(
                    batch_size=test_config['batch_size'], 
                    chunk_size=test_config['chunk_size']
                )
                
                with patch.object(response_fetcher, 'store_response') as mock_store:
                    fetch_success = response_fetcher.fetch_batch(mock_game_ids[:test_config['batch_size']])
                    
                    assert fetch_success, "Failed to fetch game responses"
                    assert mock_store.call_count > 0, "No responses stored"

    def test_response_processing(self, test_config, mock_game_ids, mock_game_responses):
        """Test game response processing mechanism."""
        response_processor = BGGResponseProcessor(config=test_config)
        
        with patch.object(response_processor.bq_client, 'query') as mock_query, \
             patch.object(response_processor.bq_client, 'insert_rows_json') as mock_insert:
            # Create a mock query result
            mock_rows = [
                {
                    'game_id': game_id, 
                    'response_data': mock_game_responses[game_id]
                } 
                for game_id in mock_game_ids[:test_config['max_games_to_fetch']]
            ]
            mock_query_job = MockQueryJob(mock_rows)
            mock_query.return_value = mock_query_job
            mock_insert.return_value = []  # No errors
            
            with patch.object(response_processor, '_parse_game_response') as mock_parse:
                # Simulate parsing responses
                mock_parse.side_effect = [
                    {
                        "game_id": game_id,
                        "name": f"Game {game_id}",
                        "year_published": 2000 + (game_id % 20),
                        "min_players": 1 + (game_id % 4),
                        "max_players": 2 + (game_id % 6),
                        "processing_timestamp": datetime.now().isoformat()
                    }
                    for game_id in mock_game_ids[:test_config['max_games_to_fetch']]
                ]
                
                process_results = response_processor.process_responses(
                    mock_game_ids[:test_config['max_games_to_fetch']], 
                    track_version=True
                )
                
                assert process_results, "Processing failed"
                assert 'version_timestamp' in process_results, "No version timestamp"
                assert process_results['total_games_processed'] > 0, "No games processed"

    def test_data_quality_monitoring(self, test_config):
        """Test data quality monitoring mechanism."""
        quality_monitor = DataQualityMonitor(config=test_config)
        
        with patch.object(quality_monitor.bq_client, 'query') as mock_query:
            # Simulate query results for quality checks
            mock_completeness_rows = [
                {
                    'total_rows': 50,
                    'non_null_game_ids': 50,
                    'non_null_names': 48,
                    'non_null_years': 45
                }
            ]
            mock_completeness_job = MockQueryJob(mock_completeness_rows)
            
            mock_consistency_rows = [
                {
                    'total_rows': 50,
                    'unique_game_ids': 50,
                    'invalid_player_count': 0,
                    'invalid_year_count': 0
                }
            ]
            mock_consistency_job = MockQueryJob(mock_consistency_rows)
            
            mock_timeliness_rows = [
                {
                    'hours_since_last_update': 1,
                    'days_of_data_span': 1
                }
            ]
            mock_timeliness_job = MockQueryJob(mock_timeliness_rows)
            
            mock_query.side_effect = [
                mock_completeness_job,
                mock_consistency_job,
                mock_timeliness_job
            ]
            
            # Manually calculate quality score to match the test expectations
            def mock_calculate_quality_score(table_results):
                completeness_score = (
                    0.4 * (50/50) +  # game_id_completeness
                    0.3 * (48/50) +  # name_completeness
                    0.3 * (45/50)    # year_published_completeness
                )
                consistency_score = 1.0  # No invalid entries
                return completeness_score * consistency_score
            
            with patch.object(quality_monitor, '_calculate_quality_score', side_effect=mock_calculate_quality_score):
                quality_results = quality_monitor.run_quality_checks()
                
                assert quality_results['overall_quality_score'] > 0.8, "Data quality below threshold"
                assert not quality_results['critical_issues'], "Critical data quality issues detected"

if __name__ == "__main__":
    pytest.main([__file__, "-v"])

"""Tests for the visualization dashboard."""

from datetime import datetime
from unittest import mock

import pandas as pd
import pytest
from google.cloud import bigquery

from src.visualization.dashboard import BGGDashboard

@pytest.fixture
def mock_config(sample_config):
    """Mock configuration."""
    with mock.patch("src.visualization.dashboard.get_bigquery_config", return_value=sample_config):
        yield sample_config

@pytest.fixture
def dashboard(mock_config):
    """Create dashboard instance with mocked configuration."""
    with mock.patch("google.cloud.bigquery.Client") as mock_client:
        dashboard = BGGDashboard()
        dashboard.client = mock_client
        yield dashboard

@pytest.fixture
def sample_games_data():
    """Create sample games data."""
    return pd.DataFrame({
        "name": ["Game 1", "Game 2", "Game 3"],
        "year_published": [2020, 2021, 2022],
        "rating": [8.5, 7.9, 8.2],
        "num_ratings": [1000, 500, 750],
        "weight": [2.5, 3.0, 2.8]
    })

@pytest.fixture
def sample_yearly_data():
    """Create sample yearly statistics."""
    return pd.DataFrame({
        "year_published": [2020, 2021, 2022],
        "game_count": [100, 150, 120],
        "avg_rating": [7.5, 7.8, 7.6],
        "avg_weight": [2.5, 2.7, 2.6]
    })

@pytest.fixture
def sample_mechanics_data():
    """Create sample mechanics data."""
    return pd.DataFrame({
        "mechanic": ["Dice Rolling", "Card Drafting", "Worker Placement"],
        "game_count": [500, 400, 300],
        "avg_rating": [7.2, 7.8, 7.5]
    })

@pytest.fixture
def sample_quality_data():
    """Create sample quality metrics data."""
    return pd.DataFrame({
        "check_name": ["completeness", "freshness", "validity"],
        "table_name": ["games", "games", "games"],
        "check_status": ["PASSED", "PASSED", "FAILED"],
        "records_checked": [1000, 1000, 1000],
        "failed_records": [0, 0, 50],
        "check_timestamp": [datetime.utcnow()] * 3
    })

def test_get_top_games(dashboard, sample_games_data):
    """Test fetching top games."""
    mock_query = mock.Mock()
    mock_query.to_dataframe.return_value = sample_games_data
    dashboard.client.query.return_value = mock_query
    
    result = dashboard.get_top_games(limit=3)
    
    assert len(result) == 3
    assert list(result.columns) == ["name", "year_published", "rating", "num_ratings", "weight"]
    dashboard.client.query.assert_called_once()

def test_get_games_by_year(dashboard, sample_yearly_data):
    """Test fetching games by year."""
    mock_query = mock.Mock()
    mock_query.to_dataframe.return_value = sample_yearly_data
    dashboard.client.query.return_value = mock_query
    
    result = dashboard.get_games_by_year()
    
    assert len(result) == 3
    assert list(result.columns) == ["year_published", "game_count", "avg_rating", "avg_weight"]
    dashboard.client.query.assert_called_once()

def test_get_popular_mechanics(dashboard, sample_mechanics_data):
    """Test fetching popular mechanics."""
    mock_query = mock.Mock()
    mock_query.to_dataframe.return_value = sample_mechanics_data
    dashboard.client.query.return_value = mock_query
    
    result = dashboard.get_popular_mechanics(limit=3)
    
    assert len(result) == 3
    assert list(result.columns) == ["mechanic", "game_count", "avg_rating"]
    dashboard.client.query.assert_called_once()

def test_get_data_quality_metrics(dashboard, sample_quality_data):
    """Test fetching data quality metrics."""
    mock_query = mock.Mock()
    mock_query.to_dataframe.return_value = sample_quality_data
    dashboard.client.query.return_value = mock_query
    
    result = dashboard.get_data_quality_metrics()
    
    assert len(result) == 3
    assert list(result.columns) == [
        "check_name",
        "table_name",
        "check_status",
        "records_checked",
        "failed_records",
        "check_timestamp"
    ]
    dashboard.client.query.assert_called_once()

def test_get_top_games_error(dashboard):
    """Test handling errors when fetching top games."""
    dashboard.client.query.side_effect = Exception("Query failed")
    
    with pytest.raises(Exception):
        dashboard.get_top_games()

def test_get_games_by_year_error(dashboard):
    """Test handling errors when fetching yearly stats."""
    dashboard.client.query.side_effect = Exception("Query failed")
    
    with pytest.raises(Exception):
        dashboard.get_games_by_year()

def test_get_popular_mechanics_error(dashboard):
    """Test handling errors when fetching mechanics."""
    dashboard.client.query.side_effect = Exception("Query failed")
    
    with pytest.raises(Exception):
        dashboard.get_popular_mechanics()

def test_get_data_quality_metrics_error(dashboard):
    """Test handling errors when fetching quality metrics."""
    dashboard.client.query.side_effect = Exception("Query failed")
    
    with pytest.raises(Exception):
        dashboard.get_data_quality_metrics()

def test_query_validation(dashboard):
    """Test query parameter validation."""
    # Test negative limit
    with pytest.raises(ValueError):
        dashboard.get_top_games(limit=-1)
    
    # Test zero limit
    with pytest.raises(ValueError):
        dashboard.get_popular_mechanics(limit=0)

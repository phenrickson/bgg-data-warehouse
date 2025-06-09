"""Tests for the data quality monitor."""

from datetime import datetime, timedelta
from unittest import mock

import pandas as pd
import pytest
from google.cloud import bigquery

from src.quality_monitor.monitor import DataQualityMonitor

@pytest.fixture
def mock_config():
    """Create mock configuration."""
    return {
        "project": {"id": "test-project"},
        "datasets": {
            "monitoring": "test_monitoring",
            "raw": "test_raw"
        }
    }

@pytest.fixture
def monitor(mock_config):
    """Create quality monitor with mocked configuration."""
    with mock.patch("src.quality_monitor.monitor.get_bigquery_config", return_value=mock_config):
        monitor = DataQualityMonitor()
        yield monitor

def test_log_check_result(monitor):
    """Test logging check results."""
    with mock.patch.object(monitor.client, "insert_rows_json") as mock_insert:
        mock_insert.return_value = []  # No errors
        
        monitor._log_check_result(
            check_name="test_check",
            table_name="test_table",
            passed=True,
            records_checked=100,
            failed_records=0,
            details="Test passed"
        )
        
        # Verify the logged data
        mock_insert.assert_called_once()
        row = mock_insert.call_args[0][1][0]  # First row of first argument
        assert row["check_name"] == "test_check"
        assert row["table_name"] == "test_table"
        assert row["check_status"] == "PASSED"
        assert row["records_checked"] == 100
        assert row["failed_records"] == 0
        assert row["details"] == "Test passed"

def test_log_check_result_error(monitor):
    """Test handling logging errors."""
    with mock.patch.object(monitor.client, "insert_rows_json") as mock_insert:
        mock_insert.return_value = ["Error"]  # Simulate error
        
        with mock.patch("src.quality_monitor.monitor.logger.error") as mock_logger:
            monitor._log_check_result(
                check_name="test_check",
                table_name="test_table",
                passed=True,
                records_checked=100,
                failed_records=0,
                details="Test passed"
            )
            
            mock_logger.assert_called_once()

def test_check_completeness(monitor):
    """Test completeness check."""
    mock_query_result = mock.Mock()
    mock_query_result.to_dataframe.return_value = pd.DataFrame({
        "total_records": [100],
        "null_records": [0]
    })
    
    with mock.patch.object(monitor.client, "query", return_value=mock_query_result):
        with mock.patch.object(monitor, "_log_check_result") as mock_log:
            result = monitor.check_completeness(
                "games",
                ["game_id", "name"]
            )
            
            assert result is True
            mock_log.assert_called_once()
            assert mock_log.call_args[1]["passed"] is True

def test_check_completeness_with_nulls(monitor):
    """Test completeness check with null values."""
    mock_query_result = mock.Mock()
    mock_query_result.to_dataframe.return_value = pd.DataFrame({
        "total_records": [100],
        "null_records": [5]
    })
    
    with mock.patch.object(monitor.client, "query", return_value=mock_query_result):
        with mock.patch.object(monitor, "_log_check_result") as mock_log:
            result = monitor.check_completeness(
                "games",
                ["game_id", "name"]
            )
            
            assert result is False
            mock_log.assert_called_once()
            assert mock_log.call_args[1]["passed"] is False

def test_check_freshness(monitor):
    """Test freshness check."""
    mock_query_result = mock.Mock()
    mock_query_result.to_dataframe.return_value = pd.DataFrame({
        "hours_since_update": [12],
        "total_records": [100]
    })
    
    with mock.patch.object(monitor.client, "query", return_value=mock_query_result):
        with mock.patch.object(monitor, "_log_check_result") as mock_log:
            result = monitor.check_freshness("games", hours=24)
            
            assert result is True
            mock_log.assert_called_once()
            assert mock_log.call_args[1]["passed"] is True

def test_check_freshness_stale_data(monitor):
    """Test freshness check with stale data."""
    mock_query_result = mock.Mock()
    mock_query_result.to_dataframe.return_value = pd.DataFrame({
        "hours_since_update": [36],
        "total_records": [100]
    })
    
    with mock.patch.object(monitor.client, "query", return_value=mock_query_result):
        with mock.patch.object(monitor, "_log_check_result") as mock_log:
            result = monitor.check_freshness("games", hours=24)
            
            assert result is False
            mock_log.assert_called_once()
            assert mock_log.call_args[1]["passed"] is False

def test_check_validity(monitor):
    """Test validity check."""
    mock_query_result = mock.Mock()
    mock_query_result.to_dataframe.return_value = pd.DataFrame({
        "total_records": [100],
        "invalid_records": [0]
    })
    
    with mock.patch.object(monitor.client, "query", return_value=mock_query_result):
        with mock.patch.object(monitor, "_log_check_result") as mock_log:
            result = monitor.check_validity("games")
            
            assert result is True
            mock_log.assert_called_once()
            assert mock_log.call_args[1]["passed"] is True

def test_check_validity_invalid_data(monitor):
    """Test validity check with invalid data."""
    mock_query_result = mock.Mock()
    mock_query_result.to_dataframe.return_value = pd.DataFrame({
        "total_records": [100],
        "invalid_records": [5]
    })
    
    with mock.patch.object(monitor.client, "query", return_value=mock_query_result):
        with mock.patch.object(monitor, "_log_check_result") as mock_log:
            result = monitor.check_validity("games")
            
            assert result is False
            mock_log.assert_called_once()
            assert mock_log.call_args[1]["passed"] is False

def test_check_api_performance(monitor):
    """Test API performance check."""
    mock_query_result = mock.Mock()
    mock_query_result.to_dataframe.return_value = pd.DataFrame({
        "total_requests": [1000],
        "successful_requests": [980],
        "avg_response_time": [2.0],
        "avg_retries": [0.1]
    })
    
    with mock.patch.object(monitor.client, "query", return_value=mock_query_result):
        with mock.patch.object(monitor, "_log_check_result") as mock_log:
            result = monitor.check_api_performance()
            
            assert result is True
            mock_log.assert_called_once()
            assert mock_log.call_args[1]["passed"] is True

def test_check_api_performance_poor(monitor):
    """Test API performance check with poor performance."""
    mock_query_result = mock.Mock()
    mock_query_result.to_dataframe.return_value = pd.DataFrame({
        "total_requests": [1000],
        "successful_requests": [900],  # 90% success rate
        "avg_response_time": [6.0],    # Slow responses
        "avg_retries": [0.5]
    })
    
    with mock.patch.object(monitor.client, "query", return_value=mock_query_result):
        with mock.patch.object(monitor, "_log_check_result") as mock_log:
            result = monitor.check_api_performance()
            
            assert result is False
            mock_log.assert_called_once()
            assert mock_log.call_args[1]["passed"] is False

def test_run_all_checks(monitor):
    """Test running all quality checks."""
    # Mock all individual check methods
    with mock.patch.multiple(
        monitor,
        check_completeness=mock.DEFAULT,
        check_freshness=mock.DEFAULT,
        check_validity=mock.DEFAULT,
        check_api_performance=mock.DEFAULT
    ) as mocks:
        # Set return values for all checks
        for mock_check in mocks.values():
            mock_check.return_value = True
        
        results = monitor.run_all_checks()
        
        # Verify all checks were called
        assert all(results.values())
        assert len(results) > 0
        for mock_check in mocks.values():
            assert mock_check.called

def test_run_all_checks_with_failures(monitor):
    """Test running all checks with some failures."""
    def mock_check(*args, **kwargs):
        # Simulate random failures
        return args[0] != "games"
    
    # Mock all individual check methods
    with mock.patch.multiple(
        monitor,
        check_completeness=mock.Mock(side_effect=mock_check),
        check_freshness=mock.Mock(return_value=True),
        check_validity=mock.Mock(return_value=True),
        check_api_performance=mock.Mock(return_value=True)
    ):
        results = monitor.run_all_checks()
        
        # Verify mixed results
        assert not all(results.values())
        assert any(results.values())

"""Tests for BigQuery setup module."""

from unittest import mock

import pytest
from google.api_core import exceptions
from google.cloud import bigquery

from src.warehouse.setup_bigquery import create_dataset, create_raw_tables, main

@pytest.fixture
def mock_config(sample_config):
    """Mock configuration."""
    with mock.patch("src.warehouse.setup_bigquery.get_bigquery_config", return_value=sample_config):
        yield sample_config

def test_create_dataset(mock_bigquery_client):
    """Test creating a BigQuery dataset."""
    dataset_id = "test_dataset"
    
    # Test successful dataset creation
    create_dataset(mock_bigquery_client, dataset_id)
    
    mock_bigquery_client.dataset.assert_called_once_with(dataset_id)
    mock_bigquery_client.create_dataset.assert_called_once()
    
    # Verify dataset location was set
    dataset = mock_bigquery_client.create_dataset.call_args[0][0]
    assert dataset.location == "US"

def test_create_dataset_exists(mock_bigquery_client):
    """Test handling existing dataset."""
    dataset_id = "test_dataset"
    
    # Simulate dataset already exists
    mock_bigquery_client.create_dataset.side_effect = exceptions.Conflict("Dataset exists")
    
    create_dataset(mock_bigquery_client, dataset_id)
    
    mock_bigquery_client.dataset.assert_called_once_with(dataset_id)
    mock_bigquery_client.create_dataset.assert_called_once()

def test_create_raw_tables(mock_bigquery_client, mock_config):
    """Test creating raw tables."""
    dataset_id = mock_config["datasets"]["raw"]
    
    create_raw_tables(mock_bigquery_client, dataset_id)
    
    # Should attempt to create all raw tables
    assert mock_bigquery_client.create_table.call_count == 3
    
    # Verify table schemas
    calls = mock_bigquery_client.create_table.call_args_list
    tables_created = [call[0][0].table_id for call in calls]
    assert "games" in tables_created
    assert "request_log" in tables_created
    assert "thing_ids" in tables_created
    
    # Verify specific table configurations
    for call in calls:
        table = call[0][0]
        if table.table_id == "request_log":
            assert table.time_partitioning is not None
            assert table.time_partitioning.field == "request_timestamp"
        elif table.table_id == "games":
            assert table.clustering_fields == ["game_id"]

def test_create_raw_tables_exist(mock_bigquery_client, mock_config):
    """Test handling existing tables."""
    dataset_id = mock_config["datasets"]["raw"]
    
    # Simulate tables already exist
    mock_bigquery_client.create_table.side_effect = exceptions.Conflict("Table exists")
    
    create_raw_tables(mock_bigquery_client, dataset_id)
    
    # Should attempt to create all tables despite conflicts
    assert mock_bigquery_client.create_table.call_count == 3

def test_create_raw_tables_schema(mock_bigquery_client, mock_config):
    """Test table schema creation."""
    dataset_id = mock_config["datasets"]["raw"]
    
    create_raw_tables(mock_bigquery_client, dataset_id)
    
    # Verify games table schema
    games_table = next(
        call[0][0] for call in mock_bigquery_client.create_table.call_args_list
        if call[0][0].table_id == "games"
    )
    schema = games_table.schema
    
    # Check required fields
    required_fields = {
        field.name: field.field_type
        for field in schema
        if field.mode == "REQUIRED"
    }
    assert "game_id" in required_fields
    assert required_fields["game_id"] == "INTEGER"
    assert "name" in required_fields
    assert required_fields["name"] == "STRING"
    
    # Check repeated fields
    repeated_fields = {
        field.name: field.field_type
        for field in schema
        if field.mode == "REPEATED"
    }
    assert "categories" in repeated_fields
    assert "mechanics" in repeated_fields
    assert "families" in repeated_fields

def test_main_function(mock_bigquery_client, mock_config):
    """Test main setup function."""
    with mock.patch("google.cloud.bigquery.Client", return_value=mock_bigquery_client):
        main()
        
        # Should create all datasets
        dataset_calls = [
            call[0][0].dataset_id
            for call in mock_bigquery_client.create_dataset.call_args_list
        ]
        assert mock_config["datasets"]["raw"] in dataset_calls
        assert mock_config["datasets"]["transformed"] in dataset_calls
        assert mock_config["datasets"]["reporting"] in dataset_calls
        assert mock_config["datasets"]["monitoring"] in dataset_calls
        
        # Should create raw tables
        assert mock_bigquery_client.create_table.call_count == 3

def test_main_function_error_handling(mock_config):
    """Test main function error handling."""
    mock_client = mock.Mock(spec=bigquery.Client)
    mock_client.create_dataset.side_effect = Exception("Failed to create dataset")
    
    with mock.patch("google.cloud.bigquery.Client", return_value=mock_client):
        with pytest.raises(Exception):
            main()

def test_table_schema_validation(mock_bigquery_client, mock_config):
    """Test table schema validation."""
    dataset_id = mock_config["datasets"]["raw"]
    
    create_raw_tables(mock_bigquery_client, dataset_id)
    
    # Verify request_log table schema
    request_log_table = next(
        call[0][0] for call in mock_bigquery_client.create_table.call_args_list
        if call[0][0].table_id == "request_log"
    )
    schema = request_log_table.schema
    
    # Check timestamp fields
    timestamp_fields = {
        field.name: field.field_type
        for field in schema
        if field.field_type == "TIMESTAMP"
    }
    assert "request_timestamp" in timestamp_fields
    assert "response_timestamp" in timestamp_fields
    
    # Check numeric fields
    numeric_fields = {
        field.name: field.field_type
        for field in schema
        if field.field_type in ["INTEGER", "FLOAT"]
    }
    assert "status_code" in numeric_fields
    assert "retry_count" in numeric_fields

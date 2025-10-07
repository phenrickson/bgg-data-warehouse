"""Tests for dataset migration functionality."""

import pytest
from unittest.mock import Mock, patch, call
from src.warehouse.migrate_datasets import get_tables_and_views, migrate_dataset, main


@pytest.fixture
def mock_bigquery_client():
    """Mock BigQuery client fixture."""
    with patch("google.cloud.bigquery.Client") as mock_client:
        # Mock list_tables response
        mock_table1 = Mock()
        mock_table1.table_id = "table1"
        mock_table2 = Mock()
        mock_table2.table_id = "table2"
        mock_client.return_value.list_tables.return_value = [mock_table1, mock_table2]

        # Mock dataset operations
        mock_dataset = Mock()
        mock_dataset.location = "US"
        mock_client.return_value.get_dataset.return_value = mock_dataset

        yield mock_client.return_value


def test_get_tables_and_views(mock_bigquery_client):
    """Test getting all tables and views from a dataset."""
    # Mock table objects with table_type
    mock_table1 = Mock()
    mock_table1.table_id = "table1"
    mock_table1.reference = Mock()

    mock_table2 = Mock()
    mock_table2.table_id = "table2"
    mock_table2.reference = Mock()

    mock_bigquery_client.list_tables.return_value = [mock_table1, mock_table2]

    # Mock get_table responses with table_type
    mock_table_obj1 = Mock()
    mock_table_obj1.table_type = "TABLE"
    mock_table_obj2 = Mock()
    mock_table_obj2.table_type = "TABLE"

    mock_bigquery_client.get_table.side_effect = [mock_table_obj1, mock_table_obj2]

    tables, views = get_tables_and_views(mock_bigquery_client, "project.dataset")

    assert tables == ["table1", "table2"]
    assert views == []
    mock_bigquery_client.list_tables.assert_called_once_with("project.dataset")


@patch("src.warehouse.migrate_datasets.get_tables_and_views")
def test_migrate_dataset_existing_destination(mock_get_tables, mock_bigquery_client):
    """Test migrating dataset when destination already exists."""
    # Configure mock
    mock_bigquery_client.project = "test-project"
    mock_get_tables.return_value = (["table1", "table2"], [])

    # Execute migration
    migrate_dataset("source_dataset", "dest_dataset", "test-project")

    # Verify dataset checks
    mock_bigquery_client.get_dataset.assert_called_with("test-project.dest_dataset")
    mock_bigquery_client.create_dataset.assert_not_called()

    # Verify table creation calls
    expected_queries = [
        """
        CREATE OR REPLACE TABLE `test-project.dest_dataset.table1` AS
        SELECT * 
        FROM `test-project.source_dataset.table1`
        """,
        """
        CREATE OR REPLACE TABLE `test-project.dest_dataset.table2` AS
        SELECT * 
        FROM `test-project.source_dataset.table2`
        """,
    ]

    assert mock_bigquery_client.query.call_count == 2
    calls = mock_bigquery_client.query.call_args_list
    for query, expected in zip(calls, expected_queries):
        assert query[0][0].strip() == expected.strip()


@patch("src.warehouse.migrate_datasets.get_tables_and_views")
def test_migrate_dataset_new_destination(mock_get_tables, mock_bigquery_client):
    """Test migrating dataset when destination needs to be created."""
    # Configure mock to raise exception on first get_dataset call
    mock_bigquery_client.project = "test-project"
    mock_source_dataset = Mock()
    mock_source_dataset.location = "US"
    mock_bigquery_client.get_dataset.side_effect = [Exception(), mock_source_dataset]
    mock_get_tables.return_value = (["table1", "table2"], [])

    # Execute migration
    migrate_dataset("source_dataset", "dest_dataset", "test-project")

    # Verify dataset creation
    mock_bigquery_client.create_dataset.assert_called_once()
    dataset_arg = mock_bigquery_client.create_dataset.call_args[0][0]
    assert dataset_arg.dataset_id == "dest_dataset"
    assert dataset_arg.location == "US"

    # Verify table creation proceeded
    assert mock_bigquery_client.query.call_count == 2


@patch("google.cloud.bigquery.Client")
@patch("src.warehouse.migrate_datasets.get_tables_and_views")
def test_migrate_dataset_custom_project(mock_get_tables, mock_client_class):
    """Test migrating dataset with custom project ID."""
    # Configure mock client
    mock_client = Mock()
    mock_client.project = "custom-project"
    mock_client_class.return_value = mock_client
    mock_get_tables.return_value = ([], [])

    # Execute migration with custom project
    migrate_dataset("source_dataset", "dest_dataset", "custom-project")

    # Verify client was created with custom project
    mock_client_class.assert_called_once_with(project="custom-project")


@patch("src.warehouse.migrate_datasets.get_tables_and_views")
def test_migrate_dataset_handles_empty_dataset(mock_get_tables, mock_bigquery_client):
    """Test migrating an empty dataset."""
    # Mock empty dataset
    mock_bigquery_client.project = "test-project"
    mock_get_tables.return_value = ([], [])

    # Execute migration
    migrate_dataset("source_dataset", "dest_dataset", "test-project")

    # Verify no table creation was attempted
    mock_bigquery_client.query.assert_not_called()


@patch("google.cloud.bigquery.Client")
@patch("argparse.ArgumentParser.parse_args")
def test_cli_script(mock_parse_args, mock_client):
    """Test CLI script operation."""
    # Mock CLI arguments
    mock_parse_args.return_value = Mock(
        source_dataset="source", dest_dataset="dest", project_id="test-project"
    )

    # Mock BigQuery client to prevent real API calls
    mock_bigquery_client = Mock()
    mock_bigquery_client.project = "test-project"
    mock_client.return_value = mock_bigquery_client

    # Mock the dataset and table operations
    mock_bigquery_client.get_dataset.return_value = Mock(location="US")
    mock_bigquery_client.list_tables.return_value = []

    # Run CLI
    main()

    # Verify BigQuery client was created with correct project
    mock_client.assert_called_once_with(project="test-project")

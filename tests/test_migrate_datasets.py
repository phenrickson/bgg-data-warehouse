"""Tests for dataset migration functionality."""

import pytest
from unittest.mock import Mock, patch, call

from src.warehouse.migrate_datasets import get_all_tables, migrate_dataset


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


def test_get_all_tables(mock_bigquery_client):
    """Test getting all tables from a dataset."""
    tables = get_all_tables(mock_bigquery_client, "project.dataset")

    assert tables == ["table1", "table2"]
    mock_bigquery_client.list_tables.assert_called_once_with("project.dataset")


def test_migrate_dataset_existing_destination(mock_bigquery_client):
    """Test migrating dataset when destination already exists."""
    # Configure mock
    mock_bigquery_client.project = "test-project"

    # Execute migration
    migrate_dataset("source_dataset", "dest_dataset")

    # Verify dataset checks
    mock_bigquery_client.get_dataset.assert_called_with("test-project.dest_dataset")
    mock_bigquery_client.create_dataset.assert_not_called()

    # Verify table creation calls
    expected_queries = [
        """
        CREATE TABLE `test-project.dest_dataset.table1` AS
        SELECT * 
        FROM `test-project.source_dataset.table1`
        """,
        """
        CREATE TABLE `test-project.dest_dataset.table2` AS
        SELECT * 
        FROM `test-project.source_dataset.table2`
        """,
    ]

    assert mock_bigquery_client.query.call_count == 2
    calls = mock_bigquery_client.query.call_args_list
    for query, expected in zip(calls, expected_queries):
        assert query[0][0].strip() == expected.strip()


def test_migrate_dataset_new_destination(mock_bigquery_client):
    """Test migrating dataset when destination needs to be created."""
    # Configure mock to raise exception on first get_dataset call
    mock_bigquery_client.project = "test-project"
    mock_bigquery_client.get_dataset.side_effect = [Exception(), Mock()]

    # Execute migration
    migrate_dataset("source_dataset", "dest_dataset")

    # Verify dataset creation
    mock_bigquery_client.create_dataset.assert_called_once()
    dataset_arg = mock_bigquery_client.create_dataset.call_args[0][0]
    assert dataset_arg.dataset_id == "dest_dataset"
    assert dataset_arg.location == "US"

    # Verify table creation proceeded
    assert mock_bigquery_client.query.call_count == 2


@patch("src.warehouse.migrate_datasets.get_bigquery_client")
def test_migrate_dataset_custom_project(mock_get_client, mock_bigquery_client):
    """Test migrating dataset with custom project ID."""
    # Execute migration with custom project
    migrate_dataset("source_dataset", "dest_dataset", "custom-project")

    # Verify client was created with custom project
    mock_get_client.assert_called_once_with("custom-project")


def test_migrate_dataset_handles_empty_dataset(mock_bigquery_client):
    """Test migrating an empty dataset."""
    # Mock empty dataset
    mock_bigquery_client.list_tables.return_value = []
    mock_bigquery_client.project = "test-project"

    # Execute migration
    migrate_dataset("source_dataset", "dest_dataset")

    # Verify no table creation was attempted
    mock_bigquery_client.query.assert_not_called()


@patch("argparse.ArgumentParser.parse_args")
@patch("src.warehouse.migrate_datasets.migrate_dataset")
def test_cli_script(mock_migrate, mock_parse_args):
    """Test CLI script operation."""
    from src.scripts.migrate_datasets import main

    # Mock CLI arguments
    mock_parse_args.return_value = Mock(
        source_dataset="source", dest_dataset="dest", project_id="test-project"
    )

    # Run CLI
    main()

    # Verify migration was called with correct arguments
    mock_migrate.assert_called_once_with("source", "dest", "test-project")

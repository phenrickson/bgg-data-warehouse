"""Tests for BigQuery setup module."""

import os
from unittest import mock

import pytest
from google.api_core import exceptions
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from src.warehouse.setup_bigquery import BigQuerySetup, main


@pytest.fixture
def mock_config(sample_config):
    """Mock configuration."""
    with mock.patch("src.warehouse.setup_bigquery.get_bigquery_config", return_value=sample_config):
        yield sample_config


def test_create_dataset(mock_bigquery_client, mock_config):
    """Test creating a BigQuery dataset."""
    setup = BigQuerySetup("dev")
    setup.client = mock_bigquery_client
    dataset_ref = f"{mock_config['project']['id']}.{mock_config['project']['dataset']}"

    # Test successful dataset creation
    setup.create_dataset(dataset_ref)

    # Verify dataset was created with correct settings
    mock_bigquery_client.create_dataset.assert_called_once()
    dataset = mock_bigquery_client.create_dataset.call_args[0][0]
    assert dataset.dataset_id == mock_config["project"]["dataset"]
    assert dataset.location == "US"
    assert mock_bigquery_client.create_dataset.call_args[1] == {"exists_ok": True}


def test_create_dataset_exists(mock_bigquery_client, mock_config):
    """Test handling existing dataset."""
    setup = BigQuerySetup("dev")
    setup.client = mock_bigquery_client
    dataset_ref = f"{mock_config['project']['id']}.{mock_config['project']['dataset']}"

    # Mock create_dataset to simulate exists_ok behavior
    def mock_create_dataset(dataset, exists_ok=False):
        if exists_ok:
            return None
        raise exceptions.Conflict("Dataset exists")

    mock_bigquery_client.create_dataset.side_effect = mock_create_dataset

    # Should not raise an exception due to exists_ok=True
    setup.create_dataset(dataset_ref)

    # Verify attempt was made to create dataset
    mock_bigquery_client.create_dataset.assert_called_once()
    dataset = mock_bigquery_client.create_dataset.call_args[0][0]
    assert dataset.dataset_id == mock_config["project"]["dataset"]
    assert mock_bigquery_client.create_dataset.call_args[1] == {"exists_ok": True}


def test_setup_warehouse_dev(mock_bigquery_client, mock_config):
    """Test warehouse setup in dev environment."""
    setup = BigQuerySetup("dev")
    setup.client = mock_bigquery_client

    # Mock get_table to simulate tables don't exist
    mock_bigquery_client.get_table.side_effect = NotFound("Table not found")

    setup.setup_warehouse()

    # Verify dataset creation
    dataset_calls = mock_bigquery_client.create_dataset.call_args_list
    assert len(dataset_calls) == 2  # Main and raw datasets

    main_dataset = dataset_calls[0][0][0]
    raw_dataset = dataset_calls[1][0][0]
    assert main_dataset.dataset_id == mock_config["project"]["dataset"]
    assert raw_dataset.dataset_id == mock_config["datasets"]["raw"]

    # Verify table creation
    table_calls = mock_bigquery_client.create_table.call_args_list
    tables_created = [call[0][0].table_id for call in table_calls]

    # Check raw tables were created
    for table_name in mock_config["raw_tables"]:
        assert table_name in tables_created

    # Check main tables were created
    for table_name in mock_config["tables"]:
        assert table_name in tables_created

    # Verify table configurations
    for call in table_calls:
        table = call[0][0]
        if table.table_id == "request_log":
            assert table.time_partitioning is not None
            assert table.time_partitioning.field == "request_timestamp"
        elif table.table_id == "games":
            assert table.clustering_fields == ["game_id"]


def test_setup_warehouse_prod(mock_bigquery_client, mock_config):
    """Test warehouse setup in prod environment."""
    # Mock get_bigquery_config to return prod config
    prod_config = dict(mock_config)
    prod_config["project"] = {
        "id": mock_config["environments"]["prod"]["project_id"],
        "dataset": mock_config["environments"]["prod"]["dataset"],
        "location": mock_config["environments"]["prod"]["location"],
    }
    with mock.patch("src.warehouse.setup_bigquery.get_bigquery_config", return_value=prod_config):
        setup = BigQuerySetup("prod")
        setup.client = mock_bigquery_client

        # Mock get_table to simulate tables don't exist
        mock_bigquery_client.get_table.side_effect = NotFound("Table not found")

        setup.setup_warehouse()

        # Verify dataset creation with prod dataset
        dataset_calls = mock_bigquery_client.create_dataset.call_args_list
        assert len(dataset_calls) == 2  # Main and raw datasets

        main_dataset = dataset_calls[0][0][0]
        raw_dataset = dataset_calls[1][0][0]
        assert main_dataset.dataset_id == mock_config["environments"]["prod"]["dataset"]
        assert raw_dataset.dataset_id == mock_config["datasets"]["raw"]

        # Verify table creation
        table_calls = mock_bigquery_client.create_table.call_args_list
        tables_created = [call[0][0].table_id for call in table_calls]

        # Check raw tables were created
        for table_name in mock_config["raw_tables"]:
            assert table_name in tables_created

        # Check main tables were created
        for table_name in mock_config["tables"]:
            assert table_name in tables_created


def test_setup_warehouse_table_exists(mock_bigquery_client, mock_config):
    """Test handling existing tables."""
    setup = BigQuerySetup("dev")
    setup.client = mock_bigquery_client

    # Mock get_table to simulate tables exist
    mock_table = mock.Mock()
    mock_table.schema = []
    mock_bigquery_client.get_table.return_value = mock_table

    setup.setup_warehouse()

    # Should check for all tables
    get_table_calls = mock_bigquery_client.get_table.call_args_list
    tables_checked = [call[0][0] for call in get_table_calls]

    # Verify all tables were checked
    for table_name in mock_config["raw_tables"]:
        assert any(table_name in table for table in tables_checked)
    for table_name in mock_config["tables"]:
        assert any(table_name in table for table in tables_checked)

    # Should not create any tables since they exist
    mock_bigquery_client.create_table.assert_not_called()


def test_main_function(mock_bigquery_client, mock_config):
    """Test main setup function."""
    # Mock get_bigquery_config to return prod config
    prod_config = dict(mock_config)
    prod_config["project"] = {
        "id": mock_config["environments"]["prod"]["project_id"],
        "dataset": mock_config["environments"]["prod"]["dataset"],
        "location": mock_config["environments"]["prod"]["location"],
    }
    with (
        mock.patch(
            "src.warehouse.setup_bigquery.bigquery.Client", return_value=mock_bigquery_client
        ),
        mock.patch.dict(os.environ, {"ENVIRONMENT": "prod"}),
        mock.patch("src.warehouse.setup_bigquery.get_bigquery_config", return_value=prod_config),
    ):
        # Mock get_table to simulate tables don't exist
        mock_bigquery_client.get_table.side_effect = NotFound("Table not found")

        main()

        # Should create datasets with prod config
        dataset_calls = mock_bigquery_client.create_dataset.call_args_list
        assert len(dataset_calls) == 2  # Main and raw datasets

        main_dataset = dataset_calls[0][0][0]
        raw_dataset = dataset_calls[1][0][0]
        assert main_dataset.dataset_id == mock_config["environments"]["prod"]["dataset"]
        assert raw_dataset.dataset_id == mock_config["datasets"]["raw"]

        # Verify table creation
        table_calls = mock_bigquery_client.create_table.call_args_list
        tables_created = [call[0][0].table_id for call in table_calls]

        # Check raw tables were created
        for table_name in mock_config["raw_tables"]:
            assert table_name in tables_created

        # Check main tables were created
        for table_name in mock_config["tables"]:
            assert table_name in tables_created


def test_main_function_error_handling(mock_config):
    """Test main function error handling."""
    mock_client = mock.Mock(spec=bigquery.Client)
    mock_client.create_dataset.side_effect = Exception("Failed to create dataset")

    with mock.patch("src.warehouse.setup_bigquery.bigquery.Client", return_value=mock_client):
        with pytest.raises(Exception):
            main()


def test_table_schema_validation(mock_bigquery_client, mock_config):
    """Test table schema validation."""
    setup = BigQuerySetup("dev")
    setup.client = mock_bigquery_client

    # Mock get_table to simulate tables don't exist
    mock_bigquery_client.get_table.side_effect = NotFound("Table not found")

    setup.setup_warehouse()

    # Find the request_log table creation call
    request_log_table = None
    for call in mock_bigquery_client.create_table.call_args_list:
        table = call[0][0]
        if table.table_id == "request_log":
            request_log_table = table
            break

    assert request_log_table is not None
    schema = request_log_table.schema

    # Check timestamp fields
    timestamp_fields = {
        field.name: field.field_type for field in schema if field.field_type == "TIMESTAMP"
    }
    assert "request_timestamp" in timestamp_fields

    # Check numeric fields
    numeric_fields = {
        field.name: field.field_type for field in schema if field.field_type in ["INTEGER", "FLOAT"]
    }
    assert "status_code" in numeric_fields

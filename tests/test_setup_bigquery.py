"""Tests for BigQuery setup module."""

from unittest import mock
from unittest.mock import MagicMock, patch

import pytest
from google.api_core import exceptions
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from src.warehouse.setup_bigquery import BigQuerySetup


@pytest.fixture
def mock_config():
    """Mock configuration matching current bigquery.yaml structure."""
    return {
        "project": {"id": "test-project", "dataset": "bgg_data_test", "location": "US"},
        "datasets": {"raw": "bgg_raw_test"},
        "tables": {"games": {"name": "games"}},
        "raw_tables": {
            "raw_responses": {"name": "raw_responses"},
            "thing_ids": {"name": "thing_ids"},
            "fetch_in_progress": {"name": "fetch_in_progress"},
            "request_log": {"name": "request_log", "time_partitioning": "request_timestamp"},
        },
    }


@pytest.fixture
def mock_bigquery_client():
    """Mock BigQuery client."""
    client = MagicMock(spec=bigquery.Client)
    client.create_dataset = MagicMock()
    client.create_table = MagicMock()
    client.get_table = MagicMock()
    client.query = MagicMock()
    return client


@pytest.fixture
def bigquery_setup(mock_config, mock_bigquery_client):
    """Create BigQuerySetup instance with mocked dependencies."""
    with patch("src.warehouse.setup_bigquery.get_bigquery_config", return_value=mock_config):
        with patch("google.cloud.bigquery.Client", return_value=mock_bigquery_client):
            setup = BigQuerySetup()
            return setup


class TestBigQuerySetup:
    """Test BigQuerySetup class."""

    def test_init(self, mock_config):
        """Test BigQuerySetup initialization."""
        with patch("src.warehouse.setup_bigquery.get_bigquery_config", return_value=mock_config):
            with patch("google.cloud.bigquery.Client") as mock_client:
                setup = BigQuerySetup()

                assert setup.config == mock_config
                assert setup.project_id == "test-project"
                assert setup.main_dataset == "test-project.bgg_data_test"
                assert setup.raw_dataset == "test-project.bgg_raw_test"
                mock_client.assert_called_once()

    def test_init_with_environment(self, mock_config):
        """Test BigQuerySetup initialization with environment."""
        with patch(
            "src.warehouse.setup_bigquery.get_bigquery_config", return_value=mock_config
        ) as mock_get_config:
            with patch("google.cloud.bigquery.Client"):
                BigQuerySetup(environment="dev")
                mock_get_config.assert_called_once_with("dev")

    def test_get_raw_schema_thing_ids(self, bigquery_setup):
        """Test getting schema for thing_ids table."""
        schema = bigquery_setup._get_raw_schema("thing_ids")

        field_names = [field.name for field in schema]
        assert "game_id" in field_names
        assert "type" in field_names
        assert "processed" in field_names
        assert "process_timestamp" in field_names
        assert "source" in field_names
        assert "load_timestamp" in field_names

        # Check required fields
        required_fields = [field.name for field in schema if field.mode == "REQUIRED"]
        assert "game_id" in required_fields
        assert "type" in required_fields
        assert "processed" in required_fields
        assert "source" in required_fields
        assert "load_timestamp" in required_fields

    def test_get_raw_schema_request_log(self, bigquery_setup):
        """Test getting schema for request_log table."""
        schema = bigquery_setup._get_raw_schema("request_log")

        field_names = [field.name for field in schema]
        assert "request_id" in field_names
        assert "url" in field_names
        assert "method" in field_names
        assert "game_ids" in field_names
        assert "status_code" in field_names
        assert "response_time" in field_names
        assert "error" in field_names
        assert "request_timestamp" in field_names

        # Check required fields
        required_fields = [field.name for field in schema if field.mode == "REQUIRED"]
        assert "request_id" in required_fields
        assert "url" in required_fields
        assert "method" in required_fields
        assert "request_timestamp" in required_fields

    def test_get_raw_schema_raw_responses(self, bigquery_setup):
        """Test getting schema for raw_responses table."""
        schema = bigquery_setup._get_raw_schema("raw_responses")

        field_names = [field.name for field in schema]
        assert "game_id" in field_names
        assert "response_data" in field_names
        assert "fetch_timestamp" in field_names
        assert "processed" in field_names
        assert "process_timestamp" in field_names
        assert "process_status" in field_names
        assert "process_attempt" in field_names
        assert "last_refresh_timestamp" in field_names
        assert "refresh_count" in field_names
        assert "next_refresh_due" in field_names

        # Check required fields
        required_fields = [field.name for field in schema if field.mode == "REQUIRED"]
        assert "game_id" in required_fields
        assert "response_data" in required_fields
        assert "fetch_timestamp" in required_fields
        assert "processed" in required_fields
        assert "process_attempt" in required_fields

    def test_get_schema_games(self, bigquery_setup):
        """Test getting schema for games table."""
        schema = bigquery_setup._get_schema("games")

        field_names = [field.name for field in schema]
        assert "game_id" in field_names
        assert "type" in field_names
        assert "primary_name" in field_names
        assert "year_published" in field_names
        assert "min_players" in field_names
        assert "max_players" in field_names
        assert "average_rating" in field_names
        assert "load_timestamp" in field_names

        # Check required fields
        required_fields = [field.name for field in schema if field.mode == "REQUIRED"]
        assert "game_id" in required_fields
        assert "type" in required_fields
        assert "primary_name" in required_fields
        assert "load_timestamp" in required_fields

    def test_create_dataset_success(self, bigquery_setup):
        """Test successful dataset creation."""
        dataset_ref = "test-project.test_dataset"

        bigquery_setup.create_dataset(dataset_ref)

        # Verify dataset creation was called
        bigquery_setup.client.create_dataset.assert_called_once()
        created_dataset = bigquery_setup.client.create_dataset.call_args[0][0]
        assert created_dataset.dataset_id == "test_dataset"  # dataset_id is just the dataset name
        assert created_dataset.location == "US"

    def test_create_dataset_error(self, bigquery_setup):
        """Test dataset creation with error."""
        dataset_ref = "test-project.test_dataset"
        bigquery_setup.client.create_dataset.side_effect = Exception("Failed to create")

        with pytest.raises(Exception, match="Failed to create"):
            bigquery_setup.create_dataset(dataset_ref)

    def test_create_table_new_table(self, bigquery_setup, mock_config):
        """Test creating a new table."""
        table_config = {"name": "thing_ids"}
        dataset_ref = "test-project.bgg_raw_test"

        # Mock table doesn't exist
        bigquery_setup.client.get_table.side_effect = NotFound("Table not found")

        bigquery_setup.create_table(table_config, dataset_ref, is_raw=True)

        # Verify table creation was called
        bigquery_setup.client.create_table.assert_called_once()
        created_table = bigquery_setup.client.create_table.call_args[0][0]
        assert created_table.table_id == "thing_ids"  # table_id is just the table name

    def test_create_table_existing_table(self, bigquery_setup, mock_config):
        """Test handling existing table."""
        table_config = {"name": "thing_ids"}
        dataset_ref = "test-project.bgg_raw_test"

        # Mock existing table
        existing_table = MagicMock()
        existing_table.schema = bigquery_setup._get_raw_schema("thing_ids")
        bigquery_setup.client.get_table.return_value = existing_table

        bigquery_setup.create_table(table_config, dataset_ref, is_raw=True)

        # Should not create new table
        bigquery_setup.client.create_table.assert_not_called()

    def test_create_table_with_missing_fields(self, bigquery_setup, mock_config):
        """Test adding missing fields to existing table."""
        table_config = {"name": "thing_ids"}
        dataset_ref = "test-project.bgg_raw_test"

        # Mock existing table with missing fields
        existing_table = MagicMock()
        # Only include some fields to simulate missing fields
        existing_table.schema = [
            bigquery.SchemaField("game_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("type", "STRING", mode="REQUIRED"),
        ]
        bigquery_setup.client.get_table.return_value = existing_table

        # Mock query result
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = None
        bigquery_setup.client.query.return_value = mock_query_job

        bigquery_setup.create_table(table_config, dataset_ref, is_raw=True)

        # Should add missing fields
        assert bigquery_setup.client.query.call_count > 0

    def test_create_table_with_time_partitioning(self, bigquery_setup, mock_config):
        """Test creating table with time partitioning."""
        table_config = {"name": "request_log", "time_partitioning": "request_timestamp"}
        dataset_ref = "test-project.bgg_raw_test"

        # Mock table doesn't exist
        bigquery_setup.client.get_table.side_effect = NotFound("Table not found")

        bigquery_setup.create_table(table_config, dataset_ref, is_raw=True)

        # Verify table was created with partitioning
        bigquery_setup.client.create_table.assert_called_once()
        created_table = bigquery_setup.client.create_table.call_args[0][0]
        assert created_table.time_partitioning is not None
        assert created_table.time_partitioning.field == "request_timestamp"

    def test_create_table_with_clustering(self, bigquery_setup, mock_config):
        """Test creating table with clustering fields."""
        table_config = {"name": "games", "clustering_fields": ["game_id"]}
        dataset_ref = "test-project.bgg_data_test"

        # Mock table doesn't exist
        bigquery_setup.client.get_table.side_effect = NotFound("Table not found")

        bigquery_setup.create_table(table_config, dataset_ref, is_raw=False)

        # Verify table was created with clustering
        bigquery_setup.client.create_table.assert_called_once()
        created_table = bigquery_setup.client.create_table.call_args[0][0]
        assert created_table.clustering_fields == ["game_id"]

    def test_create_table_no_schema(self, bigquery_setup, mock_config):
        """Test handling table with no schema defined."""
        table_config = {"name": "unknown_table"}
        dataset_ref = "test-project.bgg_raw_test"

        bigquery_setup.create_table(table_config, dataset_ref, is_raw=True)

        # Should not create table if no schema
        bigquery_setup.client.create_table.assert_not_called()

    def test_create_table_error(self, bigquery_setup, mock_config):
        """Test table creation with error."""
        table_config = {"name": "thing_ids"}
        dataset_ref = "test-project.bgg_raw_test"

        bigquery_setup.client.get_table.side_effect = NotFound("Table not found")
        bigquery_setup.client.create_table.side_effect = Exception("Failed to create table")

        with pytest.raises(Exception, match="Failed to create table"):
            bigquery_setup.create_table(table_config, dataset_ref, is_raw=True)

    def test_setup_warehouse_success(self, bigquery_setup, mock_config):
        """Test successful warehouse setup."""
        # Mock table doesn't exist for all tables
        bigquery_setup.client.get_table.side_effect = NotFound("Table not found")

        bigquery_setup.setup_warehouse()

        # Should create main dataset
        assert bigquery_setup.client.create_dataset.call_count >= 1

        # Should create raw dataset
        dataset_calls = [
            call[0][0].dataset_id for call in bigquery_setup.client.create_dataset.call_args_list
        ]
        assert "bgg_data_test" in dataset_calls
        assert "bgg_raw_test" in dataset_calls

        # Should create tables (1 main table + 4 raw tables)
        assert bigquery_setup.client.create_table.call_count == 5

    def test_setup_warehouse_error(self, bigquery_setup, mock_config):
        """Test warehouse setup with error."""
        bigquery_setup.client.create_dataset.side_effect = Exception("Setup failed")

        with pytest.raises(Exception, match="Setup failed"):
            bigquery_setup.setup_warehouse()


def test_main_function(mock_config):
    """Test main setup function."""
    with patch("src.warehouse.setup_bigquery.get_bigquery_config", return_value=mock_config):
        with patch("google.cloud.bigquery.Client") as mock_client:
            mock_client_instance = MagicMock()
            mock_client.return_value = mock_client_instance

            # Mock table doesn't exist
            mock_client_instance.get_table.side_effect = NotFound("Table not found")

            from src.warehouse.setup_bigquery import main

            main()

            # Should create datasets and tables
            assert mock_client_instance.create_dataset.call_count >= 1
            assert mock_client_instance.create_table.call_count >= 1


def test_main_function_with_environment():
    """Test main function with environment variable."""
    with patch.dict("os.environ", {"ENVIRONMENT": "dev"}):
        with patch("src.warehouse.setup_bigquery.BigQuerySetup") as mock_setup:
            mock_instance = MagicMock()
            mock_setup.return_value = mock_instance

            from src.warehouse.setup_bigquery import main

            main()

            # Should create setup with dev environment
            mock_setup.assert_called_once_with("dev")
            mock_instance.setup_warehouse.assert_called_once()


def test_main_function_error():
    """Test main function error handling."""
    with patch("src.warehouse.setup_bigquery.BigQuerySetup") as mock_setup:
        mock_instance = MagicMock()
        mock_instance.setup_warehouse.side_effect = Exception("Setup failed")
        mock_setup.return_value = mock_instance

        from src.warehouse.setup_bigquery import main

        with pytest.raises(Exception, match="Setup failed"):
            main()

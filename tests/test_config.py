"""Tests for configuration management."""

import os
from unittest import mock

import pytest
import yaml

from src.config import ConfigError, get_bigquery_config, get_environment, load_config


@pytest.fixture
def mock_config_file(tmp_path):
    """Create a mock config file matching actual structure."""
    config = {
        "environments": {
            "dev": {"project_id": "test-project", "dataset": "test_dataset_dev", "location": "US"},
            "test": {
                "project_id": "test-project",
                "dataset": "test_dataset_test",
                "location": "US",
            },
            "prod": {
                "project_id": "test-project",
                "dataset": "test_dataset_prod",
                "location": "US",
            },
        },
        "datasets": {"raw": "test_raw"},
        "tables": {"games": {"name": "games"}},
        "raw_tables": {
            "raw_responses": {"name": "raw_responses"},
            "thing_ids": {"name": "thing_ids"},
        },
    }

    config_dir = tmp_path / "config"
    config_dir.mkdir()
    config_file = config_dir / "bigquery.yaml"

    with open(config_file, "w") as f:
        yaml.dump(config, f)

    return config_file


def test_load_config_file_not_found():
    """Test loading non-existent config file."""
    with pytest.raises(FileNotFoundError):
        load_config("nonexistent")


def test_load_config_invalid_yaml(tmp_path):
    """Test loading invalid YAML config."""
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    config_file = config_dir / "invalid.yaml"

    with open(config_file, "w") as f:
        f.write("invalid: :")  # This is invalid YAML syntax

    # Mock Path(__file__).parent.parent to return our tmp_path
    with mock.patch("src.config.Path") as mock_path:
        mock_path.return_value.parent.parent = tmp_path
        with pytest.raises(yaml.YAMLError):
            load_config("invalid")


def test_load_config_success(mock_config_file):
    """Test successful config loading."""
    # Mock Path(__file__).parent.parent to return our test directory
    with mock.patch("src.config.Path") as mock_path:
        mock_path.return_value.parent.parent = mock_config_file.parent.parent
        config = load_config("bigquery")

        assert "environments" in config
        assert "dev" in config["environments"]
        assert config["environments"]["dev"]["project_id"] == "test-project"
        assert config["environments"]["dev"]["location"] == "US"
        assert config["datasets"]["raw"] == "test_raw"


def test_get_bigquery_config(mock_config_file):
    """Test BigQuery config getter."""
    # Mock Path(__file__).parent.parent to return our test directory
    # Also mock the environment to ensure we get dev environment and clear interfering env vars
    with mock.patch("src.config.Path") as mock_path:
        mock_path.return_value.parent.parent = mock_config_file.parent.parent
        # Clear environment variables that could interfere with the test
        # Only set ENVIRONMENT, exclude GCP_PROJECT_ID and GCS_BUCKET
        env_vars = {"ENVIRONMENT": "dev"}

        with mock.patch.dict(os.environ, env_vars, clear=True):
            config = get_bigquery_config()

            # Check the structure returned by get_bigquery_config
            assert "project" in config
            assert "datasets" in config
            assert "tables" in config
            assert "raw_tables" in config
            assert "environments" in config

            # Check the project structure is properly transformed
            assert config["project"]["id"] == "test-project"
            assert config["project"]["dataset"] == "test_dataset_dev"  # default to dev
            assert config["project"]["location"] == "US"


def test_get_environment_default():
    """Test get_environment with default value."""
    with mock.patch.dict(os.environ, {}, clear=True):
        env = get_environment()
        assert env == "dev"


def test_get_environment_from_env_var():
    """Test get_environment with environment variable set."""
    with mock.patch.dict(os.environ, {"ENVIRONMENT": "prod"}):
        env = get_environment()
        assert env == "prod"


def test_get_environment_invalid():
    """Test get_environment with invalid environment."""
    with mock.patch.dict(os.environ, {"ENVIRONMENT": "invalid"}):
        with pytest.raises(ConfigError):
            get_environment()


def test_get_bigquery_config_invalid_environment(mock_config_file):
    """Test get_bigquery_config with invalid environment."""
    with mock.patch("src.config.Path") as mock_path:
        mock_path.return_value.parent.parent = mock_config_file.parent.parent
        with pytest.raises(ConfigError):
            get_bigquery_config("invalid")


def test_get_bigquery_config_with_specific_environment(mock_config_file):
    """Test get_bigquery_config with specific environment parameter."""
    with mock.patch("src.config.Path") as mock_path:
        mock_path.return_value.parent.parent = mock_config_file.parent.parent
        # Clear environment variables that could interfere with the test
        # Only set minimal environment, exclude GCP_PROJECT_ID and GCS_BUCKET
        env_vars = {}

        with mock.patch.dict(os.environ, env_vars, clear=True):
            config = get_bigquery_config("test")

            assert config["project"]["id"] == "test-project"
            assert config["project"]["dataset"] == "test_dataset_test"
            assert config["_environment"] == "test"


def test_get_bigquery_config_prod_dataset_no_suffix(mock_config_file):
    """Test that prod environment doesn't get dataset suffix."""
    with mock.patch("src.config.Path") as mock_path:
        mock_path.return_value.parent.parent = mock_config_file.parent.parent
        # Clear environment variables that could interfere with the test
        # Only set minimal environment, exclude GCP_PROJECT_ID and GCS_BUCKET
        env_vars = {}

        with mock.patch.dict(os.environ, env_vars, clear=True):
            config = get_bigquery_config("prod")

            assert config["project"]["dataset"] == "test_dataset_prod"
            assert config["_environment"] == "prod"

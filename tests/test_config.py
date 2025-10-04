"""Tests for configuration management."""

import os
from pathlib import Path
from unittest import mock

import pytest
import yaml

from src.config import load_config, get_bigquery_config


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


def test_load_config_env_override(mock_config_file):
    """Test environment variable override."""
    with mock.patch.dict(os.environ, {"GCP_PROJECT_ID": "env-project", "GCS_BUCKET": "env-bucket"}):
        # Mock Path(__file__).parent.parent to return our test directory
        with mock.patch("src.config.Path") as mock_path:
            mock_path.return_value.parent.parent = mock_config_file.parent.parent
            config = load_config("bigquery")

            # Environment overrides create new top-level keys
            assert config["project"]["id"] == "env-project"
            assert config["storage"]["bucket"] == "env-bucket"
            # But original environment structure should still exist
            assert "environments" in config


def test_get_bigquery_config(mock_config_file):
    """Test BigQuery config getter."""
    # Mock Path(__file__).parent.parent to return our test directory
    # Also mock the environment to ensure we get dev environment
    with mock.patch("src.config.Path") as mock_path:
        mock_path.return_value.parent.parent = mock_config_file.parent.parent
        with mock.patch.dict(os.environ, {"ENVIRONMENT": "dev"}, clear=False):
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

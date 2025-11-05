"""Tests for configuration management."""

import os
from pathlib import Path
from unittest import mock

import pytest
import yaml

from src.config import get_bigquery_config


@pytest.fixture
def mock_config_file(tmp_path):
    """Create a mock config file."""
    config = {
        "environments": {
            "dev": {
                "project_id": "test-project",
                "dataset": "test_dataset",
                "location": "US",
            },
            "prod": {
                "project_id": "test-project-prod",
                "dataset": "test_dataset_prod",
                "location": "US",
            },
        },
        "default_environment": "dev",
        "datasets": {
            "raw": "test_raw",
            "transformed": "test_transformed",
            "reporting": "test_reporting",
            "monitoring": "test_monitoring",
        },
        "storage": {"bucket": "test-bucket"},
        "tables": {
            "games": {
                "name": "games",
                "description": "Core game information",
            },
        },
    }

    config_dir = tmp_path / "config"
    config_dir.mkdir()
    config_file = config_dir / "bigquery.yaml"

    with open(config_file, "w") as f:
        yaml.dump(config, f)

    return config_file


def test_get_bigquery_config_dev(mock_config_file):
    """Test BigQuery config getter with dev environment."""
    with mock.patch("os.path.join", return_value=str(mock_config_file)):
        config = get_bigquery_config("dev")

        assert config["project"]["id"] == "test-project"
        assert config["project"]["dataset"] == "test_dataset"
        assert config["project"]["location"] == "US"
        assert config["storage"]["bucket"] == "test-bucket"


def test_get_bigquery_config_prod(mock_config_file):
    """Test BigQuery config getter with prod environment."""
    with mock.patch("os.path.join", return_value=str(mock_config_file)):
        config = get_bigquery_config("prod")

        assert config["project"]["id"] == "test-project-prod"
        assert config["project"]["dataset"] == "test_dataset_prod"
        assert config["project"]["location"] == "US"


def test_get_bigquery_config_env_override(mock_config_file):
    """Test environment variable override."""
    with (
        mock.patch.dict(os.environ, {"ENVIRONMENT": "prod"}),
        mock.patch("os.path.join", return_value=str(mock_config_file)),
    ):
        config = get_bigquery_config()

        assert config["project"]["id"] == "test-project-prod"
        assert config["project"]["dataset"] == "test_dataset_prod"

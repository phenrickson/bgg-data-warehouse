"""Tests for configuration management."""

import os
from pathlib import Path
from unittest import mock

import pytest
import yaml

from src.config import load_config, get_bigquery_config

@pytest.fixture
def mock_config_file(tmp_path):
    """Create a mock config file."""
    config = {
        "project": {
            "id": "test-project",
            "location": "US"
        },
        "datasets": {
            "raw": "test_raw",
            "transformed": "test_transformed",
            "reporting": "test_reporting",
            "monitoring": "test_monitoring"
        },
        "storage": {
            "bucket": "test-bucket",
            "temp_prefix": "tmp/",
            "archive_prefix": "archive/"
        },
        "loading": {
            "batch_size": 1000,
            "max_bad_records": 0,
            "write_disposition": "WRITE_APPEND"
        },
        "monitoring": {
            "freshness_threshold_hours": 24,
            "quality_check_schedule": "0 */4 * * *",
            "alert_on_failures": True
        }
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
    
    with mock.patch("src.config.Path.parent", return_value=tmp_path):
        with pytest.raises(yaml.YAMLError):
            load_config("invalid")

def test_load_config_success(mock_config_file):
    """Test successful config loading."""
    with mock.patch("src.config.Path.parent", return_value=mock_config_file.parent.parent):
        config = load_config("bigquery")
        
        assert config["project"]["id"] == "test-project"
        assert config["project"]["location"] == "US"
        assert config["storage"]["bucket"] == "test-bucket"

def test_load_config_env_override(mock_config_file):
    """Test environment variable override."""
    with mock.patch.dict(os.environ, {
        "GCP_PROJECT_ID": "env-project",
        "GCS_BUCKET": "env-bucket"
    }):
        with mock.patch("src.config.Path.parent", return_value=mock_config_file.parent.parent):
            config = load_config("bigquery")
            
            assert config["project"]["id"] == "env-project"
            assert config["storage"]["bucket"] == "env-bucket"

def test_get_bigquery_config(mock_config_file):
    """Test BigQuery config getter."""
    with mock.patch("src.config.Path.parent", return_value=mock_config_file.parent.parent):
        config = get_bigquery_config()
        
        assert "project" in config
        assert "storage" in config

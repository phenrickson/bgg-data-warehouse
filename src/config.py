"""Configuration module for the BGG data warehouse."""

import logging
import os
import yaml
from pathlib import Path
from typing import Dict, Optional
from dotenv import load_dotenv

# Get logger
logger = logging.getLogger(__name__)


def load_config(config_name: str) -> Dict:
    """Load configuration from YAML file.

    Args:
        config_name: Name of the config file (without .yaml extension)

    Returns:
        Dictionary containing configuration data

    Raises:
        FileNotFoundError: If config file doesn't exist
        yaml.YAMLError: If config file contains invalid YAML
    """
    # Get the project root directory (parent of src/)
    project_root = Path(__file__).parent.parent
    config_path = project_root / "config" / f"{config_name}.yaml"

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
    except yaml.YAMLError as e:
        logger.error(f"Invalid YAML in config file {config_path}: {e}")
        raise

    # Handle case where YAML file is empty or contains only null values
    if config is None:
        config = {}

    # Apply environment variable overrides
    config = _apply_env_overrides(config)

    return config


def _apply_env_overrides(config: Dict) -> Dict:
    """Apply environment variable overrides to config.

    Args:
        config: Configuration dictionary

    Returns:
        Configuration dictionary with environment overrides applied
    """
    # Create a copy to avoid mutating the original
    config = dict(config)

    # Override project ID if environment variable is set
    if "GCP_PROJECT_ID" in os.environ:
        if "project" not in config:
            config["project"] = {}
        config["project"]["id"] = os.environ["GCP_PROJECT_ID"]

    # Override GCS bucket if environment variable is set
    if "GCS_BUCKET" in os.environ:
        if "storage" not in config:
            config["storage"] = {}
        config["storage"]["bucket"] = os.environ["GCS_BUCKET"]

    return config


def get_bigquery_config(environment: Optional[str] = None) -> Dict:
    """Get BigQuery configuration.

    Args:
        environment: Optional environment name (dev/prod). If not provided,
                    uses ENVIRONMENT from .env file or default_environment from config.

    Returns:
        Dictionary containing BigQuery configuration
    """
    # Load the base configuration
    config = load_config("bigquery")

    # Load environment from .env if not provided
    if not environment:
        load_dotenv()
        environment = os.getenv("ENVIRONMENT")

    # Get environment
    env = environment or config.get("default_environment", "dev")
    if "environments" in config and env not in config["environments"]:
        raise ValueError(f"Invalid environment: {env}")

    # If this is the old-style config (with environments), return environment-specific config
    if "environments" in config:
        env_config = config["environments"][env]
        return {
            "project": {
                "id": env_config["project_id"],
                "dataset": env_config["dataset"],
                "location": env_config["location"],
            },
            "datasets": config.get("datasets", {}),
            "tables": config.get("tables", {}),
            "raw_tables": config.get("raw_tables", {}),
            "environments": config["environments"],  # Include environments in config
        }
    else:
        # For new-style config (with environment overrides), return the config as-is
        return config


def get_refresh_config() -> Dict:
    """Get refresh strategy configuration.

    Returns:
        Dictionary containing refresh strategy settings
    """
    return {
        "enabled": True,
        "base_interval_days": 7,
        "upcoming_interval_days": 3,
        "decay_factor": 2.0,
        "max_interval_days": 90,
        "refresh_batch_size": 200,
    }

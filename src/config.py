"""Configuration module for the BGG data warehouse."""

import logging
import os
from typing import Dict, Optional

import yaml

# Get logger
logger = logging.getLogger(__name__)


def get_bigquery_config(environment: Optional[str] = None) -> Dict:
    """Get BigQuery configuration.

    Args:
        environment: Optional environment name (dev/prod). If not provided,
                    uses ENVIRONMENT from .env file or default_environment from config.

    Returns:
        Dictionary containing BigQuery configuration
    """
    # Load environment from .env if not provided
    if not environment:
        from dotenv import load_dotenv

        load_dotenv()
        environment = os.getenv("ENVIRONMENT")
    config_path = os.path.join("config", "bigquery.yaml")
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    # Get environment
    env = environment or config.get("default_environment", "dev")
    logger.info(f"Using environment: {env}")
    if env not in config["environments"]:
        raise ValueError(f"Invalid environment: {env}")

    # Build config with environment-specific values
    env_config = config["environments"][env]
    logger.info(f"Environment config: {env_config}")
    return {
        "project": {
            "id": env_config["project_id"],
            "dataset": env_config["dataset"],
            "location": env_config["location"],
        },
        "datasets": config.get("datasets", {}),
        "tables": config["tables"],
        "raw_tables": config.get("raw_tables", {}),
        "environments": config["environments"],  # Include environments in config
    }


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

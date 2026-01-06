"""Configuration module for the BGG data warehouse."""

import logging
import os
from typing import Dict

import yaml

logger = logging.getLogger(__name__)


def get_bigquery_config() -> Dict:
    """Get BigQuery configuration.

    Returns:
        Dictionary containing BigQuery configuration
    """
    config_path = os.path.join("config", "bigquery.yaml")
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    return {
        "project": {
            "id": config["project_id"],
            "location": config["location"],
        },
        "datasets": config["datasets"],
        "refresh_policy": config.get("refresh_policy", {}),
    }

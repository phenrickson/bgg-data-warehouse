"""Configuration module for the BGG data warehouse."""

import os
from typing import Dict, Optional

import yaml

def get_bigquery_config(environment: Optional[str] = None) -> Dict:
    """Get BigQuery configuration.
    
    Args:
        environment: Optional environment name (dev/prod). If not provided,
                    uses default_environment from config.
    
    Returns:
        Dictionary containing BigQuery configuration
    """
    config_path = os.path.join("config", "bigquery.yaml")
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    
    # Get environment
    env = environment or config.get("default_environment", "dev")
    if env not in config["environments"]:
        raise ValueError(f"Invalid environment: {env}")
        
    # Build config with environment-specific values
    env_config = config["environments"][env]
    return {
        "project": {
            "id": env_config["project_id"],
            "dataset": env_config["dataset"],
            "location": env_config["location"]
        },
        "storage": config["storage"],
        "datasets": config.get("datasets", {}),
        "tables": config["tables"],
        "raw_tables": config.get("raw_tables", {})
    }

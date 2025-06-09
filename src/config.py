"""Configuration management for the BGG data warehouse."""

import os
from pathlib import Path
from typing import Any, Dict

import yaml
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

def load_config(config_name: str) -> Dict[str, Any]:
    """Load configuration from a YAML file.
    
    Args:
        config_name: Name of the configuration file (without .yaml extension)
        
    Returns:
        Dictionary containing the configuration
        
    Raises:
        FileNotFoundError: If the configuration file doesn't exist
        yaml.YAMLError: If the configuration file is invalid
    """
    config_path = Path(__file__).parent.parent / "config" / f"{config_name}.yaml"
    
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_path, "r") as f:
        try:
            config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"Error parsing configuration file: {e}")
    
    # Override with environment variables if they exist
    if config_name == "bigquery":
        config["project"]["id"] = os.getenv("GCP_PROJECT_ID", config["project"]["id"])
        config["storage"]["bucket"] = os.getenv("GCS_BUCKET", config["storage"]["bucket"])
    
    return config

def get_bigquery_config() -> Dict[str, Any]:
    """Load BigQuery-specific configuration.
    
    Returns:
        Dictionary containing BigQuery configuration
    """
    return load_config("bigquery")

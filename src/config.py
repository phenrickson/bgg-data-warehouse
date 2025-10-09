"""Configuration module for the BGG data warehouse."""

import logging
import os
from pathlib import Path
from typing import Literal

import yaml
from dotenv import load_dotenv

# Load environment variables once at module level
load_dotenv()

# Get logger
logger = logging.getLogger(__name__)

# Define valid environments
ValidEnvironment = Literal["dev", "test", "prod"]
VALID_ENVIRONMENTS = ["dev", "test", "prod"]


class ConfigError(Exception):
    """Configuration related errors."""

    pass


def get_environment() -> ValidEnvironment:
    """Get the current environment from ENVIRONMENT variable.

    Returns:
        The current environment, defaults to 'dev' if not set

    Raises:
        ConfigError: If environment is invalid
    """
    env = os.getenv("ENVIRONMENT", "dev").lower()

    if env not in VALID_ENVIRONMENTS:
        raise ConfigError(f"Invalid environment '{env}'. Must be one of: {VALID_ENVIRONMENTS}")

    return env


def load_config(config_name: str) -> dict:
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
        with open(config_path) as f:
            config = yaml.safe_load(f)
    except yaml.YAMLError as e:
        logger.error(f"Invalid YAML in config file {config_path}: {e}")
        raise

    # Handle case where YAML file is empty or contains only null values
    if config is None:
        config = {}

    # Return YAML config as-is - no overrides needed
    return config


def get_bigquery_config(environment: ValidEnvironment | None = None) -> dict:
    """Get BigQuery configuration for the specified environment.

    Args:
        environment: Optional environment name. If not provided,
                    uses ENVIRONMENT variable or defaults to 'dev'.

    Returns:
        Dictionary containing BigQuery configuration

    Raises:
        ConfigError: If environment is invalid
        FileNotFoundError: If config file doesn't exist
    """
    # Get environment
    if environment is None:
        environment = get_environment()
    elif environment not in VALID_ENVIRONMENTS:
        raise ConfigError(
            f"Invalid environment '{environment}'. Must be one of: {VALID_ENVIRONMENTS}"
        )

    # Load the base configuration
    config = load_config("bigquery")

    # Extract environment-specific config from YAML
    if "environments" in config and environment in config["environments"]:
        env_config = config["environments"][environment]

        # Add standardized project structure
        config["project"] = {
            "id": env_config["project_id"],
            "dataset": env_config["dataset"],
            "location": env_config["location"],
        }

        # Add storage config if bucket is specified
        if "bucket" in env_config:
            config["storage"] = {"bucket": env_config["bucket"]}
    else:
        raise ConfigError(f"No configuration found for environment '{environment}'")

    # Add environment metadata
    config["_environment"] = environment

    return config


def get_refresh_config() -> dict:
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


def get_api_config() -> dict:
    """Get API configuration from environment variables.

    Returns:
        Dictionary containing API configuration
    """
    return {
        "rate_limit": float(os.getenv("BGG_API_RATE_LIMIT", "2.0")),
        "retry_delay": int(os.getenv("BGG_API_RETRY_DELAY", "5")),
        "max_retries": int(os.getenv("BGG_MAX_RETRIES", "3")),
        "batch_size": int(os.getenv("BATCH_SIZE", "100")),  # Keep this one as is for now
    }


def get_logging_config() -> dict:
    """Get logging configuration from environment variables.

    Returns:
        Dictionary containing logging configuration
    """
    return {
        "level": os.getenv("BGG_LOG_LEVEL", "INFO"),
    }


def get_monitoring_config() -> dict:
    """Get monitoring configuration from environment variables.

    Returns:
        Dictionary containing monitoring configuration
    """
    return {
        "freshness_threshold_hours": int(os.getenv("FRESHNESS_THRESHOLD_HOURS", "24")),
        "quality_check_interval": int(os.getenv("QUALITY_CHECK_INTERVAL", "14400")),
        "alert_on_failures": os.getenv("ALERT_ON_FAILURES", "true").lower() == "true",
        "port": int(os.getenv("PORT", "8080")),
    }


# Convenience function for backward compatibility
def get_config() -> dict:
    """Get complete configuration for current environment.

    Returns:
        Dictionary containing all configuration sections
    """
    environment = get_environment()

    return {
        "environment": environment,
        "bigquery": get_bigquery_config(environment),
        "api": get_api_config(),
        "logging": get_logging_config(),
        "monitoring": get_monitoring_config(),
        "refresh": get_refresh_config(),
    }

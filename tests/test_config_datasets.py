"""Tests that the warehouse read API's serving datasets are configured."""

from src.config import get_bigquery_config


def test_datasets_include_serving_layers():
    datasets = get_bigquery_config()["datasets"]
    for name in ("core", "raw", "predictions", "analytics"):
        assert name in datasets, f"missing dataset config: {name}"

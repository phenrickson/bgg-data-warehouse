"""Shared BigQuery access for warehouse readers.

A thin layer over ``google.cloud.bigquery`` that centralizes client creation and
resolves configured dataset keys to fully-qualified ``project.dataset`` names, so the
reader modules never hard-code project/dataset strings.
"""

from functools import lru_cache

from google.cloud import bigquery

from src.config import get_bigquery_config


@lru_cache(maxsize=1)
def _cfg() -> dict:
    return get_bigquery_config()


def get_client() -> bigquery.Client:
    """Return a BigQuery client bound to the configured project."""
    return bigquery.Client(project=_cfg()["project"]["id"])


def dataset(name: str) -> str:
    """Return the fully-qualified ``project.dataset`` for a configured dataset key.

    Args:
        name: A key from ``config/bigquery.yaml`` ``datasets`` (e.g. ``"analytics"``).

    Raises:
        KeyError: If the dataset key is not configured.
    """
    cfg = _cfg()
    return f'{cfg["project"]["id"]}.{cfg["datasets"][name]}'

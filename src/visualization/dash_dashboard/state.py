"""Shared state for the Dash application."""

from google.cloud import bigquery
from google.auth import default
import os

class AppState:
    """Shared state container for the Dash application."""
    def __init__(self):
        credentials, _ = default()
        project_id = os.getenv("GCP_PROJECT_ID")
        self.bq_client = bigquery.Client(credentials=credentials, project=project_id)

# Create singleton instance
state = AppState()

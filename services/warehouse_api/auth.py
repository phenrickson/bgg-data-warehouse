"""Authentication helpers for the warehouse read API.

Caller authorization is enforced by **Cloud Run IAM** (the service is deployed
``--no-allow-unauthenticated``), so the app does not verify inbound tokens itself. This
module only resolves the GCP project and reports the credential source used for the
BigQuery client (Application Default Credentials).
"""

import logging
import os
from typing import Optional

from google.auth import default
from google.auth.exceptions import DefaultCredentialsError

logger = logging.getLogger(__name__)


class AuthenticationError(Exception):
    """Raised when the GCP project cannot be determined."""


class GCPAuthenticator:
    """Resolves the GCP project for the service's BigQuery client via ADC."""

    def __init__(self, project_id: Optional[str] = None):
        self.project_id = project_id or self._resolve_project_id()

    @staticmethod
    def _resolve_project_id() -> str:
        project_id = os.getenv("GCP_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT")
        if project_id:
            return project_id
        try:
            _, project_id = default()
            if project_id:
                return project_id
        except DefaultCredentialsError:
            pass
        raise AuthenticationError(
            "Could not determine GCP project. Set GCP_PROJECT_ID or provide GCP credentials."
        )

    def info(self) -> dict:
        """Credential-source summary for the health endpoint."""
        source = "application_default"
        if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
            source = "service_account_key"
        elif os.getenv("K_SERVICE"):  # set inside Cloud Run
            source = "cloud_run_runtime"
        return {"project_id": self.project_id, "credentials_source": source}

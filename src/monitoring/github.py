"""GitHub Actions run history, shared by Pipeline Status and the pipeline monitor.

Queries each workflow's runs with a ``created`` date range. The range changes on every
call, so the response can't come from the stale cached listing that false-alarmed the
old scrape heartbeat (see docs/superpowers/specs/2026-09-30-pipeline-status-design.md).
"""

from datetime import UTC, datetime
from typing import Any

import requests

RUN_FIELDS = (
    "created_at",
    "updated_at",
    "event",
    "status",
    "conclusion",
    "display_title",
    "html_url",
)


def iso(ts: datetime) -> str:
    return ts.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_ts(value: str) -> datetime:
    """Parse an ISO timestamp; a naive one is taken as UTC, not local time."""
    ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return ts if ts.tzinfo else ts.replace(tzinfo=UTC)


def fetch_runs(
    repo: str,
    workflow_file: str,
    start: datetime,
    end: datetime,
    token: str,
    session=requests,
) -> list[dict[str, Any]]:
    """Runs of one workflow created in ``[start, end]``, newest first, across all pages."""
    url = f"https://api.github.com/repos/{repo}/actions/workflows/{workflow_file}/runs"
    params: dict[str, Any] | None = {"created": f"{iso(start)}..{iso(end)}", "per_page": 100}
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
    runs: list[dict[str, Any]] = []
    while url:
        resp = session.get(url, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        runs.extend({k: r.get(k) for k in RUN_FIELDS} for r in resp.json()["workflow_runs"])
        url = resp.links.get("next", {}).get("url")
        params = None  # the next link already carries the query string
    return runs

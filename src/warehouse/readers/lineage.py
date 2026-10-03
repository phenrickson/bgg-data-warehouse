"""Reader for the admin lineage view.

The latest clean Dataform compilation of main (the lineage), plus BigQuery table
metadata from free ``tables.get`` calls — never a query. See
docs/superpowers/specs/2026-10-03-lineage-view-design.md.
"""

from concurrent.futures import ThreadPoolExecutor
from typing import Any, Optional

import google.auth
from google.api_core import exceptions as gexc
from google.auth.transport.requests import AuthorizedSession
from google.cloud import bigquery

from src.monitoring.lineage import latest_compilation_name
from src.warehouse.bq import get_client

DATAFORM_REPO = (
    "https://dataform.googleapis.com/v1beta1/projects/bgg-data-warehouse"
    "/locations/us-central1/repositories/bgg-data-warehouse"
)
_EMPTY_META = {"rows": None, "bytes": None, "last_modified": None, "type": None, "error": None}


def _session() -> AuthorizedSession:
    credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    return AuthorizedSession(credentials)


def fetch_compilation(session=None) -> tuple[dict[str, Any], dict[str, Any]]:
    """(summary, actions) of the newest clean compilation of main."""
    session = session or _session()
    resp = session.get(f"{DATAFORM_REPO}/compilationResults",
                       params={"pageSize": 20, "orderBy": "create_time desc"}, timeout=30)
    resp.raise_for_status()
    results = resp.json().get("compilationResults", [])
    name = latest_compilation_name(results)
    if name is None:
        raise RuntimeError("no clean compilation of main among the latest 20")
    summary = next(r for r in results if r.get("name") == name)

    actions: list[dict[str, Any]] = []
    params: dict[str, str] | None = None
    while True:
        resp = session.get(f"https://dataform.googleapis.com/v1beta1/{name}:query",
                           params=params, timeout=30)
        resp.raise_for_status()
        page = resp.json()
        actions.extend(page.get("compilationResultActions", []))
        token = page.get("nextPageToken")
        if not token:
            return summary, {"compilationResultActions": actions}
        params = {"pageToken": token}


def _meta(client: bigquery.Client, table_id: str) -> dict[str, Any]:
    try:
        table = client.get_table(table_id)
    except gexc.NotFound:
        return _EMPTY_META | {"error": "not found"}
    except gexc.Forbidden:
        return _EMPTY_META | {"error": "no access"}
    except gexc.GoogleAPICallError as exc:
        return _EMPTY_META | {"error": f"{exc.code}: {exc.message}"}
    except Exception as exc:  # timeout, connection reset, auth refresh: stays on this node
        return _EMPTY_META | {"error": f"{type(exc).__name__}: {exc}"}
    return {"rows": table.num_rows, "bytes": table.num_bytes, "last_modified": table.modified,
            "type": table.table_type, "error": None}


def fetch_table_meta(ids: list[str], client: Optional[bigquery.Client] = None) -> dict[str, dict]:
    """Row count, size, last modified and type per table; a failure stays on its node."""
    client = client or get_client()
    with ThreadPoolExecutor(max_workers=8) as pool:
        return dict(zip(ids, pool.map(lambda table_id: _meta(client, table_id), ids)))


def _flatten(fields, prefix: str = "") -> list[dict[str, Any]]:
    out = []
    for f in fields:
        name = f"{prefix}{f.name}"
        out.append({"name": name, "type": f.field_type, "mode": f.mode,
                    "description": f.description})
        if f.fields:
            out.extend(_flatten(f.fields, f"{name}."))
    return out


def fetch_table_schema(table_id: str, client: Optional[bigquery.Client] = None) -> list[dict]:
    """Column name, type, mode and description; nested RECORD fields as parent.child."""
    client = client or get_client()
    return _flatten(client.get_table(table_id).schema)

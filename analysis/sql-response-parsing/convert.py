"""Copy raw.raw_responses into scratch_parsing.responses_json as faithful JSON.

response_data is the Python repr of the xmltodict output. Each row is
literal_eval'd, checked to survive a JSON round trip unchanged, and stored as a
BigQuery JSON value with a content hash so verify_conversion.py can prove
BigQuery stored it unchanged too. Reads use list_rows (free).

  uv run python analysis/sql-response-parsing/convert.py
"""

import ast
import hashlib
import json
from typing import Any, Optional

from google.cloud import bigquery

PROJECT = "bgg-data-warehouse"
SCRATCH = f"{PROJECT}.scratch_parsing"
SOURCE = f"{PROJECT}.raw.raw_responses"
DEST = f"{SCRATCH}.responses_json"
CHUNK = 20_000

SCHEMA = [
    bigquery.SchemaField("record_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("game_id", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("fetch_timestamp", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("convert_status", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("content_hash", "STRING"),
    bigquery.SchemaField("response_json", "JSON"),
]


def assert_scratch(table_id: str) -> None:
    if not table_id.startswith(SCRATCH + "."):
        raise ValueError(f"refusing to write {table_id}: outside {SCRATCH}")


def content_hash(parsed: Any) -> str:
    canonical = json.dumps(parsed, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def convert_response(response_data: Optional[str]) -> tuple[str, Optional[dict]]:
    if response_data is None or not response_data.strip():
        return "empty", None
    try:
        parsed = json.loads(response_data)
        status = "json"
    except json.JSONDecodeError:
        try:
            parsed = ast.literal_eval(response_data)
        except (ValueError, SyntaxError, MemoryError, RecursionError):
            return "unparseable", None
        status = "repr"
    if not isinstance(parsed, dict):
        return "unparseable", None
    if json.loads(json.dumps(parsed, ensure_ascii=False)) != parsed:
        raise ValueError("JSON round trip changed the response")
    return status, parsed


def _flush(client: bigquery.Client, rows: list, first: bool) -> None:
    assert_scratch(DEST)
    config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        write_disposition="WRITE_TRUNCATE" if first else "WRITE_APPEND",
        time_partitioning=bigquery.TimePartitioning(field="fetch_timestamp"),
        clustering_fields=["game_id", "record_id"],
    )
    client.load_table_from_json(rows, DEST, job_config=config).result()


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="trial run on the first N rows")
    args = parser.parse_args()
    assert_scratch(DEST)
    client = bigquery.Client(project=PROJECT)
    source = client.get_table(SOURCE)
    fields = [f for f in source.schema if f.name in ("record_id", "game_id", "fetch_timestamp", "response_data")]
    counts: dict[str, int] = {}
    rows, first = [], True
    for row in client.list_rows(source, selected_fields=fields, page_size=5_000, max_results=args.limit):
        status, parsed = convert_response(row["response_data"])
        counts[status] = counts.get(status, 0) + 1
        rows.append({
            "record_id": row["record_id"],
            "game_id": row["game_id"],
            "fetch_timestamp": row["fetch_timestamp"].isoformat(),
            "convert_status": status,
            "content_hash": content_hash(parsed) if parsed is not None else None,
            "response_json": parsed,
        })
        if len(rows) >= CHUNK:
            _flush(client, rows, first)
            first, rows = False, []
            print(counts, flush=True)
    if rows:
        _flush(client, rows, first)
    print("done", counts)


if __name__ == "__main__":
    main()

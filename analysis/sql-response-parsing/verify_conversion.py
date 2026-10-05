"""Prove BigQuery stored every converted response unchanged.

Re-hashes each stored response_json (read with list_rows, free) and compares it
to the content_hash computed from the literal_eval'd original in convert.py.

  uv run python analysis/sql-response-parsing/verify_conversion.py
"""

import json
from typing import Any, Optional

from google.cloud import bigquery

from convert import DEST, PROJECT, content_hash


def check_row(stored: Any, expected_hash: Optional[str]) -> bool:
    if isinstance(stored, str):
        stored = json.loads(stored)
    if stored is None:
        return expected_hash is None
    return content_hash(stored) == expected_hash


def main() -> None:
    client = bigquery.Client(project=PROJECT)
    table = client.get_table(DEST)
    checked = mismatched = 0
    for row in client.list_rows(table, page_size=5_000):
        checked += 1
        if not check_row(row["response_json"], row["content_hash"]):
            mismatched += 1
            print("MISMATCH", row["record_id"])
    print(f"checked {checked}, mismatched {mismatched}")
    if mismatched:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

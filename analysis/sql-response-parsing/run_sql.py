"""Run one proof .sql file: guard, then dry-run or a capped run.

  uv run python analysis/sql-response-parsing/run_sql.py sql/parse_games.sql --dry-run
  uv run python analysis/sql-response-parsing/run_sql.py sql/parse_games.sql --max-gb 4
  ... --param as_of:TIMESTAMP:2026-04-05T00:00:00Z
"""

import argparse
from pathlib import Path

from google.cloud import bigquery

from sqlguard import check_sql

PROJECT = "bgg-data-warehouse"


def _param(spec: str) -> bigquery.ScalarQueryParameter:
    name, type_, value = spec.split(":", 2)
    return bigquery.ScalarQueryParameter(name, type_, value)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("file")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--max-gb", type=float)
    parser.add_argument("--param", action="append", default=[])
    args = parser.parse_args()

    path = Path(args.file)
    if not path.is_absolute():
        path = Path(__file__).parent / path
    sql = path.read_text(encoding="utf-8")
    check_sql(sql)

    client = bigquery.Client(project=PROJECT)
    params = [_param(p) for p in args.param]
    if args.dry_run:
        config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False, query_parameters=params)
        job = client.query(sql, job_config=config)
        print(f"{path.name}: {job.total_bytes_processed / 1e9:.3f} GB")
        return

    config = bigquery.QueryJobConfig(
        maximum_bytes_billed=int(args.max_gb * 1e9), query_parameters=params
    )
    job = client.query(sql, job_config=config)
    rows = list(job.result())
    print(f"{path.name}: billed {(job.total_bytes_billed or 0) / 1e9:.3f} GB")
    for row in rows[:200]:
        print(dict(row.items()))


if __name__ == "__main__":
    main()

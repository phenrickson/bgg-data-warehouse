"""Daily status of the discovery -> fetch -> refresh chain.

Answers, for a time window: did the jobs run, did we search, did we add (and
fetch) new game IDs, did we refresh old ones. Job answers come from each
workflow's Actions runs; data answers from what landed in ``raw.thing_ids`` and
``raw.fetched_responses``. A finding never fails the run - it sets ``flagged``,
which the Pipeline Status workflow turns into a GitHub issue update.

See docs/superpowers/specs/2026-09-30-pipeline-status-design.md.

Local replay of a past window:
    GH_TOKEN=$(gh auth token) PYTHONIOENCODING=utf-8 \
        uv run python -m src.pipeline.pipeline_status --start 2026-09-12T10:00:00Z \
        --end 2026-09-13T12:00:00Z
"""

import argparse
import os
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from google.cloud import bigquery

from src.monitoring.github import fetch_runs, iso, parse_ts
from src.warehouse.bq import dataset, get_client

DEFAULT_REPO = "phenrickson/bgg-data-warehouse"

# (label, workflow file) in chain order. Fetch Thing IDs is also the record that we searched.
JOBS = [
    ("Fetch Thing IDs", "fetch_thing_ids.yml"),
    ("Run Fetch New Games", "fetch_new_games.yml"),
    ("Run Refresh Old Games", "refresh.yml"),
]
SEARCH_JOB = "fetch_thing_ids.yml"


@dataclass
class JobRuns:
    label: str
    workflow_file: str
    runs: list[dict[str, Any]]

    @property
    def succeeded(self) -> bool:
        return any(r.get("conclusion") == "success" for r in self.runs)

    def describe(self) -> str:
        if not self.runs:
            return "no run in window"
        return ", ".join(
            f"{r.get('conclusion') or r.get('status')} ({r['event']}, {r['created_at']})"
            for r in self.runs
        )


@dataclass
class Check:
    question: str
    answer: str
    ok: bool


def fetch_warehouse_counts(
    start: datetime,
    end: datetime,
    client: bigquery.Client | None = None,
) -> dict[str, int]:
    """One row of counts for the window.

    Only ``boardgame`` IDs are fetched, so fetch coverage is measured on those. A
    refresh attempt is a fetch of a game that already had a successful fetch; it
    counts as refreshed only if it succeeded, so a batch BGG rejected still flags.
    """
    client = client or get_client()
    raw = dataset("raw")
    sql = f"""
        WITH
        new_ids AS (
          SELECT game_id, type
          FROM `{raw}.thing_ids`
          WHERE load_timestamp >= @window_start AND load_timestamp < @window_end
        ),
        fetches AS (
          SELECT game_id, fetch_timestamp, fetch_status
          FROM `{raw}.fetched_responses`
        ),
        first_success AS (
          SELECT game_id, MIN(fetch_timestamp) AS first_ok
          FROM fetches
          WHERE fetch_status = 'success'
          GROUP BY game_id
        ),
        window_fetches AS (
          SELECT
            f.game_id,
            f.fetch_status,
            IFNULL(f.fetch_timestamp > fs.first_ok, FALSE) AS is_refresh
          FROM fetches f
          LEFT JOIN first_success fs USING (game_id)
          WHERE f.fetch_timestamp >= @window_start AND f.fetch_timestamp < @window_end
        ),
        new_boardgames AS (
          SELECT
            n.game_id,
            COUNTIF(f.fetch_status = 'success') > 0 AS fetched,
            COUNT(f.game_id) > 0 AS attempted
          FROM new_ids n
          LEFT JOIN fetches f USING (game_id)
          WHERE n.type = 'boardgame'
          GROUP BY n.game_id
        )
        SELECT
          (SELECT COUNT(*) FROM new_ids) AS ids_found,
          (SELECT COUNTIF(type = 'boardgame') FROM new_ids) AS ids_boardgame,
          (SELECT COUNTIF(type = 'boardgameexpansion') FROM new_ids) AS ids_expansion,
          (SELECT COUNTIF(type = 'boardgameaccessory') FROM new_ids) AS ids_accessory,
          (SELECT COUNTIF(fetched) FROM new_boardgames) AS boardgames_fetched,
          (SELECT COUNTIF(attempted AND NOT fetched) FROM new_boardgames) AS boardgames_pending,
          (SELECT COUNTIF(NOT attempted) FROM new_boardgames) AS boardgames_unattempted,
          (SELECT COUNTIF(is_refresh) FROM window_fetches) AS refresh_attempts,
          (SELECT COUNTIF(is_refresh AND fetch_status = 'success') FROM window_fetches)
            AS refreshed,
          (SELECT COUNTIF(fetch_status != 'success') FROM window_fetches) AS failed_fetches
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("window_start", "TIMESTAMP", start),
            bigquery.ScalarQueryParameter("window_end", "TIMESTAMP", end),
        ]
    )
    rows = list(client.query(sql, job_config=job_config).result())
    return dict(rows[0].items())


def evaluate(jobs: list[JobRuns], counts: dict[str, int]) -> list[Check]:
    """Apply the spec's flag rules. ``ok=False`` means it needs a look."""
    checks = [Check(f"{j.label} ran", j.describe(), j.succeeded) for j in jobs]

    search = next(j for j in jobs if j.workflow_file == SEARCH_JOB)
    checks.append(
        Check(
            "Searched for new IDs",
            "yes" if search.succeeded else "no successful Fetch Thing IDs run",
            search.succeeded,
        )
    )

    # Zero has only happened during outages, but it can happen legitimately.
    checks.append(
        Check(
            "New IDs found",
            f"{counts['ids_found']} ({counts['ids_boardgame']} boardgame, "
            f"{counts['ids_expansion']} expansion, {counts['ids_accessory']} accessory)",
            counts["ids_found"] > 0,
        )
    )

    # Pending = attempted but not yet successful; the fetcher retries those itself.
    checks.append(
        Check(
            "New boardgames fetched",
            f"{counts['boardgames_fetched']} of {counts['ids_boardgame']} "
            f"({counts['boardgames_pending']} pending retry, "
            f"{counts['boardgames_unattempted']} never attempted)",
            counts["boardgames_unattempted"] == 0,
        )
    )

    checks.append(
        Check(
            "Old games refreshed",
            f"{counts['refreshed']} of {counts['refresh_attempts']} attempts succeeded",
            counts["refreshed"] > 0,
        )
    )
    checks.append(Check("Failed fetches", str(counts["failed_fetches"]), True))
    return checks


def render(checks: list[Check], start: datetime, end: datetime) -> str:
    flagged = [c for c in checks if not c.ok]
    heading = f"**{len(flagged)} check(s) need a look**" if flagged else "**All checks passed**"
    lines = [
        f"## Pipeline status: {iso(start)} → {iso(end)}",
        "",
        heading,
        "",
        "| | Question | Answer |",
        "|---|---|---|",
        *(f"| {'✅' if c.ok else '⚠️'} | {c.question} | {c.answer} |" for c in checks),
    ]
    return "\n".join(lines) + "\n"


def _append_env_file(var: str, text: str) -> None:
    path = os.environ.get(var)
    if path:
        with open(path, "a", encoding="utf-8") as f:
            f.write(text)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Report the daily pipeline status")
    parser.add_argument("--window-hours", type=int, default=26)
    parser.add_argument("--start", help="Window start (ISO, UTC). Overrides --window-hours.")
    parser.add_argument("--end", help="Window end (ISO, UTC). Default: now.")
    parser.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY", DEFAULT_REPO))
    parser.add_argument("--body-file", help="Also write the markdown status here.")
    args = parser.parse_args(argv)

    token = os.environ.get("GH_TOKEN")
    if not token:
        sys.exit("GH_TOKEN is not set (locally: GH_TOKEN=$(gh auth token))")

    end = parse_ts(args.end) if args.end else datetime.now(UTC)
    start = parse_ts(args.start) if args.start else end - timedelta(hours=args.window_hours)

    jobs = [
        JobRuns(label, wf, fetch_runs(args.repo, wf, start, end, token)) for label, wf in JOBS
    ]
    checks = evaluate(jobs, fetch_warehouse_counts(start, end))
    body = render(checks, start, end)
    flagged = any(not c.ok for c in checks)

    print(body)
    if args.body_file:
        with open(args.body_file, "w", encoding="utf-8") as f:
            f.write(body)
    _append_env_file("GITHUB_STEP_SUMMARY", body)
    _append_env_file("GITHUB_OUTPUT", f"flagged={'true' if flagged else 'false'}\n")


if __name__ == "__main__":
    main()

# Pipeline Status Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the Scrape Heartbeat with a daily Pipeline Status. It reports whether the jobs ran, whether we searched, whether new IDs were found and fetched, and whether old games were refreshed. When something doesn't pass, it updates a GitHub issue.

**Architecture:** `src/pipeline/pipeline_status.py` gathers two sources for a time window: the Actions runs of three workflows (GitHub REST, date-filtered) and one BigQuery counts query over `raw.thing_ids` and `raw.fetched_responses`. It evaluates the flag rules, renders a markdown table and writes `flagged=true|false` for the workflow. `.github/workflows/pipeline_status.yml` runs the script daily, then opens, comments on or closes a `pipeline-status` issue.

**Tech Stack:** Python 3.12, `google-cloud-bigquery`, `requests`, pytest, GitHub Actions, `gh` CLI.

**Spec:** `docs/superpowers/specs/2026-09-30-pipeline-status-design.md`

## Global Constraints

- Window default: 26 hours ending at run time. The schedule is `0 12 * * *` UTC.
- Monitored workflows: `fetch_thing_ids.yml`, `fetch_new_games.yml`, `refresh.yml`. A job passes only if at least one run in the window has `conclusion == "success"`; skipped, failed and missing all flag.
- New IDs found = 0 flags. Unattempted new boardgames > 0 flags. Pending-retry boardgames are reported, not flagged. Refreshed = 0 flags. Failed fetches never flag.
- The issue label is `pipeline-status`. The workflow goes red only when the check itself errors, never on a finding.
- Code style: ruff/black at line length 100. BigQuery access goes through `src.warehouse.bq` (`get_client`, `dataset`).
- Delivery: branch `feat/pipeline-status`, one PR, which Phil merges. Never commit to `main`.

## Review Focus

1. **A skipped run next to a successful one in the same window** (09-16: the home box dispatch succeeded at 06:01, then a `workflow_run` was skipped at 06:21). Any success passes. Pinned in Task 1 `test_job_passes_if_any_run_succeeded`.
2. **The Actions listing comes back empty or stale.** That reads as "no run in window" and must flag, not crash. Pinned in Task 1 `test_no_runs_flags_the_job`.
3. **The GitHub API returns an error (401/5xx).** The script must raise so the workflow goes red, not silently report "no runs". Pinned in Task 1 `test_fetch_job_runs_raises_on_http_error`.
4. **Timezone-naive `--start/--end` in a local replay.** These must be treated as UTC, not local time, or the window shifts by the local UTC offset. Pinned in Task 1 `test_parse_ts_treats_naive_as_utc`.
5. **The Windows console can't print ✅/⚠️ (cp1252).** The body file must be written as UTF-8, and the local replay command sets `PYTHONIOENCODING=utf-8`. Pinned in Task 1 `test_main_writes_outputs` (reads the body file back as UTF-8).

---

### Task 0: Branch

- [ ] **Step 1: Branch off the spec branch so spec, plan and code ship in one PR**

```bash
cd /c/Users/philh/projects/bgg-data-warehouse
git switch docs/pipeline-status-design
git switch -c feat/pipeline-status
```

---

### Task 1: `pipeline_status` module

**Files:**
- Create: `src/pipeline/pipeline_status.py`
- Test: `tests/test_pipeline_status.py`

**Interfaces:**
- Produces:
  - `fetch_job_runs(repo: str, workflow_file: str, start: datetime, end: datetime, token: str, session=requests) -> list[dict]`. Each dict has `created_at`, `event`, `status` and `conclusion`.
  - `fetch_warehouse_counts(start: datetime, end: datetime, client: bigquery.Client | None = None) -> dict[str, int]`. Keys: `ids_found`, `ids_boardgame`, `ids_expansion`, `ids_accessory`, `boardgames_fetched`, `boardgames_pending`, `boardgames_unattempted`, `refreshed`, `failed_fetches`.
  - `JobRuns(label, workflow_file, runs)` with `.succeeded` and `.describe()`. `Check(question, answer, ok)`.
  - `evaluate(jobs: list[JobRuns], counts: dict[str, int]) -> list[Check]`
  - `render(checks: list[Check], start: datetime, end: datetime) -> str`
  - `main(argv: list[str] | None = None) -> None`. CLI flags: `--window-hours`, `--start`, `--end`, `--repo`, `--body-file`. It writes `flagged=true|false` to `$GITHUB_OUTPUT` and the body to `$GITHUB_STEP_SUMMARY`.

- [ ] **Step 1: Write the failing tests**

`tests/test_pipeline_status.py`:

```python
"""Unit tests for the pipeline status check (GitHub and BigQuery mocked — no network)."""

from datetime import UTC, datetime

import pytest

from src.pipeline import pipeline_status as ps

START = datetime(2026, 9, 29, 10, 0, tzinfo=UTC)
END = datetime(2026, 9, 30, 12, 0, tzinfo=UTC)

HEALTHY_COUNTS = {
    "ids_found": 40,
    "ids_boardgame": 23,
    "ids_expansion": 1,
    "ids_accessory": 16,
    "boardgames_fetched": 23,
    "boardgames_pending": 0,
    "boardgames_unattempted": 0,
    "refreshed": 1000,
    "failed_fetches": 15,
}


def _run(conclusion, event="schedule", created_at="2026-09-30T06:26:42Z"):
    return {"created_at": created_at, "event": event, "status": "completed", "conclusion": conclusion}


def _jobs(thing_ids=None, new_games=None, refresh=None):
    return [
        ps.JobRuns("Fetch Thing IDs", "fetch_thing_ids.yml", thing_ids or [_run("success")]),
        ps.JobRuns("Run Fetch New Games", "fetch_new_games.yml", new_games or [_run("success")]),
        ps.JobRuns("Run Refresh Old Games", "refresh.yml", refresh or [_run("success")]),
    ]


def _flagged(checks):
    return [c.question for c in checks if not c.ok]


# --- evaluate ---------------------------------------------------------------


def test_healthy_day_flags_nothing():
    assert _flagged(ps.evaluate(_jobs(), HEALTHY_COUNTS)) == []


def test_job_passes_if_any_run_succeeded():
    # 09-16: home-box dispatch succeeded, then the workflow_run trigger was skipped.
    runs = [_run("skipped", "workflow_run"), _run("success", "repository_dispatch")]
    assert _flagged(ps.evaluate(_jobs(new_games=runs), HEALTHY_COUNTS)) == []


def test_skipped_only_flags_the_job():
    checks = ps.evaluate(_jobs(new_games=[_run("skipped", "workflow_run")]), HEALTHY_COUNTS)
    assert _flagged(checks) == ["Run Fetch New Games ran"]


def test_no_runs_flags_the_job():
    jobs = _jobs()
    jobs[2] = ps.JobRuns("Run Refresh Old Games", "refresh.yml", [])
    checks = ps.evaluate(jobs, HEALTHY_COUNTS)
    assert _flagged(checks) == ["Run Refresh Old Games ran"]
    assert "no run in window" in checks[2].answer


def test_failed_discovery_flags_job_and_search():
    checks = ps.evaluate(_jobs(thing_ids=[_run("failure")]), HEALTHY_COUNTS)
    assert _flagged(checks) == ["Fetch Thing IDs ran", "Searched for new IDs"]


def test_zero_new_ids_flags():
    counts = HEALTHY_COUNTS | {
        "ids_found": 0, "ids_boardgame": 0, "ids_expansion": 0, "ids_accessory": 0,
        "boardgames_fetched": 0,
    }
    assert _flagged(ps.evaluate(_jobs(), counts)) == ["New IDs found"]


def test_unattempted_boardgames_flag():
    counts = HEALTHY_COUNTS | {"boardgames_fetched": 20, "boardgames_unattempted": 3}
    checks = ps.evaluate(_jobs(), counts)
    assert _flagged(checks) == ["New boardgames fetched"]
    assert "3 never attempted" in next(c for c in checks if not c.ok).answer


def test_pending_retry_is_reported_not_flagged():
    counts = HEALTHY_COUNTS | {"boardgames_fetched": 21, "boardgames_pending": 2}
    checks = ps.evaluate(_jobs(), counts)
    assert _flagged(checks) == []
    assert "2 pending retry" in next(c for c in checks if c.question == "New boardgames fetched").answer


def test_zero_refreshed_flags():
    assert _flagged(ps.evaluate(_jobs(), HEALTHY_COUNTS | {"refreshed": 0})) == ["Old games refreshed"]


def test_failed_fetches_never_flag():
    assert _flagged(ps.evaluate(_jobs(), HEALTHY_COUNTS | {"failed_fetches": 500})) == []


# --- render -----------------------------------------------------------------


def test_render_marks_flagged_rows():
    checks = ps.evaluate(_jobs(), HEALTHY_COUNTS | {"refreshed": 0})
    body = ps.render(checks, START, END)
    assert "2026-09-29T10:00:00Z → 2026-09-30T12:00:00Z" in body
    assert "1 check(s) need a look" in body
    assert "| ⚠️ | Old games refreshed | 0 |" in body
    assert "| ✅ | Fetch Thing IDs ran |" in body


def test_render_all_clear_heading():
    assert "All checks passed" in ps.render(ps.evaluate(_jobs(), HEALTHY_COUNTS), START, END)


# --- fetch_job_runs ---------------------------------------------------------


class _Resp:
    def __init__(self, payload, status=200):
        self.payload = payload
        self.status_code = status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise ps.requests.HTTPError(f"{self.status_code}")

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, resp):
        self.resp = resp
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append((url, params, headers))
        return self.resp


def test_fetch_job_runs_filters_by_created_range():
    payload = {"workflow_runs": [
        {"created_at": "2026-09-30T06:29:14Z", "event": "workflow_run",
         "status": "completed", "conclusion": "success", "id": 1, "name": "x"},
    ]}
    session = FakeSession(_Resp(payload))
    runs = ps.fetch_job_runs("o/r", "fetch_new_games.yml", START, END, "tok", session=session)
    url, params, headers = session.calls[0]
    assert url == "https://api.github.com/repos/o/r/actions/workflows/fetch_new_games.yml/runs"
    assert params["created"] == "2026-09-29T10:00:00Z..2026-09-30T12:00:00Z"
    assert headers["Authorization"] == "Bearer tok"
    assert runs == [{"created_at": "2026-09-30T06:29:14Z", "event": "workflow_run",
                     "status": "completed", "conclusion": "success"}]


def test_fetch_job_runs_raises_on_http_error():
    session = FakeSession(_Resp({}, status=401))
    with pytest.raises(ps.requests.HTTPError):
        ps.fetch_job_runs("o/r", "refresh.yml", START, END, "tok", session=session)


# --- fetch_warehouse_counts -------------------------------------------------


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def result(self):
        return self._rows


class FakeClient:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def query(self, sql, job_config=None):
        self.calls.append((sql, job_config))
        return _Result(list(self.rows))


def test_fetch_warehouse_counts_binds_window_and_reads_raw():
    client = FakeClient([HEALTHY_COUNTS])
    counts = ps.fetch_warehouse_counts(START, END, client=client)
    sql, job_config = client.calls[0]
    params = {p.name: p.value for p in job_config.query_parameters}
    assert counts == HEALTHY_COUNTS
    assert params == {"window_start": START, "window_end": END}
    assert "raw.thing_ids" in sql and "raw.fetched_responses" in sql


# --- main -------------------------------------------------------------------


def test_parse_ts_treats_naive_as_utc():
    assert ps._parse_ts("2026-09-12 10:00:00") == datetime(2026, 9, 12, 10, 0, tzinfo=UTC)
    assert ps._parse_ts("2026-09-12T10:00:00Z") == datetime(2026, 9, 12, 10, 0, tzinfo=UTC)


def test_main_writes_outputs(tmp_path, monkeypatch):
    monkeypatch.setenv("GH_TOKEN", "tok")
    monkeypatch.setenv("GITHUB_OUTPUT", str(tmp_path / "out"))
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(tmp_path / "summary"))
    seen = {}

    def fake_runs(repo, workflow_file, start, end, token):
        seen["window"] = (start, end)
        return [] if workflow_file == "refresh.yml" else [_run("success")]

    monkeypatch.setattr(ps, "fetch_job_runs", fake_runs)
    monkeypatch.setattr(ps, "fetch_warehouse_counts", lambda start, end: HEALTHY_COUNTS)

    body_file = tmp_path / "status.md"
    ps.main(["--start", "2026-09-29T10:00:00Z", "--end", "2026-09-30T12:00:00Z",
             "--repo", "o/r", "--body-file", str(body_file)])

    assert seen["window"] == (START, END)
    assert (tmp_path / "out").read_text() == "flagged=true\n"
    body = body_file.read_text(encoding="utf-8")
    assert "| ⚠️ | Run Refresh Old Games ran | no run in window |" in body
    assert (tmp_path / "summary").read_text(encoding="utf-8") == body


def test_main_requires_token(monkeypatch):
    monkeypatch.delenv("GH_TOKEN", raising=False)
    with pytest.raises(SystemExit):
        ps.main(["--window-hours", "26"])
```

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `uv run --extra test python -m pytest tests/test_pipeline_status.py -q`
Expected: collection error, `ModuleNotFoundError` / `ImportError` for `src.pipeline.pipeline_status`.

- [ ] **Step 3: Write the implementation**

`src/pipeline/pipeline_status.py`:

```python
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
from typing import Any, Optional

import requests
from google.cloud import bigquery

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


def _iso(ts: datetime) -> str:
    return ts.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_ts(value: str) -> datetime:
    """Parse an ISO timestamp; a naive one is taken as UTC, not local time."""
    ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return ts if ts.tzinfo else ts.replace(tzinfo=UTC)


def fetch_job_runs(
    repo: str,
    workflow_file: str,
    start: datetime,
    end: datetime,
    token: str,
    session=requests,
) -> list[dict[str, Any]]:
    """Runs of one workflow created in ``[start, end]``.

    The ``created`` range changes every run, so the request can't be answered from
    the stale listing that false-alarmed the old heartbeat.
    """
    resp = session.get(
        f"https://api.github.com/repos/{repo}/actions/workflows/{workflow_file}/runs",
        params={"created": f"{_iso(start)}..{_iso(end)}", "per_page": 100},
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
        timeout=30,
    )
    resp.raise_for_status()
    return [
        {k: r[k] for k in ("created_at", "event", "status", "conclusion")}
        for r in resp.json()["workflow_runs"]
    ]


def fetch_warehouse_counts(
    start: datetime,
    end: datetime,
    client: Optional[bigquery.Client] = None,
) -> dict[str, int]:
    """One row of counts for the window.

    Only ``boardgame`` IDs are fetched, so fetch coverage is measured on those. A
    refresh is any fetch of a game that already had an earlier fetch.
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
        first_fetch AS (
          SELECT game_id, MIN(fetch_timestamp) AS first_ts
          FROM fetches
          GROUP BY game_id
        ),
        window_fetches AS (
          SELECT f.game_id, f.fetch_status, f.fetch_timestamp > ff.first_ts AS is_refresh
          FROM fetches f
          JOIN first_fetch ff USING (game_id)
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
          (SELECT COUNTIF(is_refresh) FROM window_fetches) AS refreshed,
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

    checks.append(Check("Old games refreshed", str(counts["refreshed"]), counts["refreshed"] > 0))
    checks.append(Check("Failed fetches", str(counts["failed_fetches"]), True))
    return checks


def render(checks: list[Check], start: datetime, end: datetime) -> str:
    flagged = [c for c in checks if not c.ok]
    heading = f"**{len(flagged)} check(s) need a look**" if flagged else "**All checks passed**"
    lines = [
        f"## Pipeline status: {_iso(start)} → {_iso(end)}",
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


def main(argv: Optional[list[str]] = None) -> None:
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

    end = _parse_ts(args.end) if args.end else datetime.now(UTC)
    start = _parse_ts(args.start) if args.start else end - timedelta(hours=args.window_hours)

    jobs = [
        JobRuns(label, wf, fetch_job_runs(args.repo, wf, start, end, token)) for label, wf in JOBS
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
```

- [ ] **Step 4: Run the tests and confirm they pass**

Run: `uv run --extra test python -m pytest tests/test_pipeline_status.py -q`
Expected: all 18 tests pass.

- [ ] **Step 5: Lint**

Run: `uv run --extra dev ruff check src/pipeline/pipeline_status.py tests/test_pipeline_status.py`
Expected: no new errors. If a rule in the repo's `select` set fires (e.g. PLR2004 magic number on `100`/`30`), fix it in the code rather than adding a `noqa`, unless the rule already fires across existing `src/` files the same way (check with `uv run --extra dev ruff check src/pipeline/`).

- [ ] **Step 6: Replay three September windows against real data**

This is read-only: one GitHub listing per workflow and one BigQuery query of about 18 MB per window.

```bash
cd /c/Users/philh/projects/bgg-data-warehouse
export GH_TOKEN=$(gh auth token) PYTHONIOENCODING=utf-8
uv run python -m src.pipeline.pipeline_status --start 2026-09-12T10:00:00Z --end 2026-09-13T12:00:00Z
uv run python -m src.pipeline.pipeline_status --start 2026-09-15T12:00:00Z --end 2026-09-16T12:00:00Z
uv run python -m src.pipeline.pipeline_status --start 2026-09-29T10:00:00Z --end 2026-09-30T12:00:00Z
```

Expected:
- 09-12→13: ⚠️ Fetch Thing IDs ran (no run in window: discovery was on the home box then), ⚠️ Run Fetch New Games ran, ⚠️ Searched, ⚠️ New IDs found 0. Refresh 1000 ✅.
- 09-15→16: ⚠️ Fetch Thing IDs ran (`failure (schedule, 2026-09-16T06:20:38Z)`), ⚠️ Searched. Fetch New Games ✅ (the 06:01 dispatch success). New IDs 1, 1 of 1 fetched. Refreshed ≥ 1000 ✅.
- 09-29→30: all ✅. New IDs 40 (23/1/16), 23 of 23 fetched, refreshed 1000, failed fetches 15.

If any count differs from these, stop and investigate before continuing.

- [ ] **Step 7: Commit**

```bash
git add src/pipeline/pipeline_status.py tests/test_pipeline_status.py
git commit -m "feat(status): pipeline status check over job runs and landed data" \
  -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Workflow, heartbeat removal, README

**Files:**
- Create: `.github/workflows/pipeline_status.yml`
- Delete: `.github/workflows/scrape_heartbeat.yml`
- Modify: `README.md` (heartbeat paragraph, around line 59; workflow table row, around line 71)

**Interfaces:**
- Consumes: `python -m src.pipeline.pipeline_status --window-hours N --body-file status.md` from Task 1, which writes `flagged=true|false` to `$GITHUB_OUTPUT`.

- [ ] **Step 1: Write the workflow**

`.github/workflows/pipeline_status.yml`:

```yaml
name: Pipeline Status

# Daily status of the discovery -> fetch -> refresh chain: did each job run, did
# we search, were new IDs found and fetched, were old games refreshed. It reads
# job runs and what landed in raw.*, not a single workflow's history.
# Not a gate: a finding opens (or comments on) the open `pipeline-status` issue,
# and the next clean day closes it. The job goes red only if the check itself
# can't run. See docs/superpowers/specs/2026-09-30-pipeline-status-design.md.
on:
  schedule:
    - cron: '0 12 * * *'  # ~6h after the 06:00 UTC chain
  workflow_dispatch:
    inputs:
      window_hours:
        description: 'Hours to look back'
        type: number
        default: 26

permissions:
  contents: read
  actions: read
  issues: write

jobs:
  status:
    name: Pipeline status
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4

      - name: Google Cloud Auth
        uses: google-github-actions/auth@v2
        with:
          credentials_json: ${{ secrets.GCP_SA_KEY_BGG_DW }}

      - name: Set up uv
        uses: astral-sh/setup-uv@v5
        with:
          python-version: '3.12'

      - name: Install dependencies
        run: uv sync

      - name: Check pipeline
        id: check
        env:
          GH_TOKEN: ${{ github.token }}
        run: |
          uv run python -m src.pipeline.pipeline_status \
            --window-hours "${{ inputs.window_hours || 26 }}" \
            --body-file status.md

      - name: Update status issue
        env:
          GH_TOKEN: ${{ github.token }}
          FLAGGED: ${{ steps.check.outputs.flagged }}
          RUN_URL: ${{ github.server_url }}/${{ github.repository }}/actions/runs/${{ github.run_id }}
        run: |
          printf '\n[Workflow run](%s)\n' "$RUN_URL" >> status.md
          gh label create pipeline-status --color D93F0B \
            --description "Daily pipeline status needs a look" --force
          ISSUE=$(gh issue list --label pipeline-status --state open \
            --json number --jq '.[0].number // empty')

          if [ "$FLAGGED" = "true" ]; then
            if [ -n "$ISSUE" ]; then
              gh issue comment "$ISSUE" --body-file status.md
            else
              gh issue create --title "Pipeline status needs a look" \
                --label pipeline-status --body-file status.md
            fi
          elif [ -n "$ISSUE" ]; then
            { echo "All clear."; echo; cat status.md; } > clear.md
            gh issue comment "$ISSUE" --body-file clear.md
            gh issue close "$ISSUE"
          fi
```

- [ ] **Step 2: Delete the heartbeat and find any other references to it**

```bash
git rm .github/workflows/scrape_heartbeat.yml
grep -rn -i "heartbeat\|scrape_heartbeat" --exclude-dir=.git --exclude-dir=docs . | grep -v "^./src/pipeline/pipeline_status.py"
```

Expected: only the two README lines remain (they're fixed in Step 3). If anything else shows up, such as another workflow, a script or a doc outside `docs/`, update it to point at Pipeline Status.

- [ ] **Step 3: Update README**

Replace:

```markdown
`Scrape Heartbeat` runs daily at 12:00 UTC and fails loudly if `Run Fetch New Games`
has not succeeded in ~26h (cron not firing, workflow disabled, discovery broken).
```

with:

```markdown
`Pipeline Status` runs daily at 12:00 UTC and reports on the last ~26h: whether each
job ran, whether we searched, how many new IDs were found and fetched, and how many
old games were refreshed. A finding doesn't fail the run: it opens (or comments on)
an issue labelled `pipeline-status`, which the next clean day closes.
```

and replace the table row:

```markdown
| `scrape_heartbeat.yml` | daily `0 12 * * *` |
```

with:

```markdown
| `pipeline_status.yml` | daily `0 12 * * *`, or manual (`window_hours` input) |
```

- [ ] **Step 4: Validate the YAML parses**

Run: `uv run python -c "import yaml,sys; d=yaml.safe_load(open('.github/workflows/pipeline_status.yml')); print(sorted(d[True]), list(d['jobs']['status']['steps'][i].get('name', 'checkout') for i in range(6)))"`
Expected: `['schedule', 'workflow_dispatch']` and the six step names. PyYAML parses the `on:` key as `True`.

- [ ] **Step 5: Run the full test suite**

Run: `uv run --extra test python -m pytest tests/ -q -m "not integration"`
Expected: all pass, the same as `main` plus the 18 new tests.

- [ ] **Step 6: Commit**

```bash
git add .github/workflows/pipeline_status.yml README.md
git commit -m "feat(ci): pipeline status replaces the scrape heartbeat" \
  -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: PR and post-merge validation

- [ ] **Step 1: Push and open the PR**

```bash
gh pr list --head feat/pipeline-status --state all   # expect none
git push -u origin feat/pipeline-status
gh pr create --title "feat(ci): pipeline status replaces the scrape heartbeat" --body-file - <<'EOF'
## Why
The Scrape Heartbeat checked one workflow's run history. It false-alarmed on a
stale runs listing (09-18, 09-22, 09-30) and couldn't see whether we searched,
found, fetched or refreshed anything.

## What
- `src/pipeline/pipeline_status.py` answers four questions for the window: each
  job ran (per-workflow, date-filtered runs), we searched, new IDs found and
  fetched, old games refreshed. The data comes from `raw.thing_ids` and
  `raw.fetched_responses`.
- `pipeline_status.yml` runs it daily at 12:00 UTC. A finding opens or comments
  on the `pipeline-status` issue, and a clean day closes it.
- Removes `scrape_heartbeat.yml`.

Spec: `docs/superpowers/specs/2026-09-30-pipeline-status-design.md`

## Validation
- Unit tests for every flag rule.
- Local replay of the 09-12→13 outage, the 09-16 token failure and a clean
  09-29→30 window matched the hand-checked counts.

## After merge
Run `Pipeline Status` manually with `window_hours: 26` (clean, no issue), then
`window_hours: 1` (flags, opens issue), then `26` again (closes it).

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
```

Hand off to Phil to merge. Never run `gh pr merge`.

- [ ] **Step 2: After Phil merges, ask before dispatching**

The dispatch runs create and close a real issue, so confirm with Phil first. Then:

```bash
gh workflow run pipeline_status.yml -f window_hours=26   # expect green, no issue
gh workflow run pipeline_status.yml -f window_hours=1    # expect green run, issue opened
gh workflow run pipeline_status.yml -f window_hours=26   # expect "All clear" comment + issue closed
```

Check each with `gh run list --workflow pipeline_status.yml --limit 1` and
`gh issue list --label pipeline-status --state all --limit 1`.

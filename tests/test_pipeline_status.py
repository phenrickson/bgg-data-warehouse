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
    "refresh_attempts": 1000,
    "refreshed": 1000,
    "failed_fetches": 15,
}


def _run(conclusion, event="schedule", created_at="2026-09-30T06:26:42Z"):
    return {
        "created_at": created_at,
        "event": event,
        "status": "completed",
        "conclusion": conclusion,
    }


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
        "ids_found": 0,
        "ids_boardgame": 0,
        "ids_expansion": 0,
        "ids_accessory": 0,
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
    answer = next(c for c in checks if c.question == "New boardgames fetched").answer
    assert "2 pending retry" in answer


def test_zero_refreshed_flags():
    checks = ps.evaluate(_jobs(), HEALTHY_COUNTS | {"refreshed": 0})
    assert _flagged(checks) == ["Old games refreshed"]


def test_refresh_attempts_that_all_failed_flag():
    # BGG rejected the whole refresh batch: the job ran green but nothing refreshed.
    counts = HEALTHY_COUNTS | {"refreshed": 0, "failed_fetches": 1000}
    checks = ps.evaluate(_jobs(), counts)
    assert _flagged(checks) == ["Old games refreshed"]
    answer = next(c for c in checks if c.question == "Old games refreshed").answer
    assert answer == "0 of 1000 attempts succeeded"


def test_failed_fetches_never_flag():
    assert _flagged(ps.evaluate(_jobs(), HEALTHY_COUNTS | {"failed_fetches": 500})) == []


# --- render -----------------------------------------------------------------


def test_render_marks_flagged_rows():
    checks = ps.evaluate(_jobs(), HEALTHY_COUNTS | {"refreshed": 0})
    body = ps.render(checks, START, END)
    assert "2026-09-29T10:00:00Z → 2026-09-30T12:00:00Z" in body
    assert "1 check(s) need a look" in body
    assert "| ⚠️ | Old games refreshed | 0 of 1000 attempts succeeded |" in body
    assert "| ✅ | Fetch Thing IDs ran |" in body


def test_render_all_clear_heading():
    assert "All checks passed" in ps.render(ps.evaluate(_jobs(), HEALTHY_COUNTS), START, END)


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


def test_refresh_counts_only_refetches_after_a_successful_fetch():
    # A retry of a never-fetched game is not a refresh, and a failed refetch is an
    # attempt, not a refresh.
    client = FakeClient([HEALTHY_COUNTS])
    ps.fetch_warehouse_counts(START, END, client=client)
    sql = " ".join(client.calls[0][0].split())
    assert "WHERE fetch_status = 'success' GROUP BY game_id" in sql
    assert "COUNTIF(is_refresh AND fetch_status = 'success')" in sql


# --- main -------------------------------------------------------------------


def test_main_writes_outputs(tmp_path, monkeypatch):
    monkeypatch.setenv("GH_TOKEN", "tok")
    monkeypatch.setenv("GITHUB_OUTPUT", str(tmp_path / "out"))
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(tmp_path / "summary"))
    seen = {}

    def fake_runs(repo, workflow_file, start, end, token):
        seen["window"] = (start, end)
        return [] if workflow_file == "refresh.yml" else [_run("success")]

    monkeypatch.setattr(ps, "fetch_runs", fake_runs)
    monkeypatch.setattr(ps, "fetch_warehouse_counts", lambda start, end: HEALTHY_COUNTS)

    body_file = tmp_path / "status.md"
    ps.main(
        [
            "--start",
            "2026-09-29T10:00:00Z",
            "--end",
            "2026-09-30T12:00:00Z",
            "--repo",
            "o/r",
            "--body-file",
            str(body_file),
        ]
    )

    assert seen["window"] == (START, END)
    assert (tmp_path / "out").read_text() == "flagged=true\n"
    body = body_file.read_text(encoding="utf-8")
    assert "| ⚠️ | Run Refresh Old Games ran | no run in window |" in body
    assert (tmp_path / "summary").read_text(encoding="utf-8") == body


def test_main_requires_token(monkeypatch):
    monkeypatch.delenv("GH_TOKEN", raising=False)
    with pytest.raises(SystemExit):
        ps.main(["--window-hours", "26"])

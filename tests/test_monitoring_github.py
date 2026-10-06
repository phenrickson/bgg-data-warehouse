"""Unit tests for GitHub Actions run fetching (no network)."""

from datetime import UTC, datetime
from http import HTTPStatus

import pytest

from src.monitoring import github

START = datetime(2026, 9, 29, 10, 0, tzinfo=UTC)
END = datetime(2026, 9, 30, 12, 0, tzinfo=UTC)

RAW_RUN = {
    "id": 1,
    "name": "Run Dataform",
    "created_at": "2026-09-30T07:18:57Z",
    "updated_at": "2026-09-30T07:20:49Z",
    "event": "repository_dispatch",
    "status": "completed",
    "conclusion": "success",
    "display_title": "complexity_complete",
    "html_url": "https://github.com/o/r/actions/runs/1",
}


class _Resp:
    def __init__(self, payload, status=200, next_url=None):
        self.payload = payload
        self.status_code = status
        self.links = {"next": {"url": next_url}} if next_url else {}

    def raise_for_status(self):
        if self.status_code >= HTTPStatus.BAD_REQUEST:
            raise github.requests.HTTPError(f"{self.status_code}")

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, *resps):
        self.resps = list(resps)
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append((url, params, headers))
        return self.resps.pop(0)


def test_fetch_runs_filters_by_created_range_and_keeps_run_fields():
    session = FakeSession(_Resp({"workflow_runs": [RAW_RUN]}))
    runs = github.fetch_runs("o/r", "dataform.yml", START, END, "tok", session=session)
    url, params, headers = session.calls[0]
    assert url == "https://api.github.com/repos/o/r/actions/workflows/dataform.yml/runs"
    assert params["created"] == "2026-09-29T10:00:00Z..2026-09-30T12:00:00Z"
    assert headers["Authorization"] == "Bearer tok"
    assert runs == [{k: RAW_RUN[k] for k in github.RUN_FIELDS}]
    assert set(github.RUN_FIELDS) == {
        "id", "created_at", "updated_at", "event", "status", "conclusion", "display_title",
        "html_url",
    }


def test_fetch_runs_follows_next_link():
    second = dict(RAW_RUN, created_at="2026-09-29T07:18:57Z")
    session = FakeSession(
        _Resp({"workflow_runs": [RAW_RUN]}, next_url="https://api.github.com/next?page=2"),
        _Resp({"workflow_runs": [second]}),
    )
    runs = github.fetch_runs("o/r", "dataform.yml", START, END, "tok", session=session)
    assert [r["created_at"] for r in runs] == ["2026-09-30T07:18:57Z", "2026-09-29T07:18:57Z"]
    url, params, _ = session.calls[1]
    assert url == "https://api.github.com/next?page=2"
    assert params is None, "the next link already carries the query string"


def test_fetch_runs_raises_on_http_error():
    session = FakeSession(_Resp({}, status=HTTPStatus.UNAUTHORIZED))
    with pytest.raises(github.requests.HTTPError):
        github.fetch_runs("o/r", "refresh.yml", START, END, "tok", session=session)


def test_parse_ts_treats_naive_as_utc():
    assert github.parse_ts("2026-09-30T06:00:00") == datetime(2026, 9, 30, 6, tzinfo=UTC)
    assert github.parse_ts("2026-09-30T06:00:00Z") == datetime(2026, 9, 30, 6, tzinfo=UTC)
    assert github.parse_ts("2026-09-12 10:00:00") == datetime(2026, 9, 12, 10, tzinfo=UTC)


def test_iso_formats_utc():
    assert github.iso(START) == "2026-09-29T10:00:00Z"


RAW_JOB = {
    "id": 9, "run_id": 1, "name": "complexity / score-complexity", "status": "completed",
    "conclusion": "success", "started_at": "2026-10-06T16:09:06Z",
    "completed_at": "2026-10-06T16:13:31Z", "html_url": "https://github.com/o/r/actions/runs/1/job/9",
    "steps": [],
}


def test_fetch_jobs_keeps_job_fields():
    session = FakeSession(_Resp({"jobs": [RAW_JOB]}))
    jobs = github.fetch_jobs("o/r", 1, "tok", session=session)
    url, params, headers = session.calls[0]
    assert url == "https://api.github.com/repos/o/r/actions/runs/1/jobs"
    assert headers["Authorization"] == "Bearer tok"
    assert jobs == [{k: RAW_JOB[k] for k in github.JOB_FIELDS}]
    assert set(github.JOB_FIELDS) == {
        "name", "status", "conclusion", "started_at", "completed_at", "html_url",
    }


def test_fetch_jobs_asks_for_latest_attempt():
    """A re-run's jobs replace the failed attempt's, so the step shows the re-run."""
    session = FakeSession(_Resp({"jobs": []}))
    github.fetch_jobs("o/r", 1, "tok", session=session)
    _, params, _ = session.calls[0]
    assert params == {"filter": "latest", "per_page": 100}


def test_fetch_jobs_follows_next_link():
    second = dict(RAW_JOB, name="scoring / score-simulations")
    session = FakeSession(
        _Resp({"jobs": [RAW_JOB]}, next_url="https://api.github.com/next?page=2"),
        _Resp({"jobs": [second]}),
    )
    jobs = github.fetch_jobs("o/r", 1, "tok", session=session)
    assert [j["name"] for j in jobs] == ["complexity / score-complexity", "scoring / score-simulations"]
    assert session.calls[1][1] is None


def test_fetch_jobs_raises_on_http_error():
    session = FakeSession(_Resp({}, status=HTTPStatus.NOT_FOUND))
    with pytest.raises(github.requests.HTTPError):
        github.fetch_jobs("o/r", 1, "tok", session=session)

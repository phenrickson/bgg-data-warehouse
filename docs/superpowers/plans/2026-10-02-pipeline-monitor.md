# Pipeline Monitor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** An admin-only `/admin/pipeline` page in bgg-viewer that shows whether today's 12-stage chain ran end to end, how fresh and complete each downstream table is, and which models are live, all served by a new `GET /monitoring/pipeline` on the warehouse API.

**Architecture:** The warehouse repo gets a small `src/monitoring/` package with two modules. `github.py` fetches GitHub Actions runs and is shared with Pipeline Status. `chain.py` holds the stage list and pure status rules. A BigQuery reader handles freshness and deployed models, and a route on the existing monitoring router serves it all, cached for 5 minutes. bgg-viewer adds a typed client method, pure display helpers, an admin-gated route and six small Svelte components that follow the mockup.

**Tech Stack:**
- Warehouse: Python 3.12, FastAPI, google-cloud-bigquery, requests, pytest (`uv run --extra test python -m pytest`)
- Viewer: SvelteKit 2, Svelte 5 runes, vitest (`pnpm test`), svelte-check (`pnpm check`)

**Spec:** `docs/superpowers/specs/2026-10-02-pipeline-monitor-design.md` (this repo). Mockup: `bgg-viewer/docs/design/pipeline-monitor-mockup.html`.

## Global Constraints

- Repos: `phenrickson/bgg-data-warehouse`, `phenrickson/bgg-predictive-models`, `phenrickson/bgg-viewer`.
- Branch `feat/pipeline-monitor` in both bgg-data-warehouse and bgg-viewer (already created; spec and mockup already committed).
- Chain window: `[day 05:00 UTC, day+1 05:00 UTC)`. Hand-off window: **30 minutes**. Stage 1 due by **07:00 UTC**.
- Stage statuses (exact strings): `ok`, `warn`, `fail`, `running`, `pending`, `not_reached`.
- Scoring years for `bgg_predictions` coverage: **2025–2030**, one constant.
- Endpoint: `GET /monitoring/pipeline?days=` with `days` 1–30, default 14. Cache 5 minutes, keyed by `days`.
- `GH_TOKEN` missing → **503**. GitHub or BigQuery error → **502**.
- Status colours: **no green/red**. `--status-ok` blue, `--status-warn` amber, `--status-fail` violet. Every status shows a glyph and a word, never colour alone.
- Viewer freshness: "Fresh" if `last_updated` ≥ today's stage-1 start (fallback `<day>T05:00:00Z`), else "A day old" / "N days old". Coverage bar amber below **99.5%**.
- Non-admins get a **404** from `/admin/pipeline`. The nav link renders only for the admin.
- Keep `__init__.py` files empty.
- Every commit message ends with:
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`

## Review Focus

1. **A manual rerun after a failure.** The latest run in the window must win, so a red 06:26 run followed by a green 09:00 rerun reads `ok`. Pinned in Task 2 (`test_rerun_after_failure_wins`).
2. **A stale run from earlier in the window sitting in the next stage's slot**, for example a Dataform pass from a `push` before today's chain. A next-stage run created *before* this stage's run is not a hand-off. Pinned in Task 2 (`test_next_run_before_this_one_is_not_a_handoff`).
3. **Loading the page before today's chain has started (05:00–06:26 UTC).** Stage 1 is `pending`, not `fail`, and the verdict says "Waiting on Fetch Thing IDs". Pinned in Task 2 (`test_before_stage_one_due_is_pending`).
4. **The warehouse API being down or the token being missing.** The page must render the error message, not a 500 page. Pinned in Task 9 (`returns the error message when the warehouse call fails`).
5. **A GitHub listing with more than 100 runs**, which happens when `days=30` covers 120+ Dataform runs. Pagination must follow `Link: next`. Pinned in Task 1 (`test_fetch_runs_follows_next_link`).

---

# Part A — bgg-data-warehouse

All Part A commands run from `/Users/phenrickson/Documents/projects/bgg-data-warehouse`.

### Task 1: Shared GitHub run fetching

Move `fetch_job_runs` out of `pipeline_status.py` into `src/monitoring/github.py`. While moving it, return more fields and follow pagination.

**Files:**
- Create: `src/monitoring/__init__.py` (empty)
- Create: `src/monitoring/github.py`
- Create: `tests/test_monitoring_github.py`
- Modify: `src/pipeline/pipeline_status.py`. Delete `_iso`, `_parse_ts` and `fetch_job_runs` (lines 66–99), and the `import requests`.
- Modify: `tests/test_pipeline_status.py`. Delete the `# --- fetch_job_runs ---` section (lines 139–198, up to `# --- fetch_warehouse_counts ---`). In `test_main_writes_outputs`, patch `fetch_runs` instead of `fetch_job_runs`.

**Interfaces:**
- Produces:
  - `RUN_FIELDS: tuple[str, ...]`
  - `iso(ts: datetime) -> str`
  - `parse_ts(value: str) -> datetime`
  - `fetch_runs(repo: str, workflow_file: str, start: datetime, end: datetime, token: str, session=requests) -> list[dict]`. Each dict has exactly the keys in `RUN_FIELDS`.

- [ ] **Step 1: Write the failing tests**

`tests/test_monitoring_github.py`:

```python
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
        "created_at", "updated_at", "event", "status", "conclusion", "display_title", "html_url",
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


def test_iso_formats_utc():
    assert github.iso(START) == "2026-09-29T10:00:00Z"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --extra test python -m pytest tests/test_monitoring_github.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.monitoring'`

- [ ] **Step 3: Implement**

Create `src/monitoring/__init__.py` as an empty file.

Create `src/monitoring/github.py`:

```python
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
```

Edit `src/pipeline/pipeline_status.py`:
1. Delete `import requests`. Add `from src.monitoring.github import fetch_runs, iso, parse_ts`.
2. Delete the `_iso`, `_parse_ts` and `fetch_job_runs` functions.
3. Replace every `_iso(` with `iso(` (in `render`), and every `_parse_ts(` with `parse_ts(` (in `main`).
4. In `main`, replace `fetch_job_runs(args.repo, wf, start, end, token)` with `fetch_runs(args.repo, wf, start, end, token)`.
5. Change the local-replay line in the module docstring only if it names `fetch_job_runs`. It doesn't today, so leave it.

Edit `tests/test_pipeline_status.py`:
1. Delete from the line `# --- fetch_job_runs ---...` through the end of `test_fetch_job_runs_raises_on_http_error`, stopping just before `# --- fetch_warehouse_counts ---`. If the `from http import HTTPStatus` import is now unused, delete it too.
2. In `test_main_writes_outputs`, change `monkeypatch.setattr(ps, "fetch_job_runs", fake_runs)` to `monkeypatch.setattr(ps, "fetch_runs", fake_runs)`.

- [ ] **Step 4: Run both test files to verify they pass**

Run: `uv run --extra test python -m pytest tests/test_monitoring_github.py tests/test_pipeline_status.py -v`
Expected: all PASS.

Run: `grep -rn "fetch_job_runs\|_parse_ts\|_iso(" src tests`
Expected: no output.

- [ ] **Step 5: Commit**

```bash
git add src/monitoring tests/test_monitoring_github.py src/pipeline/pipeline_status.py tests/test_pipeline_status.py
git commit -m "refactor(status): move GitHub run fetching to src/monitoring, add pagination

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 2: Stage definitions and `build_chain`

**Files:**
- Create: `src/monitoring/chain.py`
- Create: `tests/fixtures/chain_runs_2026-10-02.json` (captured from GitHub, not hand-written)
- Create: `tests/test_chain.py`

**Interfaces:**
- Consumes: `fetch_runs`, `iso` and `parse_ts` from Task 1.
- Produces:
  - `STAGES: list[Stage]`, `OFF_CHAIN: list[Stage]`, `SOURCES: list[tuple[str, str]]`
  - `Stage(key, label, repo, workflow_file, lane, event=None, title=None)` with `.source -> (repo, workflow_file)` and `.matches(run) -> bool`
  - `StageStatus(key, label, lane, status, started=None, finished=None, url=None, event=None, title=None, note=None)`
  - `Chain(day: date, stages: list[StageStatus], off_chain: list[StageStatus])` with `.to_dict()`
  - `Runs = dict[tuple[str, str], list[dict]]`
  - `window(day) -> (datetime, datetime)`, `chain_day(now) -> date`
  - `build_chain(runs: Runs, day: date, now: datetime) -> Chain`

- [ ] **Step 1: Write the stage data**

Create `src/monitoring/chain.py` with only the data part for now. Step 5 adds the rules.

```python
"""The daily chain as data, and the rules that turn GitHub runs into stage statuses.

Twelve stages across three repos, stitched together by ``workflow_run`` and
``repository_dispatch``. A stage's status comes from its own latest run in the day's
window, plus whether the next stage picked it up. See
docs/superpowers/specs/2026-10-02-pipeline-monitor-design.md.
"""

from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Any

from src.monitoring.github import iso, parse_ts

WAREHOUSE = "phenrickson/bgg-data-warehouse"
MODELS = "phenrickson/bgg-predictive-models"
VIEWER = "phenrickson/bgg-viewer"

# The chain day runs 05:00 -> 05:00 UTC, so the 06:00 cron and everything it sets off
# land in one window.
DAY_START = time(5, 0)
# Stage 1 is a 06:00 cron that GitHub routinely starts ~25 minutes late.
STAGE_ONE_DUE = time(7, 0)
# How long a finished stage waits for the next one before it reads as "no hand-off".
# Real hand-offs take seconds to a few minutes.
HANDOFF = timedelta(minutes=30)

Runs = dict[tuple[str, str], list[dict[str, Any]]]


@dataclass(frozen=True)
class Stage:
    key: str
    label: str
    repo: str
    workflow_file: str
    lane: str
    event: str | None = None
    title: str | None = None

    @property
    def source(self) -> tuple[str, str]:
        return (self.repo, self.workflow_file)

    def matches(self, run: dict[str, Any]) -> bool:
        """Every run of the workflow, unless the stage shares its workflow with others
        (``dataform.yml``), in which case the trigger tells them apart."""
        if self.event and run.get("event") != self.event:
            return False
        if self.title and run.get("display_title") != self.title:
            return False
        return True


STAGES = [
    Stage("fetch_thing_ids", "Fetch Thing IDs", WAREHOUSE, "fetch_thing_ids.yml", "warehouse"),
    Stage("fetch_new_games", "Fetch New Games", WAREHOUSE, "fetch_new_games.yml", "warehouse"),
    Stage("refresh_old_games", "Refresh Old Games", WAREHOUSE, "refresh.yml", "warehouse"),
    Stage("dataform_1", "Dataform · pass 1", WAREHOUSE, "dataform.yml", "warehouse",
          event="workflow_run"),
    Stage("text_embeddings", "Text Embeddings", MODELS, "run-generate-text-embeddings.yml",
          "models"),
    Stage("dataform_2", "Dataform · pass 2", WAREHOUSE, "dataform.yml", "warehouse",
          title="text_embeddings_complete"),
    Stage("score_complexity", "Score Complexity", MODELS, "run-complexity-scoring.yml",
          "models"),
    Stage("dataform_3", "Dataform · pass 3", WAREHOUSE, "dataform.yml", "warehouse",
          title="complexity_complete"),
    Stage("score_games", "Score Games", MODELS, "run-scoring-service.yml", "models"),
    Stage("game_embeddings", "Game Embeddings + Coords", MODELS,
          "run-generate-embeddings.yml", "models"),
    Stage("dataform_4", "Dataform · pass 4", WAREHOUSE, "dataform.yml", "warehouse",
          title="embeddings_complete"),
    Stage("viewer_artifacts", "Viewer Artifacts", VIEWER, "viewer-artifacts.yml", "viewer"),
]

OFF_CHAIN = [
    Stage("collection_scoring", "Collection Scoring", MODELS, "run-collection-scoring.yml",
          "models"),
    Stage("collection_reports", "Collection Reports", MODELS, "build-collection-reports.yml",
          "models"),
    Stage("pipeline_status", "Pipeline Status", WAREHOUSE, "pipeline_status.yml", "warehouse"),
]

# Every (repo, workflow file) to list runs for. Dataform appears once.
SOURCES = sorted({s.source for s in STAGES + OFF_CHAIN})
```

- [ ] **Step 2: Capture the real-runs fixture**

Run:

```bash
mkdir -p tests/fixtures
GH_TOKEN=$(gh auth token) uv run python - <<'EOF'
import json, os
from datetime import UTC, datetime
from src.monitoring.chain import SOURCES
from src.monitoring.github import fetch_runs

start, end = datetime(2026, 10, 2, 5, tzinfo=UTC), datetime(2026, 10, 3, 5, tzinfo=UTC)
out = {f"{repo}/{wf}": fetch_runs(repo, wf, start, end, os.environ["GH_TOKEN"])
       for repo, wf in SOURCES}
with open("tests/fixtures/chain_runs_2026-10-02.json", "w") as f:
    json.dump(out, f, indent=2, sort_keys=True)
print({k: len(v) for k, v in out.items()})
EOF
```

Expected: 13 keys. `dataform.yml` has 4 runs, with display titles `Run Dataform`, `text_embeddings_complete`, `complexity_complete` and `embeddings_complete`. Every other key has 1 run, all `"conclusion": "success"`. If a key has 0 runs, stop and check the workflow file name in `STAGES`.

- [ ] **Step 3: Write the failing tests**

`tests/test_chain.py`:

```python
"""Unit tests for the chain status rules (fixture of real runs + synthetic days)."""

import copy
import json
from datetime import UTC, date, datetime
from pathlib import Path

from src.monitoring import chain

DAY = date(2026, 10, 2)
NOON = datetime(2026, 10, 2, 12, 0, tzinfo=UTC)
BY_KEY = {s.key: s for s in chain.STAGES + chain.OFF_CHAIN}


def _fixture() -> chain.Runs:
    raw = json.loads((Path(__file__).parent / "fixtures/chain_runs_2026-10-02.json").read_text())
    return {tuple(k.rsplit("/", 1)): v for k, v in raw.items()}


def _drop(runs: chain.Runs, *keys: str) -> chain.Runs:
    """Remove every run belonging to the given stages."""
    runs = copy.deepcopy(runs)
    for key in keys:
        stage = BY_KEY[key]
        runs[stage.source] = [r for r in runs[stage.source] if not stage.matches(r)]
    return runs


def _run_of(runs: chain.Runs, key: str) -> dict:
    stage = BY_KEY[key]
    return next(r for r in runs[stage.source] if stage.matches(r))


def _statuses(c: chain.Chain) -> dict[str, str]:
    return {s.key: s.status for s in c.stages}


def test_fixture_day_is_all_ok():
    c = chain.build_chain(_fixture(), DAY, NOON)
    assert set(_statuses(c).values()) == {"ok"}
    assert {s.key: s.status for s in c.off_chain} == {
        "collection_scoring": "ok", "collection_reports": "ok", "pipeline_status": "ok",
    }
    first = c.stages[0]
    assert first.started and first.finished and first.url.startswith("https://github.com/")


def test_four_dataform_passes_are_told_apart():
    c = chain.build_chain(_fixture(), DAY, NOON)
    titles = {s.key: s.title for s in c.stages if s.key.startswith("dataform_")}
    assert titles == {
        "dataform_1": "Run Dataform",
        "dataform_2": "text_embeddings_complete",
        "dataform_3": "complexity_complete",
        "dataform_4": "embeddings_complete",
    }


def test_stall_after_pass_3():
    runs = _drop(_fixture(), "score_games", "game_embeddings", "dataform_4", "viewer_artifacts")
    c = chain.build_chain(runs, DAY, NOON)
    s = _statuses(c)
    assert s["dataform_3"] == "warn"
    assert next(x for x in c.stages if x.key == "dataform_3").note.startswith("No hand-off")
    assert [s[k] for k in ("score_games", "game_embeddings", "dataform_4", "viewer_artifacts")] == [
        "not_reached"] * 4
    assert next(x for x in c.off_chain if x.key == "collection_scoring").status == "warn"


def test_handoff_window_not_closed_is_pending():
    runs = _drop(_fixture(), "score_games", "game_embeddings", "dataform_4", "viewer_artifacts")
    finished = chain.parse_ts(_run_of(runs, "dataform_3")["updated_at"])
    c = chain.build_chain(runs, DAY, finished.replace(second=0) + chain.HANDOFF / 3)
    s = _statuses(c)
    assert s["dataform_3"] == "ok"
    assert s["score_games"] == "pending"
    assert s["viewer_artifacts"] == "pending"


def test_in_progress_run_is_running():
    runs = copy.deepcopy(_fixture())
    run = _run_of(runs, "viewer_artifacts")
    run.update(status="in_progress", conclusion=None)
    c = chain.build_chain(runs, DAY, NOON)
    last = c.stages[-1]
    assert last.status == "running"
    assert last.finished is None


def test_rerun_after_failure_wins():
    runs = copy.deepcopy(_fixture())
    good = _run_of(runs, "fetch_thing_ids")
    bad = dict(good, created_at="2026-10-02T06:01:00Z", updated_at="2026-10-02T06:02:00Z",
               conclusion="failure")
    runs[BY_KEY["fetch_thing_ids"].source].append(bad)
    assert _statuses(chain.build_chain(runs, DAY, NOON))["fetch_thing_ids"] == "ok"


def test_only_a_failed_run_is_fail():
    runs = copy.deepcopy(_fixture())
    _run_of(runs, "fetch_thing_ids")["conclusion"] = "failure"
    assert _statuses(chain.build_chain(runs, DAY, NOON))["fetch_thing_ids"] == "fail"


def test_skipped_run_is_not_reached():
    runs = copy.deepcopy(_fixture())
    _run_of(runs, "fetch_new_games")["conclusion"] = "skipped"
    assert _statuses(chain.build_chain(runs, DAY, NOON))["fetch_new_games"] == "not_reached"


def test_missing_stage_one_after_due_is_fail():
    c = chain.build_chain({}, DAY, datetime(2026, 10, 2, 7, 30, tzinfo=UTC))
    assert c.stages[0].status == "fail"
    assert c.stages[0].note == "No run by 07:00 UTC"
    assert {s.status for s in c.stages[1:]} == {"not_reached"}


def test_before_stage_one_due_is_pending():
    c = chain.build_chain({}, DAY, datetime(2026, 10, 2, 5, 30, tzinfo=UTC))
    assert {s.status for s in c.stages} == {"pending"}


def test_next_run_before_this_one_is_not_a_handoff():
    runs = copy.deepcopy(_fixture())
    p3 = _run_of(runs, "dataform_3")
    scoring = _run_of(runs, "score_games")
    scoring["created_at"] = "2026-10-02T05:10:00Z"  # an earlier, unrelated run
    c = chain.build_chain(runs, DAY, NOON)
    assert chain.parse_ts(scoring["created_at"]) < chain.parse_ts(p3["created_at"])
    assert _statuses(c)["dataform_3"] == "warn"


def test_collection_scoring_before_final_pass_is_warn():
    runs = copy.deepcopy(_fixture())
    _run_of(runs, "collection_scoring")["created_at"] = "2026-10-02T07:00:00Z"
    c = chain.build_chain(runs, DAY, NOON)
    scoring = next(s for s in c.off_chain if s.key == "collection_scoring")
    assert scoring.status == "warn"
    assert scoring.note == "Ran before today's predictions landed"


def test_runs_outside_the_window_are_ignored():
    c = chain.build_chain(_fixture(), date(2026, 10, 1), NOON)
    assert c.stages[0].status == "fail"  # the fixture holds only 10-02's runs


def test_chain_day_rolls_at_0500_utc():
    assert chain.chain_day(datetime(2026, 10, 2, 4, 59, tzinfo=UTC)) == date(2026, 10, 1)
    assert chain.chain_day(datetime(2026, 10, 2, 5, 0, tzinfo=UTC)) == date(2026, 10, 2)


def test_to_dict_is_json_ready():
    d = chain.build_chain(_fixture(), DAY, NOON).to_dict()
    json.dumps(d)
    assert d["day"] == "2026-10-02"
    assert set(d["stages"][0]) == {
        "key", "label", "lane", "status", "started", "finished", "url", "event", "title", "note",
    }
```

- [ ] **Step 4: Run the tests to verify they fail**

Run: `uv run --extra test python -m pytest tests/test_chain.py -v`
Expected: FAIL with `AttributeError: module 'src.monitoring.chain' has no attribute 'build_chain'` (or `parse_ts`/`chain_day`).

- [ ] **Step 5: Implement the rules**

Append to `src/monitoring/chain.py`. `parse_ts` is already imported there and the tests use it as `chain.parse_ts`.

```python
@dataclass
class StageStatus:
    key: str
    label: str
    lane: str
    status: str
    started: str | None = None
    finished: str | None = None
    url: str | None = None
    event: str | None = None
    title: str | None = None
    note: str | None = None


@dataclass
class Chain:
    day: date
    stages: list[StageStatus]
    off_chain: list[StageStatus]

    def to_dict(self) -> dict[str, Any]:
        return {
            "day": self.day.isoformat(),
            "stages": [asdict(s) for s in self.stages],
            "off_chain": [asdict(s) for s in self.off_chain],
        }


def window(day: date) -> tuple[datetime, datetime]:
    start = datetime.combine(day, DAY_START, tzinfo=UTC)
    return start, start + timedelta(days=1)


def chain_day(now: datetime) -> date:
    """The chain day ``now`` falls in (days roll over at 05:00 UTC)."""
    return (now.astimezone(UTC) - timedelta(hours=DAY_START.hour)).date()


def _latest(stage: Stage, runs: Runs, day: date) -> dict[str, Any] | None:
    """The stage's newest run created in the day's window (a rerun supersedes a failure)."""
    start, end = window(day)
    found = [
        r for r in runs.get(stage.source, [])
        if stage.matches(r) and start <= parse_ts(r["created_at"]) < end
    ]
    return max(found, key=lambda r: parse_ts(r["created_at"]), default=None)


def _own_status(run: dict[str, Any]) -> str:
    if run.get("status") != "completed":
        return "running"
    if run.get("conclusion") == "success":
        return "ok"
    if run.get("conclusion") == "skipped":
        return "not_reached"
    return "fail"


def _status(stage: Stage, run: dict[str, Any] | None, status: str,
            note: str | None = None) -> StageStatus:
    if run is None:
        return StageStatus(stage.key, stage.label, stage.lane, status, note=note)
    done = run.get("status") == "completed"
    return StageStatus(
        stage.key, stage.label, stage.lane, status,
        started=run["created_at"],
        finished=run["updated_at"] if done else None,
        url=run.get("html_url"),
        event=run.get("event"),
        title=run.get("display_title"),
        note=note,
    )


def _handed_off(run: dict[str, Any], nxt: dict[str, Any] | None) -> bool:
    return nxt is not None and parse_ts(nxt["created_at"]) >= parse_ts(run["created_at"])


def build_chain(runs: Runs, day: date, now: datetime) -> Chain:
    picked = [_latest(s, runs, day) for s in STAGES]
    stages: list[StageStatus] = []
    blocked = False  # an upstream stage failed, stalled or never ran
    for i, (stage, run) in enumerate(zip(STAGES, picked)):
        note = None
        if run is None:
            if i == 0:
                due = datetime.combine(day, STAGE_ONE_DUE, tzinfo=UTC)
                status, note = ("fail", "No run by 07:00 UTC") if now >= due else ("pending", None)
            else:
                status = "not_reached" if blocked else "pending"
        else:
            status = _own_status(run)
            is_last = i == len(STAGES) - 1
            if status == "ok" and not is_last and not _handed_off(run, picked[i + 1]):
                if now - parse_ts(run["updated_at"]) > HANDOFF:
                    status, note = "warn", "No hand-off: the next stage never started"
        stages.append(_status(stage, run, status, note))
        blocked = blocked or status in ("fail", "warn", "not_reached")

    final_pass = picked[[s.key for s in STAGES].index("dataform_4")]
    return Chain(day, stages, _off_chain(runs, day, final_pass))


def _off_chain(runs: Runs, day: date, final_pass: dict[str, Any] | None) -> list[StageStatus]:
    out = []
    for stage in OFF_CHAIN:
        run = _latest(stage, runs, day)
        if run is None:
            out.append(_status(stage, None, "not_reached", "No run"))
            continue
        status, note = _own_status(run), None
        if stage.key == "collection_scoring" and status == "ok":
            landed = final_pass is not None and _own_status(final_pass) == "ok"
            if not landed or parse_ts(run["created_at"]) < parse_ts(final_pass["updated_at"]):
                status, note = "warn", "Ran before today's predictions landed"
        out.append(_status(stage, run, status, note))
    return out
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `uv run --extra test python -m pytest tests/test_chain.py -v`
Expected: all PASS.

- [ ] **Step 7: Commit**

```bash
git add src/monitoring/chain.py tests/test_chain.py tests/fixtures/chain_runs_2026-10-02.json
git commit -m "feat(monitor): stage definitions and status rules for the daily chain

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 3: Verdict, history and the report

**Files:**
- Modify: `src/monitoring/chain.py` (append)
- Modify: `tests/test_chain.py` (append)

**Interfaces:**
- Consumes: `build_chain`, `chain_day`, `window` and `Chain` from Task 2.
- Produces:
  - `verdict(c: Chain) -> dict`, with keys `status` (`ok|running|warn|fail`), `stage`, `headline`, `since` and `duration_minutes`
  - `history_start(now: datetime, days: int) -> datetime`
  - `build_report(runs: Runs, days: int, now: datetime) -> dict`, with keys `generated_at`, `verdict`, `today` (`Chain.to_dict()`) and `history` (`[{"day", "stages": {key: status}}]`, oldest first)

- [ ] **Step 1: Write the failing tests** (append to `tests/test_chain.py`)

```python
def test_verdict_ok_reports_duration():
    v = chain.verdict(chain.build_chain(_fixture(), DAY, NOON))
    assert v["status"] == "ok"
    assert v["headline"] == "Chain completed"
    assert v["stage"] is None
    assert 30 <= v["duration_minutes"] <= 180


def test_verdict_names_the_stall():
    runs = _drop(_fixture(), "score_games", "game_embeddings", "dataform_4", "viewer_artifacts")
    v = chain.verdict(chain.build_chain(runs, DAY, NOON))
    assert v == {
        "status": "warn",
        "stage": "dataform_3",
        "headline": "Stalled after Dataform · pass 3",
        "since": _run_of(runs, "dataform_3")["updated_at"],
        "duration_minutes": None,
    }


def test_verdict_failed_stage():
    runs = copy.deepcopy(_fixture())
    _run_of(runs, "score_complexity")["conclusion"] = "failure"
    v = chain.verdict(chain.build_chain(runs, DAY, NOON))
    assert (v["status"], v["stage"], v["headline"]) == (
        "fail", "score_complexity", "Score Complexity failed")


def test_verdict_waiting_before_the_chain_starts():
    v = chain.verdict(chain.build_chain({}, DAY, datetime(2026, 10, 2, 5, 30, tzinfo=UTC)))
    assert (v["status"], v["headline"]) == ("running", "Waiting on Fetch Thing IDs")


def test_verdict_running():
    runs = copy.deepcopy(_fixture())
    _run_of(runs, "viewer_artifacts").update(status="in_progress", conclusion=None)
    v = chain.verdict(chain.build_chain(runs, DAY, NOON))
    assert (v["status"], v["headline"]) == ("running", "Running: Viewer Artifacts")


def test_history_start_covers_the_requested_days():
    assert chain.history_start(NOON, 3) == datetime(2026, 9, 30, 5, 0, tzinfo=UTC)


def test_build_report_shape():
    report = chain.build_report(_fixture(), 3, NOON)
    assert set(report) == {"generated_at", "verdict", "today", "history"}
    assert report["generated_at"] == "2026-10-02T12:00:00Z"
    assert [h["day"] for h in report["history"]] == ["2026-09-30", "2026-10-01", "2026-10-02"]
    assert report["history"][-1]["stages"]["viewer_artifacts"] == "ok"
    assert report["history"][0]["stages"]["fetch_thing_ids"] == "fail"  # no runs in fixture
    assert report["today"]["day"] == "2026-10-02"
    json.dumps(report)
```

- [ ] **Step 2: Run to verify they fail**

Run: `uv run --extra test python -m pytest tests/test_chain.py -v -k "verdict or history or report"`
Expected: FAIL with `AttributeError: ... has no attribute 'verdict'`.

- [ ] **Step 3: Implement** (append to `src/monitoring/chain.py`)

```python
def _minutes(start: str, end: str) -> int:
    return round((parse_ts(end) - parse_ts(start)).total_seconds() / 60)


def verdict(c: Chain) -> dict[str, Any]:
    """The page headline: the first stage that isn't ok, or the end-to-end time."""
    bad = next((s for s in c.stages if s.status != "ok"), None)
    if bad is None:
        first, last = c.stages[0], c.stages[-1]
        return {
            "status": "ok",
            "stage": None,
            "headline": "Chain completed",
            "since": last.finished,
            "duration_minutes": _minutes(first.started, last.finished),
        }
    if bad.status == "running":
        status, headline = "running", f"Running: {bad.label}"
    elif bad.status == "pending":
        status, headline = "running", f"Waiting on {bad.label}"
    elif bad.status == "warn":
        status, headline = "warn", f"Stalled after {bad.label}"
    elif bad.status == "fail":
        status, headline = "fail", f"{bad.label} failed"
    else:  # not_reached as the first non-ok stage: a skipped run
        status, headline = "warn", f"{bad.label} never ran"
    return {
        "status": status,
        "stage": bad.key,
        "headline": headline,
        "since": bad.finished or bad.started,
        "duration_minutes": None,
    }


def history_start(now: datetime, days: int) -> datetime:
    """Start of the oldest chain window in a ``days``-long history ending today."""
    return window(chain_day(now) - timedelta(days=days - 1))[0]


def build_report(runs: Runs, days: int, now: datetime) -> dict[str, Any]:
    today = chain_day(now)
    chains = [build_chain(runs, today - timedelta(days=d), now) for d in range(days - 1, -1, -1)]
    current = chains[-1]
    return {
        "generated_at": iso(now),
        "verdict": verdict(current),
        "today": current.to_dict(),
        "history": [
            {"day": c.day.isoformat(), "stages": {s.key: s.status for s in c.stages}}
            for c in chains
        ],
    }
```

- [ ] **Step 4: Run to verify they pass**

Run: `uv run --extra test python -m pytest tests/test_chain.py -v`
Expected: all PASS.

- [ ] **Step 5: Replay a known bad day (manual check)**

Run:

```bash
GH_TOKEN=$(gh auth token) uv run python - <<'EOF'
import os
from datetime import UTC, date, datetime
from src.monitoring import chain
from src.monitoring.github import fetch_runs

day = date(2026, 9, 16)
start, end = chain.window(day)
runs = {s: fetch_runs(*s, start, end, os.environ["GH_TOKEN"]) for s in chain.SOURCES}
c = chain.build_chain(runs, day, end)
for s in c.stages:
    print(f"{s.status:12} {s.label:28} {s.started or ''}")
print(chain.verdict(c))
EOF
```

Expected: on 09-16, Fetch Thing IDs failed (token newline, #121) and was rerun by hand. The latest run wins, so check against the run list in GitHub: if the rerun fell inside the window, the stage reads `ok`; otherwise `fail`. Note what you see in the PR description. This is a sanity check, not a gate.

- [ ] **Step 6: Commit**

```bash
git add src/monitoring/chain.py tests/test_chain.py
git commit -m "feat(monitor): verdict, history and report for the daily chain

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 4: Freshness and deployed-models reader

**Files:**
- Modify: `config/bigquery.yaml`. Add `monitoring: monitoring` under `datasets:`.
- Create: `src/warehouse/readers/pipeline.py`
- Create: `tests/test_pipeline_reader.py`

**Interfaces:**
- Produces:
  - `SCORING_YEARS = (2025, 2030)`
  - `TABLES: list[TableSpec]`
  - `fetch_table_status(client=None) -> list[dict]`, rows with keys `table`, `last_updated`, `games`, `covered`, `universe` and `users`, in `TABLES` order
  - `fetch_deployed_models(client=None) -> list[dict]`, rows with keys `model_category`, `model_type`, `model_name`, `model_version`, `experiment`, `games_count` and `last_updated`

- [ ] **Step 1: Write the failing tests**

`tests/test_pipeline_reader.py`:

```python
"""Unit tests for the pipeline monitor reader (BigQuery mocked — no network)."""

from src.warehouse.readers import pipeline


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


def _params(job_config):
    return {p.name: p.value for p in job_config.query_parameters}


def _row(ord_, name, **kw):
    return {"ord": ord_, "table_name": name, "last_updated": "2026-10-02T07:29:00Z",
            "games": 10, "covered": None, "universe": None, "users": None} | kw


def test_table_status_maps_and_orders_rows():
    rows = [_row(1, "raw.fetched_responses", covered=9, universe=10), _row(0, "raw.thing_ids")]
    result = pipeline.fetch_table_status(client=FakeClient(rows))
    assert [r["table"] for r in result] == ["raw.thing_ids", "raw.fetched_responses"]
    assert result[1] == {"table": "raw.fetched_responses", "last_updated": "2026-10-02T07:29:00Z",
                         "games": 10, "covered": 9, "universe": 10, "users": None}


def test_table_status_query_covers_every_table_once():
    client = FakeClient([])
    pipeline.fetch_table_status(client=client)
    sql, job_config = client.calls[0]
    assert len(client.calls) == 1
    for spec in pipeline.TABLES:
        assert f"'{spec.name}'" in sql
    assert sql.count("UNION ALL") == len(pipeline.TABLES) - 1
    assert _params(job_config) == {"year_start": 2025, "year_end": 2030}


def test_table_status_universes():
    by_name = {t.name: t for t in pipeline.TABLES}
    assert by_name["predictions.bgg_predictions"].universe == "scoring_games"
    assert by_name["raw.fetched_responses"].universe == "boardgame_ids"
    assert by_name["predictions.bgg_game_coordinates"].universe == "all_games"
    assert by_name["raw.thing_ids"].universe is None
    assert by_name["predictions.user_collection_predictions"].count_users


def test_deployed_models_latest_per_type():
    row = {"model_category": "prediction", "model_type": "hurdle", "model_name": "h",
           "model_version": "14", "experiment": "e", "games_count": 39316,
           "last_updated": "2026-10-02T07:25:00Z"}
    client = FakeClient([row])
    assert pipeline.fetch_deployed_models(client=client) == [row]
    sql, _ = client.calls[0]
    assert "monitoring.deployed_models" in sql
    assert "QUALIFY ROW_NUMBER() OVER (PARTITION BY model_type" in sql
```

- [ ] **Step 2: Run to verify they fail**

Run: `uv run --extra test python -m pytest tests/test_pipeline_reader.py -v`
Expected: FAIL with `ImportError: cannot import name 'pipeline'`.

- [ ] **Step 3: Implement**

`config/bigquery.yaml`: under `datasets:`, add the line `  monitoring: monitoring` after `analytics: analytics`.

`src/warehouse/readers/pipeline.py`:

```python
"""Reader for the pipeline monitor.

How fresh each downstream table is, how much of the games it should cover it actually
covers, and which model versions are live. One query for the tables, one for models.
See docs/superpowers/specs/2026-10-02-pipeline-monitor-design.md.
"""

from dataclasses import dataclass
from typing import Any, Optional

from google.cloud import bigquery

from src.warehouse.bq import dataset, get_client

# The daily scoring run's default year range (bgg-predictive-models
# .github/workflows/run-scoring-service.yml). bgg_predictions only covers these years.
SCORING_YEARS = (2025, 2030)


@dataclass(frozen=True)
class TableSpec:
    name: str
    dataset: str
    table: str
    ts_column: str
    universe: str | None = None  # a CTE in _UNIVERSES; None = no coverage
    where: str | None = None
    count_users: bool = False


TABLES = [
    TableSpec("raw.thing_ids", "raw", "thing_ids", "load_timestamp"),
    TableSpec("raw.fetched_responses", "raw", "fetched_responses", "fetch_timestamp",
              universe="boardgame_ids", where="t.fetch_status = 'success'"),
    TableSpec("analytics.games_features", "analytics", "games_features", "load_timestamp"),
    TableSpec("predictions.bgg_description_embeddings", "predictions",
              "bgg_description_embeddings", "created_ts", universe="all_games"),
    TableSpec("predictions.bgg_complexity_predictions", "predictions",
              "bgg_complexity_predictions", "score_ts", universe="all_games"),
    TableSpec("predictions.bgg_predictions", "predictions", "bgg_predictions", "score_ts",
              universe="scoring_games"),
    TableSpec("predictions.bgg_game_embeddings", "predictions", "bgg_game_embeddings",
              "created_ts", universe="all_games"),
    TableSpec("predictions.bgg_game_coordinates", "predictions", "bgg_game_coordinates",
              "created_ts", universe="all_games"),
    TableSpec("predictions.user_collection_predictions", "predictions",
              "user_collection_predictions", "score_ts", count_users=True),
]


def _universes() -> str:
    features = f"`{dataset('analytics')}.games_features`"
    return f"""
        all_games AS (SELECT DISTINCT game_id FROM {features}),
        scoring_games AS (
          SELECT DISTINCT game_id FROM {features}
          WHERE year_published BETWEEN @year_start AND @year_end
        ),
        boardgame_ids AS (
          SELECT DISTINCT game_id FROM `{dataset('raw')}.thing_ids` WHERE type = 'boardgame'
        )"""


def _select(i: int, t: TableSpec) -> str:
    src = f"`{dataset(t.dataset)}.{t.table}`"
    where = f"WHERE {t.where}" if t.where else ""
    users = "COUNT(DISTINCT t.username)" if t.count_users else "CAST(NULL AS INT64)"
    if t.universe:
        covered = "COUNT(DISTINCT u.game_id)"
        universe = f"(SELECT COUNT(*) FROM {t.universe})"
        join = f"LEFT JOIN {t.universe} u ON u.game_id = t.game_id"
    else:
        covered = universe = "CAST(NULL AS INT64)"
        join = ""
    return f"""
        SELECT {i} AS ord, '{t.name}' AS table_name, MAX(t.{t.ts_column}) AS last_updated,
               COUNT(DISTINCT t.game_id) AS games, {covered} AS covered,
               {universe} AS universe, {users} AS users
        FROM {src} t {join} {where}"""


def fetch_table_status(client: Optional[bigquery.Client] = None) -> list[dict[str, Any]]:
    """One row per monitored table, in ``TABLES`` order."""
    client = client or get_client()
    sql = f"WITH {_universes()}\n" + "\nUNION ALL\n".join(
        _select(i, t) for i, t in enumerate(TABLES)
    )
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("year_start", "INT64", SCORING_YEARS[0]),
            bigquery.ScalarQueryParameter("year_end", "INT64", SCORING_YEARS[1]),
        ]
    )
    rows = sorted(
        (dict(r) for r in client.query(sql, job_config=job_config).result()),
        key=lambda r: r["ord"],
    )
    return [
        {
            "table": r["table_name"],
            "last_updated": r["last_updated"],
            "games": r["games"],
            "covered": r["covered"],
            "universe": r["universe"],
            "users": r["users"],
        }
        for r in rows
    ]


def fetch_deployed_models(client: Optional[bigquery.Client] = None) -> list[dict[str, Any]]:
    """The newest deployed model per type, from ``monitoring.deployed_models``.

    The view aggregates the raw landing tables, so this scans ~360MB per call; the
    route's 5-minute cache keeps that to a few calls an hour while the page is open.
    """
    client = client or get_client()
    sql = f"""
        SELECT model_category, model_type, model_name, model_version, experiment,
               games_count, last_updated
        FROM `{dataset('monitoring')}.deployed_models`
        WHERE TRUE
        QUALIFY ROW_NUMBER() OVER (PARTITION BY model_type ORDER BY last_updated DESC) = 1
        ORDER BY model_category, model_type
    """
    return [dict(r) for r in client.query(sql).result()]
```

- [ ] **Step 4: Run to verify they pass**

Run: `uv run --extra test python -m pytest tests/test_pipeline_reader.py tests/test_config_datasets.py -v`
Expected: all PASS.

- [ ] **Step 5: Dry-run the real SQL against BigQuery**

Run:

```bash
uv run python - <<'EOF'
from google.cloud import bigquery
from src.warehouse.bq import get_client
from src.warehouse.readers import pipeline

client = get_client()
cfg = dict(dry_run=True, use_query_cache=False)

class Capture:
    def query(self, sql, job_config=None):
        self.sql, self.job_config = sql, job_config
        class R:
            def result(self_inner): return []
        return R()

cap = Capture()
pipeline.fetch_table_status(client=cap)
job = client.query(cap.sql, job_config=bigquery.QueryJobConfig(
    query_parameters=cap.job_config.query_parameters, **cfg))
print("tables OK, bytes:", job.total_bytes_processed)
pipeline.fetch_deployed_models(client=cap)
job = client.query(cap.sql, job_config=bigquery.QueryJobConfig(**cfg))
print("models OK, bytes:", job.total_bytes_processed)
EOF
```

Expected: both print `OK`. Tables come in around 50–150 MB and models around 360 MB. If either query fails, fix the SQL. Don't drop a table to make it pass.

- [ ] **Step 6: Commit**

```bash
git add config/bigquery.yaml src/warehouse/readers/pipeline.py tests/test_pipeline_reader.py
git commit -m "feat(monitor): table freshness/coverage and deployed-models reader

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 5: `GET /monitoring/pipeline`

**Files:**
- Modify: `services/warehouse_api/routers/monitoring.py`
- Modify: `tests/test_monitoring_router.py` (append)

**Interfaces:**
- Consumes: `fetch_runs` (Task 1); `SOURCES`, `history_start` and `build_report` (Tasks 2–3); `fetch_table_status` and `fetch_deployed_models` (Task 4).
- Produces: `GET /monitoring/pipeline?days=N`, returning `build_report(...) | {"tables": [...], "models": [...]}`. Errors: 503 when there's no `GH_TOKEN`, 502 on an upstream error, 422 when `days` is out of range.

- [ ] **Step 1: Write the failing tests** (append to `tests/test_monitoring_router.py`)

```python
# --- /monitoring/pipeline ---------------------------------------------------

import requests  # noqa: E402 — appended section; only these tests need it

TABLE_ROW = {"table": "raw.thing_ids", "last_updated": "2026-10-02T06:30:00Z", "games": 5,
             "covered": None, "universe": None, "users": None}
MODEL_ROW = {"model_category": "prediction", "model_type": "hurdle", "model_name": "h",
             "model_version": "14", "experiment": "e", "games_count": 1,
             "last_updated": "2026-10-02T07:25:00Z"}


@pytest.fixture
def pipeline_ok(monkeypatch):
    calls = {"runs": 0, "tables": 0}

    def fake_runs(repo, workflow_file, start, end, token):
        calls["runs"] += 1
        assert token == "tok"
        return []

    def fake_tables():
        calls["tables"] += 1
        return [TABLE_ROW]

    monkeypatch.setenv("GH_TOKEN", "tok")
    monkeypatch.setattr(monitoring_router, "fetch_runs", fake_runs)
    monkeypatch.setattr(monitoring_router.pipeline_reader, "fetch_table_status", fake_tables)
    monkeypatch.setattr(monitoring_router.pipeline_reader, "fetch_deployed_models",
                        lambda: [MODEL_ROW])
    return calls


def test_pipeline_report_shape(pipeline_ok):
    r = client.get("/monitoring/pipeline?days=3")
    assert r.status_code == 200
    body = r.json()
    assert set(body) == {"generated_at", "verdict", "today", "history", "tables", "models"}
    assert len(body["history"]) == 3
    assert len(body["today"]["stages"]) == 12
    assert body["tables"] == [TABLE_ROW]
    assert body["models"] == [MODEL_ROW]
    assert pipeline_ok["runs"] == len(monitoring_router.chain.SOURCES)


def test_pipeline_defaults_to_14_days(pipeline_ok):
    assert len(client.get("/monitoring/pipeline").json()["history"]) == 14


def test_pipeline_caches_per_days(pipeline_ok):
    client.get("/monitoring/pipeline?days=3")
    client.get("/monitoring/pipeline?days=3")
    assert pipeline_ok["tables"] == 1
    client.get("/monitoring/pipeline?days=4")
    assert pipeline_ok["tables"] == 2


def test_pipeline_requires_github_token(monkeypatch):
    monkeypatch.delenv("GH_TOKEN", raising=False)
    r = client.get("/monitoring/pipeline")
    assert r.status_code == 503
    assert "GH_TOKEN" in r.json()["detail"]


def test_pipeline_upstream_error_is_502(pipeline_ok, monkeypatch):
    def boom(*args, **kwargs):
        raise requests.HTTPError("401 Unauthorized")

    monkeypatch.setattr(monitoring_router, "fetch_runs", boom)
    r = client.get("/monitoring/pipeline")
    assert r.status_code == 502
    assert "401" in r.json()["detail"]


def test_pipeline_rejects_out_of_range_days(pipeline_ok):
    assert client.get("/monitoring/pipeline?days=0").status_code == 422
    assert client.get("/monitoring/pipeline?days=31").status_code == 422
```

- [ ] **Step 2: Run to verify they fail**

Run: `uv run --extra test python -m pytest tests/test_monitoring_router.py -v -k pipeline`
Expected: FAIL. `fetch_runs` isn't an attribute of the router, and requests get 404.

- [ ] **Step 3: Implement**

In `services/warehouse_api/routers/monitoring.py`:

1. Update the module docstring to read: `Serves bgg-viewer's "what's new" page and the admin pipeline monitor.`
2. Replace the imports block with:

```python
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

from fastapi import APIRouter, HTTPException, Query

from src.monitoring import chain
from src.monitoring.github import fetch_runs
from src.warehouse.readers import monitoring as reader
from src.warehouse.readers import pipeline as pipeline_reader
```

3. Replace `_reset_cache` with:

```python
_pipeline_cache: dict[int, tuple[float, dict]] = {}


def _reset_cache() -> None:
    """Test seam — clears the module-level caches between tests."""
    _cache.clear()
    _pipeline_cache.clear()
```

4. Append:

```python
def _collect_runs(start: datetime, end: datetime, token: str) -> chain.Runs:
    """Every chain workflow's runs in the window, listed in parallel (~13 requests)."""
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {
            source: pool.submit(fetch_runs, source[0], source[1], start, end, token)
            for source in chain.SOURCES
        }
        return {source: f.result() for source, f in futures.items()}


@router.get("/monitoring/pipeline")
def get_pipeline(days: int = Query(14, ge=1, le=30)):
    """Today's chain, a ``days``-long history, table freshness and live models."""
    cached = _pipeline_cache.get(days)
    if cached is not None and time.time() - cached[0] < _CACHE_TTL_SECONDS:
        return cached[1]

    token = os.environ.get("GH_TOKEN")
    if not token:
        raise HTTPException(503, "GH_TOKEN is not set on the warehouse API")

    now = datetime.now(UTC)
    try:
        runs = _collect_runs(chain.history_start(now, days), now, token)
        tables = pipeline_reader.fetch_table_status()
        models = pipeline_reader.fetch_deployed_models()
    except Exception as exc:  # GitHub or BigQuery: report it, don't serve a partial status
        raise HTTPException(502, f"pipeline status unavailable: {exc}") from exc

    result = chain.build_report(runs, days, now) | {"tables": tables, "models": models}
    _pipeline_cache[days] = (time.time(), result)
    return result
```

- [ ] **Step 4: Run the whole warehouse suite**

Run: `uv run --extra test python -m pytest tests/test_monitoring_router.py tests/test_monitoring_reader.py tests/test_chain.py tests/test_pipeline_reader.py tests/test_monitoring_github.py tests/test_pipeline_status.py tests/test_games_router.py -v`
Expected: all PASS.

- [ ] **Step 5: Run the API locally and call it**

Run (background, or in a second terminal):

```bash
GH_TOKEN=$(gh auth token) uv run python -m services.warehouse_api.main
```

Then:

```bash
curl -s "localhost:8080/monitoring/pipeline?days=3" | uv run python -c "
import json, sys
d = json.load(sys.stdin)
print(d['verdict'])
for s in d['today']['stages']: print(f\"{s['status']:12} {s['label']}\")
for t in d['tables']: print(t)
print(len(d['models']), 'models')"
```

Expected: a real verdict, which on a normal afternoon is `Chain completed`. You should also see 12 stages, 9 table rows with plausible counts and timestamps, and 7 models (5 prediction + 2 embedding types). Stop the server afterwards.

- [ ] **Step 6: Commit**

```bash
git add services/warehouse_api/routers/monitoring.py tests/test_monitoring_router.py
git commit -m "feat(api): GET /monitoring/pipeline — chain status, freshness, models

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 6: README and spec touch-ups, then open the warehouse PR

**Files:**
- Modify: `README.md`, in the paragraph that starts "`Scrape Heartbeat` runs daily" (if #133 already replaced it with Pipeline Status, edit that paragraph instead)
- Modify: `docs/superpowers/specs/2026-10-02-pipeline-monitor-design.md`, in the `fetch_table_status` row shape

- [ ] **Step 1: README**

After the Pipeline Status paragraph in the Orchestration section, add:

```markdown
The whole chain (all twelve stages across the three repos), table freshness and
coverage, and the live model versions are served by the warehouse API at
`GET /monitoring/pipeline` and shown on bgg-viewer's admin `/admin/pipeline` page. The
API needs `GH_TOKEN` to read Actions history (locally: `GH_TOKEN=$(gh auth token)`).
```

In the `Consumers` section, replace `bgg-dash-viewer` with `bgg-viewer` (`https://github.com/phenrickson/bgg-viewer`). bgg-dash-viewer is retired.

- [ ] **Step 2: Spec**

In the spec's `fetch_table_status` description, change "returns `table`, `last_updated`, `games`, `universe` (nullable)" to "returns `table`, `last_updated`, `games`, `covered`, `universe` (nullable) and `users` (collection predictions only)". Make the same change to the `"tables"` example in the response JSON: `{"table": "…", "last_updated": "…", "games": 0, "covered": 0, "universe": 0, "users": null}`.

- [ ] **Step 3: Commit and push, and open the PR**

```bash
git add README.md docs/superpowers/specs/2026-10-02-pipeline-monitor-design.md
git commit -m "docs: pipeline monitor in README; spec row shape

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git push -u origin feat/pipeline-monitor
gh pr create --title "feat(monitor): GET /monitoring/pipeline for the whole daily chain" --body "$(cat <<'EOF'
Implements docs/superpowers/specs/2026-10-02-pipeline-monitor-design.md (warehouse half).

- src/monitoring/github.py: run fetching shared with Pipeline Status, now paginated
- src/monitoring/chain.py: the 12 stages + off-chain jobs, status rules, verdict, history
- src/warehouse/readers/pipeline.py: table freshness/coverage + deployed models
- GET /monitoring/pipeline on the warehouse API (5-min cache; 503 without GH_TOKEN)

**Do not deploy-merge until the GH_TOKEN secret exists** — see Task 7 of the plan.
Without it the endpoint returns 503 in production; everything else is unaffected.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Merging is Phil's call. Don't merge.

### Task 7: Production wiring for `GH_TOKEN` (separate PR, ordered)

The order matters. `gcloud run deploy --set-secrets` fails if the secret has no version yet, and that would break the API's deploy.

**Files:**
- Modify: `terraform/warehouse_api.tf`
- Modify: `config/cloudbuild.warehouse-api.yaml`

- [ ] **Step 1: Terraform — the secret and its accessor** (append to `terraform/warehouse_api.tf`)

```hcl
# GitHub token the read API uses to list Actions runs for GET /monitoring/pipeline.
# The three repos are public; the token only lifts the rate limit, so a fine-grained
# token with no extra permissions is enough. The version is added by hand (never in
# git or state):
#   printf %s "$TOKEN" | gcloud secrets versions add warehouse-api-github-token --data-file=-
resource "google_secret_manager_secret" "warehouse_api_github_token" {
  secret_id = "warehouse-api-github-token"
  project   = var.project_id

  replication {
    auto {}
  }

  labels = {
    environment = var.environment
    managed_by  = "terraform"
    purpose     = "monitoring"
  }
}

resource "google_secret_manager_secret_iam_member" "warehouse_api_github_token_access" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.warehouse_api_github_token.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:bgg-data-warehouse@${var.project_id}.iam.gserviceaccount.com"
}
```

Run: `cd terraform && terraform fmt -check && terraform validate`
Expected: no formatting changes, `Success! The configuration is valid.`

- [ ] **Step 2: Commit and open the PR (Terraform only)**

```bash
git switch main && git pull --ff-only && git switch -c feat/pipeline-monitor-secret
git add terraform/warehouse_api.tf
git commit -m "feat(terraform): secret for the warehouse API's GitHub token

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git push -u origin feat/pipeline-monitor-secret
gh pr create --title "feat(terraform): warehouse-api-github-token secret" --body "Secret + accessor for GET /monitoring/pipeline. After merge (terraform.yml applies it), Phil adds a version by hand, then the cloudbuild change can land.

🤖 Generated with [Claude Code](https://claude.com/claude-code)"
```

- [ ] **Step 3: Hand-off to Phil (manual, after the Terraform PR is applied)**

Phil creates a fine-grained GitHub token (public repos, read-only, no extra permissions) and adds it:

```bash
printf %s "$TOKEN" | gcloud secrets versions add warehouse-api-github-token --data-file=- --project=bgg-data-warehouse
```

- [ ] **Step 4: Mount the secret on deploy** (only once Step 3 is done)

In `config/cloudbuild.warehouse-api.yaml`, in the `run deploy` step's `args`, add after `'--port=8080'`:

```yaml
      - '--set-secrets=GH_TOKEN=warehouse-api-github-token:latest'
```

Commit it on `feat/pipeline-monitor` (or as a follow-up PR if that branch has merged):

```bash
git add config/cloudbuild.warehouse-api.yaml
git commit -m "feat(deploy): mount GH_TOKEN on the warehouse API

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

After that merge deploys, a call to `/monitoring/pipeline` through the viewer returns 200.

---

# Part B — bgg-viewer

All Part B commands run from `/Users/phenrickson/Documents/projects/bgg-viewer`, on branch `feat/pipeline-monitor` (which already has the mockup commit). Part B builds and tests without Part A deployed. For the end-to-end check, run the Part A API locally (Task 5, Step 5).

### Task 8: Types and `getPipelineStatus`

**Files:**
- Modify: `src/lib/server/warehouse/types.ts` (append)
- Modify: `src/lib/server/warehouse/client.ts`
- Modify: `src/lib/server/warehouse/index.ts` (export the new types)
- Modify: `src/lib/server/warehouse/client.test.ts` (append)

**Interfaces:**
- Produces:
  - types `StageStatusName`, `Lane`, `PipelineStage`, `PipelineVerdict`, `PipelineTableRow`, `DeployedModelRow`, `PipelineStatus`
  - `WarehouseClient.getPipelineStatus(days?: number): Promise<PipelineStatus>`

- [ ] **Step 1: Write the failing tests** (append to `client.test.ts`)

```ts
describe('createWarehouseClient.getPipelineStatus', () => {
	const status = {
		generated_at: '2026-10-02T12:00:00Z',
		verdict: { status: 'ok', stage: null, headline: 'Chain completed', since: '2026-10-02T07:31:00Z', duration_minutes: 65 },
		today: { day: '2026-10-02', stages: [], off_chain: [] },
		history: [],
		tables: [],
		models: []
	};

	it('GETs /monitoring/pipeline with the day count', async () => {
		const { fetchImpl, calls } = stubFetch(status);
		const client = createWarehouseClient({
			baseUrl: 'https://warehouse.example',
			getIdToken: async () => 'tok',
			fetch: fetchImpl
		});

		const result = await client.getPipelineStatus(7);

		expect(calls[0].url).toBe('https://warehouse.example/monitoring/pipeline?days=7');
		expect(result).toEqual(status);
	});

	it('defaults to 14 days', async () => {
		const { fetchImpl, calls } = stubFetch(status);
		const client = createWarehouseClient({
			baseUrl: 'https://warehouse.example',
			getIdToken: async () => 'tok',
			fetch: fetchImpl
		});

		await client.getPipelineStatus();

		expect(calls[0].url).toBe('https://warehouse.example/monitoring/pipeline?days=14');
	});

	it('throws WarehouseError carrying the API detail on failure', async () => {
		const { fetchImpl } = stubFetch({ detail: 'GH_TOKEN is not set on the warehouse API' }, { status: 503 });
		const client = createWarehouseClient({
			baseUrl: 'https://warehouse.example',
			getIdToken: async () => 'tok',
			fetch: fetchImpl
		});

		const err = await client.getPipelineStatus().catch((e) => e);
		expect(err).toBeInstanceOf(WarehouseError);
		expect(err.status).toBe(503);
		expect(err.message).toContain('GH_TOKEN is not set');
	});
});
```

- [ ] **Step 2: Run to verify they fail**

Run: `pnpm vitest run src/lib/server/warehouse/client.test.ts`
Expected: FAIL with `client.getPipelineStatus is not a function`.

- [ ] **Step 3: Implement**

Append to `src/lib/server/warehouse/types.ts`:

```ts
/** Stage statuses from GET /monitoring/pipeline — the warehouse's src/monitoring/chain.py. */
export type StageStatusName = 'ok' | 'warn' | 'fail' | 'running' | 'pending' | 'not_reached';
export type Lane = 'warehouse' | 'models' | 'viewer';

export interface PipelineStage {
	key: string;
	label: string;
	lane: Lane;
	status: StageStatusName;
	started: string | null;
	finished: string | null;
	url: string | null;
	event: string | null;
	title: string | null;
	note: string | null;
}

export interface PipelineVerdict {
	status: 'ok' | 'running' | 'warn' | 'fail';
	stage: string | null;
	headline: string;
	since: string | null;
	duration_minutes: number | null;
}

export interface PipelineTableRow {
	table: string;
	last_updated: string | null;
	games: number;
	covered: number | null;
	universe: number | null;
	users: number | null;
}

export interface DeployedModelRow {
	model_category: string;
	model_type: string;
	model_name: string | null;
	model_version: string | null;
	experiment: string | null;
	games_count: number;
	last_updated: string | null;
}

export interface PipelineStatus {
	generated_at: string;
	verdict: PipelineVerdict;
	today: { day: string; stages: PipelineStage[]; off_chain: PipelineStage[] };
	history: { day: string; stages: Record<string, StageStatusName> }[];
	tables: PipelineTableRow[];
	models: DeployedModelRow[];
}
```

In `client.ts`:
- Extend the import: `import { GameNotFoundError, WarehouseError, type GameDocument, type NewGameRow, type PipelineStatus } from './types';`
- Add to `interface WarehouseClient`: `getPipelineStatus(days?: number): Promise<PipelineStatus>;`
- Add to the returned object, after `getNewGames`:

```ts
		async getPipelineStatus(days = 14): Promise<PipelineStatus> {
			const res = await authedGet(`/monitoring/pipeline?days=${days}`);
			if (!res.ok) {
				// The API explains a 502/503 in `detail` (missing token, GitHub/BigQuery down);
				// the admin page shows it, so carry it through rather than just the status.
				const detail = await res
					.json()
					.then((b: { detail?: string }) => b.detail ?? '')
					.catch(() => '');
				throw new WarehouseError(
					res.status,
					`warehouse GET /monitoring/pipeline failed (${res.status})${detail ? `: ${detail}` : ''}`
				);
			}
			return (await res.json()) as PipelineStatus;
		}
```

In `index.ts`, extend the type export:

```ts
export type {
	GameDocument,
	GameFeatures,
	NewGameRow,
	SimilarWireRow,
	StageStatusName,
	Lane,
	PipelineStage,
	PipelineVerdict,
	PipelineTableRow,
	DeployedModelRow,
	PipelineStatus
} from './types';
```

- [ ] **Step 4: Run to verify they pass**

Run: `pnpm vitest run src/lib/server/warehouse/client.test.ts`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/lib/server/warehouse
git commit -m "feat(warehouse): getPipelineStatus client + types

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 9: Display rules, status tokens, and the admin route

**Files:**
- Create: `src/lib/monitoring/display.ts`
- Create: `src/lib/monitoring/display.test.ts`
- Modify: `src/app.css`, adding the status tokens
- Create: `src/routes/(app)/admin/pipeline/+page.server.ts`
- Create: `src/routes/(app)/admin/pipeline/page.server.test.ts`

**Interfaces:**
- Consumes: the types and `getPipelineStatus` from Task 8, `isAdmin` from `$lib/server/auth/admin`, and `getCatalogPointer` from `$lib/server/catalog/gcs`.
- Produces:
  - `display.ts` exports `Tone`, `STATUS_TONE`, `STATUS_WORD`, `STATUS_GLYPH`, `duration(from, to)`, `clock(iso)`, `elapsed(fromIso, now)`, `freshness(lastUpdated, reference)`, `freshnessReference(today)`, `coverage(covered, universe)` and `COVERAGE_FLOOR`
  - The page data is `{ status: PipelineStatus | null; error: string | null; catalog: { builtAt: string; rows: number } | null }`

- [ ] **Step 1: Write the failing tests**

`src/lib/monitoring/display.test.ts`:

```ts
import { describe, expect, it } from 'vitest';
import {
	clock,
	coverage,
	duration,
	elapsed,
	freshness,
	freshnessReference,
	STATUS_GLYPH,
	STATUS_TONE,
	STATUS_WORD
} from './display';

describe('status vocabulary', () => {
	it('gives every status a word, and a tone that is never green/red by name', () => {
		for (const s of ['ok', 'warn', 'fail', 'running', 'pending', 'not_reached'] as const) {
			expect(STATUS_WORD[s]).toBeTruthy();
			expect(['ok', 'warn', 'fail', 'idle']).toContain(STATUS_TONE[s]);
			expect(STATUS_GLYPH[s]).toBeTypeOf('string');
		}
	});
});

describe('duration / clock / elapsed', () => {
	it('formats minutes and hours', () => {
		expect(duration('2026-10-02T06:26:05Z', '2026-10-02T06:30:10Z')).toBe('4m');
		expect(duration('2026-10-02T06:26:00Z', '2026-10-02T07:31:00Z')).toBe('1h 05m');
		expect(duration(null, '2026-10-02T07:31:00Z')).toBe('');
	});
	it('shows UTC wall time', () => {
		expect(clock('2026-10-02T07:18:57Z')).toBe('07:18');
		expect(clock(null)).toBe('—');
	});
	it('says how long ago', () => {
		const now = new Date('2026-10-02T12:16:00Z');
		expect(elapsed('2026-10-02T07:24:00Z', now)).toBe('4h 52m');
		expect(elapsed(null, now)).toBe('');
	});
});

describe('freshness', () => {
	const ref = '2026-10-02T06:26:05Z';
	it('is fresh when updated after the chain started', () => {
		expect(freshness('2026-10-02T07:29:00Z', ref)).toEqual({ tone: 'ok', label: 'Fresh' });
	});
	it('counts days behind', () => {
		expect(freshness('2026-10-01T07:29:00Z', ref)).toEqual({ tone: 'warn', label: 'A day old' });
		expect(freshness('2026-09-29T07:29:00Z', ref)).toEqual({ tone: 'warn', label: '3 days old' });
	});
	it('handles never', () => {
		expect(freshness(null, ref)).toEqual({ tone: 'warn', label: 'Never updated' });
	});
	it('uses stage 1 start, else 05:00 UTC on the chain day', () => {
		expect(freshnessReference({ day: '2026-10-02', stages: [{ started: ref }] })).toBe(ref);
		expect(freshnessReference({ day: '2026-10-02', stages: [{ started: null }] })).toBe(
			'2026-10-02T05:00:00Z'
		);
	});
});

describe('coverage', () => {
	it('computes a share and flags below 99.5%', () => {
		expect(coverage(39316, 39316)).toEqual({ pct: 1, low: false });
		expect(coverage(994, 1000)).toEqual({ pct: 0.994, low: true });
	});
	it('is null without a universe', () => {
		expect(coverage(null, null)).toBeNull();
		expect(coverage(5, 0)).toBeNull();
	});
});
```

`src/routes/(app)/admin/pipeline/page.server.test.ts`:

```ts
import { beforeEach, describe, expect, it, vi } from 'vitest';

// vi.mock is hoisted above every other statement, so the mocks it closes over must be too.
const { getPipelineStatus, getCatalogPointer } = vi.hoisted(() => ({
	getPipelineStatus: vi.fn(),
	getCatalogPointer: vi.fn()
}));

vi.mock('$lib/server/warehouse', () => ({
	warehouseClient: () => ({ getPipelineStatus })
}));
vi.mock('$lib/server/catalog/gcs', () => ({ getCatalogPointer }));

const { load } = await import('./+page.server');

const ADMIN = { email: 'phil.henrickson@gmail.com' };
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const run = (user: unknown) => (load as any)({ locals: { user } });

describe('/admin/pipeline load', () => {
	beforeEach(() => {
		getPipelineStatus.mockReset().mockResolvedValue({ verdict: { status: 'ok' } });
		getCatalogPointer
			.mockReset()
			.mockResolvedValue({ name: 'c', hash: 'h', builtAt: '2026-10-02T07:31:00Z', rows: 39316, bytes: 1 });
	});

	it('404s for a non-admin', async () => {
		await expect(run({ email: 'someone@example.com' })).rejects.toMatchObject({ status: 404 });
		expect(getPipelineStatus).not.toHaveBeenCalled();
	});

	it('loads status and the catalog pointer for the admin', async () => {
		const data = await run(ADMIN);
		expect(getPipelineStatus).toHaveBeenCalledWith(14);
		expect(data).toEqual({
			status: { verdict: { status: 'ok' } },
			error: null,
			catalog: { builtAt: '2026-10-02T07:31:00Z', rows: 39316 }
		});
	});

	it('returns the error message when the warehouse call fails', async () => {
		getPipelineStatus.mockRejectedValue(new Error('warehouse GET /monitoring/pipeline failed (503): GH_TOKEN is not set'));
		const data = await run(ADMIN);
		expect(data.status).toBeNull();
		expect(data.error).toContain('GH_TOKEN is not set');
	});

	it('degrades to no catalog row when the pointer cannot be read', async () => {
		getCatalogPointer.mockRejectedValue(new Error('no bucket'));
		const data = await run(ADMIN);
		expect(data.catalog).toBeNull();
		expect(data.status).not.toBeNull();
	});
});
```

- [ ] **Step 2: Run to verify they fail**

Run: `pnpm vitest run src/lib/monitoring/display.test.ts "src/routes/(app)/admin/pipeline/page.server.test.ts"`
Expected: FAIL. Neither `./display` nor `./+page.server` resolves.

- [ ] **Step 3: Implement**

`src/lib/monitoring/display.ts`:

```ts
/**
 * Display rules for the admin pipeline monitor. The backend returns raw statuses,
 * timestamps and counts; how they read (words, glyphs, freshness, coverage cut-offs)
 * is decided here so it can be retuned without touching the data path.
 *
 * Colour: no green/red. `ok` is blue, `warn` amber, `fail` violet (see --status-* in
 * app.css), and every status also carries a glyph and a word.
 */
import type { StageStatusName } from '$lib/server/warehouse';

export type Tone = 'ok' | 'warn' | 'fail' | 'idle';

export const STATUS_TONE: Record<StageStatusName, Tone> = {
	ok: 'ok',
	warn: 'warn',
	fail: 'fail',
	running: 'idle',
	pending: 'idle',
	not_reached: 'idle'
};

export const STATUS_WORD: Record<StageStatusName, string> = {
	ok: 'Succeeded',
	warn: 'Needs a look',
	fail: 'Failed',
	running: 'Running',
	pending: 'Waiting',
	not_reached: 'Not reached'
};

export const STATUS_GLYPH: Record<StageStatusName, string> = {
	ok: '✓',
	warn: '!',
	fail: '✕',
	running: '◐',
	pending: '',
	not_reached: ''
};

const MINUTE = 60_000;
const DAY = 24 * 60 * MINUTE;

function hm(mins: number): string {
	if (mins < 60) return `${mins}m`;
	return `${Math.floor(mins / 60)}h ${String(mins % 60).padStart(2, '0')}m`;
}

/** "4m", "1h 05m" between two timestamps. */
export function duration(fromIso: string | null, toIso: string | null): string {
	if (!fromIso || !toIso) return '';
	return hm(Math.max(0, Math.round((Date.parse(toIso) - Date.parse(fromIso)) / MINUTE)));
}

/** "4h 52m" since a timestamp. */
export function elapsed(fromIso: string | null, now: Date): string {
	return fromIso ? duration(fromIso, now.toISOString()) : '';
}

/** "07:18" — UTC, the pipeline's own clock. */
export function clock(iso: string | null): string {
	return iso ? new Date(iso).toISOString().slice(11, 16) : '—';
}

export interface Freshness {
	tone: 'ok' | 'warn';
	label: string;
}

/** Fresh if the table moved after `reference`; otherwise how many days behind it is. */
export function freshness(lastUpdated: string | null, reference: string): Freshness {
	if (!lastUpdated) return { tone: 'warn', label: 'Never updated' };
	const behind = Date.parse(reference) - Date.parse(lastUpdated);
	if (behind <= 0) return { tone: 'ok', label: 'Fresh' };
	const days = Math.max(1, Math.ceil(behind / DAY));
	return { tone: 'warn', label: days === 1 ? 'A day old' : `${days} days old` };
}

/** Today's stage-1 start, or 05:00 UTC on the chain day if stage 1 hasn't run yet. */
export function freshnessReference(today: {
	day: string;
	stages: { started: string | null }[];
}): string {
	return today.stages[0]?.started ?? `${today.day}T05:00:00Z`;
}

export const COVERAGE_FLOOR = 0.995;

export interface Coverage {
	pct: number;
	low: boolean;
}

export function coverage(covered: number | null, universe: number | null): Coverage | null {
	if (covered == null || !universe) return null;
	const pct = covered / universe;
	return { pct, low: pct < COVERAGE_FLOOR };
}
```

`src/routes/(app)/admin/pipeline/+page.server.ts`:

```ts
import { error } from '@sveltejs/kit';
import { isAdmin } from '$lib/server/auth/admin';
import { getCatalogPointer } from '$lib/server/catalog/gcs';
import { warehouseClient, type PipelineStatus } from '$lib/server/warehouse';
import type { PageServerLoad } from './$types';

/**
 * Admin-only pipeline monitor. A non-admin gets a 404, not a 403: the page doesn't
 * advertise that it exists. A warehouse failure renders as a message on the page
 * (missing GH_TOKEN, GitHub/BigQuery down), never as a partial status.
 */
export const load: PageServerLoad = async ({ locals }) => {
	if (!isAdmin(locals.user)) error(404, 'Not found');

	const [result, catalog] = await Promise.all([
		warehouseClient()
			.getPipelineStatus(14)
			.then(
				(status: PipelineStatus) => ({ status, error: null }),
				(e: unknown) => ({ status: null, error: e instanceof Error ? e.message : String(e) })
			),
		getCatalogPointer().then(
			(p) => ({ builtAt: p.builtAt, rows: p.rows }),
			() => null
		)
	]);
	return { ...result, catalog };
};
```

In `src/app.css`:

- Right after `--color-negative: oklch(0.60 0.17 25);` (in `:root`), add:

```css
  /*
   * Pipeline monitor status. Deliberately NOT green/red: blue = succeeded, amber = needs
   * a look, violet = failed. Always paired with a glyph and a word (StatusBadge.svelte).
   */
  --status-ok: oklch(0.62 0.14 250);
  --status-warn: oklch(0.72 0.14 75);
  --status-fail: oklch(0.55 0.16 300);
```

- Inside the `.dark { … }` block (starts line 198), add:

```css
  --status-ok: oklch(0.70 0.13 250);
  --status-warn: oklch(0.78 0.13 75);
  --status-fail: oklch(0.68 0.15 300);
```

- [ ] **Step 4: Run to verify they pass**

Run: `pnpm vitest run src/lib/monitoring/display.test.ts "src/routes/(app)/admin/pipeline/page.server.test.ts"`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/lib/monitoring/display.ts src/lib/monitoring/display.test.ts src/app.css "src/routes/(app)/admin/pipeline"
git commit -m "feat(admin): pipeline monitor route, display rules, status tokens

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 10: Components and the page

Six components in `src/lib/monitoring/` and the page that composes them. Follow `docs/design/pipeline-monitor-mockup.html` for layout and spacing. Everything that has logic lives in `display.ts` (already tested), so these are presentation only. Verify them with `pnpm check` and a look in the browser.

**Files:**
- Create: `src/lib/monitoring/StatusBadge.svelte`
- Create: `src/lib/monitoring/Verdict.svelte`
- Create: `src/lib/monitoring/ChainLanes.svelte`
- Create: `src/lib/monitoring/HistoryGrid.svelte`
- Create: `src/lib/monitoring/TableStatus.svelte`
- Create: `src/lib/monitoring/DeployedModels.svelte`
- Create: `src/routes/(app)/admin/pipeline/+page.svelte`

**Interfaces:**
- Consumes: the types from Task 8, `display.ts` from Task 9, and the page data `{ status, error, catalog }`.

- [ ] **Step 1: `StatusBadge.svelte`**

```svelte
<script lang="ts">
  import type { StageStatusName } from '$lib/server/warehouse';
  import { STATUS_GLYPH, STATUS_TONE, STATUS_WORD } from './display';

  let { status, label }: { status: StageStatusName; label?: string } = $props();
</script>

<span class="st {STATUS_TONE[status]}" class:dashed={status === 'pending' || status === 'not_reached'}>
  <i aria-hidden="true">{STATUS_GLYPH[status]}</i>{label ?? STATUS_WORD[status]}
</span>

<style>
  .st { display: inline-flex; align-items: center; gap: 0.35rem; font-size: 0.78rem; font-weight: 600; white-space: nowrap; }
  .st i { font-style: normal; width: 1.1rem; height: 1.1rem; border-radius: 50%; display: inline-grid; place-items: center; font-size: 0.7rem; color: var(--card); }
  .ok { color: var(--status-ok); } .ok i { background: var(--status-ok); }
  .warn { color: color-mix(in oklch, var(--status-warn) 75%, var(--foreground)); }
  .warn i { background: var(--status-warn); color: oklch(0.25 0.02 260); }
  .fail { color: var(--status-fail); } .fail i { background: var(--status-fail); }
  .idle { color: var(--muted-foreground); }
  .idle i { background: transparent; border: 1.5px solid var(--muted-foreground); color: var(--muted-foreground); }
  .dashed i { border-style: dashed; }
</style>
```

- [ ] **Step 2: `Verdict.svelte`**

```svelte
<script lang="ts">
  import type { PipelineVerdict } from '$lib/server/warehouse';
  import { clock, elapsed } from './display';

  let { verdict, now }: { verdict: PipelineVerdict; now: Date } = $props();

  const GLYPH = { ok: '✓', running: '◐', warn: '!', fail: '✕' } as const;
  const meta = $derived.by(() => {
    if (verdict.status === 'ok')
      return { label: 'Finished', value: `${clock(verdict.since)} UTC`, sub: `${verdict.duration_minutes}m end to end` };
    if (verdict.status === 'running') return { label: 'Started', value: `${clock(verdict.since)} UTC`, sub: '' };
    return { label: 'Stuck for', value: elapsed(verdict.since, now) || '—', sub: verdict.since ? `since ${clock(verdict.since)} UTC` : '' };
  });
</script>

<section class="verdict {verdict.status}" aria-live="polite">
  <div class="big" aria-hidden="true">{GLYPH[verdict.status]}</div>
  <div><h2>{verdict.headline}</h2></div>
  <div class="meta">{meta.label}<b class="tnum">{meta.value}</b>{meta.sub}</div>
</section>

<style>
  .verdict { display: grid; grid-template-columns: auto 1fr auto; gap: var(--space-lg); align-items: center; padding: var(--space-lg);
    background: var(--card); border: 1px solid var(--border); border-left: 4px solid var(--vc); border-radius: var(--radius); }
  .ok { --vc: var(--status-ok); } .warn { --vc: var(--status-warn); } .fail { --vc: var(--status-fail); } .running { --vc: var(--muted-foreground); }
  .big { width: 2.6rem; height: 2.6rem; border-radius: 50%; display: grid; place-items: center; font-weight: 700; font-size: 1.2rem; background: var(--vc); color: var(--card); }
  h2 { margin: 0; font-size: 1.15rem; letter-spacing: -0.01em; }
  .meta { text-align: right; font-size: 0.8rem; color: var(--muted-foreground); }
  .meta b { display: block; color: var(--foreground); font-size: 0.95rem; font-weight: 600; }
  .tnum { font-variant-numeric: tabular-nums; }
  @media (max-width: 640px) { .verdict { grid-template-columns: auto 1fr; } .meta { grid-column: 1 / -1; text-align: left; } }
</style>
```

- [ ] **Step 3: `ChainLanes.svelte`**

```svelte
<script lang="ts">
  import type { Lane, PipelineStage } from '$lib/server/warehouse';
  import StatusBadge from './StatusBadge.svelte';
  import { clock, duration } from './display';

  let { stages, offChain }: { stages: PipelineStage[]; offChain: PipelineStage[] } = $props();

  const LANES: { key: Lane; label: string; repo: string }[] = [
    { key: 'warehouse', label: 'Warehouse', repo: 'bgg-data-warehouse' },
    { key: 'models', label: 'Models', repo: 'bgg-predictive-models' },
    { key: 'viewer', label: 'Viewer', repo: 'bgg-viewer' }
  ];
</script>

{#snippet card(s: PipelineStage)}
  <div class="step {s.status}">
    <div class="n">
      <span>{s.label}</span>
      {#if s.url}<a href={s.url} target="_blank" rel="noreferrer">run ↗</a>{/if}
    </div>
    <div class="d">
      <span class="lane-tag">{s.lane} ·</span>
      <StatusBadge status={s.status} />
      {#if s.finished}<span class="tnum">{duration(s.started, s.finished)}</span>{/if}
    </div>
    {#if s.note}<div class="note">{s.note}</div>{/if}
  </div>
{/snippet}

<div class="card">
  <div class="lanes">
    <div class="lh">Start</div>
    {#each LANES as l (l.key)}<div class="lh lane-h">{l.label}<small>{l.repo}</small></div>{/each}
    {#each stages as s (s.key)}
      <div class="t tnum">{clock(s.started)}</div>
      {#each LANES as l (l.key)}
        <div class="cell" class:empty={l.key !== s.lane}>
          {#if l.key === s.lane}{@render card(s)}{/if}
        </div>
      {/each}
    {/each}
  </div>
  <div class="offchain">
    <span class="oh">Off-chain (cron)</span>
    {#each offChain as s (s.key)}{@render card(s)}{/each}
  </div>
</div>

<style>
  .card { background: var(--card); border: 1px solid var(--border); border-radius: var(--radius); overflow: hidden; }
  .lanes { display: grid; grid-template-columns: 4.5rem repeat(3, minmax(0, 1fr)); }
  .lh { font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.1em; color: var(--muted-foreground); font-weight: 600; padding: 0.6rem 0.75rem; border-bottom: 1px solid var(--border); }
  .lh small { display: block; text-transform: none; letter-spacing: 0; font-weight: 400; }
  .t { font-size: 0.75rem; color: var(--muted-foreground); padding: 0.55rem 0.75rem; border-top: 1px dashed var(--border); }
  .cell { padding: 0.4rem 0.5rem; border-top: 1px dashed var(--border); border-left: 1px solid var(--border); min-height: 3.2rem; }
  .cell.empty { background: repeating-linear-gradient(135deg, transparent 0 6px, color-mix(in oklch, var(--muted) 60%, transparent) 6px 7px); }
  .step { border: 1px solid var(--border); border-radius: 8px; padding: 0.4rem 0.55rem; background: var(--background); min-width: 0; }
  .step.ok { border-left: 3px solid var(--status-ok); }
  .step.warn { border-left: 3px solid var(--status-warn); background: color-mix(in oklch, var(--status-warn) 8%, var(--background)); }
  .step.fail { border-left: 3px solid var(--status-fail); }
  .step.running { border-left: 3px solid var(--muted-foreground); }
  .step.pending, .step.not_reached { border-style: dashed; opacity: 0.7; }
  .n { font-size: 0.85rem; font-weight: 600; display: flex; justify-content: space-between; gap: 0.5rem; }
  .n a { color: var(--muted-foreground); font-weight: 400; font-size: 0.72rem; text-decoration: none; }
  .d { font-size: 0.75rem; color: var(--muted-foreground); display: flex; gap: 0.5rem; flex-wrap: wrap; align-items: center; margin-top: 0.1rem; }
  .note { font-size: 0.75rem; margin-top: 0.3rem; }
  .lane-tag { display: none; font-size: 0.68rem; text-transform: uppercase; letter-spacing: 0.08em; }
  .offchain { padding: 0.6rem 0.75rem; border-top: 1px solid var(--border); display: flex; gap: var(--space-md); flex-wrap: wrap; background: var(--muted); }
  .offchain .step { flex: 1 1 14rem; }
  .oh { font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.1em; color: var(--muted-foreground); font-weight: 600; align-self: center; }
  .tnum { font-variant-numeric: tabular-nums; }
  @media (max-width: 760px) {
    .lanes { grid-template-columns: 3.6rem minmax(0, 1fr); }
    .lane-h { display: none; }
    .lh:first-child { grid-column: 1 / -1; }
    .cell.empty { display: none; }
    .lane-tag { display: inline; }
  }
</style>
```

- [ ] **Step 4: `HistoryGrid.svelte`**

```svelte
<script lang="ts">
  import type { PipelineStage, PipelineStatus } from '$lib/server/warehouse';
  import StatusBadge from './StatusBadge.svelte';
  import { STATUS_GLYPH, STATUS_WORD } from './display';

  let { history, stages }: { history: PipelineStatus['history']; stages: PipelineStage[] } = $props();
  const dayLabel = (d: string, i: number) => (i === history.length - 1 ? 'today' : String(Number(d.slice(8, 10))));
</script>

<div class="card">
  <div class="legend">
    <StatusBadge status="ok" /><StatusBadge status="warn" /><StatusBadge status="fail" /><StatusBadge status="not_reached" />
  </div>
  <div class="scroll">
    <table>
      <thead><tr><th></th>{#each history as h, i (h.day)}<th class="tnum">{dayLabel(h.day, i)}</th>{/each}</tr></thead>
      <tbody>
        {#each stages as s (s.key)}
          <tr>
            <th scope="row">{s.label}</th>
            {#each history as h (h.day)}
              {@const st = h.stages[s.key] ?? 'not_reached'}
              <td class={st} title="{s.label} · {h.day} · {STATUS_WORD[st]}">{STATUS_GLYPH[st]}</td>
            {/each}
          </tr>
        {/each}
      </tbody>
    </table>
  </div>
</div>

<style>
  .card { background: var(--card); border: 1px solid var(--border); border-radius: var(--radius); }
  .legend { display: flex; gap: var(--space-lg); flex-wrap: wrap; padding: 0.6rem 0.75rem 0; }
  .scroll { overflow-x: auto; padding: 0.4rem 0.5rem 0.75rem; }
  table { border-collapse: separate; border-spacing: 3px; font-size: 0.75rem; min-width: 34rem; width: 100%; }
  th { font-weight: 500; color: var(--muted-foreground); text-align: left; white-space: nowrap; padding-right: 0.6rem; }
  thead th { text-align: center; font-size: 0.68rem; padding: 0; }
  td { height: 1.15rem; border-radius: 3px; text-align: center; font-size: 0.62rem; font-weight: 700; color: var(--card); }
  /* Healthy recedes; problems carry the ink. */
  td.ok { background: color-mix(in oklch, var(--status-ok) 28%, var(--card)); color: color-mix(in oklch, var(--status-ok) 70%, var(--card)); }
  td.warn { background: var(--status-warn); color: oklch(0.25 0.02 260); }
  td.fail { background: var(--status-fail); }
  td.running, td.pending, td.not_reached { background: transparent; box-shadow: inset 0 0 0 1.5px var(--border); }
  .tnum { font-variant-numeric: tabular-nums; }
</style>
```

- [ ] **Step 5: `TableStatus.svelte`**

```svelte
<script lang="ts">
  import type { PipelineTableRow } from '$lib/server/warehouse';
  import StatusBadge from './StatusBadge.svelte';
  import { clock, coverage, freshness } from './display';

  let {
    tables,
    catalog,
    reference
  }: { tables: PipelineTableRow[]; catalog: { builtAt: string; rows: number } | null; reference: string } = $props();

  const fmt = new Intl.NumberFormat('en-US');
  const day = (iso: string | null) =>
    iso ? new Date(iso).toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' }) : '—';
  const group = (t: string) => (t.startsWith('predictions.') ? 'Predictions' : 'Warehouse');
</script>

{#snippet fresh(last: string | null)}
  {@const f = freshness(last, reference)}
  <StatusBadge status={f.tone === 'ok' ? 'ok' : 'warn'} label={f.label} />
{/snippet}

<div class="card scroll">
  <table>
    <thead><tr><th>Table</th><th>Status</th><th>Last updated (UTC)</th><th class="num">Games</th><th>Coverage</th></tr></thead>
    <tbody>
      {#each tables as t, i (t.table)}
        {#if i === 0 || group(t.table) !== group(tables[i - 1].table)}
          <tr class="group"><td colspan="5">{group(t.table)}</td></tr>
        {/if}
        {@const c = coverage(t.covered, t.universe)}
        <tr>
          <td class="mono">{t.table}{#if t.users != null}<div class="sub">{t.users} users</div>{/if}</td>
          <td>{@render fresh(t.last_updated)}</td>
          <td class="tnum">{day(t.last_updated)} · {clock(t.last_updated)}</td>
          <td class="num">{fmt.format(t.games)}</td>
          <td>
            {#if c}
              <div class="cov" class:low={c.low}>
                <div class="bar"><b style="width: {(c.pct * 100).toFixed(1)}%"></b></div>
                <span class="tnum">{(c.pct * 100).toFixed(1)}%</span>
              </div>
            {:else}<span class="sub">—</span>{/if}
          </td>
        </tr>
      {/each}
      <tr class="group"><td colspan="5">Viewer</td></tr>
      <tr>
        <td class="mono">catalog artifact (GCS)</td>
        {#if catalog}
          <td>{@render fresh(catalog.builtAt)}</td>
          <td class="tnum">{day(catalog.builtAt)} · {clock(catalog.builtAt)}</td>
          <td class="num">{fmt.format(catalog.rows)}</td>
        {:else}
          <td colspan="3" class="sub">Pointer unavailable</td>
        {/if}
        <td><span class="sub">—</span></td>
      </tr>
    </tbody>
  </table>
</div>

<style>
  .card { background: var(--card); border: 1px solid var(--border); border-radius: var(--radius); }
  .scroll { overflow-x: auto; }
  table { width: 100%; border-collapse: collapse; font-size: 0.83rem; }
  th { text-align: left; font-weight: 500; font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.08em; color: var(--muted-foreground); padding: 0.55rem 0.75rem; border-bottom: 1px solid var(--border); white-space: nowrap; }
  td { padding: 0.5rem 0.75rem; border-top: 1px solid var(--border); vertical-align: middle; }
  tr.group td { background: var(--muted); font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.1em; color: var(--muted-foreground); font-weight: 600; padding: 0.3rem 0.75rem; }
  .num { text-align: right; font-variant-numeric: tabular-nums; }
  .mono { font-family: ui-monospace, Menlo, monospace; font-size: 0.78rem; }
  .sub { color: var(--muted-foreground); font-size: 0.75rem; font-family: inherit; }
  .cov { display: flex; align-items: center; gap: 0.5rem; min-width: 9rem; }
  .bar { flex: 1; height: 0.45rem; background: var(--muted); border-radius: 999px; overflow: hidden; }
  .bar b { display: block; height: 100%; background: var(--status-ok); border-radius: 999px; }
  .cov.low .bar b { background: var(--status-warn); }
  .tnum { font-variant-numeric: tabular-nums; }
</style>
```

- [ ] **Step 6: `DeployedModels.svelte`**

```svelte
<script lang="ts">
  import type { DeployedModelRow } from '$lib/server/warehouse';
  import { clock } from './display';

  let { models }: { models: DeployedModelRow[] } = $props();
  const fmt = new Intl.NumberFormat('en-US');
  const day = (iso: string | null) =>
    iso ? new Date(iso).toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' }) : '—';
</script>

<div class="card scroll">
  <table>
    <thead><tr><th>Model</th><th>Version</th><th class="num">Games</th><th>Last scored (UTC)</th></tr></thead>
    <tbody>
      {#each models as m (m.model_type)}
        <tr>
          <td>{m.model_type}<div class="sub mono">{m.model_name ?? '—'}</div></td>
          <td class="mono">{m.model_version ?? '—'}</td>
          <td class="num">{fmt.format(m.games_count)}</td>
          <td class="tnum">{day(m.last_updated)} · {clock(m.last_updated)}</td>
        </tr>
      {/each}
    </tbody>
  </table>
</div>

<style>
  .card { background: var(--card); border: 1px solid var(--border); border-radius: var(--radius); }
  .scroll { overflow-x: auto; }
  table { width: 100%; border-collapse: collapse; font-size: 0.83rem; }
  th { text-align: left; font-weight: 500; font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.08em; color: var(--muted-foreground); padding: 0.55rem 0.75rem; border-bottom: 1px solid var(--border); white-space: nowrap; }
  td { padding: 0.5rem 0.75rem; border-top: 1px solid var(--border); }
  .num { text-align: right; font-variant-numeric: tabular-nums; }
  .mono { font-family: ui-monospace, Menlo, monospace; font-size: 0.78rem; }
  .sub { color: var(--muted-foreground); font-size: 0.75rem; }
  .tnum { font-variant-numeric: tabular-nums; }
</style>
```

- [ ] **Step 7: The page**

`src/routes/(app)/admin/pipeline/+page.svelte`:

```svelte
<script lang="ts">
  import { Container, Stack } from '$lib/components/ui/layout';
  import ChainLanes from '$lib/monitoring/ChainLanes.svelte';
  import DeployedModels from '$lib/monitoring/DeployedModels.svelte';
  import HistoryGrid from '$lib/monitoring/HistoryGrid.svelte';
  import TableStatus from '$lib/monitoring/TableStatus.svelte';
  import Verdict from '$lib/monitoring/Verdict.svelte';
  import { clock, freshnessReference } from '$lib/monitoring/display';
  import type { PageData } from './$types';

  let { data }: { data: PageData } = $props();
  const now = new Date();
</script>

<svelte:head><title>Pipeline · bgg-viewer</title></svelte:head>

<Container>
  <Stack>
    <header class="head">
      <h1>Pipeline</h1>
      {#if data.status}<span>as of {clock(data.status.generated_at)} UTC · refreshes every 5 min</span>{/if}
    </header>

    {#if data.error || !data.status}
      <div class="err" role="alert">
        <b>Couldn't load pipeline status.</b>
        <p>{data.error}</p>
      </div>
    {:else}
      {@const s = data.status}
      <Verdict verdict={s.verdict} {now} />

      <section>
        <h2>Today's chain <span>{s.today.day}</span></h2>
        <ChainLanes stages={s.today.stages} offChain={s.today.off_chain} />
      </section>

      <section>
        <h2>Last {s.history.length} days <span>one cell per stage per day · newest on the right</span></h2>
        <HistoryGrid history={s.history} stages={s.today.stages} />
      </section>

      <section>
        <h2>Data freshness &amp; coverage <span>coverage = games in the table ÷ games it should cover</span></h2>
        <TableStatus tables={s.tables} catalog={data.catalog} reference={freshnessReference(s.today)} />
      </section>

      <section>
        <h2>Deployed models <span>from monitoring.deployed_models</span></h2>
        <DeployedModels models={s.models} />
      </section>
    {/if}
  </Stack>
</Container>

<style>
  .head { display: flex; align-items: baseline; gap: 0.75rem; flex-wrap: wrap; }
  .head h1 { margin: 0; font-size: var(--text-heading); }
  .head span, h2 span { color: var(--muted-foreground); font-size: 0.8rem; font-weight: 400; }
  h2 { font-size: 1.05rem; font-weight: 650; margin: 0 0 var(--space-md); display: flex; gap: 0.6rem; align-items: baseline; flex-wrap: wrap; }
  section { min-width: 0; }
  .err { background: var(--card); border: 1px solid var(--border); border-left: 4px solid var(--status-fail); border-radius: var(--radius); padding: var(--space-lg); }
  .err p { margin: 0.3rem 0 0; color: var(--muted-foreground); font-family: ui-monospace, Menlo, monospace; font-size: 0.82rem; }
</style>
```

- [ ] **Step 8: Type-check and run the unit tests**

Run: `pnpm check && pnpm test`
Expected: `svelte-check found 0 errors` and all vitest suites PASS. Fix any Svelte 5 snippet or `{@const}` placement errors that svelte-check reports before continuing.

- [ ] **Step 9: Commit**

```bash
git add src/lib/monitoring "src/routes/(app)/admin/pipeline/+page.svelte"
git commit -m "feat(admin): pipeline monitor page — verdict, chain, history, tables, models

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 11: Admin nav link, end-to-end check, PR

**Files:**
- Modify: `src/routes/+layout.server.ts`
- Modify: `src/routes/+layout.svelte`. Add the link next to the desktop About link (around line 209) and in the mobile menu next to About (around line 251).

- [ ] **Step 1: Expose `isAdmin` to the root layout**

Replace the body of `src/routes/+layout.server.ts` with:

```ts
import { isAdmin } from '$lib/server/auth/admin';
import type { LayoutServerLoad } from './$types';

// Expose the current user to every page (including /login) so the shell can
// render auth state, and whether to show admin-only nav. Pure read of locals.
export const load: LayoutServerLoad = async ({ locals }) => ({
	user: locals.user,
	isAdmin: isAdmin(locals.user)
});
```

- [ ] **Step 2: Add the nav link**

In `src/routes/+layout.svelte`:
- Near the other `onX` derivations (around line 64), add: `const onPipeline = $derived(path.startsWith('/admin/pipeline'));`
- Right after the desktop `<a href="/about" class:active={onAbout}>About</a>`, add:

```svelte
        {#if data.isAdmin}<a href="/admin/pipeline" class:active={onPipeline}>Pipeline</a>{/if}
```

- Right after the mobile-menu `<a href="/about" role="menuitem" class:on={onAbout}><b>About</b></a>`, add:

```svelte
              {#if data.isAdmin}<a href="/admin/pipeline" role="menuitem" class:on={onPipeline}><b>Pipeline</b></a>{/if}
```

- [ ] **Step 3: Type-check and test**

Run: `pnpm check && pnpm test`
Expected: 0 errors, all PASS.

- [ ] **Step 4: End-to-end, locally (ask Phil before starting any server)**

Phil runs the dev server himself. Ask him to:
1. Start the Part A API: in bgg-data-warehouse on branch `feat/pipeline-monitor`, run `GH_TOKEN=$(gh auth token) uv run python -m services.warehouse_api.main`.
2. In bgg-viewer `.env`, set `WAREHOUSE_API_URL=http://localhost:8080` and `DEV_AUTH_EMAIL=phil.henrickson@gmail.com`.
3. Run `just dev` and open `/admin/pipeline`.

Check with him:
- The verdict, the 12 stages and the off-chain strip match today's Actions runs.
- The history grid shows 14 days.
- The table rows show plausible counts.
- The Pipeline link appears in the nav.
- Setting `DEV_AUTH_EMAIL` to another address gives a 404 at `/admin/pipeline` and hides the link.
- With the API stopped, the page shows "Couldn't load pipeline status." and the error text.

Afterwards, put `WAREHOUSE_API_URL` back to its previous value.

- [ ] **Step 5: Commit, push and open the PR**

```bash
git add src/routes/+layout.server.ts src/routes/+layout.svelte
git commit -m "feat(nav): admin-only Pipeline link

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git push -u origin feat/pipeline-monitor
gh pr create --title "feat(admin): pipeline monitor page" --body "$(cat <<'EOF'
Admin-only /admin/pipeline, per bgg-data-warehouse docs/superpowers/specs/2026-10-02-pipeline-monitor-design.md.
Mockup: docs/design/pipeline-monitor-mockup.html.

Needs the warehouse's GET /monitoring/pipeline (bgg-data-warehouse PR) deployed with
GH_TOKEN before it works in production; until then the page shows the 503 message.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Merging is Phil's call. Don't merge.

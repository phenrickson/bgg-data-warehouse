# Pipeline Monitor v2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `/admin/pipeline` answer "where did today's chain stop", including which step inside ML Pipeline, and "which model is each scoring step using now".

**Architecture:** The warehouse API (`src/monitoring/chain.py`) also fetches the ML Pipeline run's jobs, groups them into steps and returns them nested under the `ml_pipeline` stage. History days before the cutover collapse to one "old chain" cell. `monitoring.deployed_models` is rebuilt in Dataform from each scoring step's latest landing rows. bgg-viewer renders the nested steps, the old-era cells and the new models table.

**Tech Stack:** Python 3.12, FastAPI, pytest; Dataform (core 3.0.0) on BigQuery; SvelteKit 5, TypeScript, vitest.

**Spec:** `docs/superpowers/specs/2026-10-06-pipeline-monitor-v2-design.md` (PR #152, branch `docs/pipeline-monitor-v2`)

## Global Constraints

- `CUTOVER = date(2026, 10, 7)`: the first chain day run by the new daily chain.
- ML Pipeline steps, grouped by the job-name prefix before ` / ` (`notify-warehouse` has none):
  - main: `text-embeddings` → Text embeddings, `complexity` → Complexity, `scoring` → Scoring, `game-embeddings` → Game embeddings, `notify-warehouse` → ml_complete sent;
  - side: `collection-scoring` → Collection scoring, `collection-reports` → Collection reports.
- A side-step failure never fails the chain. Verdict: `{"status": "warn", "headline": "Chain completed · <label> failed"}`.
- Jobs are fetched only for today's ML Pipeline run and for history runs that did not succeed.
- `monitoring.deployed_models` holds one row per step (per `username`, `outcome` for collections), for its latest run within 30 days. Columns: `model_category`, `model_type`, `username`, `model_name`, `model_version`, `last_scored`, `games_scored`, `job_id`.
- The `models` response changes shape in place. No compatibility shape.
- Stale model row: `last_scored` more than 48 hours before `generated_at`.
- Viewer: reuse the existing status tokens, glyphs and components (`StatusBadge`, `--status-*`). No new colours.
- Delivery:
  - Warehouse branch `feat/pipeline-monitor-v2` off `origin/main` → PR.
  - Viewer branch `feat/pipeline-monitor-v2` off `origin/main` → PR, after the warehouse PR merges.
  - Phil merges. Commit and push each task as it passes. Subjects are `type(scope): …`.
- **bgg-viewer checkout:** Phil's checkout is on `feat/recommend-tool`. Before Task 7, ask him how to handle it. Never switch it unasked.
- No production-writing runs. BigQuery dry runs only; ask Phil before anything that would bill over 1 GB.

## Review Focus

- **A re-run of a failed ML step.** Expected: the step shows the re-run's result. Pinned by `test_fetch_jobs_asks_for_latest_attempt` in Task 1 (`filter=latest`).
- **The collection-reports matrix part-way done** (some `render-user` jobs completed, others queued). Expected: the step reads `running`, not `pending` or `ok`. Pinned by `test_partly_done_step_is_running` in Task 2.
- **The cutover day itself.** Expected: `2026-10-07` is new-era, and `2026-10-06` is old. Pinned by `test_cutover_day_is_new_era` in Task 4.
- **A scoring step with no rows in 30 days** (it vanishes from `deployed_models`). Expected: the page still lists it, as "no run in 30 days", rather than silently dropping it. Pinned by `splitModels lists missing game model types` in Task 7.
- **A history day whose ML Pipeline run failed only on collection scoring.** Expected: its jobs are fetched and the cell gets the amber side mark, while the main-line colour stays `ok`. Pinned by `test_failed_history_run_needs_jobs` (Task 4) and `test_side_mark_on_history_cell` (Task 4).

---

## File Structure

**bgg-data-warehouse**
- `src/monitoring/github.py`: `RUN_FIELDS` gains `id`; new `JOB_FIELDS`, `fetch_jobs`.
- `src/monitoring/chain.py`:
  - `MLStep`, `ML_STEPS`, `StepStatus`, `group_steps`;
  - ML stage rules in `build_chain`;
  - side warning in `verdict`;
  - `CUTOVER`, the old-era history and `jobs_needed` in `build_report`.
- `services/warehouse_api/routers/monitoring.py`: fetches the needed jobs and passes them on.
- `definitions/deployed_models.sqlx`: rebuilt.
- `src/warehouse/readers/pipeline.py`: `fetch_deployed_models` selects the new columns.
- Tests:
  - `tests/test_monitoring_github.py`;
  - `tests/test_chain.py` (plus `tests/test_chain_ml_steps.py`);
  - `tests/fixtures/ml_pipeline_jobs_2026-10-06.json`;
  - `tests/test_monitoring_router.py`;
  - `tests/test_pipeline_reader.py`.

**bgg-viewer**
- `src/lib/server/warehouse/types.ts`: `PipelineStep`, `steps`, the history `era` / `side` fields, new `DeployedModelRow`.
- `src/lib/monitoring/display.ts` (+ `display.test.ts`): `historyDay`, `isStale`, `splitModels`, `GAME_MODEL_TYPES`, new `modelKey`.
- `src/lib/monitoring/ChainLanes.svelte`, `HistoryGrid.svelte`, `DeployedModels.svelte`.
- `src/routes/(app)/admin/pipeline/+page.svelte`: the models subtitle and the `generated_at` prop.

---

### Task 1: `fetch_jobs`, and run ids in `fetch_runs`

**Files:**
- Modify: `src/monitoring/github.py`
- Test: `tests/test_monitoring_github.py`

**Interfaces:**
- Produces:
  - `github.RUN_FIELDS` includes `"id"`.
  - `github.JOB_FIELDS = ("name", "status", "conclusion", "started_at", "completed_at", "html_url")`.
  - `github.fetch_jobs(repo: str, run_id: int, token: str, session=requests) -> list[dict[str, Any]]`, whose dicts have exactly `JOB_FIELDS`.

- [ ] **Step 1: Create the branch**

```bash
cd /c/Users/philh/projects/bgg-data-warehouse
git fetch origin && git switch -c feat/pipeline-monitor-v2 origin/main
```

- [ ] **Step 2: Write the failing tests**

In `tests/test_monitoring_github.py`, change the `RUN_FIELDS` assertion in `test_fetch_runs_filters_by_created_range_and_keeps_run_fields` to:

```python
    assert set(github.RUN_FIELDS) == {
        "id", "created_at", "updated_at", "event", "status", "conclusion", "display_title",
        "html_url",
    }
```

and append:

```python
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
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `uv run --extra test python -m pytest tests/test_monitoring_github.py -q`
Expected: 5 FAIL. The `RUN_FIELDS` set lacks `id`, and the four `fetch_jobs` tests fail on `AttributeError: module ... has no attribute 'JOB_FIELDS'` / `'fetch_jobs'`.

- [ ] **Step 4: Implement**

In `src/monitoring/github.py`, change `RUN_FIELDS` to start with `"id",`, and append:

```python
JOB_FIELDS = ("name", "status", "conclusion", "started_at", "completed_at", "html_url")


def fetch_jobs(repo: str, run_id: int, token: str, session=requests) -> list[dict[str, Any]]:
    """Jobs of one run's latest attempt, across all pages (the reports matrix adds a job
    per user)."""
    url = f"https://api.github.com/repos/{repo}/actions/runs/{run_id}/jobs"
    params: dict[str, Any] | None = {"filter": "latest", "per_page": 100}
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
    jobs: list[dict[str, Any]] = []
    while url:
        resp = session.get(url, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        jobs.extend({k: j.get(k) for k in JOB_FIELDS} for j in resp.json()["jobs"])
        url = resp.links.get("next", {}).get("url")
        params = None  # the next link already carries the query string
    return jobs
```

- [ ] **Step 5: Run the tests, and the other users of `fetch_runs`**

Run: `uv run --extra test python -m pytest -q`
Expected: all pass except the known pre-existing `test_refresh_games.py::...::test_config_loading`. `fetch_runs` is also used by Pipeline Status (`pipeline_status.py`). If one of its tests pins the exact run keys, add `"id"` there too and record a ruling.

- [ ] **Step 6: Commit and push**

```bash
git add src/monitoring/github.py tests/test_monitoring_github.py
git commit -m "feat(monitor): fetch a run's jobs; keep run ids"
git push -u origin feat/pipeline-monitor-v2
```

---

### Task 2: ML steps from jobs (`group_steps`)

**Files:**
- Modify: `src/monitoring/chain.py`
- Create: `tests/fixtures/ml_pipeline_jobs_2026-10-06.json`
- Create: `tests/test_chain_ml_steps.py`

**Interfaces:**
- Consumes: job dicts shaped like `github.JOB_FIELDS` (Task 1).
- Produces:
  - `chain.MLStep(key: str, label: str, prefix: str, branch: str)`;
  - `chain.ML_STEPS: list[MLStep]`;
  - `chain.StepStatus(key, label, branch, status, started=None, finished=None, url=None, note=None)`, a dataclass;
  - `chain.group_steps(jobs: list[dict]) -> list[StepStatus]`, in `ML_STEPS` order.

- [ ] **Step 1: Write the fixture**

`tests/fixtures/ml_pipeline_jobs_2026-10-06.json` holds the real 2026-10-06 16:07 ML Pipeline run (run `37493138719`). Job ids are illustrative.

```json
[
  {"name": "text-embeddings / generate-text-embeddings", "status": "completed", "conclusion": "success", "started_at": "2026-10-06T16:07:08Z", "completed_at": "2026-10-06T16:09:03Z", "html_url": "https://github.com/phenrickson/bgg-predictive-models/actions/runs/37493138719/job/1"},
  {"name": "complexity / score-complexity", "status": "completed", "conclusion": "success", "started_at": "2026-10-06T16:09:06Z", "completed_at": "2026-10-06T16:13:31Z", "html_url": "https://github.com/phenrickson/bgg-predictive-models/actions/runs/37493138719/job/2"},
  {"name": "scoring / score-simulations", "status": "completed", "conclusion": "success", "started_at": "2026-10-06T16:13:34Z", "completed_at": "2026-10-06T16:14:10Z", "html_url": "https://github.com/phenrickson/bgg-predictive-models/actions/runs/37493138719/job/3"},
  {"name": "game-embeddings / generate-embeddings", "status": "completed", "conclusion": "success", "started_at": "2026-10-06T16:14:15Z", "completed_at": "2026-10-06T16:16:53Z", "html_url": "https://github.com/phenrickson/bgg-predictive-models/actions/runs/37493138719/job/4"},
  {"name": "game-embeddings / generate-coordinates", "status": "completed", "conclusion": "skipped", "started_at": "2026-10-06T16:16:54Z", "completed_at": "2026-10-06T16:16:54Z", "html_url": "https://github.com/phenrickson/bgg-predictive-models/actions/runs/37493138719/job/5"},
  {"name": "collection-scoring / score-collections", "status": "completed", "conclusion": "success", "started_at": "2026-10-06T16:16:56Z", "completed_at": "2026-10-06T16:20:03Z", "html_url": "https://github.com/phenrickson/bgg-predictive-models/actions/runs/37493138719/job/6"},
  {"name": "notify-warehouse", "status": "completed", "conclusion": "success", "started_at": "2026-10-06T16:20:05Z", "completed_at": "2026-10-06T16:20:11Z", "html_url": "https://github.com/phenrickson/bgg-predictive-models/actions/runs/37493138719/job/7"},
  {"name": "collection-reports / discover", "status": "completed", "conclusion": "success", "started_at": "2026-10-06T16:20:06Z", "completed_at": "2026-10-06T16:20:37Z", "html_url": "https://github.com/phenrickson/bgg-predictive-models/actions/runs/37493138719/job/8"},
  {"name": "collection-reports / render-user (phenrickson)", "status": "completed", "conclusion": "success", "started_at": "2026-10-06T16:20:41Z", "completed_at": "2026-10-06T16:24:16Z", "html_url": "https://github.com/phenrickson/bgg-predictive-models/actions/runs/37493138719/job/9"},
  {"name": "collection-reports / render-user (merry_meeple)", "status": "completed", "conclusion": "success", "started_at": "2026-10-06T16:20:39Z", "completed_at": "2026-10-06T16:28:42Z", "html_url": "https://github.com/phenrickson/bgg-predictive-models/actions/runs/37493138719/job/10"},
  {"name": "collection-reports / deploy", "status": "completed", "conclusion": "success", "started_at": "2026-10-06T16:28:47Z", "completed_at": "2026-10-06T16:30:37Z", "html_url": "https://github.com/phenrickson/bgg-predictive-models/actions/runs/37493138719/job/11"}
]
```

- [ ] **Step 2: Write the failing tests**

`tests/test_chain_ml_steps.py`:

```python
"""ML Pipeline jobs grouped into steps (the real 2026-10-06 run as the fixture)."""

import copy
import json
from pathlib import Path

from src.monitoring import chain

FIXTURE = Path(__file__).parent / "fixtures/ml_pipeline_jobs_2026-10-06.json"


def jobs() -> list[dict]:
    return json.loads(FIXTURE.read_text())


def _set(js: list[dict], name: str, **fields) -> list[dict]:
    js = copy.deepcopy(js)
    next(j for j in js if j["name"] == name).update(fields)
    return js


def _by_key(steps):
    return {s.key: s for s in steps}


def test_steps_in_table_order_with_branches():
    steps = chain.group_steps(jobs())
    assert [(s.key, s.branch) for s in steps] == [
        ("text_embeddings", "main"), ("complexity", "main"), ("scoring", "main"),
        ("game_embeddings", "main"), ("ml_complete", "main"),
        ("collection_scoring", "side"), ("collection_reports", "side"),
    ]


def test_real_run_is_all_ok_with_coordinates_skipped():
    s = _by_key(chain.group_steps(jobs()))
    assert {k: v.status for k, v in s.items()} == dict.fromkeys(s, "ok")
    assert s["game_embeddings"].note == "generate-coordinates skipped"
    assert s["complexity"].started == "2026-10-06T16:09:06Z"
    assert s["complexity"].finished == "2026-10-06T16:13:31Z"
    assert s["collection_reports"].finished == "2026-10-06T16:30:37Z"
    assert s["scoring"].url.endswith("/job/3")


def test_prefix_does_not_match_a_longer_job_name():
    """`scoring` must not swallow `collection-scoring / …`."""
    s = _by_key(chain.group_steps(jobs()))
    assert s["scoring"].started == "2026-10-06T16:13:34Z"


def test_failed_job_fails_its_step():
    js = _set(jobs(), "complexity / score-complexity", conclusion="failure")
    assert _by_key(chain.group_steps(js))["complexity"].status == "fail"


def test_all_skipped_step_is_not_reached():
    js = _set(jobs(), "scoring / score-simulations", conclusion="skipped")
    assert _by_key(chain.group_steps(js))["scoring"].status == "not_reached"


def test_partly_done_step_is_running():
    """The reports matrix with some users rendered and others queued."""
    js = _set(jobs(), "collection-reports / render-user (merry_meeple)",
              status="queued", conclusion=None, started_at=None, completed_at=None)
    js = _set(js, "collection-reports / deploy",
              status="waiting", conclusion=None, started_at=None, completed_at=None)
    step = _by_key(chain.group_steps(js))["collection_reports"]
    assert step.status == "running"
    assert step.finished is None


def test_in_progress_job_is_running():
    js = _set(jobs(), "scoring / score-simulations", status="in_progress", conclusion=None,
              completed_at=None)
    assert _by_key(chain.group_steps(js))["scoring"].status == "running"


def test_unstarted_step_is_pending():
    js = [j for j in jobs() if not j["name"].startswith("collection-reports")]
    js = _set(js, "notify-warehouse", status="waiting", conclusion=None, started_at=None,
              completed_at=None)
    s = _by_key(chain.group_steps(js))
    assert s["ml_complete"].status == "pending"
    assert s["collection_reports"].status == "pending"
    assert s["collection_reports"].url is None


def test_step_status_is_json_ready():
    from dataclasses import asdict
    json.dumps([asdict(s) for s in chain.group_steps(jobs())])
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `uv run --extra test python -m pytest tests/test_chain_ml_steps.py -q`
Expected: 9 FAIL with `AttributeError: module 'src.monitoring.chain' has no attribute 'group_steps'`.

- [ ] **Step 4: Implement**

In `src/monitoring/chain.py`, after `STAGE_BY_KEY`, add:

```python
@dataclass(frozen=True)
class MLStep:
    key: str
    label: str
    prefix: str
    branch: str  # "main" gates publish; "side" is visible but never blocks it

    def owns(self, job_name: str) -> bool:
        return job_name == self.prefix or job_name.startswith(f"{self.prefix} / ")


# The ML Pipeline run's jobs, grouped by their caller job (the text before " / ").
ML_STEPS = [
    MLStep("text_embeddings", "Text embeddings", "text-embeddings", "main"),
    MLStep("complexity", "Complexity", "complexity", "main"),
    MLStep("scoring", "Scoring", "scoring", "main"),
    MLStep("game_embeddings", "Game embeddings", "game-embeddings", "main"),
    MLStep("ml_complete", "ml_complete sent", "notify-warehouse", "main"),
    MLStep("collection_scoring", "Collection scoring", "collection-scoring", "side"),
    MLStep("collection_reports", "Collection reports", "collection-reports", "side"),
]

_FAILED = ("failure", "cancelled", "timed_out")
_NOT_STARTED = ("queued", "waiting", "pending", "requested")
```

After the `StageStatus` dataclass, add:

```python
@dataclass
class StepStatus:
    key: str
    label: str
    branch: str
    status: str
    started: str | None = None
    finished: str | None = None
    url: str | None = None
    note: str | None = None


def _step_status(js: list[dict[str, Any]]) -> tuple[str, str | None]:
    """Rules top to bottom, first match wins (spec: Status rules → ML Pipeline steps)."""
    if not js:
        return "pending", None
    if any(j.get("conclusion") in _FAILED for j in js):
        return "fail", None
    if any(j.get("status") == "in_progress" for j in js):
        return "running", None
    if all(j.get("status") in _NOT_STARTED for j in js):
        return "pending", None
    if any(j.get("status") != "completed" for j in js):
        return "running", None
    if all(j.get("conclusion") == "skipped" for j in js):
        return "not_reached", None
    skipped = [j["name"].split(" / ", 1)[-1] for j in js if j.get("conclusion") == "skipped"]
    return "ok", ", ".join(f"{n} skipped" for n in skipped) or None


def group_steps(jobs: list[dict[str, Any]]) -> list[StepStatus]:
    """The ML Pipeline run's jobs as steps, in ``ML_STEPS`` order."""
    out = []
    for step in ML_STEPS:
        js = [j for j in jobs if step.owns(j["name"])]
        status, note = _step_status(js)
        starts = [j["started_at"] for j in js if j.get("started_at")]
        done = js and all(j.get("status") == "completed" for j in js)
        out.append(StepStatus(
            step.key, step.label, step.branch, status,
            started=min(starts, key=parse_ts) if starts else None,
            finished=max((j["completed_at"] for j in js), key=parse_ts) if done else None,
            url=js[0].get("html_url") if js else None,
            note=note,
        ))
    return out
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run --extra test python -m pytest tests/test_chain_ml_steps.py tests/test_chain.py -q`
Expected: all pass. `test_chain.py` is unaffected.

- [ ] **Step 6: Commit and push**

```bash
git add src/monitoring/chain.py tests/test_chain_ml_steps.py tests/fixtures/ml_pipeline_jobs_2026-10-06.json
git commit -m "feat(monitor): group ML Pipeline jobs into steps"
git push
```

---

### Task 3: ML Pipeline stage status and the side-branch verdict

**Files:**
- Modify: `src/monitoring/chain.py` (`StageStatus`, `build_chain`, `verdict`)
- Test: `tests/test_chain_ml_steps.py`

**Interfaces:**
- Consumes: `group_steps`, `StepStatus` (Task 2); runs carrying `id` (Task 1).
- Produces:
  - `chain.Jobs = dict[int, list[dict[str, Any]]]`, mapping a run id to its jobs;
  - `chain.build_chain(runs, day, now, jobs: Jobs | None = None) -> Chain`;
  - `StageStatus.steps: list[StepStatus] | None`, set only on `ml_pipeline` when its jobs were given;
  - `verdict` returns the side warning.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_chain_ml_steps.py`:

```python
from datetime import UTC, date, datetime

DAY = date(2026, 10, 2)
NOON = datetime(2026, 10, 2, 12, 0, tzinfo=UTC)
ML_ID = 4242


def _runs():
    """The 2026-10-02 chain fixture (shared with test_chain.py), with an id on its ML
    Pipeline run."""
    raw = json.loads((Path(__file__).parent / "fixtures/chain_runs_daily_2026-10-02.json").read_text())
    runs = {tuple(k.rsplit("/", 1)): v for k, v in raw.items()}
    ml = chain.STAGE_BY_KEY["ml_pipeline"]
    for r in runs[ml.source]:
        r["id"] = ML_ID
    return runs


def _stage(c, key):
    return next(s for s in c.stages if s.key == key)


def test_ml_stage_carries_steps_when_jobs_given():
    c = chain.build_chain(_runs(), DAY, NOON, jobs={ML_ID: jobs()})
    ml = _stage(c, "ml_pipeline")
    assert ml.status == "ok"
    assert [s.key for s in ml.steps][:2] == ["text_embeddings", "complexity"]


def test_ml_stage_without_jobs_keeps_run_rule():
    c = chain.build_chain(_runs(), DAY, NOON)
    assert _stage(c, "ml_pipeline").steps is None
    assert _stage(c, "ml_pipeline").status == "ok"


def test_main_line_failure_names_the_step():
    runs = _runs()
    ml = chain.STAGE_BY_KEY["ml_pipeline"]
    runs[ml.source][0]["conclusion"] = "failure"
    js = _set(jobs(), "complexity / score-complexity", conclusion="failure")
    for name in ("scoring / score-simulations", "game-embeddings / generate-embeddings",
                 "game-embeddings / generate-coordinates", "notify-warehouse"):
        js = _set(js, name, conclusion="skipped")
    c = chain.build_chain(runs, DAY, NOON, jobs={ML_ID: js})
    assert _stage(c, "ml_pipeline").status == "fail"
    assert _stage(c, "ml_pipeline").note == "failed at Complexity"
    assert chain.verdict(c)["headline"] == "ML Pipeline failed at Complexity"


def test_collection_failure_is_a_side_warning_not_a_chain_failure():
    runs = _runs()
    ml = chain.STAGE_BY_KEY["ml_pipeline"]
    runs[ml.source][0]["conclusion"] = "failure"  # GitHub fails the whole run
    js = _set(jobs(), "collection-scoring / score-collections", conclusion="failure")
    c = chain.build_chain(runs, DAY, NOON, jobs={ML_ID: js})
    assert _stage(c, "ml_pipeline").status == "ok"
    assert _stage(c, "dataform_publish").status == "ok"
    v = chain.verdict(c)
    assert v["status"] == "warn"
    assert v["stage"] == "collection_scoring"
    assert v["headline"] == "Chain completed · Collection scoring failed"
    assert v["duration_minutes"] is not None


def test_completed_run_without_ml_complete_is_fail():
    runs = _runs()
    js = [j for j in jobs() if j["name"] != "notify-warehouse"]
    c = chain.build_chain(runs, DAY, NOON, jobs={ML_ID: js})
    assert _stage(c, "ml_pipeline").status == "fail"
    assert _stage(c, "ml_pipeline").note == "ml_complete not sent"


def test_running_ml_pipeline_is_running():
    runs = _runs()
    ml = chain.STAGE_BY_KEY["ml_pipeline"]
    runs[ml.source][0].update(status="in_progress", conclusion=None)
    js = _set(jobs(), "notify-warehouse", status="waiting", conclusion=None,
              started_at=None, completed_at=None)
    c = chain.build_chain(runs, DAY, NOON, jobs={ML_ID: js})
    assert _stage(c, "ml_pipeline").status == "running"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --extra test python -m pytest tests/test_chain_ml_steps.py -q`
Expected: the 6 new tests FAIL (`TypeError: build_chain() got an unexpected keyword argument 'jobs'`). The 9 from Task 2 pass.

- [ ] **Step 3: Implement**

In `src/monitoring/chain.py`:

1. Add `Jobs = dict[int, list[dict[str, Any]]]` next to `Runs`.
2. Add `steps: list[StepStatus] | None = None` as the last field of `StageStatus`. `StepStatus` must be defined above `StageStatus`, so move the `StepStatus` class and `_step_status` / `group_steps` above it.
3. Add:

```python
def _ml_status(run: dict[str, Any], steps: list[StepStatus]) -> tuple[str, str | None]:
    """ML Pipeline from its main-line steps; the run's own conclusion is ignored, since a
    collection failure fails the run without blocking publish."""
    main = [s for s in steps if s.branch == "main"]
    failed = next((s for s in main if s.status == "fail"), None)
    if failed:
        return "fail", f"failed at {failed.label}"
    if next(s for s in main if s.key == "ml_complete").status == "ok":
        return "ok", None
    if run.get("status") == "completed":
        return "fail", "ml_complete not sent"
    return "running", None
```

4. Change `build_chain`'s signature to `def build_chain(runs: Runs, day: date, now: datetime, jobs: Jobs | None = None) -> Chain:`. In its loop, replace `status = _own_status(run)` with:

```python
            steps = None
            if stage.key == "ml_pipeline" and jobs and run.get("id") in jobs:
                steps = group_steps(jobs[run["id"]])
                status, note = _ml_status(run, steps)
            else:
                status = _own_status(run)
```

Then replace `stages.append(_status(stage, run, status, note))` with:

```python
        stage_status = _status(stage, run, status, note)
        stage_status.steps = steps if run is not None else None
        stages.append(stage_status)
```

Initialise `steps = None` at the top of each loop iteration, so the `run is None` branch has it.

5. In `verdict`, replace the `if bad is None:` block with:

```python
    if bad is None:
        first, last = c.stages[0], c.stages[-1]
        ml = next((s for s in c.stages if s.key == "ml_pipeline"), None)
        side = next((s for s in (ml.steps or []) if s.branch == "side" and s.status == "fail"),
                    None) if ml else None
        return {
            "status": "warn" if side else "ok",
            "stage": side.key if side else None,
            "headline": f"Chain completed · {side.label} failed" if side else "Chain completed",
            "since": last.finished,
            "duration_minutes": _minutes(first.started, last.finished),
        }
```

and change the `fail` branch's headline to:

```python
    elif bad.status == "fail":
        status = "fail"
        headline = (f"{bad.label} {bad.note}" if bad.note and bad.note.startswith("failed at ")
                    else f"{bad.label} failed")
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run --extra test python -m pytest tests/test_chain_ml_steps.py tests/test_chain.py -q`
Expected: all pass, including the existing `test_ml_pipeline_failure_stays_visible_when_publish_ran` ("ML Pipeline failed", with no jobs given).

- [ ] **Step 5: Commit and push**

```bash
git add src/monitoring/chain.py tests/test_chain_ml_steps.py
git commit -m "feat(monitor): ML Pipeline status from its main-line steps; collection failures warn"
git push
```

---

### Task 4: Cutover history, the side mark, and `jobs_needed`

**Files:**
- Modify: `src/monitoring/chain.py` (`build_report`, new `CUTOVER`, `OLD_FINAL`, `jobs_needed`)
- Test: `tests/test_chain.py`, `tests/test_chain_ml_steps.py`

**Interfaces:**
- Consumes: `build_chain(..., jobs=)` (Task 3).
- Produces:
  - `chain.CUTOVER: date`;
  - `chain.build_report(runs, days, now, jobs: Jobs | None = None, cutover: date = CUTOVER) -> dict`;
  - `chain.jobs_needed(runs, days, now, cutover: date = CUTOVER) -> list[int]`.
- History entries are either `{"day", "era": "old", "status", "url"}` or `{"day", "era": "new", "stages": {key: {"status", "url"}}}`. The `ml_pipeline` cell also carries `"side": "ok" | "fail" | None`.

- [ ] **Step 1: Write the failing tests**

In `tests/test_chain.py`, change `test_build_report_shape`'s first line to pass a cutover before the fixture day, so it keeps testing the new-era shape:

```python
    report = chain.build_report(_fixture(), 3, NOON, cutover=date(2026, 9, 1))
```

and add, right after its other assertions:

```python
    assert all(h["era"] == "new" for h in report["history"])
```

Append to `tests/test_chain_ml_steps.py`:

```python
def test_days_before_cutover_are_one_old_chain_cell():
    runs = _runs()
    dataform = chain.STAGE_BY_KEY["dataform_core"].source
    runs[dataform].append({
        "id": 7, "created_at": "2026-10-01T07:11:17Z", "updated_at": "2026-10-01T07:12:40Z",
        "event": "repository_dispatch", "status": "completed", "conclusion": "success",
        "display_title": "embeddings_complete", "html_url": "https://github.com/x/old",
    })
    report = chain.build_report(runs, 3, NOON, cutover=date(2026, 10, 2))
    assert report["history"][0] == {"day": "2026-09-30", "era": "old", "status": "not_reached",
                                    "url": None}
    assert report["history"][1] == {"day": "2026-10-01", "era": "old", "status": "ok",
                                    "url": "https://github.com/x/old"}
    assert report["history"][2]["era"] == "new"


def test_cutover_day_is_new_era():
    report = chain.build_report({}, 2, datetime(2026, 10, 7, 12, 0, tzinfo=UTC))
    assert [(h["day"], h["era"]) for h in report["history"]] == [
        ("2026-10-06", "old"), ("2026-10-07", "new"),
    ]


def test_side_mark_on_history_cell():
    js = _set(jobs(), "collection-scoring / score-collections", conclusion="failure")
    report = chain.build_report(_runs(), 1, NOON, jobs={ML_ID: js}, cutover=date(2026, 9, 1))
    cell = report["history"][-1]["stages"]["ml_pipeline"]
    assert cell["status"] == "ok"
    assert cell["side"] == "fail"


def test_side_is_null_without_jobs():
    report = chain.build_report(_runs(), 1, NOON, cutover=date(2026, 9, 1))
    assert report["history"][-1]["stages"]["ml_pipeline"]["side"] is None
    assert "side" not in report["history"][-1]["stages"]["dataform_core"]


def test_jobs_needed_is_todays_run():
    assert chain.jobs_needed(_runs(), 3, NOON, cutover=date(2026, 9, 1)) == [ML_ID]


def test_successful_history_run_needs_no_jobs():
    tomorrow = datetime(2026, 10, 3, 12, 0, tzinfo=UTC)
    assert chain.jobs_needed(_runs(), 3, tomorrow, cutover=date(2026, 9, 1)) == []


def test_failed_history_run_needs_jobs():
    runs = _runs()
    ml = chain.STAGE_BY_KEY["ml_pipeline"]
    runs[ml.source][0]["conclusion"] = "failure"
    tomorrow = datetime(2026, 10, 3, 12, 0, tzinfo=UTC)
    assert chain.jobs_needed(runs, 3, tomorrow, cutover=date(2026, 9, 1)) == [ML_ID]


def test_no_jobs_needed_before_cutover():
    assert chain.jobs_needed(_runs(), 3, NOON, cutover=date(2026, 10, 7)) == []
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --extra test python -m pytest tests/test_chain.py tests/test_chain_ml_steps.py -q`
Expected: `test_build_report_shape` and the 8 new tests FAIL (`TypeError: ... unexpected keyword argument 'cutover'`, `AttributeError: ... 'jobs_needed'`).

- [ ] **Step 3: Implement**

In `src/monitoring/chain.py`, below `HANDOFF`:

```python
# The first chain day run by the new daily chain (core → ML Pipeline → publish). Earlier
# days ran the twelve-stage chain and are shown as one "old chain" cell, not re-judged.
CUTOVER = date(2026, 10, 7)
```

Below `SOURCES`:

```python
# The old chain's last Dataform pass: its result is that day's result.
OLD_FINAL = Stage("old_final", "Old chain", WAREHOUSE, "dataform.yml", "warehouse",
                  title="embeddings_complete")
```

Replace `build_report` with:

```python
def _old_day(runs: Runs, day: date) -> dict[str, Any]:
    run = _latest(OLD_FINAL, runs, day)
    return {"day": day.isoformat(), "era": "old",
            "status": _own_status(run) if run else "not_reached",
            "url": run.get("html_url") if run else None}


def _cell(s: StageStatus) -> dict[str, Any]:
    cell: dict[str, Any] = {"status": s.status, "url": s.url}
    if s.key == "ml_pipeline":
        side = [x for x in (s.steps or []) if x.branch == "side"]
        cell["side"] = ("fail" if any(x.status == "fail" for x in side) else "ok") if side else None
    return cell


def build_report(runs: Runs, days: int, now: datetime, jobs: Jobs | None = None,
                 cutover: date = CUTOVER) -> dict[str, Any]:
    today = chain_day(now)
    chains = [build_chain(runs, today - timedelta(days=d), now, jobs)
              for d in range(days - 1, -1, -1)]
    current = chains[-1]
    return {
        "generated_at": iso(now),
        "verdict": verdict(current),
        "today": current.to_dict(),
        "history": [
            _old_day(runs, c.day) if c.day < cutover else
            {"day": c.day.isoformat(), "era": "new", "stages": {s.key: _cell(s) for s in c.stages}}
            for c in chains
        ],
    }


def jobs_needed(runs: Runs, days: int, now: datetime, cutover: date = CUTOVER) -> list[int]:
    """ML Pipeline runs whose jobs can change the answer: today's, and any history run that
    didn't succeed (a successful run means every step, side branch included, succeeded)."""
    today = chain_day(now)
    ml = STAGE_BY_KEY["ml_pipeline"]
    ids = []
    for d in range(days - 1, -1, -1):
        day = today - timedelta(days=d)
        run = _latest(ml, runs, day) if day >= cutover else None
        if run is None or run.get("id") is None:
            continue
        if day == today or run.get("conclusion") != "success":
            ids.append(run["id"])
    return ids
```

`Chain.to_dict` uses `asdict`, which already serialises the nested `steps`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run --extra test python -m pytest tests/test_chain.py tests/test_chain_ml_steps.py -q`
Expected: all pass.

- [ ] **Step 5: Commit and push**

```bash
git add src/monitoring/chain.py tests/test_chain.py tests/test_chain_ml_steps.py
git commit -m "feat(monitor): old-chain history before the cutover; fetch jobs only where they matter"
git push
```

---

### Task 5: The router fetches the needed jobs

**Files:**
- Modify: `services/warehouse_api/routers/monitoring.py`
- Test: `tests/test_monitoring_router.py`

**Interfaces:**
- Consumes: `github.fetch_jobs` (Task 1); `chain.jobs_needed`, `chain.build_report(..., jobs=)` (Task 4).

- [ ] **Step 1: Write the failing tests**

In `tests/test_monitoring_router.py`, extend the `pipeline_ok` fixture: add `"jobs": []` to `calls`, and register a fake before `return calls`:

```python
    def fake_jobs(repo, run_id, token):
        calls["jobs"].append((repo, run_id))
        return []

    monkeypatch.setattr(monitoring_router, "fetch_jobs", fake_jobs)
```

Append:

```python
def test_pipeline_fetches_jobs_for_the_runs_chain_names(pipeline_ok, monkeypatch):
    monkeypatch.setattr(monitoring_router.chain, "jobs_needed", lambda runs, days, now: [11, 12])
    r = client.get("/monitoring/pipeline?days=3")
    assert r.status_code == 200
    assert sorted(pipeline_ok["jobs"]) == [
        ("phenrickson/bgg-predictive-models", 11), ("phenrickson/bgg-predictive-models", 12),
    ]


def test_pipeline_job_fetch_error_is_502(pipeline_ok, monkeypatch):
    monkeypatch.setattr(monitoring_router.chain, "jobs_needed", lambda runs, days, now: [11])

    def boom(*args, **kwargs):
        raise requests.HTTPError("404 Not Found")

    monkeypatch.setattr(monitoring_router, "fetch_jobs", boom)
    r = client.get("/monitoring/pipeline")
    assert r.status_code == 502
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --extra test python -m pytest tests/test_monitoring_router.py -q`
Expected: the fixture errors with `AttributeError: <module ...monitoring> has no attribute 'fetch_jobs'`, so every pipeline test fails.

- [ ] **Step 3: Implement**

In `services/warehouse_api/routers/monitoring.py`:
- Change the import to `from src.monitoring.github import fetch_jobs, fetch_runs, iso`.
- Add after `_collect_runs`:

```python
def _collect_jobs(run_ids: list[int], token: str) -> chain.Jobs:
    """Jobs of the ML Pipeline runs ``chain.jobs_needed`` names (usually just today's)."""
    if not run_ids:
        return {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {rid: pool.submit(fetch_jobs, chain.MODELS, rid, token) for rid in run_ids}
        return {rid: f.result() for rid, f in futures.items()}
```

In `get_pipeline`, inside the `try`, after `runs = …`:

```python
        jobs = _collect_jobs(chain.jobs_needed(runs, days, now), token)
```

and change the result line to:

```python
    result = chain.build_report(runs, days, now, jobs) | {"tables": tables, "models": models}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run --extra test python -m pytest tests/test_monitoring_router.py -q`
Expected: all pass.

- [ ] **Step 5: Commit and push**

```bash
git add services/warehouse_api/routers/monitoring.py tests/test_monitoring_router.py
git commit -m "feat(api): pipeline report includes ML Pipeline steps"
git push
```

---

### Task 6: `deployed_models` as the model each step used in its latest run

**Files:**
- Modify: `definitions/deployed_models.sqlx` (whole file)
- Modify: `src/warehouse/readers/pipeline.py` (`fetch_deployed_models`)
- Modify: `tests/test_pipeline_reader.py`, and `MODEL_ROW` in `tests/test_monitoring_router.py`

**Interfaces:**
- Produces: rows of `{model_category, model_type, username, model_name, model_version, last_scored, games_scored, job_id}`, ordered by `model_category`, `model_type`, `username`. Task 7 types these.

- [ ] **Step 1: Write the failing test**

In `tests/test_pipeline_reader.py`, replace `test_deployed_models_returns_every_live_version` with:

```python
def test_deployed_models_is_one_row_per_step():
    game = {"model_category": "game", "model_type": "hurdle", "username": None,
            "model_name": "hurdle-v2026", "model_version": "3",
            "last_scored": "2026-10-06T16:14:00Z", "games_scored": 512, "job_id": "j1"}
    coll = {"model_category": "collection", "model_type": "own", "username": "phenrickson",
            "model_name": "collection-own", "model_version": "2",
            "last_scored": "2026-10-06T16:19:00Z", "games_scored": 40000, "job_id": "j2"}
    client = FakeClient([coll, game])
    assert pipeline.fetch_deployed_models(client=client) == [coll, game]
    sql, _ = client.calls[0]
    assert "monitoring.deployed_models" in sql
    for col in ("username", "last_scored", "games_scored", "job_id"):
        assert col in sql
    assert "ORDER BY model_category, model_type, username" in sql
```

In `tests/test_monitoring_router.py`, replace `MODEL_ROW` with:

```python
MODEL_ROW = {"model_category": "game", "model_type": "hurdle", "username": None,
             "model_name": "hurdle-v2026", "model_version": "3",
             "last_scored": "2026-10-06T16:14:00Z", "games_scored": 512, "job_id": "j1"}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run --extra test python -m pytest tests/test_pipeline_reader.py -q`
Expected: `test_deployed_models_is_one_row_per_step` FAILS on `"username" in sql`.

- [ ] **Step 3: Update the reader**

```python
def fetch_deployed_models(client: Optional[bigquery.Client] = None) -> list[dict[str, Any]]:
    """The model each scoring step used in its latest run (one row per step, per user and
    outcome for collections), from ``monitoring.deployed_models``."""
    client = client or get_client()
    sql = f"""
        SELECT model_category, model_type, username, model_name, model_version,
               last_scored, games_scored, job_id
        FROM `{dataset('monitoring')}.deployed_models`
        ORDER BY model_category, model_type, username
    """
    return [dict(r) for r in client.query(sql).result()]
```

Run: `uv run --extra test python -m pytest tests/test_pipeline_reader.py tests/test_monitoring_router.py -q`
Expected: all pass.

- [ ] **Step 4: Confirm the source columns (read-only)**

The spec requires the column names to be checked against the live tables before the SQL is written. Run (metadata only, no query cost):

```bash
for t in ml_predictions_landing complexity_predictions description_embeddings game_embeddings collection_predictions_landing; do
  echo "== $t"; bq show --schema --format=prettyjson bgg-predictive-models:raw.$t | python -c "import json,sys;print([f['name'] for f in json.load(sys.stdin)])"
done
```

Expected: each list includes `job_id` and the columns used below. These are:
- `ml_predictions_landing`: `<type>_model_name` and `<type>_model_version` for hurdle, rating, users_rated and geek_rating, plus `score_ts`;
- `complexity_predictions`: `complexity_model_name`, `complexity_model_version`, `score_ts`;
- both embeddings tables: `embedding_model`, `embedding_version`, `created_ts`;
- `collection_predictions_landing`: `username`, `outcome`, `model_name`, `model_version`, `score_ts`.

If any differs, rule on the smallest change and record it.

- [ ] **Step 5: Rewrite `definitions/deployed_models.sqlx`**

```sql
config {
  type: "table",
  tags: ["publish"],
  schema: "monitoring",
  name: "deployed_models",
  description: "The model each scoring step used in its latest run within 30 days: one row per step, and per user and outcome for collections. Read from the landing tables each service writes, so it shows what ran, not what is configured. A step with no rows in 30 days has no row."
}

-- Each source is pruned to 30 days on its partition column, then cut to its newest job.
WITH preds AS (
  SELECT * FROM ${ref("bgg-predictive-models", "raw", "ml_predictions_landing")}
  WHERE score_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
),
preds_latest AS (
  SELECT * FROM preds
  WHERE job_id = (SELECT job_id FROM preds ORDER BY score_ts DESC LIMIT 1)
),
complexity AS (
  SELECT * FROM ${ref("bgg-predictive-models", "raw", "complexity_predictions")}
  WHERE score_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
),
complexity_latest AS (
  SELECT * FROM complexity
  WHERE job_id = (SELECT job_id FROM complexity ORDER BY score_ts DESC LIMIT 1)
),
text_emb AS (
  SELECT job_id, embedding_model, embedding_version, created_ts
  FROM ${ref("bgg-predictive-models", "raw", "description_embeddings")}
  WHERE created_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
),
text_emb_latest AS (
  SELECT * FROM text_emb
  WHERE job_id = (SELECT job_id FROM text_emb ORDER BY created_ts DESC LIMIT 1)
),
game_emb AS (
  SELECT job_id, embedding_model, embedding_version, created_ts
  FROM ${ref("bgg-predictive-models", "raw", "game_embeddings")}
  WHERE created_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
),
game_emb_latest AS (
  SELECT * FROM game_emb
  WHERE job_id = (SELECT job_id FROM game_emb ORDER BY created_ts DESC LIMIT 1)
),
coll AS (
  SELECT job_id, username, outcome, model_name, model_version, score_ts
  FROM ${ref("bgg-predictive-models", "raw", "collection_predictions_landing")}
  WHERE score_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
),
coll_latest_job AS (
  SELECT username, outcome, job_id FROM coll
  WHERE TRUE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY username, outcome ORDER BY score_ts DESC) = 1
)

SELECT 'game' AS model_category, 'hurdle' AS model_type, CAST(NULL AS STRING) AS username,
       hurdle_model_name AS model_name, CAST(hurdle_model_version AS STRING) AS model_version,
       MAX(score_ts) AS last_scored, COUNT(*) AS games_scored, ANY_VALUE(job_id) AS job_id
FROM preds_latest GROUP BY 4, 5
UNION ALL
SELECT 'game', 'rating', NULL, rating_model_name, CAST(rating_model_version AS STRING),
       MAX(score_ts), COUNT(*), ANY_VALUE(job_id)
FROM preds_latest GROUP BY 4, 5
UNION ALL
SELECT 'game', 'users_rated', NULL, users_rated_model_name,
       CAST(users_rated_model_version AS STRING), MAX(score_ts), COUNT(*), ANY_VALUE(job_id)
FROM preds_latest GROUP BY 4, 5
UNION ALL
SELECT 'game', 'geek_rating', NULL, geek_rating_model_name,
       CAST(geek_rating_model_version AS STRING), MAX(score_ts), COUNT(*), ANY_VALUE(job_id)
FROM preds_latest GROUP BY 4, 5
UNION ALL
SELECT 'game', 'complexity', NULL, complexity_model_name,
       CAST(complexity_model_version AS STRING), MAX(score_ts), COUNT(*), ANY_VALUE(job_id)
FROM complexity_latest GROUP BY 4, 5
UNION ALL
SELECT 'game', 'text_embedding', NULL, embedding_model, CAST(embedding_version AS STRING),
       MAX(created_ts), COUNT(*), ANY_VALUE(job_id)
FROM text_emb_latest GROUP BY 4, 5
UNION ALL
SELECT 'game', 'game_embedding', NULL, embedding_model, CAST(embedding_version AS STRING),
       MAX(created_ts), COUNT(*), ANY_VALUE(job_id)
FROM game_emb_latest GROUP BY 4, 5
UNION ALL
SELECT 'collection', c.outcome, c.username, c.model_name, CAST(c.model_version AS STRING),
       MAX(c.score_ts), COUNT(*), c.job_id
FROM coll c JOIN coll_latest_job USING (username, outcome, job_id)
GROUP BY c.outcome, c.username, c.model_name, c.model_version, c.job_id
```

- [ ] **Step 6: Validate: Dataform compile, then a dry run of the `CREATE TABLE`**

```bash
W=.superpowers/sdd/2026-10-06-pipeline-monitor-v2
npx --yes @dataform/cli@3.0.0 compile --json > $W/compiled.json
python - <<'EOF' > $W/deployed_models.sql
import json
c = json.load(open(".superpowers/sdd/2026-10-06-pipeline-monitor-v2/compiled.json"))
t = next(t for t in c["tables"] if t["target"]["name"] == "deployed_models")
print(f"CREATE OR REPLACE TABLE `bgg-data-warehouse.monitoring.deployed_models_dryrun` AS\n{t['query']}")
EOF
bq query --dry_run --nouse_legacy_sql < $W/deployed_models.sql
```

Expected: the compile has no `compilationErrors`. The dry run prints "Query successfully validated… this query will process N bytes", with N under 1 GB. If N exceeds 1 GB, stop and ask Phil. A dry run writes nothing.

- [ ] **Step 7: Run the suite, commit and push**

Run: `uv run --extra test python -m pytest -q`
Expected: all pass except the known pre-existing `tests/test_refresh_games.py::...::test_config_loading`.

```bash
git add definitions/deployed_models.sqlx src/warehouse/readers/pipeline.py tests/test_pipeline_reader.py tests/test_monitoring_router.py
git commit -m "feat(dataform): deployed_models is the model each scoring step used in its latest run"
git push
```

- [ ] **Step 8: Open the warehouse PR**

Check `gh pr list --head feat/pipeline-monitor-v2 --state all` first. Then:

```bash
gh pr create --base main --title "feat(monitor): pipeline monitor v2 — ML steps, cutover history, models in use" --body-file <scratch>/pr-warehouse.md
```

The body covers:
- that it implements the spec in #152 (warehouse side);
- that merging redeploys the API (the #150 path filter) and starts a full Dataform run, which rebuilds `monitoring.deployed_models`;
- that the live Deployed models panel shows blanks until the viewer PR ships (accepted);
- the test counts, and the dry-run byte estimate.

---

### Task 7: Viewer types and display helpers

Before starting, ask Phil how to handle the bgg-viewer checkout, which is on `feat/recommend-tool`. Record his answer as a ruling. The steps below assume a checkout of `feat/pipeline-monitor-v2` off `origin/main` at `$VIEWER`.

**Files:**
- Modify: `src/lib/server/warehouse/types.ts`
- Modify: `src/lib/monitoring/display.ts`
- Test: `src/lib/monitoring/display.test.ts`

**Interfaces:**
- Consumes: the API shape from Tasks 3, 4 and 6.
- Produces:
  - `PipelineStep`;
  - `PipelineStage.steps?: PipelineStep[] | null`;
  - `HistoryDay`;
  - `HistoryCell.side?: 'ok' | 'fail' | null`;
  - the new `DeployedModelRow`;
  - `historyDay(h) -> { era: 'old'; cell: HistoryCell } | { era: 'new'; stages: Record<string, HistoryCell | StageStatusName> }`;
  - `isStale(lastScored: string | null, generatedAt: string, hours = 48): boolean`;
  - `GAME_MODEL_TYPES`;
  - `splitModels(models) -> { game: (DeployedModelRow | MissingModel)[]; collections: DeployedModelRow[] }`;
  - `modelKey(m)`.

- [ ] **Step 1: Branch**

```bash
cd $VIEWER && git fetch origin && git switch -c feat/pipeline-monitor-v2 origin/main
```

- [ ] **Step 2: Write the failing tests**

In `src/lib/monitoring/display.test.ts`, add `historyDay`, `isStale`, `splitModels` and `GAME_MODEL_TYPES` to the import, delete the existing `modelKey` test block, and append:

```ts
const row = (o: Partial<import('$lib/server/warehouse').DeployedModelRow>) => ({
	model_category: 'game' as const, model_type: 'hurdle', username: null, model_name: 'hurdle-v2026',
	model_version: '3', last_scored: '2026-10-06T16:14:00Z', games_scored: 512, job_id: 'j', ...o
});

describe('historyDay', () => {
	it('reads an old-era day as one cell', () => {
		expect(historyDay({ day: '2026-10-05', era: 'old', status: 'ok', url: 'u' })).toEqual({
			era: 'old', cell: { status: 'ok', url: 'u' }
		});
	});
	it('reads a new-era day, and a day from an API without era, as stages', () => {
		const stages = { ml_pipeline: { status: 'ok' as const, url: null, side: 'fail' as const } };
		expect(historyDay({ day: '2026-10-07', era: 'new', stages })).toEqual({ era: 'new', stages });
		expect(historyDay({ day: '2026-10-02', stages })).toEqual({ era: 'new', stages });
	});
});

describe('isStale', () => {
	it('is stale past 48 hours before generated_at, or with no run', () => {
		expect(isStale('2026-10-06T16:00:00Z', '2026-10-08T15:59:00Z')).toBe(false);
		expect(isStale('2026-10-06T16:00:00Z', '2026-10-08T16:01:00Z')).toBe(true);
		expect(isStale(null, '2026-10-08T00:00:00Z')).toBe(true);
	});
});

describe('splitModels', () => {
	it('splits game and collection rows', () => {
		const c = row({ model_category: 'collection', model_type: 'own', username: 'phenrickson' });
		const { game, collections } = splitModels([row({}), c]);
		expect(collections).toEqual([c]);
		expect(game[0]).toEqual(row({}));
	});
	it('lists missing game model types', () => {
		const { game } = splitModels([row({})]);
		expect(game.map((g) => g.model_type)).toEqual([...GAME_MODEL_TYPES]);
		expect(game.find((g) => g.model_type === 'complexity')).toEqual({ model_type: 'complexity', missing: true });
	});
	it('keeps two rows when one run used two versions', () => {
		const { game } = splitModels([row({}), row({ model_version: '4' })]);
		expect(game.filter((g) => g.model_type === 'hurdle')).toHaveLength(2);
	});
});

describe('modelKey', () => {
	it('separates collection users and outcomes', () => {
		const a = row({ model_category: 'collection', model_type: 'own', username: 'a' });
		const b = row({ model_category: 'collection', model_type: 'own', username: 'b' });
		expect(modelKey(a)).not.toBe(modelKey(b));
	});
});
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `pnpm vitest run src/lib/monitoring/display.test.ts`
Expected: FAIL on imports (`historyDay`, `isStale`, `splitModels`, `GAME_MODEL_TYPES` are not exported).

- [ ] **Step 4: Update the types**

In `src/lib/server/warehouse/types.ts`, replace `PipelineStage`, `DeployedModelRow`, `PipelineStatus` and `HistoryCell` with:

```ts
export interface PipelineStep {
	key: string;
	label: string;
	/** `main` gates publish; `side` (collections) is shown but never blocks it. */
	branch: 'main' | 'side';
	status: StageStatusName;
	started: string | null;
	finished: string | null;
	url: string | null;
	note: string | null;
}

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
	/** ML Pipeline only: its jobs grouped into steps. */
	steps?: PipelineStep[] | null;
}

/** The model a scoring step used in its latest run. Collections: one per user and outcome. */
export interface DeployedModelRow {
	model_category: 'game' | 'collection';
	model_type: string;
	username: string | null;
	model_name: string | null;
	model_version: string | null;
	last_scored: string | null;
	games_scored: number;
	job_id: string | null;
}

/** A history day: one "old chain" cell before the cutover, per-stage cells after. */
export interface HistoryDay {
	day: string;
	era?: 'old' | 'new';
	status?: StageStatusName;
	url?: string | null;
	stages?: Record<string, HistoryCell | StageStatusName>;
}

export interface PipelineStatus {
	generated_at: string;
	verdict: PipelineVerdict;
	today: { day: string; stages: PipelineStage[]; off_chain: PipelineStage[] };
	history: HistoryDay[];
	tables: PipelineTableRow[];
	models: DeployedModelRow[];
}

/** One history cell: its status and the run it came from. Old APIs sent the status alone. */
export interface HistoryCell {
	status: StageStatusName;
	url: string | null;
	/** ML Pipeline: whether a side step (collections) failed that day; null if not fetched. */
	side?: 'ok' | 'fail' | null;
}
```

Check `src/lib/server/warehouse/index.ts` re-exports `PipelineStep` and `HistoryDay`. It re-exports via `export type * from './types'` or an explicit list; if the list is explicit, add both.

- [ ] **Step 5: Update the display helpers**

In `src/lib/monitoring/display.ts`, update the type import to include `DeployedModelRow` and `HistoryDay`, replace `modelKey`, and append:

```ts
/** Stable #each key for a deployed-model row: one per step, per user and outcome. */
export function modelKey(m: {
	model_category: string;
	model_type: string;
	username: string | null;
	model_name: string | null;
	model_version: string | null;
}): string {
	return [m.model_category, m.model_type, m.username, m.model_name, m.model_version].join('|');
}

/** A history day as one old-chain cell (before the cutover) or per-stage cells. */
export function historyDay(
	h: HistoryDay
): { era: 'old'; cell: HistoryCell } | { era: 'new'; stages: Record<string, HistoryCell | StageStatusName> } {
	if (h.era === 'old') return { era: 'old', cell: { status: h.status ?? 'not_reached', url: h.url ?? null } };
	return { era: 'new', stages: h.stages ?? {} };
}

/** A model whose latest run is more than `hours` before the report is not actively scoring. */
export function isStale(lastScored: string | null, generatedAt: string, hours = 48): boolean {
	if (!lastScored) return true;
	return Date.parse(generatedAt) - Date.parse(lastScored) > hours * 60 * MINUTE;
}

/** Every game scoring step, in page order. A missing one has had no run in 30 days. */
export const GAME_MODEL_TYPES = [
	'hurdle', 'rating', 'users_rated', 'geek_rating', 'complexity', 'text_embedding', 'game_embedding'
] as const;

export interface MissingModel {
	model_type: string;
	missing: true;
}

export function splitModels(models: DeployedModelRow[]): {
	game: (DeployedModelRow | MissingModel)[];
	collections: DeployedModelRow[];
} {
	const game = models.filter((m) => m.model_category === 'game');
	return {
		game: GAME_MODEL_TYPES.flatMap((t) => {
			const rows = game.filter((m) => m.model_type === t);
			return rows.length ? rows : [{ model_type: t, missing: true as const }];
		}),
		collections: models.filter((m) => m.model_category === 'collection')
	};
}
```

- [ ] **Step 6: Run the tests and the type check**

Run: `pnpm vitest run src/lib/monitoring/display.test.ts && pnpm check`
Expected: the tests pass. `pnpm check` may report type errors in `HistoryGrid.svelte` and `DeployedModels.svelte` (`h.stages` possibly undefined, the old model fields). Task 8 fixes those, so note them and don't fix them here.

- [ ] **Step 7: Commit and push**

```bash
git add src/lib/server/warehouse/types.ts src/lib/server/warehouse/index.ts src/lib/monitoring/display.ts src/lib/monitoring/display.test.ts
git commit -m "feat(admin): pipeline monitor types for ML steps, old-chain days and models in use"
git push -u origin feat/pipeline-monitor-v2
```

---

### Task 8: Viewer components

**Files:**
- Modify: `src/lib/monitoring/ChainLanes.svelte`, `HistoryGrid.svelte`, `DeployedModels.svelte`
- Modify: `src/routes/(app)/admin/pipeline/+page.svelte`

**Interfaces:**
- Consumes: Task 7's types and helpers.

- [ ] **Step 1: ChainLanes: steps under ML Pipeline**

In `ChainLanes.svelte`, inside `{#snippet card(s)}`, after the `{#if s.note}` line, add:

```svelte
    {#if s.steps?.length}
      <ol class="steps">
        {#each s.steps.filter((x) => x.branch === 'main') as x (x.key)}{@render step(x)}{/each}
      </ol>
      <div class="side-h">Doesn't block publish</div>
      <ol class="steps side">
        {#each s.steps.filter((x) => x.branch === 'side') as x (x.key)}{@render step(x)}{/each}
      </ol>
    {/if}
```

Add a snippet above `{#snippet card …}`:

```svelte
{#snippet step(x: PipelineStep)}
  <li>
    <span class="sl">{x.label}</span>
    <StatusBadge status={x.status} />
    {#if x.finished}<span class="tnum">{duration(x.started, x.finished)}</span>{/if}
    {#if x.note}<span class="sn">{x.note}</span>{/if}
    {#if x.url}<a href={x.url} target="_blank" rel="noreferrer" aria-label="{x.label} job">↗</a>{/if}
  </li>
{/snippet}
```

Import `PipelineStep` alongside `Lane, PipelineStage`. In `<style>`, add:

```css
  .steps { list-style: none; margin: 0.4rem 0 0; padding: 0.35rem 0 0; border-top: 1px dashed var(--border); display: grid; gap: 0.2rem; }
  .steps li { display: flex; align-items: center; gap: 0.45rem; font-size: 0.75rem; flex-wrap: wrap; }
  .steps .sl { min-width: 8.5rem; }
  .steps .sn { color: var(--muted-foreground); }
  .steps a { color: var(--muted-foreground); text-decoration: none; margin-left: auto; }
  .side-h { margin-top: 0.4rem; font-size: 0.68rem; text-transform: uppercase; letter-spacing: 0.08em; color: var(--muted-foreground); }
  .steps.side { border-top: none; padding-top: 0.15rem; }
```

- [ ] **Step 2: HistoryGrid: old-chain columns and the side mark**

Replace the `<tbody>` in `HistoryGrid.svelte` with:

```svelte
      <tbody>
        {#each stages as s, si (s.key)}
          <tr>
            <th scope="row">{s.label}</th>
            {#each history as h (h.day)}
              {@const d = historyDay(h)}
              {#if d.era === 'old'}
                {#if si === 0}
                  <td class="old {d.cell.status}" rowspan={stages.length} title="{h.day} · old chain · {STATUS_WORD[d.cell.status]}">
                    {#if d.cell.url}
                      <a href={d.cell.url} target="_blank" rel="noreferrer" aria-label="Old chain on {h.day}: {STATUS_WORD[d.cell.status]}, open run">{STATUS_GLYPH[d.cell.status]}</a>
                    {:else}{STATUS_GLYPH[d.cell.status]}{/if}
                  </td>
                {/if}
              {:else}
                {@const cell = historyCell(d.stages[s.key])}
                <td class={cell.status} class:side-fail={cell.side === 'fail'}
                    title="{s.label} · {h.day} · {STATUS_WORD[cell.status]}{cell.side === 'fail' ? ' · a collection step failed' : ''}">
                  {#if cell.url}
                    <a href={cell.url} target="_blank" rel="noreferrer" aria-label="{s.label} on {h.day}: {STATUS_WORD[cell.status]}, open run">{STATUS_GLYPH[cell.status]}</a>
                  {:else}{STATUS_GLYPH[cell.status]}{/if}
                </td>
              {/if}
            {/each}
          </tr>
        {/each}
      </tbody>
```

Import `historyDay` from `./display`. In `<style>`, add:

```css
  td.old { opacity: 0.55; vertical-align: middle; }
  td.side-fail { box-shadow: inset -5px 5px 0 -2px var(--status-warn); }
```

Under the legend's badges, add `<span class="old-note">Faded column: the old chain (before 7 Oct)</span>` with `.old-note { font-size: 0.72rem; color: var(--muted-foreground); }`.

- [ ] **Step 3: DeployedModels: two tables**

Replace `DeployedModels.svelte` with:

```svelte
<script lang="ts">
  import type { DeployedModelRow } from '$lib/server/warehouse';
  import StatusBadge from './StatusBadge.svelte';
  import { clock, isStale, modelKey, splitModels } from './display';

  let { models, generatedAt }: { models: DeployedModelRow[]; generatedAt: string } = $props();
  const fmt = new Intl.NumberFormat('en-US');
  const day = (iso: string | null) =>
    iso ? new Date(iso).toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' }) : '—';
  const split = $derived(splitModels(models));
</script>

{#snippet when(m: DeployedModelRow)}
  <td class="tnum">
    {day(m.last_scored)} · {clock(m.last_scored)}
    {#if isStale(m.last_scored, generatedAt)}<StatusBadge status="warn" label="Stale" />{/if}
  </td>
{/snippet}

<div class="card scroll">
  <table>
    <caption>Scoring models</caption>
    <thead><tr><th>Step</th><th>Model</th><th>Version</th><th class="num">Games</th><th>Last scored (UTC)</th></tr></thead>
    <tbody>
      {#each split.game as m, i ('missing' in m ? `${m.model_type}|missing` : `${modelKey(m)}|${i}`)}
        <tr>
          <td>{m.model_type}</td>
          {#if 'missing' in m}
            <td colspan="4"><StatusBadge status="warn" label="No run in 30 days" /></td>
          {:else}
            <td class="mono">{m.model_name ?? '—'}</td>
            <td class="mono">{m.model_version != null ? `v${m.model_version}` : '—'}</td>
            <td class="num">{fmt.format(m.games_scored)}</td>
            {@render when(m)}
          {/if}
        </tr>
      {/each}
    </tbody>
  </table>
</div>

<div class="card scroll">
  <table>
    <caption>Collections</caption>
    <thead><tr><th>User</th><th>Outcome</th><th>Model</th><th>Version</th><th>Last scored (UTC)</th></tr></thead>
    <tbody>
      {#each split.collections as m, i (`${modelKey(m)}|${i}`)}
        <tr>
          <td>{m.username}</td>
          <td>{m.model_type}</td>
          <td class="mono">{m.model_name ?? '—'}</td>
          <td class="mono">{m.model_version != null ? `v${m.model_version}` : '—'}</td>
          {@render when(m)}
        </tr>
      {:else}
        <tr><td colspan="5" class="sub">No collection scoring in 30 days.</td></tr>
      {/each}
    </tbody>
  </table>
</div>

<style>
  .card { background: var(--card); border: 1px solid var(--border); border-radius: var(--radius); }
  .card + .card { margin-top: var(--space-md); }
  .scroll { overflow-x: auto; }
  table { width: 100%; border-collapse: collapse; font-size: 0.83rem; }
  caption { text-align: left; font-weight: 600; font-size: 0.85rem; padding: 0.55rem 0.75rem 0; }
  th { text-align: left; font-weight: 500; font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.08em; color: var(--muted-foreground); padding: 0.55rem 0.75rem; border-bottom: 1px solid var(--border); white-space: nowrap; }
  td { padding: 0.5rem 0.75rem; border-top: 1px solid var(--border); }
  .num { text-align: right; font-variant-numeric: tabular-nums; }
  .mono { font-family: ui-monospace, Menlo, monospace; font-size: 0.78rem; }
  .sub { color: var(--muted-foreground); font-size: 0.75rem; }
  .tnum { font-variant-numeric: tabular-nums; }
</style>
```

- [ ] **Step 4: The page**

In `src/routes/(app)/admin/pipeline/+page.svelte`, change the models section to:

```svelte
      <section>
        <h2>Models in use <span>the model each scoring step used in its latest run · stale = no run in 48h</span></h2>
        <DeployedModels models={s.models} generatedAt={s.generated_at} />
      </section>
```

- [ ] **Step 5: Verify**

Run: `pnpm check && pnpm test && pnpm build`
Expected:
- `svelte-check` reports 0 errors.
- vitest passes.
- The build succeeds.

Ask Phil to look at `/admin/pipeline` in his own `just dev` against the deployed warehouse API. Don't start a dev server or browser yourself.

- [ ] **Step 6: Commit, push, PR**

```bash
git add src/lib/monitoring/ChainLanes.svelte src/lib/monitoring/HistoryGrid.svelte src/lib/monitoring/DeployedModels.svelte "src/routes/(app)/admin/pipeline/+page.svelte"
git commit -m "feat(admin): ML Pipeline steps, old-chain history and models in use on the pipeline page"
git push
```

Check `gh pr list --head feat/pipeline-monitor-v2 --state all` first. Open the PR against `main` in bgg-viewer, with a body that:
- links the spec (#152) and the warehouse PR;
- notes that it ships via bgg-viewer's release PR.

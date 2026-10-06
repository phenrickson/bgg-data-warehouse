"""The daily chain as data, and the rules that turn GitHub runs into stage statuses.

Seven stages across three repos, stitched together by ``workflow_run`` and
``repository_dispatch``: the fetches, Dataform's ``core`` piece, the ML Pipeline
(one orchestrating run whose jobs include collection scoring and reports),
Dataform's ``publish`` piece, then the viewer. A stage's status comes from its own latest run in the day's
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

# The first chain day run by the new daily chain (core → ML Pipeline → publish). Earlier
# days ran the twelve-stage chain and are shown as one "old chain" cell, not re-judged.
CUTOVER = date(2026, 10, 7)

Runs = dict[tuple[str, str], list[dict[str, Any]]]
# ML Pipeline run id -> that run's jobs (``github.fetch_jobs``).
Jobs = dict[int, list[dict[str, Any]]]


@dataclass(frozen=True)
class Stage:
    key: str
    label: str
    repo: str
    workflow_file: str
    lane: str
    event: str | None = None
    title: str | None = None
    # Take the earliest run created at/after this stage's run instead of the latest in
    # the window. Pass 1's trigger (`workflow_run`) also fires after a manual Run Fetch
    # Games, which would otherwise replace the chain's own pass 1.
    after: str | None = None

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
    Stage("dataform_core", "Dataform · core", WAREHOUSE, "dataform.yml", "warehouse",
          event="workflow_run", after="refresh_old_games"),
    # Collection scoring and reports run as jobs inside this run. Their failure fails
    # the run (visible here) without blocking publish.
    Stage("ml_pipeline", "ML Pipeline", MODELS, "ml-pipeline.yml", "models"),
    Stage("dataform_publish", "Dataform · publish", WAREHOUSE, "dataform.yml", "warehouse",
          title="ml_complete"),
    Stage("viewer_artifacts", "Viewer Artifacts", VIEWER, "viewer-artifacts.yml", "viewer"),
]

OFF_CHAIN = [
    Stage("pipeline_status", "Pipeline Status", WAREHOUSE, "pipeline_status.yml", "warehouse"),
]

STAGE_BY_KEY = {s.key: s for s in STAGES}

# Every (repo, workflow file) to list runs for. Dataform appears once.
SOURCES = sorted({s.source for s in STAGES + OFF_CHAIN})

# The old chain's last Dataform pass: its result is that day's result.
OLD_FINAL = Stage("old_final", "Old chain", WAREHOUSE, "dataform.yml", "warehouse",
                  title="embeddings_complete")


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
    # ML Pipeline only, when its jobs were fetched: the jobs grouped into steps.
    steps: list[StepStatus] | None = None


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


def _in_window(stage: Stage, runs: Runs, day: date) -> list[dict[str, Any]]:
    start, end = window(day)
    return [
        r for r in runs.get(stage.source, [])
        if stage.matches(r) and start <= parse_ts(r["created_at"]) < end
    ]


def _latest(stage: Stage, runs: Runs, day: date) -> dict[str, Any] | None:
    """The stage's newest run created in the day's window (a rerun supersedes a failure)."""
    return max(_in_window(stage, runs, day), key=lambda r: parse_ts(r["created_at"]), default=None)


def _pick(stage: Stage, runs: Runs, day: date,
          picked: dict[str, dict[str, Any] | None]) -> dict[str, Any] | None:
    anchor = picked.get(stage.after) if stage.after else None
    if anchor is None:
        return _latest(stage, runs, day)
    after = [r for r in _in_window(stage, runs, day)
             if parse_ts(r["created_at"]) >= parse_ts(anchor["created_at"])]
    return min(after, key=lambda r: parse_ts(r["created_at"]), default=None)


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


def _handed_off(run: dict[str, Any], nxt: dict[str, Any] | None) -> bool:
    return nxt is not None and parse_ts(nxt["created_at"]) >= parse_ts(run["created_at"])


def build_chain(runs: Runs, day: date, now: datetime, jobs: Jobs | None = None) -> Chain:
    by_key: dict[str, dict[str, Any] | None] = {}
    for s in STAGES:
        by_key[s.key] = _pick(s, runs, day, by_key)
    picked = [by_key[s.key] for s in STAGES]
    stages: list[StageStatus] = []
    blocked = False  # an upstream stage failed, stalled or never ran
    for i, (stage, run) in enumerate(zip(STAGES, picked)):
        note = None
        steps = None
        if run is None:
            if i == 0:
                due = datetime.combine(day, STAGE_ONE_DUE, tzinfo=UTC)
                status, note = ("fail", "No run by 07:00 UTC") if now >= due else ("pending", None)
            else:
                status = "not_reached" if blocked else "pending"
        else:
            if stage.key == "ml_pipeline" and jobs and run.get("id") in jobs:
                steps = group_steps(jobs[run["id"]])
                status, note = _ml_status(run, steps)
            else:
                status = _own_status(run)
            is_last = i == len(STAGES) - 1
            if status == "ok" and not is_last and not _handed_off(run, picked[i + 1]):
                if now - parse_ts(run["updated_at"]) > HANDOFF:
                    status, note = "warn", "No hand-off: the next stage never started"
        stage_status = _status(stage, run, status, note)
        stage_status.steps = steps
        stages.append(stage_status)
        blocked = blocked or status in ("fail", "warn", "not_reached")

    return Chain(day, stages, _off_chain(runs, day))


def _off_chain(runs: Runs, day: date) -> list[StageStatus]:
    out = []
    for stage in OFF_CHAIN:
        run = _latest(stage, runs, day)
        if run is None:
            out.append(_status(stage, None, "not_reached", "No run"))
            continue
        out.append(_status(stage, run, _own_status(run)))
    return out


def _minutes(start: str, end: str) -> int:
    return round((parse_ts(end) - parse_ts(start)).total_seconds() / 60)


def verdict(c: Chain) -> dict[str, Any]:
    """The page headline: the first stage that isn't ok, or the end-to-end time."""
    bad = next((s for s in c.stages if s.status != "ok"), None)
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
    if bad.status == "running":
        status, headline = "running", f"Running: {bad.label}"
    elif bad.status == "pending":
        status, headline = "running", f"Waiting on {bad.label}"
    elif bad.status == "warn":
        status, headline = "warn", f"Stalled after {bad.label}"
    elif bad.status == "fail":
        status = "fail"
        headline = (f"{bad.label} {bad.note}" if bad.note and bad.note.startswith("failed at ")
                    else f"{bad.label} failed")
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

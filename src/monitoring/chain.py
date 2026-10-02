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

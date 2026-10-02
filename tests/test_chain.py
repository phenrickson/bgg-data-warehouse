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

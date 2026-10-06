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

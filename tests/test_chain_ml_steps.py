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

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


def test_today_needs_jobs_even_before_cutover():
    """Today always uses the new stages, so its run's steps are always fetched."""
    assert chain.jobs_needed(_runs(), 3, NOON, cutover=date(2026, 10, 7)) == [ML_ID]


def test_no_jobs_needed_for_history_before_cutover():
    runs = _runs()
    ml = chain.STAGE_BY_KEY["ml_pipeline"]
    runs[ml.source][0]["conclusion"] = "failure"
    tomorrow = datetime(2026, 10, 3, 12, 0, tzinfo=UTC)
    assert chain.jobs_needed(runs, 3, tomorrow, cutover=date(2026, 10, 7)) == []


def test_branch_dispatched_ml_run_does_not_replace_the_chain_run():
    """A test dispatch from a feature branch never sends ml_complete; it isn't the chain's."""
    runs = _runs()
    ml = chain.STAGE_BY_KEY["ml_pipeline"]
    chain_run = runs[ml.source][0]
    chain_run["head_branch"] = "main"
    runs[ml.source].append(dict(chain_run, id=99, head_branch="feat/x",
                                created_at="2026-10-02T11:00:00Z", conclusion="success"))
    c = chain.build_chain(runs, DAY, NOON, jobs={ML_ID: jobs(), 99: []})
    assert _stage(c, "ml_pipeline").status == "ok"
    assert chain.jobs_needed(runs, 1, NOON, cutover=date(2026, 9, 1)) == [ML_ID]


def test_completed_run_with_no_jobs_falls_back_to_run_rule():
    runs = _runs()
    c = chain.build_chain(runs, DAY, NOON, jobs={ML_ID: []})
    assert _stage(c, "ml_pipeline").status == "ok"
    assert _stage(c, "ml_pipeline").steps is None


def test_deployed_models_counts_the_whole_run_not_its_last_batch():
    """Services mint a job_id per batch request, so one run spans many job_ids."""
    from pathlib import Path as _P
    sql = (_P(__file__).parents[1] / "definitions/deployed_models.sqlx").read_text()
    assert "LIMIT 1)" not in sql
    assert sql.count("INTERVAL 3 HOUR") == 5


def test_deployed_models_counts_games_served_by_the_current_model():
    """How far the current model has reached: games in each serving table on it, of all."""
    from pathlib import Path as _P
    sql = (_P(__file__).parents[1] / "definitions/deployed_models.sqlx").read_text()
    assert "games_scored" not in sql
    assert "games_served" in sql and "games_total" in sql
    for table in ("bgg_predictions", "bgg_complexity_predictions", "bgg_description_embeddings",
                  "bgg_game_embeddings", "user_collection_predictions"):
        assert f'ref("{table}")' in sql, table

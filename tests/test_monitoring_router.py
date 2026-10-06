"""Router tests for the monitoring resource (reader mocked — no BigQuery)."""

import pytest
from fastapi.testclient import TestClient

from services.warehouse_api.main import app
from services.warehouse_api.routers import monitoring as monitoring_router

client = TestClient(app)


@pytest.fixture(autouse=True)
def _clear_cache():
    monitoring_router._reset_cache()
    yield
    monitoring_router._reset_cache()


def test_new_games_defaults(monkeypatch):
    seen = {}

    def fake(days_back, limit):
        seen.update(days_back=days_back, limit=limit)
        return [{"game_id": 13, "name": "Catan"}]

    monkeypatch.setattr(monitoring_router.reader, "fetch_recently_added", fake)
    r = client.get("/new-games")
    assert r.status_code == 200
    assert r.json() == [{"game_id": 13, "name": "Catan"}]
    assert seen == {"days_back": 7, "limit": 20000}


def test_new_games_passes_days_and_limit(monkeypatch):
    seen = {}

    def fake(days_back, limit):
        seen.update(days_back=days_back, limit=limit)
        return []

    monkeypatch.setattr(monitoring_router.reader, "fetch_recently_added", fake)
    r = client.get("/new-games?days=30&limit=50")
    assert r.status_code == 200
    assert seen == {"days_back": 30, "limit": 50}


def test_new_games_rejects_out_of_range_days(monkeypatch):
    monkeypatch.setattr(
        monitoring_router.reader, "fetch_recently_added",
        lambda days_back, limit: [],
    )
    assert client.get("/new-games?days=0").status_code == 422
    assert client.get("/new-games?days=366").status_code == 422


def test_new_games_caches_repeat_calls_with_same_params(monkeypatch):
    calls = []

    def fake(days_back, limit):
        calls.append((days_back, limit))
        return [{"game_id": 13}]

    monkeypatch.setattr(monitoring_router.reader, "fetch_recently_added", fake)
    client.get("/new-games?days=7")
    client.get("/new-games?days=7")
    assert len(calls) == 1, "second call with identical params should hit the cache"


def test_new_games_does_not_cache_across_different_params(monkeypatch):
    calls = []

    def fake(days_back, limit):
        calls.append((days_back, limit))
        return [{"game_id": 13}]

    monkeypatch.setattr(monitoring_router.reader, "fetch_recently_added", fake)
    client.get("/new-games?days=7")
    client.get("/new-games?days=30")
    assert len(calls) == 2


# --- /monitoring/pipeline ---------------------------------------------------

import requests  # noqa: E402 — appended section; only these tests need it

TABLE_ROW = {"table": "raw.thing_ids", "last_updated": "2026-10-02T06:30:00Z", "games": 5,
             "covered": None, "universe": None, "users": None}
MODEL_ROW = {"model_category": "prediction", "model_type": "hurdle", "model_name": "hurdle-v2026",
             "model_version": "3", "experiment": "e", "algorithm": None, "games_count": 1,
             "last_updated": "2026-10-02T07:23:11Z"}


@pytest.fixture
def pipeline_ok(monkeypatch):
    calls = {"runs": 0, "tables": 0, "jobs": []}

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

    def fake_jobs(repo, run_id, token):
        calls["jobs"].append((repo, run_id))
        return []

    monkeypatch.setattr(monitoring_router, "fetch_jobs", fake_jobs)
    return calls


def test_pipeline_report_shape(pipeline_ok):
    r = client.get("/monitoring/pipeline?days=3")
    assert r.status_code == 200
    body = r.json()
    assert set(body) == {"generated_at", "verdict", "today", "history", "tables", "models"}
    assert len(body["history"]) == 3
    assert len(body["today"]["stages"]) == len(monitoring_router.chain.STAGES) == 7
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


# --- /monitoring/lineage and /monitoring/tables/{id} ------------------------

import json as _json  # noqa: E402
from pathlib import Path as _Path  # noqa: E402

from google.api_core import exceptions as _gexc  # noqa: E402

_COMPILATION = _json.loads(
    (_Path(__file__).parent / "fixtures/dataform_compilation_2026-10-03.json").read_text())
_SUMMARY = {"name": "projects/p/compilationResults/c1", "createTime": "2026-10-03T17:46:47Z",
            "resolvedGitCommitSha": "4b04de42f410"}
_GF = "bgg-data-warehouse.analytics.games_features"


@pytest.fixture
def lineage_ok(monkeypatch):
    calls = {"compilation": 0, "schema": 0}

    def fake_compilation():
        calls["compilation"] += 1
        return _SUMMARY, _COMPILATION

    def fake_meta(ids):
        return {i: {"rows": 1, "bytes": 2, "last_modified": "2026-10-03T07:19:00Z",
                    "type": "TABLE", "error": None} for i in ids}

    def fake_schema(table_id):
        calls["schema"] += 1
        return [{"name": "game_id", "type": "INTEGER", "mode": "NULLABLE", "description": None}]

    monkeypatch.setattr(monitoring_router.lineage_reader, "fetch_compilation", fake_compilation)
    monkeypatch.setattr(monitoring_router.lineage_reader, "fetch_table_meta", fake_meta)
    monkeypatch.setattr(monitoring_router.lineage_reader, "fetch_table_schema", fake_schema)
    return calls


def test_lineage_shape(lineage_ok):
    r = client.get("/monitoring/lineage")
    assert r.status_code == 200
    body = r.json()
    assert set(body) == {"generated_at", "compilation", "nodes", "edges"}
    assert body["compilation"] == {"name": _SUMMARY["name"], "created": _SUMMARY["createTime"],
                                   "commit": "4b04de4"}
    node = next(n for n in body["nodes"] if n["id"] == _GF)
    assert node["kind"] == "incremental" and node["rows"] == 1 and node["error"] is None
    assert ["bgg-data-warehouse.analytics.games_active", _GF] in body["edges"]


def test_lineage_is_cached(lineage_ok):
    client.get("/monitoring/lineage")
    client.get("/monitoring/lineage")
    assert lineage_ok["compilation"] == 1


def test_lineage_upstream_error_is_502(lineage_ok, monkeypatch):
    def boom():
        raise RuntimeError("no clean compilation of main among the latest 20")

    monkeypatch.setattr(monitoring_router.lineage_reader, "fetch_compilation", boom)
    r = client.get("/monitoring/lineage")
    assert r.status_code == 502
    assert "no clean compilation" in r.json()["detail"]


def test_table_schema(lineage_ok):
    r = client.get(f"/monitoring/tables/{_GF}")
    assert r.status_code == 200
    assert r.json() == {"id": _GF, "schema": [
        {"name": "game_id", "type": "INTEGER", "mode": "NULLABLE", "description": None}]}
    client.get(f"/monitoring/tables/{_GF}")
    assert lineage_ok["schema"] == 1, "schema is cached per table"


def test_table_schema_rejects_ids_outside_lineage(lineage_ok):
    r = client.get("/monitoring/tables/some-other-project.secrets.passwords")
    assert r.status_code == 400
    assert lineage_ok["schema"] == 0


def test_table_schema_not_found_and_no_access(lineage_ok, monkeypatch):
    def missing(table_id):
        raise _gexc.NotFound("gone")

    monkeypatch.setattr(monitoring_router.lineage_reader, "fetch_table_schema", missing)
    assert client.get(f"/monitoring/tables/{_GF}").status_code == 404

    def denied(table_id):
        raise _gexc.Forbidden("nope")

    monkeypatch.setattr(monitoring_router.lineage_reader, "fetch_table_schema", denied)
    assert client.get(f"/monitoring/tables/{_GF}").status_code == 403

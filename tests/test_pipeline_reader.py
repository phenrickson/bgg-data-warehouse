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
    assert by_name["predictions.bgg_game_coordinates"].universe == "dated_games"
    assert by_name["raw.thing_ids"].universe is None
    assert by_name["predictions.user_collection_predictions"].count_users


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


def test_ml_tables_measure_coverage_against_games_with_a_year():
    # The embedding, complexity and coordinate services only score games with a
    # year_published (bgg-predictive-models services/*/main.py), so ~12k yearless
    # games must not count against them.
    by_name = {t.name: t for t in pipeline.TABLES}
    for name in ("predictions.bgg_description_embeddings", "predictions.bgg_complexity_predictions",
                 "predictions.bgg_game_embeddings", "predictions.bgg_game_coordinates"):
        assert by_name[name].universe == "dated_games", name
    client = FakeClient([])
    pipeline.fetch_table_status(client=client)
    sql, _ = client.calls[0]
    assert "dated_games AS (" in sql
    assert "year_published IS NOT NULL" in sql

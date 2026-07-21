"""Unit tests for the games reader (BigQuery mocked — no network)."""

from src.warehouse.readers import games


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def result(self):
        return self._rows


class RoutingClient:
    """Fake BigQuery client that returns canned rows based on the table named in the SQL.

    Records each (sql, job_config) so tests can assert parameterization.
    """

    def __init__(self, tables):
        self.tables = tables
        self.calls = []

    def query(self, sql, job_config=None):
        self.calls.append((sql, job_config))
        for table, rows in self.tables.items():
            if table in sql:
                return _Result(list(rows))
        return _Result([])


FEATURES_ROW = {"game_id": 13, "name": "Catan", "year_published": 1995, "publishers": ["KOSMOS"]}
PLAYER_COUNTS = [{"player_count": "3", "best_percentage": 60.0}, {"player_count": "4", "best_percentage": 55.0}]
PREDICTION_ROW = {"game_id": 13, "predicted_rating": 7.1, "first_prediction_ts": "2026-01-01T00:00:00Z"}
COORD_ROW = {"game_id": 13, "umap_1": 1.2, "umap_2": 3.4}
SIMILAR_ROWS = [{"game_id": 21, "name": "Carcassonne", "distance": 0.11}]
PROVENANCE_ROW = {"game_id": 13, "fetch_timestamp": "2026-07-10T00:00:00Z"}


def _full_client():
    return RoutingClient({
        "games_features": [FEATURES_ROW],
        "player_count_recommendations": PLAYER_COUNTS,
        "bgg_predictions": [PREDICTION_ROW],
        "bgg_game_coordinates": [COORD_ROW],
        "game_similarity_search": SIMILAR_ROWS,
        "fetched_responses": [PROVENANCE_ROW],
    })


class TestBlocks:
    def test_features_includes_player_counts(self):
        client = _full_client()
        result = games.get_features(13, client=client)
        assert result["name"] == "Catan"
        assert result["player_counts"] == PLAYER_COUNTS

    def test_features_missing_returns_none(self):
        client = RoutingClient({"games_features": []})
        assert games.get_features(999999, client=client) is None

    def test_predictions_joins_first_ts(self):
        client = _full_client()
        result = games.get_predictions(13, client=client)
        assert result["predicted_rating"] == 7.1
        assert "first_prediction_ts" in result

    def test_embedding(self):
        assert games.get_embedding(13, client=_full_client())["umap_1"] == 1.2

    def test_similar_uses_ml_distance(self):
        client = _full_client()
        result = games.get_similar(13, n=5, client=client)
        assert result == SIMILAR_ROWS
        assert "ML.DISTANCE" in client.calls[-1][0]

    def test_provenance(self):
        assert games.get_provenance(13, client=_full_client())["fetch_timestamp"]


class TestGetGame:
    def test_composes_all_blocks(self):
        result = games.get_game(13, client=_full_client())
        assert set(result) == {"game_id", "features", "predictions", "embedding", "similar", "provenance"}
        assert result["game_id"] == 13
        assert result["features"]["name"] == "Catan"
        assert result["similar"] == SIMILAR_ROWS

    def test_missing_game_returns_none(self):
        client = RoutingClient({"games_features": []})
        assert games.get_game(999999, client=client) is None


class TestSafety:
    def test_queries_are_parameterized(self):
        """game_id is bound as a query parameter, never interpolated into SQL."""
        client = _full_client()
        games.get_game(13, client=client)
        for sql, job_config in client.calls:
            assert "@game_id" in sql
            assert "13" not in sql  # the literal id never appears in the SQL text
            names = {p.name for p in job_config.query_parameters}
            assert "game_id" in names


class TestConcurrency:
    """get_game must issue its block queries in parallel, not one after another."""

    def test_blocks_run_concurrently(self):
        import time

        class SlowClient(RoutingClient):
            def query(self, sql, job_config=None):
                time.sleep(0.1)
                return super().query(sql, job_config)

        client = SlowClient({
            "games_features": [FEATURES_ROW],
            "player_count_recommendations": PLAYER_COUNTS,
            "bgg_predictions": [PREDICTION_ROW],
            "bgg_game_coordinates": [COORD_ROW],
            "game_similarity_search": SIMILAR_ROWS,
            "fetched_responses": [PROVENANCE_ROW],
        })
        start = time.monotonic()
        result = games.get_game(13, client=client)
        elapsed = time.monotonic() - start

        assert result is not None
        assert len(client.calls) == 6
        # Sequential would be >= 0.6s (6 x 0.1s). Bound kept generous for CI jitter.
        assert elapsed < 0.4, f"queries appear sequential: {elapsed:.2f}s for 6 x 0.1s"

    def test_still_composes_and_handles_missing_game(self):
        assert set(games.get_game(13, client=_full_client())) == {
            "game_id", "features", "predictions", "embedding", "similar", "provenance"
        }
        assert games.get_game(999999, client=RoutingClient({"games_features": []})) is None


class TestExplicitColumns:
    """`SELECT *` scans every column of every row on these unclustered tables."""

    def test_no_select_star_anywhere(self):
        client = _full_client()
        games.get_game(13, client=client)
        for sql, _ in client.calls:
            assert "SELECT *" not in sql, f"SELECT * found in: {sql[:80]}"

    def test_features_query_still_selects_description(self):
        """description stays in the default payload — no response-contract change."""
        client = _full_client()
        games.get_feature_row(13, client=client)
        assert "description" in client.calls[-1][0]

    def test_player_counts_readable_without_touching_games_features(self):
        """/players should not pay for a games_features scan it never uses."""
        client = _full_client()
        assert games.get_player_counts(13, client=client) == PLAYER_COUNTS
        assert all("games_features" not in sql for sql, _ in client.calls)

    def test_get_features_still_composes_row_and_counts(self):
        result = games.get_features(13, client=_full_client())
        assert result["name"] == "Catan"
        assert result["player_counts"] == PLAYER_COUNTS

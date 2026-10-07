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
RECOMMENDER_ROWS = [{"game_id": 822, "name": "Carcassonne", "distance": 0.14}]
PROVENANCE_ROW = {"game_id": 13, "fetch_timestamp": "2026-07-10T00:00:00Z"}

# game_neighbors: one row per (profile, game). `sicko` is deliberately absent — this
# game sits below its 30-rating floor, which the reader must surface as an empty list.
NEIGHBOR_ROWS = [
    {"profile": "similar", "similar": SIMILAR_ROWS},
    {"profile": "recommender", "similar": RECOMMENDER_ROWS},
]


# A game_profile row: flat feature columns + nested player_counts + block structs.
PROFILE_ROW = {
    "game_id": 13, "name": "Catan", "year_published": 1995, "publishers": ["KOSMOS"],
    "player_counts": PLAYER_COUNTS,
    "predictions": PREDICTION_ROW,
    "embedding": COORD_ROW,
    "provenance": PROVENANCE_ROW,
}


def _profile_client():
    return RoutingClient({
        "game_profile": [PROFILE_ROW],
        "game_neighbors": NEIGHBOR_ROWS,
    })


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
    """get_game reads the pre-joined profile plus precomputed neighbours — two lookups,
    not the old six-table fan-out."""

    def test_reads_only_profile_and_neighbors(self):
        client = _profile_client()
        games.get_game(13, client=client)
        sqls = [s for s, _ in client.calls]
        assert len(sqls) == 2, f"expected 2 queries, got {len(sqls)}"
        assert any("game_profile" in s for s in sqls)
        assert any("game_neighbors" in s for s in sqls)
        for stale in ("games_features", "player_count_recommendations", "fetched_responses"):
            assert not any(stale in s for s in sqls), f"should no longer query {stale}"

    def test_composes_expected_shape(self):
        result = games.get_game(13, client=_profile_client())
        assert set(result) == {
            "game_id", "features", "predictions", "embedding",
            "similar", "similar_profiles", "provenance",
        }
        assert result["game_id"] == 13
        assert result["features"]["name"] == "Catan"
        assert result["features"]["player_counts"] == PLAYER_COUNTS
        # nested blocks are unpacked out of features, not left inside it
        assert "predictions" not in result["features"]
        assert result["predictions"]["predicted_rating"] == 7.1
        assert result["similar"] == SIMILAR_ROWS

    def test_similar_profiles_carries_every_list_empty_when_absent(self):
        """All three profiles are keyed; a profile with no row for this game is []."""
        result = games.get_game(13, client=_profile_client())
        assert set(result["similar_profiles"]) == {"similar", "recommender", "sicko"}
        assert result["similar_profiles"]["similar"] == SIMILAR_ROWS
        assert result["similar_profiles"]["recommender"] == RECOMMENDER_ROWS
        assert result["similar_profiles"]["sicko"] == []

    def test_top_level_similar_mirrors_the_requested_profile(self):
        result = games.get_game(13, client=_profile_client(), profile="recommender")
        assert result["similar"] == RECOMMENDER_ROWS
        assert result["similar_profiles"]["similar"] == SIMILAR_ROWS

    def test_missing_game_returns_none(self):
        client = RoutingClient({"game_profile": []})
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


class TestSimilarRouting:
    """Untuned similarity is served precomputed; any tuning parameter goes live."""

    def _client(self):
        return RoutingClient({
            "game_neighbors": [{"similar": SIMILAR_ROWS}],
            "game_similarity_search": SIMILAR_ROWS,
        })

    def test_untuned_reads_precomputed_table(self):
        client = self._client()
        assert games.get_similar(13, client=client) == SIMILAR_ROWS
        sql = client.calls[-1][0]
        assert "game_neighbors" in sql
        assert "ML.DISTANCE" not in sql

    def test_named_profile_is_passed_through(self):
        client = self._client()
        games.get_similar(13, profile="sicko", client=client)
        names = {p.name: p.value for p in client.calls[-1][1].query_parameters}
        assert names["profile"] == "sicko"

    def test_unknown_profile_raises(self):
        import pytest

        with pytest.raises(ValueError):
            games.get_similar(13, profile="nope", client=self._client())
        with pytest.raises(ValueError):
            games.get_game(13, profile="nope", client=_profile_client())

    def test_default_profile_is_similar(self):
        client = self._client()
        games.get_similar(13, client=client)
        names = {p.name: p.value for p in client.calls[-1][1].query_parameters}
        assert names["profile"] == "similar"

    def test_any_tuning_param_goes_live(self):
        for kwargs in ({"band": 0.5}, {"metric": "EUCLIDEAN"}, {"min_ratings": 500},
                       {"dims": 32}, {"n": 25}):
            client = self._client()
            games.get_similar(13, client=client, **kwargs)
            sql = client.calls[-1][0]
            assert "ML.DISTANCE" in sql, f"{kwargs} should route live"
            assert "game_neighbors" not in sql

    def test_live_query_applies_the_filters(self):
        """Regression: the live path must not be an unfiltered global ranking."""
        client = self._client()
        games.get_similar(13, band=0.5, client=client)
        sql = client.calls[-1][0]
        assert "users_rated >=" in sql
        assert "complexity BETWEEN" in sql

    def test_metric_and_dims_are_allowlisted(self):
        """Metric/dims are interpolated (ML.DISTANCE needs literals) so they must be validated."""
        import pytest
        for bad in ({"metric": "COSINE'); DROP TABLE x--"}, {"dims": 999}):
            with pytest.raises(ValueError):
                games.get_similar(13, client=self._client(), **bad)


class TestConcurrency:
    """get_game must issue its block queries in parallel, not one after another."""

    def test_profile_and_neighbors_run_concurrently(self):
        import time

        class SlowClient(RoutingClient):
            def query(self, sql, job_config=None):
                time.sleep(0.2)
                return super().query(sql, job_config)

        client = SlowClient({
            "game_profile": [PROFILE_ROW],
            "game_neighbors": NEIGHBOR_ROWS,
        })
        start = time.monotonic()
        result = games.get_game(13, client=client)
        elapsed = time.monotonic() - start

        assert result is not None
        # Sequential would be >= 0.4s (2 x 0.2s). Bound generous for CI jitter.
        assert elapsed < 0.35, f"queries appear sequential: {elapsed:.2f}s for 2 x 0.2s"


class TestExplicitColumns:
    """`SELECT *` scans every column of every row on these unclustered tables."""

    def test_no_select_star_in_block_readers(self):
        """The per-block readers scan whole (unclustered) tables, so columns must be
        pinned. `game_profile` is exempt: it's a partitioned single-row lookup where
        reading the full row is the point."""
        client = _full_client()
        for fn in (games.get_feature_row, games.get_player_counts,
                   games.get_embedding, games.get_provenance):
            fn(13, client=client)
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


class TestSimilarPooled:
    """Live, pool-scoped search: the game_neighbors profile logic over a restricted pool."""

    POOLED_ROWS = [{"profile": "similar", "similar": SIMILAR_ROWS},
                   {"profile": "recommender", "similar": RECOMMENDER_ROWS}]

    def _client(self):
        return RoutingClient({"game_similarity_search": self.POOLED_ROWS})

    def test_requires_a_pool(self):
        import pytest
        with pytest.raises(ValueError):
            games.get_similar_pooled(13, client=self._client())

    def test_keys_every_profile_empty_when_absent(self):
        out = games.get_similar_pooled(13, year_min=2016, client=self._client())
        assert out == {"similar": SIMILAR_ROWS, "recommender": RECOMMENDER_ROWS, "sicko": []}

    def test_carries_the_profile_logic(self):
        """Regression: the pooled path must be the game_neighbors logic, not a bare distance rank."""
        client = self._client()
        games.get_similar_pooled(13, year_min=2016, client=client)
        sql = client.calls[-1][0]
        assert "similarity_profiles" in sql
        assert "PERCENT_RANK()" in sql
        assert "game_product_line" in sql
        assert "min_similarity" in sql and "max_rating_pct" in sql
        assert "source_min_users_rated" in sql

    def test_pool_values_are_parameters_not_sql(self):
        client = self._client()
        games.get_similar_pooled(
            13, collection="phenrickson", year_min=2016, ids=[1, 2], client=client)
        sql, cfg = client.calls[-1]
        assert "phenrickson" not in sql and "2016" not in sql
        names = {p.name for p in cfg.query_parameters}
        assert {"game_id", "username", "year_min", "ids"} <= names

    def test_collection_reads_owned_games(self):
        client = self._client()
        games.get_similar_pooled(13, collection="phenrickson", client=client)
        sql = client.calls[-1][0]
        assert "user_collections" in sql and "owned = TRUE" in sql

    def test_a_large_posted_pool_is_accepted(self):
        """A single broad answer in the viewer can match ~12,000 games."""
        client = self._client()
        games.get_similar_pooled(13, ids=list(range(1, 12_001)), client=client)
        sql, cfg = client.calls[-1]
        assert "UNNEST(@ids)" in sql and "12000" not in sql

    def test_ids_are_capped(self):
        import pytest
        with pytest.raises(ValueError):
            games.get_similar_pooled(
                13, ids=list(range(games.MAX_POOL_IDS + 1)), client=self._client())

    def test_exclude_collection_drops_owned_games(self):
        client = self._client()
        out = games.get_similar_pooled(13, exclude_collection="phenrickson", client=client)
        sql, cfg = client.calls[-1]
        assert "NOT IN" in sql and "user_collections" in sql
        assert "phenrickson" not in sql
        assert {p.name for p in cfg.query_parameters} >= {"exclude_username"}
        assert set(out) == {"similar", "recommender", "sicko"}

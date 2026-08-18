"""Unit tests for the monitoring reader (BigQuery mocked — no network)."""

from src.warehouse.readers import monitoring


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def result(self):
        return self._rows


class FakeClient:
    """Fake BigQuery client for a single joined query — records the call so tests
    can assert on the SQL shape and the bound parameters."""

    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def query(self, sql, job_config=None):
        self.calls.append((sql, job_config))
        return _Result(list(self.rows))


ROW = {
    "game_id": 13,
    "name": "Catan",
    "year_published": 1995,
    "thumbnail": "https://example.com/catan.jpg",
    "first_seen": "2026-08-10T00:00:00Z",
    "predicted_hurdle_prob": 0.62,
}


def _params(job_config):
    return {p.name: p.value for p in job_config.query_parameters}


def test_returns_rows_as_dicts():
    client = FakeClient([ROW])
    result = monitoring.fetch_recently_added(days_back=30, limit=50, client=client)
    assert result == [ROW]


def test_query_joins_fetched_responses_features_and_predictions():
    client = FakeClient([])
    monitoring.fetch_recently_added(client=client)
    sql, _ = client.calls[0]
    assert "fetched_responses" in sql
    assert "games_features" in sql
    assert "bgg_predictions" in sql
    assert "MIN(fetch_timestamp)" in sql
    assert "LEFT JOIN" in sql  # predictions must not drop games with no prediction row


def test_days_back_and_limit_are_bound_parameters_not_interpolated():
    client = FakeClient([])
    monitoring.fetch_recently_added(days_back=42, limit=7, client=client)
    sql, job_config = client.calls[0]
    assert "42" not in sql
    assert "7 " not in sql and not sql.rstrip().endswith("7")
    params = _params(job_config)
    assert params["days_back"] == 42
    assert params["limit"] == 7


def test_defaults_are_7_days_and_5000_rows():
    client = FakeClient([])
    monitoring.fetch_recently_added(client=client)
    _, job_config = client.calls[0]
    params = _params(job_config)
    assert params["days_back"] == 7
    assert params["limit"] == 5000

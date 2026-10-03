"""Unit tests for the lineage readers (Dataform session and BigQuery client faked)."""

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from google.api_core import exceptions as gexc

from src.warehouse.readers import lineage as reader


class _Resp:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, *payloads):
        self.payloads = list(payloads)
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params))
        return _Resp(self.payloads.pop(0))


def test_fetch_compilation_picks_clean_main_and_joins_pages():
    listing = {"compilationResults": [
        {"name": "c2", "gitCommitish": "feat/x"},
        {"name": "c1", "gitCommitish": "main", "createTime": "t", "resolvedGitCommitSha": "abc"},
    ]}
    session = FakeSession(
        listing,
        {"compilationResultActions": [{"target": {"name": "a"}}], "nextPageToken": "p2"},
        {"compilationResultActions": [{"target": {"name": "b"}}]},
    )
    summary, data = reader.fetch_compilation(session=session)
    assert summary["name"] == "c1"
    assert [a["target"]["name"] for a in data["compilationResultActions"]] == ["a", "b"]
    assert session.calls[0][1]["orderBy"] == "create_time desc"
    assert session.calls[1][0] == "https://dataform.googleapis.com/v1beta1/c1:query"
    assert session.calls[2][1] == {"pageToken": "p2"}


def test_fetch_compilation_without_a_clean_main_raises():
    session = FakeSession({"compilationResults": [{"name": "c", "gitCommitish": "x"}]})
    with pytest.raises(RuntimeError):
        reader.fetch_compilation(session=session)


MODIFIED = datetime(2026, 10, 3, 7, 19, tzinfo=UTC)


class FakeBQ:
    def __init__(self, tables):
        self.tables = tables

    def get_table(self, table_id):
        found = self.tables[table_id]
        if isinstance(found, Exception):
            raise found
        return found


def _table(rows=10, table_type="TABLE", schema=()):
    return SimpleNamespace(num_rows=rows, num_bytes=100, modified=MODIFIED,
                           table_type=table_type, schema=list(schema))


def test_meta_failure_on_one_table_does_not_fail_the_rest():
    client = FakeBQ({
        "p.d.ok": _table(),
        "p.d.gone": gexc.NotFound("x"),
        "p.d.denied": gexc.Forbidden("x"),
    })
    meta = reader.fetch_table_meta(["p.d.ok", "p.d.gone", "p.d.denied"], client=client)
    assert meta["p.d.ok"] == {"rows": 10, "bytes": 100, "last_modified": MODIFIED,
                              "type": "TABLE", "error": None}
    assert meta["p.d.gone"]["error"] == "not found"
    assert meta["p.d.denied"] == {"rows": None, "bytes": None, "last_modified": None,
                                  "type": None, "error": "no access"}


def _field(name, field_type="STRING", fields=(), description=None, mode="NULLABLE"):
    return SimpleNamespace(name=name, field_type=field_type, mode=mode,
                           description=description, fields=list(fields))


def test_schema_flattens_nested_records():
    client = FakeBQ({"p.d.t": _table(schema=[
        _field("game_id", "INTEGER", description="BGG id"),
        _field("player_counts", "RECORD", mode="REPEATED",
               fields=[_field("count", "INTEGER"), _field("best", "BOOLEAN")]),
    ])})
    assert reader.fetch_table_schema("p.d.t", client=client) == [
        {"name": "game_id", "type": "INTEGER", "mode": "NULLABLE", "description": "BGG id"},
        {"name": "player_counts", "type": "RECORD", "mode": "REPEATED", "description": None},
        {"name": "player_counts.count", "type": "INTEGER", "mode": "NULLABLE", "description": None},
        {"name": "player_counts.best", "type": "BOOLEAN", "mode": "NULLABLE", "description": None},
    ]


def test_schema_lets_not_found_through():
    with pytest.raises(gexc.NotFound):
        reader.fetch_table_schema("p.d.gone", client=FakeBQ({"p.d.gone": gexc.NotFound("x")}))


def test_meta_turns_any_per_table_failure_into_a_node_error():
    # A transport error or timeout on one table must stay on that node, not 502 the graph.
    client = FakeBQ({"p.d.ok": _table(), "p.d.flaky": ConnectionError("reset by peer")})
    meta = reader.fetch_table_meta(["p.d.ok", "p.d.flaky"], client=client)
    assert meta["p.d.ok"]["error"] is None
    assert meta["p.d.flaky"]["rows"] is None
    assert "reset by peer" in meta["p.d.flaky"]["error"]

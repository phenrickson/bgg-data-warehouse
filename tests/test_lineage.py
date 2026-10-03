"""Unit tests for the shared Dataform lineage parser (fixture of a real compilation)."""

import json
from collections import Counter
from pathlib import Path

from src.monitoring import lineage

FIX = json.loads(
    (Path(__file__).parent / "fixtures/dataform_compilation_2026-10-03.json").read_text()
)
WH = "bgg-data-warehouse"


def test_parses_every_action_with_its_kind():
    nodes, _ = lineage.parse_compilation_result(FIX)
    kinds = Counter(n.kind for n in nodes)
    assert (kinds["table"], kinds["incremental"], kinds["view"], kinds["operation"]) == (14, 8, 2, 1)
    assert kinds["source"] >= 25
    assert len({n.id for n in nodes}) == len(nodes)


def test_keeps_the_project_in_the_id():
    nodes, _ = lineage.parse_compilation_result(FIX)
    ids = {n.id for n in nodes}
    assert "bgg-predictive-models.raw.ml_predictions_landing" in ids
    assert f"{WH}.analytics.games_features" in ids
    landing = next(n for n in nodes if n.name == "ml_predictions_landing")
    assert (landing.project, landing.dataset, landing.kind) == ("bgg-predictive-models", "raw", "source")


def test_edges_run_upstream_to_downstream_without_duplicates():
    nodes, edges = lineage.parse_compilation_result(FIX)
    ids = {n.id for n in nodes}
    assert edges == sorted(set(edges))
    assert (f"{WH}.analytics.games_active", f"{WH}.analytics.games_features") in edges
    assert all(a in ids and b in ids for a, b in edges)


def _action(name, deps=(), kind="relation", relation_type="TABLE", schema="analytics"):
    target = {"database": WH, "schema": schema, "name": name}
    if kind == "declaration":
        return {"target": target, "declaration": {}}
    body = {"dependencyTargets": [{"database": WH, "schema": "core", "name": d} for d in deps]}
    if kind == "relation":
        body["relationType"] = relation_type
    return {"target": target, kind: body}


def test_dependency_without_its_own_action_becomes_a_source():
    nodes, edges = lineage.parse_compilation_result(
        {"compilationResultActions": [_action("t", deps=["games"])]})
    assert {n.id: n.kind for n in nodes} == {f"{WH}.analytics.t": "table", f"{WH}.core.games": "source"}
    assert edges == [(f"{WH}.core.games", f"{WH}.analytics.t")]


def test_an_actions_own_kind_beats_the_source_placeholder():
    data = {"compilationResultActions": [
        _action("down", deps=["up"]),
        _action("up", schema="core", relation_type="VIEW"),
    ]}
    nodes, _ = lineage.parse_compilation_result(data)
    assert {n.id: n.kind for n in nodes}[f"{WH}.core.up"] == "view"


def test_latest_compilation_skips_branches_and_errors():
    results = [
        {"name": "c4", "gitCommitish": "feat/x"},
        {"name": "c3", "gitCommitish": "main", "compilationErrors": [{"message": "boom"}]},
        {"name": "c2", "gitCommitish": "main"},
        {"name": "c1", "gitCommitish": "main"},
    ]
    assert lineage.latest_compilation_name(results) == "c2"
    assert lineage.latest_compilation_name([]) is None

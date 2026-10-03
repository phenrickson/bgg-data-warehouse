"""Dataform lineage: a compilation result as nodes and edges.

Standard library only, so the lineage GitHub action
(.github/actions/dataform-lineage) can import it without the project's dependencies.
See docs/superpowers/specs/2026-10-03-lineage-view-design.md.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

_RELATION_KINDS = {"TABLE": "table", "INCREMENTAL_TABLE": "incremental", "VIEW": "view"}


@dataclass(frozen=True, order=True)
class Node:
    id: str
    project: str
    dataset: str
    name: str
    kind: str


def _node(target: dict[str, Any], kind: str) -> Node:
    project = target.get("database", "unknown")
    dataset = target.get("schema", "unknown")
    name = target.get("name", "unknown")
    return Node(f"{project}.{dataset}.{name}", project, dataset, name, kind)


def _kind(action: dict[str, Any]) -> str:
    if "relation" in action:
        return _RELATION_KINDS.get(action["relation"].get("relationType", ""), "table")
    if "operations" in action:
        return "operation"
    if "assertion" in action:
        return "assertion"
    return "source"


def _dependencies(action: dict[str, Any]) -> list[dict[str, Any]]:
    for key in ("relation", "operations", "assertion"):
        if key in action:
            return action[key].get("dependencyTargets", [])
    return []


def parse_compilation_result(data: dict[str, Any]) -> tuple[list[Node], list[tuple[str, str]]]:
    """Nodes sorted by id; edges as (upstream_id, downstream_id), sorted and unique.

    A dependency with no action of its own still becomes a node, as a "source"; an
    action's own kind replaces that placeholder whichever order they appear in.
    """
    nodes: dict[str, Node] = {}
    edges: set[tuple[str, str]] = set()
    for action in data.get("compilationResultActions", []):
        node = _node(action.get("target", {}), _kind(action))
        if node.id not in nodes or nodes[node.id].kind == "source":
            nodes[node.id] = node
        for dep_target in _dependencies(action):
            dep = _node(dep_target, "source")
            nodes.setdefault(dep.id, dep)
            edges.add((dep.id, node.id))
    return sorted(nodes.values()), sorted(edges)


def latest_compilation_name(results: list[dict[str, Any]]) -> str | None:
    """The newest clean compilation of main, from a list already ordered newest first."""
    for result in results:
        if result.get("gitCommitish") == "main" and not result.get("compilationErrors"):
            return result.get("name")
    return None

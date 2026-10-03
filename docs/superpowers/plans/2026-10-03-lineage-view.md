# Lineage View Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A "Lineage" section in bgg-viewer's admin panel. It shows the Dataform lineage as a fixed left-to-right graph, and clicking a table shows its status, row count, size and schema. Pipeline's 14-day history cells also become links to their runs.

**Architecture:** One standard-library parser (`src/monitoring/lineage.py`) turns a Dataform compilation result into nodes and edges, and both the existing lineage GitHub action and the warehouse API use it. The API serves `GET /monitoring/lineage`, built from the latest clean compilation of `main` plus free BigQuery `tables.get` metadata, and `GET /monitoring/tables/{id}`, the schema. The viewer lays the graph out in pure TypeScript and draws it as SVG.

**Tech Stack:**
- Warehouse: Python 3.12, FastAPI, google-cloud-bigquery, google-auth, pytest (`uv run --extra test --extra api python -m pytest`)
- Viewer: SvelteKit 2, Svelte 5 runes, vitest (`pnpm test`), svelte-check (`pnpm check`)

**Spec:** `docs/superpowers/specs/2026-10-03-lineage-view-design.md` (this repo), building on `docs/superpowers/specs/2026-10-02-pipeline-monitor-design.md`.

## Global Constraints

- Warehouse branch: `feat/lineage-view` (exists; holds the spec). Viewer branch: `feat/lineage-view`, cut from `feat/pipeline-monitor` while bgg-viewer PR #81 is open, or from `main` once #81 has merged.
- Dataform API base: `https://dataform.googleapis.com/v1beta1/projects/bgg-data-warehouse/locations/us-central1/repositories/bgg-data-warehouse`. To find the latest result, list with `orderBy=create_time desc`. In zsh, write `${VAR}:query`, never `$VAR:query`: zsh treats `:q` as a modifier and swallows it.
- Node id: `"{project}.{dataset}.{name}"`. Kinds: `table`, `incremental`, `view`, `source`, `operation`, `assertion`.
- Table details come from `tables.get` metadata only. **No BigQuery queries** for lineage.
- Cache both new routes for 5 minutes in-process, like `/monitoring/pipeline`.
- `/monitoring/tables/{id}` only serves ids that are nodes in the current lineage (`400` otherwise); it returns `404` for a missing table and `403` for one it can't read.
- Status colours as on Pipeline: no green/red. Glyph + word, never colour alone.
- Admin-only: `404` for non-admins on every new viewer route, including the schema endpoint.
- Keep `__init__.py` files empty.
- Every commit message ends with:
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`
- Don't push, open PRs or merge without Phil's go-ahead.

## Review Focus

1. **Deploy order between the repos.** The warehouse PR changes each history cell from a status string to `{status, url}`. If it deploys before the viewer PR, the viewer already in production must still render the grid. `historyCell()` accepts both shapes. Pinned in Task 8 (`historyCell accepts the old string shape`).
2. **A table the API can't read**, such as a `bgg-predictive-models` table with no grant. That node shows "no access", and the rest of the graph still loads. Pinned in Task 3 (`test_meta_failure_on_one_table_does_not_fail_the_rest`).
3. **A schema request for an arbitrary table.** `GET /monitoring/tables/{id}` must refuse ids outside the lineage. Pinned in Task 4 (`test_table_schema_rejects_ids_outside_lineage`).
4. **The newest compilation is a branch build or has errors.** The view must use the latest clean compilation of `main`. Pinned in Task 1 (`test_latest_compilation_skips_branches_and_errors`).
5. **A cycle, or an edge to an unknown node.** The layout must still finish and place every node. Pinned in Task 8 (`places nodes in a cycle after everything else`).

---

# Part A — bgg-data-warehouse

All Part A commands run from `/Users/phenrickson/Documents/projects/bgg-data-warehouse`, on `feat/lineage-view`.

### Task 1: Shared lineage parser

**Files:**
- Create: `src/monitoring/lineage.py`
- Create: `tests/fixtures/dataform_compilation_2026-10-03.json` (captured; force-add, since `.gitignore` ignores `*.json`)
- Create: `tests/test_lineage.py`

**Interfaces:**
- Produces:
  - `Node(id: str, project: str, dataset: str, name: str, kind: str)`, a frozen, ordered dataclass
  - `parse_compilation_result(data: dict) -> tuple[list[Node], list[tuple[str, str]]]`, which returns nodes sorted by `id` and edges sorted, unique, as `(upstream_id, downstream_id)`
  - `latest_compilation_name(results: list[dict]) -> str | None`

- [ ] **Step 1: Capture the fixture** (Dataform API only; no BigQuery cost)

```bash
mkdir -p tests/fixtures
T=$(gcloud auth print-access-token)
B="https://dataform.googleapis.com/v1beta1/projects/bgg-data-warehouse/locations/us-central1/repositories/bgg-data-warehouse"
N=$(curl -s -H "Authorization: Bearer $T" "$B/compilationResults?pageSize=20&orderBy=create_time%20desc" \
  | uv run --no-project python -c "import sys,json; r=json.load(sys.stdin)['compilationResults']; print(next(x['name'] for x in r if x.get('gitCommitish')=='main' and not x.get('compilationErrors')))")
curl -s -H "Authorization: Bearer $T" "https://dataform.googleapis.com/v1beta1/${N}:query" | uv run --no-project python -c "
import sys, json
d = json.load(sys.stdin)
keep = []
for a in d['compilationResultActions']:
    slim = {'target': a['target']}
    for k in ('relation', 'operations', 'assertion'):
        if k in a:
            slim[k] = {kk: a[k][kk] for kk in ('relationType', 'dependencyTargets') if kk in a[k]}
    if 'declaration' in a:
        slim['declaration'] = {}
    keep.append(slim)
json.dump({'compilationResultActions': keep}, open('tests/fixtures/dataform_compilation_2026-10-03.json', 'w'), indent=2, sort_keys=True)
print(len(keep), 'actions')"
```

Expected: `50 actions`. The file keeps only targets, relation types and dependencies, with no SQL text.

- [ ] **Step 2: Write the failing tests** — `tests/test_lineage.py`:

```python
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
```

- [ ] **Step 3: Run to verify they fail**

Run: `uv run --extra test python -m pytest tests/test_lineage.py -q`
Expected: FAIL — `ImportError: cannot import name 'lineage' from 'src.monitoring'`.

- [ ] **Step 4: Implement** — `src/monitoring/lineage.py`:

```python
"""Dataform lineage: a compilation result as nodes and edges.

Standard library only, so the lineage GitHub action
(.github/actions/dataform-lineage) can import it without the project's dependencies.
See docs/superpowers/specs/2026-10-03-lineage-view-design.md.
"""

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
```

- [ ] **Step 5: Run to verify they pass**

Run: `uv run --extra test python -m pytest tests/test_lineage.py -q`
Expected: 6 passed.

- [ ] **Step 6: Commit**

```bash
git add -f tests/fixtures/dataform_compilation_2026-10-03.json
git add src/monitoring/lineage.py tests/test_lineage.py
git commit -m "feat(lineage): shared Dataform lineage parser that keeps the project

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 2: The lineage action uses the shared parser

**Files:**
- Modify: `.github/actions/dataform-lineage/generate_lineage.py`. Replace `make_node_id` and `parse_compilation_result` (lines 17–58) with an adapter; the renderers and `main()` stay as they are.
- Create: `tests/test_lineage_action.py`

**Interfaces:**
- Consumes: `parse_compilation_result` from Task 1.
- Produces: the same `docs/lineage.md` and `docs/lineage.html`. Node ids now include the project, and labels stay `dataset.name`.

- [ ] **Step 1: Write the failing test** — `tests/test_lineage_action.py`:

```python
"""The lineage GitHub action still writes both diagrams, now via the shared parser."""

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FIX = ROOT / "tests/fixtures/dataform_compilation_2026-10-03.json"


def _load_action():
    path = ROOT / ".github/actions/dataform-lineage/generate_lineage.py"
    spec = importlib.util.spec_from_file_location("generate_lineage", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_action_uses_the_shared_parser():
    action = _load_action()
    from src.monitoring import lineage
    assert action._parse is lineage.parse_compilation_result


def test_action_writes_mermaid_and_visjs(tmp_path, monkeypatch):
    action = _load_action()
    monkeypatch.setenv("COMPILATION_DETAILS", FIX.read_text())
    monkeypatch.setenv("OUTPUT_FORMATS", "mermaid,visjs")
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path))
    monkeypatch.delenv("GITHUB_OUTPUT", raising=False)
    action.main()
    md = (tmp_path / "lineage.md").read_text()
    assert "graph LR" in md
    assert ('bgg_data_warehouse_analytics_games_active["analytics.games_active"] --> '
            'bgg_data_warehouse_analytics_games_features["analytics.games_features"]') in md
    assert 'bgg_predictive_models_raw_ml_predictions_landing["raw.ml_predictions_landing"]' in md
    assert "vis.DataSet" in (tmp_path / "lineage.html").read_text()
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run --extra test python -m pytest tests/test_lineage_action.py -q`
Expected: FAIL. `action._parse` doesn't exist, and the Mermaid ids have no project prefix.

- [ ] **Step 3: Implement**

In `generate_lineage.py`, add `import re` to the imports. Then replace the `make_node_id` and `parse_compilation_result` functions (everything from `def make_node_id` through the `return nodes, edges` of `parse_compilation_result`) with:

```python
# The parser is shared with the warehouse API (src/monitoring/lineage.py, standard
# library only). The checked-out repo root is three levels above this file.
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from src.monitoring.lineage import parse_compilation_result as _parse  # noqa: E402


def make_node_id(node_id: str) -> str:
    """A Mermaid/vis.js-safe id (alphanumeric with underscores)."""
    return re.sub(r"[^0-9A-Za-z_]", "_", node_id)


def parse_compilation_result(compilation_data: dict) -> tuple[set, list]:
    """The shared parser, adapted to this script's (id, label, group) node tuples."""
    nodes, edges = _parse(compilation_data)
    print(f"Processing {len(nodes)} nodes from compilation result")
    node_tuples = {(make_node_id(n.id), f"{n.dataset}.{n.name}", n.dataset) for n in nodes}
    return node_tuples, [(make_node_id(a), make_node_id(b)) for a, b in edges]
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run --extra test python -m pytest tests/test_lineage_action.py tests/test_lineage.py -q`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add .github/actions/dataform-lineage/generate_lineage.py tests/test_lineage_action.py
git commit -m "refactor(lineage): action renders from the shared parser

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 3: Lineage readers — Dataform and table metadata

**Files:**
- Create: `src/warehouse/readers/lineage.py`
- Create: `tests/test_lineage_reader.py`

**Interfaces:**
- Consumes: `latest_compilation_name` from Task 1.
- Produces:
  - `DATAFORM_REPO: str`
  - `fetch_compilation(session=None) -> tuple[dict, dict]`, which returns the summary of the chosen compilation result (`name`, `createTime`, `resolvedGitCommitSha`) and its `{"compilationResultActions": [...]}`, with all pages joined
  - `fetch_table_meta(ids: list[str], client=None) -> dict[str, dict]`, where each value has keys `rows`, `bytes`, `last_modified`, `type`, `error`
  - `fetch_table_schema(table_id: str, client=None) -> list[dict]`, with `name`, `type`, `mode`, `description`, and nested fields as `parent.child`. It raises `google.api_core.exceptions.NotFound` / `Forbidden`

- [ ] **Step 1: Write the failing tests** — `tests/test_lineage_reader.py`:

```python
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
```

- [ ] **Step 2: Run to verify they fail**

Run: `uv run --extra test python -m pytest tests/test_lineage_reader.py -q`
Expected: FAIL — `ImportError` on `src.warehouse.readers.lineage`.

- [ ] **Step 3: Implement** — `src/warehouse/readers/lineage.py`:

```python
"""Reader for the admin lineage view.

The latest clean Dataform compilation of main (the lineage), plus BigQuery table
metadata from free ``tables.get`` calls — never a query. See
docs/superpowers/specs/2026-10-03-lineage-view-design.md.
"""

from concurrent.futures import ThreadPoolExecutor
from typing import Any, Optional

import google.auth
from google.api_core import exceptions as gexc
from google.auth.transport.requests import AuthorizedSession
from google.cloud import bigquery

from src.monitoring.lineage import latest_compilation_name
from src.warehouse.bq import get_client

DATAFORM_REPO = (
    "https://dataform.googleapis.com/v1beta1/projects/bgg-data-warehouse"
    "/locations/us-central1/repositories/bgg-data-warehouse"
)
_EMPTY_META = {"rows": None, "bytes": None, "last_modified": None, "type": None, "error": None}


def _session() -> AuthorizedSession:
    credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    return AuthorizedSession(credentials)


def fetch_compilation(session=None) -> tuple[dict[str, Any], dict[str, Any]]:
    """(summary, actions) of the newest clean compilation of main."""
    session = session or _session()
    resp = session.get(f"{DATAFORM_REPO}/compilationResults",
                       params={"pageSize": 20, "orderBy": "create_time desc"}, timeout=30)
    resp.raise_for_status()
    results = resp.json().get("compilationResults", [])
    name = latest_compilation_name(results)
    if name is None:
        raise RuntimeError("no clean compilation of main among the latest 20")
    summary = next(r for r in results if r.get("name") == name)

    actions: list[dict[str, Any]] = []
    params: dict[str, str] | None = None
    while True:
        resp = session.get(f"https://dataform.googleapis.com/v1beta1/{name}:query",
                           params=params, timeout=30)
        resp.raise_for_status()
        page = resp.json()
        actions.extend(page.get("compilationResultActions", []))
        token = page.get("nextPageToken")
        if not token:
            return summary, {"compilationResultActions": actions}
        params = {"pageToken": token}


def _meta(client: bigquery.Client, table_id: str) -> dict[str, Any]:
    try:
        table = client.get_table(table_id)
    except gexc.NotFound:
        return _EMPTY_META | {"error": "not found"}
    except gexc.Forbidden:
        return _EMPTY_META | {"error": "no access"}
    except gexc.GoogleAPICallError as exc:
        return _EMPTY_META | {"error": f"{exc.code}: {exc.message}"}
    return {"rows": table.num_rows, "bytes": table.num_bytes, "last_modified": table.modified,
            "type": table.table_type, "error": None}


def fetch_table_meta(ids: list[str], client: Optional[bigquery.Client] = None) -> dict[str, dict]:
    """Row count, size, last modified and type per table; a failure stays on its node."""
    client = client or get_client()
    with ThreadPoolExecutor(max_workers=8) as pool:
        return dict(zip(ids, pool.map(lambda table_id: _meta(client, table_id), ids)))


def _flatten(fields, prefix: str = "") -> list[dict[str, Any]]:
    out = []
    for f in fields:
        name = f"{prefix}{f.name}"
        out.append({"name": name, "type": f.field_type, "mode": f.mode,
                    "description": f.description})
        if f.fields:
            out.extend(_flatten(f.fields, f"{name}."))
    return out


def fetch_table_schema(table_id: str, client: Optional[bigquery.Client] = None) -> list[dict]:
    """Column name, type, mode and description; nested RECORD fields as parent.child."""
    client = client or get_client()
    return _flatten(client.get_table(table_id).schema)
```

- [ ] **Step 4: Run to verify they pass**

Run: `uv run --extra test python -m pytest tests/test_lineage_reader.py -q`
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add src/warehouse/readers/lineage.py tests/test_lineage_reader.py
git commit -m "feat(lineage): readers for the Dataform compilation and table metadata

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 4: `GET /monitoring/lineage` and `GET /monitoring/tables/{id}`

**Files:**
- Modify: `services/warehouse_api/routers/monitoring.py`
- Modify: `tests/test_monitoring_router.py` (append)

**Interfaces:**
- Consumes: `parse_compilation_result` (Task 1); `fetch_compilation`, `fetch_table_meta` and `fetch_table_schema` (Task 3); `iso` from `src.monitoring.github`.
- Produces:
  - `GET /monitoring/lineage` → `{"generated_at", "compilation": {"name", "created", "commit"}, "nodes": [{id, project, dataset, name, kind, rows, bytes, last_modified, type, error}], "edges": [[up, down], ...]}`
  - `GET /monitoring/tables/{table_id}` → `{"id", "schema": [...]}`, or `400`/`403`/`404`

- [ ] **Step 1: Write the failing tests** (append to `tests/test_monitoring_router.py`):

```python
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
    assert node["kind"] == "table" and node["rows"] == 1 and node["error"] is None
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
```

- [ ] **Step 2: Run to verify they fail**

Run: `uv run --extra test --extra api python -m pytest tests/test_monitoring_router.py -q -k "lineage or table_schema"`
Expected: FAIL — `AttributeError: ... has no attribute 'lineage_reader'` (fixture errors).

- [ ] **Step 3: Implement** in `services/warehouse_api/routers/monitoring.py`:

1. Add to the imports:

```python
from dataclasses import asdict

from google.api_core import exceptions as gexc

from src.monitoring import lineage as lineage_mod
from src.monitoring.github import iso
from src.warehouse.readers import lineage as lineage_reader
```

(Merge `from src.monitoring.github import fetch_runs` and `iso` into one line.)

2. Next to `_pipeline_cache`, add `_lineage_cache: dict[str, tuple[float, dict]] = {}` and `_schema_cache: dict[str, tuple[float, dict]] = {}`, and have `_reset_cache()` also clear them.

3. Append:

```python
def _lineage() -> dict:
    """The lineage graph with per-table metadata, cached like the pipeline report."""
    cached = _lineage_cache.get("lineage")
    if cached is not None and time.time() - cached[0] < _CACHE_TTL_SECONDS:
        return cached[1]
    try:
        summary, data = lineage_reader.fetch_compilation()
        nodes, edges = lineage_mod.parse_compilation_result(data)
        meta = lineage_reader.fetch_table_meta([n.id for n in nodes])
    except Exception as exc:  # Dataform or BigQuery: report it, don't serve a partial graph
        raise HTTPException(502, f"lineage unavailable: {exc}") from exc
    sha = summary.get("resolvedGitCommitSha") or ""
    result = {
        "generated_at": iso(datetime.now(UTC)),
        "compilation": {"name": summary.get("name"), "created": summary.get("createTime"),
                        "commit": sha[:7] or None},
        "nodes": [asdict(n) | meta.get(n.id, {}) for n in nodes],
        "edges": [list(e) for e in edges],
    }
    _lineage_cache["lineage"] = (time.time(), result)
    return result


@router.get("/monitoring/lineage")
def get_lineage():
    """Dataform lineage of the latest clean compilation of main, with table metadata."""
    return _lineage()


@router.get("/monitoring/tables/{table_id}")
def get_table_schema(table_id: str):
    """Schema of one table in the current lineage (``project.dataset.table``)."""
    cached = _schema_cache.get(table_id)
    if cached is not None and time.time() - cached[0] < _CACHE_TTL_SECONDS:
        return cached[1]
    if table_id not in {n["id"] for n in _lineage()["nodes"]}:
        raise HTTPException(400, "not a table in the current lineage")
    try:
        schema = lineage_reader.fetch_table_schema(table_id)
    except gexc.NotFound as exc:
        raise HTTPException(404, f"{table_id} not found") from exc
    except gexc.Forbidden as exc:
        raise HTTPException(403, f"the warehouse API cannot read {table_id}") from exc
    result = {"id": table_id, "schema": schema}
    _schema_cache[table_id] = (time.time(), result)
    return result
```

- [ ] **Step 4: Run the router suite**

Run: `uv run --extra test --extra api python -m pytest tests/test_monitoring_router.py -q`
Expected: all pass, old and new.

- [ ] **Step 5: Commit**

```bash
git add services/warehouse_api/routers/monitoring.py tests/test_monitoring_router.py
git commit -m "feat(api): GET /monitoring/lineage and /monitoring/tables/{id}

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 5: History cells carry their run URL

**Files:**
- Modify: `src/monitoring/chain.py`, in the `history` list in `build_report`
- Modify: `tests/test_chain.py`, in `test_build_report_shape`

**Interfaces:**
- Produces: `history[].stages[key]`, which is now `{"status": str, "url": str | None}`.

- [ ] **Step 1: Change the test first.** In `test_build_report_shape`, replace the two `history` assertions with:

```python
    assert report["history"][-1]["stages"]["viewer_artifacts"]["status"] == "ok"
    assert report["history"][-1]["stages"]["viewer_artifacts"]["url"].startswith("https://github.com/")
    assert report["history"][0]["stages"]["fetch_thing_ids"] == {"status": "fail", "url": None}
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run --extra test python -m pytest tests/test_chain.py -q -k build_report_shape`
Expected: FAIL — `TypeError: string indices must be integers`.

- [ ] **Step 3: Implement.** In `build_report`, change the history comprehension's `stages` value to:

```python
{s.key: {"status": s.status, "url": s.url} for s in c.stages}
```

- [ ] **Step 4: Run the chain and router suites**

Run: `uv run --extra test --extra api python -m pytest tests/test_chain.py tests/test_monitoring_router.py -q`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/monitoring/chain.py tests/test_chain.py
git commit -m "feat(monitor): history cells carry their run URL

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 6: Full suite, local check, README

- [ ] **Step 1: Full monitoring suite**

Run: `uv run --extra test --extra api python -m pytest tests/test_lineage.py tests/test_lineage_action.py tests/test_lineage_reader.py tests/test_monitoring_router.py tests/test_chain.py tests/test_pipeline_reader.py tests/test_monitoring_github.py tests/test_pipeline_status.py -q`
Expected: all pass.

- [ ] **Step 2: Local API check** (Phil's ADC must be personal: run `use-personal` first in that terminal). This reads Dataform and table metadata only, with no BigQuery query cost:

```bash
GH_TOKEN=$(gh auth token) uv run python -m services.warehouse_api.main &   # :8080
sleep 5
curl -s localhost:8080/monitoring/lineage | uv run --no-project python -c "
import json, sys; d = json.load(sys.stdin)
print(d['compilation']); print(len(d['nodes']), 'nodes,', len(d['edges']), 'edges')
print([n['id'] for n in d['nodes'] if n['error']])"
curl -s localhost:8080/monitoring/tables/bgg-data-warehouse.analytics.games_features | head -c 300
kill %1
```

Expected: about 50 nodes and 70+ edges. Only `bgg-predictive-models` tables (if any) have errors, and the schema returns columns. Record which nodes had errors in the ledger, for the PR description.

- [ ] **Step 3: README.** In the paragraph that describes `GET /monitoring/pipeline`, add:

```markdown
`GET /monitoring/lineage` serves the Dataform lineage (latest clean compilation of
`main`, parsed by `src/monitoring/lineage.py`, which the lineage action also uses) with
row counts and last-modified times from BigQuery table metadata, and
`GET /monitoring/tables/{project.dataset.table}` serves one table's schema; both back
the admin Lineage page.
```

- [ ] **Step 4: Commit** (no push; pushing and the PR are Phil's call)

```bash
git add README.md
git commit -m "docs: lineage endpoints in README

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

# Part B — bgg-viewer

All Part B commands run from `/Users/phenrickson/Documents/projects/bgg-viewer`. First: `git switch feat/pipeline-monitor && git pull --ff-only && git switch -c feat/lineage-view` if PR #81 is still open; otherwise `git switch main && git pull --ff-only && git switch -c feat/lineage-view`.

### Task 7: Types and client methods

**Files:**
- Modify: `src/lib/server/warehouse/types.ts`, `client.ts`, `index.ts`
- Modify: `src/lib/server/warehouse/client.test.ts` (append)

**Interfaces:**
- Produces:
  - types `LineageKind`, `LineageNode`, `Lineage`, `SchemaField`, `TableSchema` and `HistoryCell`
  - `PipelineStatus.history[].stages` becomes `Record<string, HistoryCell | StageStatusName>`, both shapes, for deploy order
  - `getLineage(): Promise<Lineage>` and `getTableSchema(id: string): Promise<TableSchema>`
  - A failure throws `WarehouseError(status, message)` with the API `detail` included

- [ ] **Step 1: Write the failing tests** (append to `client.test.ts`):

```ts
describe('createWarehouseClient lineage', () => {
	const lineage = {
		generated_at: '2026-10-03T18:00:00Z',
		compilation: { name: 'c1', created: '2026-10-03T17:46:47Z', commit: '4b04de4' },
		nodes: [],
		edges: []
	};
	const make = (fetchImpl: typeof fetch) =>
		createWarehouseClient({ baseUrl: 'https://warehouse.example', getIdToken: async () => 'tok', fetch: fetchImpl });

	it('GETs /monitoring/lineage', async () => {
		const { fetchImpl, calls } = stubFetch(lineage);
		expect(await make(fetchImpl).getLineage()).toEqual(lineage);
		expect(calls[0].url).toBe('https://warehouse.example/monitoring/lineage');
	});

	it('GETs one table schema with the id encoded', async () => {
		const { fetchImpl, calls } = stubFetch({ id: 'p.d.t', schema: [] });
		await make(fetchImpl).getTableSchema('bgg-data-warehouse.analytics.games features');
		expect(calls[0].url).toBe(
			'https://warehouse.example/monitoring/tables/bgg-data-warehouse.analytics.games%20features'
		);
	});

	it('carries the API detail and status on failure', async () => {
		const { fetchImpl } = stubFetch({ detail: 'the warehouse API cannot read p.d.t' }, { status: 403 });
		const err = await make(fetchImpl).getTableSchema('p.d.t').catch((e) => e);
		expect(err).toBeInstanceOf(WarehouseError);
		expect(err.status).toBe(403);
		expect(err.message).toContain('cannot read p.d.t');
	});
});
```

- [ ] **Step 2: Run to verify they fail**

Run: `pnpm vitest run src/lib/server/warehouse/client.test.ts`
Expected: FAIL — `getLineage is not a function`.

- [ ] **Step 3: Implement**

Append to `types.ts`:

```ts
/** One history cell: its status and the run it came from. Old APIs sent the status alone. */
export interface HistoryCell {
	status: StageStatusName;
	url: string | null;
}

export type LineageKind = 'table' | 'incremental' | 'view' | 'source' | 'operation' | 'assertion';

export interface LineageNode {
	id: string;
	project: string;
	dataset: string;
	name: string;
	kind: LineageKind;
	rows: number | null;
	bytes: number | null;
	last_modified: string | null;
	type: string | null;
	error: string | null;
}

export interface Lineage {
	generated_at: string;
	compilation: { name: string; created: string | null; commit: string | null };
	nodes: LineageNode[];
	edges: [string, string][];
}

export interface SchemaField {
	name: string;
	type: string;
	mode: string | null;
	description: string | null;
}

export interface TableSchema {
	id: string;
	schema: SchemaField[];
}
```

In `PipelineStatus`, change the `history` line to:

```ts
	history: { day: string; stages: Record<string, HistoryCell | StageStatusName> }[];
```

In `client.ts`:
- Import `Lineage` and `TableSchema` alongside the other types.
- Add to `WarehouseClient`: `getLineage(): Promise<Lineage>;` and `getTableSchema(id: string): Promise<TableSchema>;`.
- Above `return {`, add a shared failure helper:

```ts
	/** A WarehouseError carrying the API's `detail` (missing token, no access, …), shown on admin pages. */
	async function failure(res: Response, path: string): Promise<WarehouseError> {
		const detail = await res
			.json()
			.then((b: { detail?: string }) => b.detail ?? '')
			.catch(() => '');
		return new WarehouseError(
			res.status,
			`warehouse GET ${path} failed (${res.status})${detail ? `: ${detail}` : ''}`
		);
	}
```

- In `getPipelineStatus`, replace its inline detail-reading `if (!res.ok) { … }` block with `if (!res.ok) throw await failure(res, '/monitoring/pipeline');`.
- Add, after `getPipelineStatus`:

```ts
		async getLineage(): Promise<Lineage> {
			const res = await authedGet('/monitoring/lineage');
			if (!res.ok) throw await failure(res, '/monitoring/lineage');
			return (await res.json()) as Lineage;
		},

		async getTableSchema(id: string): Promise<TableSchema> {
			const path = `/monitoring/tables/${encodeURIComponent(id)}`;
			const res = await authedGet(path);
			if (!res.ok) throw await failure(res, path);
			return (await res.json()) as TableSchema;
		}
```

In `index.ts`, add `HistoryCell, LineageKind, LineageNode, Lineage, SchemaField, TableSchema` to the `export type { … }` list.

- [ ] **Step 4: Run to verify they pass**

Run: `pnpm vitest run src/lib/server/warehouse/client.test.ts`
Expected: all pass (the existing getPipelineStatus tests still pass after the refactor).

- [ ] **Step 5: Commit**

```bash
git add src/lib/server/warehouse
git commit -m "feat(warehouse): getLineage and getTableSchema client methods

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 8: Layout, node status and history cells (pure helpers)

**Files:**
- Create: `src/lib/monitoring/lineage-layout.ts`, `src/lib/monitoring/lineage-layout.test.ts`
- Modify: `src/lib/monitoring/display.ts` and `display.test.ts`, adding `nodeStatus` and `historyCell`

**Interfaces:**
- Produces:
  - `layoutLineage(nodes: {id, dataset, name}[], edges: [string, string][]): { placements: Map<string, {column, row}>; columns: number; rows: number; upstream: Map<string, string[]>; downstream: Map<string, string[]> }`
  - `DATASET_ORDER: string[]`
  - `nodeStatus(node: LineageNode, reference: string): Freshness | null`
  - `historyCell(cell: HistoryCell | StageStatusName | undefined): HistoryCell`

- [ ] **Step 1: Write the failing tests**

`src/lib/monitoring/lineage-layout.test.ts`:

```ts
import { describe, expect, it } from 'vitest';
import { layoutLineage } from './lineage-layout';

const n = (id: string, dataset = 'analytics') => ({ id, dataset, name: id });

describe('layoutLineage', () => {
	it('puts each node one column right of its deepest input', () => {
		const out = layoutLineage([n('a', 'raw'), n('b'), n('c')], [['a', 'b'], ['b', 'c'], ['a', 'c']]);
		expect(out.placements.get('a')!.column).toBe(0);
		expect(out.placements.get('b')!.column).toBe(1);
		expect(out.placements.get('c')!.column).toBe(2);
		expect(out.columns).toBe(3);
		expect(out.upstream.get('c')).toEqual(['b', 'a']);
		expect(out.downstream.get('a')).toEqual(['b', 'c']);
	});

	it('orders a column by dataset, then name', () => {
		const out = layoutLineage([n('z', 'analytics'), n('y', 'raw'), n('x', 'analytics')], []);
		const row = (id: string) => out.placements.get(id)!.row;
		expect([row('y'), row('x'), row('z')]).toEqual([0, 1, 2]);
		expect(out.rows).toBe(3);
	});

	it('places nodes in a cycle after everything else, and ignores edges to unknown nodes', () => {
		const out = layoutLineage([n('a'), n('x'), n('y')], [['a', 'x'], ['x', 'y'], ['y', 'x'], ['a', 'ghost']]);
		expect(out.placements.get('a')!.column).toBe(0);
		expect(out.placements.get('x')!.column).toBe(1);
		expect(out.placements.get('y')!.column).toBe(1);
		expect(out.placements.size).toBe(3);
	});
});
```

Append to `display.test.ts` (and add `historyCell, nodeStatus` to its import list):

```ts
describe('historyCell', () => {
	it('accepts the old string shape, the new object shape, and a missing cell', () => {
		expect(historyCell('ok')).toEqual({ status: 'ok', url: null });
		expect(historyCell({ status: 'fail', url: 'https://github.com/x' })).toEqual({
			status: 'fail',
			url: 'https://github.com/x'
		});
		expect(historyCell(undefined)).toEqual({ status: 'not_reached', url: null });
	});
});

describe('nodeStatus', () => {
	const ref = '2026-10-03T06:26:05Z';
	const node = {
		id: 'p.d.t', project: 'p', dataset: 'd', name: 't', kind: 'table' as const,
		rows: 1, bytes: 1, last_modified: '2026-10-03T07:19:00Z', type: 'TABLE', error: null
	};
	it('is the freshness of a table', () => {
		expect(nodeStatus(node, ref)).toEqual({ tone: 'ok', label: 'Fresh' });
		expect(nodeStatus({ ...node, last_modified: '2026-10-01T07:00:00Z' }, ref)?.tone).toBe('warn');
	});
	it('is null for views, unreadable tables and tables without a timestamp', () => {
		expect(nodeStatus({ ...node, kind: 'view' }, ref)).toBeNull();
		expect(nodeStatus({ ...node, error: 'no access' }, ref)).toBeNull();
		expect(nodeStatus({ ...node, last_modified: null }, ref)).toBeNull();
	});
});
```

- [ ] **Step 2: Run to verify they fail**

Run: `pnpm vitest run src/lib/monitoring/`
Expected: FAIL — the layout module is missing, and `historyCell`/`nodeStatus` aren't exported.

- [ ] **Step 3: Implement**

`src/lib/monitoring/lineage-layout.ts`:

```ts
/**
 * Fixed left-to-right layout for the Dataform lineage: a node sits one column right
 * of its deepest input (longest path from a source). Within a column, nodes are
 * ordered by dataset, then name. Pure, so it is unit-tested.
 */

export const DATASET_ORDER = ['raw', 'core', 'staging', 'analytics', 'predictions', 'monitoring', 'collections'];

export interface LayoutInput {
	id: string;
	dataset: string;
	name: string;
}

export interface Placement {
	column: number;
	row: number;
}

export interface Layout {
	placements: Map<string, Placement>;
	columns: number;
	rows: number;
	upstream: Map<string, string[]>;
	downstream: Map<string, string[]>;
}

const rank = (dataset: string) => {
	const i = DATASET_ORDER.indexOf(dataset);
	return i === -1 ? DATASET_ORDER.length : i;
};

export function layoutLineage(nodes: LayoutInput[], edges: [string, string][]): Layout {
	const upstream = new Map(nodes.map((n) => [n.id, [] as string[]]));
	const downstream = new Map(nodes.map((n) => [n.id, [] as string[]]));
	for (const [up, down] of edges) {
		if (!upstream.has(up) || !upstream.has(down)) continue; // an edge to an unknown node
		upstream.get(down)!.push(up);
		downstream.get(up)!.push(down);
	}

	// Kahn's algorithm, keeping the longest path. Nodes never resolved sit in a cycle.
	const depth = new Map<string, number>();
	const waiting = new Map(nodes.map((n) => [n.id, upstream.get(n.id)!.length]));
	const queue = nodes.filter((n) => waiting.get(n.id) === 0).map((n) => n.id);
	for (const id of queue) depth.set(id, 0);
	const resolved = new Set<string>();
	while (queue.length) {
		const id = queue.shift()!;
		resolved.add(id);
		for (const down of downstream.get(id)!) {
			depth.set(down, Math.max(depth.get(down) ?? 0, depth.get(id)! + 1));
			waiting.set(down, waiting.get(down)! - 1);
			if (waiting.get(down) === 0) queue.push(down);
		}
	}
	const deepest = Math.max(-1, ...[...resolved].map((id) => depth.get(id)!));
	const columnOf = (id: string) => (resolved.has(id) ? depth.get(id)! : deepest + 1);

	const byColumn = new Map<number, LayoutInput[]>();
	for (const node of nodes) {
		const column = columnOf(node.id);
		byColumn.set(column, [...(byColumn.get(column) ?? []), node]);
	}
	const placements = new Map<string, Placement>();
	let rows = 0;
	for (const [column, members] of byColumn) {
		members.sort(
			(a, b) => rank(a.dataset) - rank(b.dataset) || a.name.localeCompare(b.name) || a.id.localeCompare(b.id)
		);
		members.forEach((node, row) => placements.set(node.id, { column, row }));
		rows = Math.max(rows, members.length);
	}
	return { placements, columns: byColumn.size ? Math.max(...byColumn.keys()) + 1 : 0, rows, upstream, downstream };
}
```

Append to `display.ts`, and extend its type import to
`import type { HistoryCell, LineageNode, StageStatusName } from '$lib/server/warehouse';`:

```ts
/** A history cell from either API shape: `{status, url}` now, a bare status before. */
export function historyCell(cell: HistoryCell | StageStatusName | undefined): HistoryCell {
	if (cell == null) return { status: 'not_reached', url: null };
	return typeof cell === 'string' ? { status: cell, url: null } : cell;
}

/**
 * A lineage node's status: the same freshness rule as Pipeline's tables. None for
 * views (their last-modified is when the definition changed) or unreadable tables.
 */
export function nodeStatus(node: LineageNode, reference: string): Freshness | null {
	if (node.kind === 'view' || node.error || !node.last_modified) return null;
	return freshness(node.last_modified, reference);
}
```

- [ ] **Step 4: Run to verify they pass**

Run: `pnpm vitest run src/lib/monitoring/`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/lib/monitoring
git commit -m "feat(admin): lineage layout, node status and history-cell helpers

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 9: Routes — Lineage section, page load, schema endpoint

**Files:**
- Modify: `src/routes/(app)/admin/+layout.server.ts`, adding a `SECTIONS` entry
- Modify: `src/routes/(app)/admin/layout.server.test.ts`, updating the expected sections
- Create: `src/routes/(app)/admin/lineage/+page.server.ts` and `page.server.test.ts`
- Create: `src/routes/(app)/admin/lineage/schema/+server.ts` and `server.test.ts`

**Interfaces:**
- Consumes: `getLineage`, `getTableSchema` and `getPipelineStatus` (Task 7); `isAdmin`.
- Produces:
  - Page data: `{ lineage: Lineage | null; error: string | null; pipeline: PipelineStatus | null }`
  - `GET /admin/lineage/schema?id=…` → `TableSchema` JSON, or `400`, `404` (non-admin) or the warehouse status

- [ ] **Step 1: Write the failing tests**

In `layout.server.test.ts`, change the expected sections to:

```ts
		expect(data.sections).toEqual([
			{ href: '/admin/pipeline', label: 'Pipeline' },
			{ href: '/admin/lineage', label: 'Lineage' }
		]);
```

`src/routes/(app)/admin/lineage/page.server.test.ts`:

```ts
import { beforeEach, describe, expect, it, vi } from 'vitest';

const { getLineage, getPipelineStatus } = vi.hoisted(() => ({
	getLineage: vi.fn(),
	getPipelineStatus: vi.fn()
}));
vi.mock('$lib/server/warehouse', () => ({ warehouseClient: () => ({ getLineage, getPipelineStatus }) }));

const { load } = await import('./+page.server');
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const run = (user: unknown) => (load as any)({ locals: { user } });
const ADMIN = { email: 'phil.henrickson@gmail.com' };

describe('/admin/lineage load', () => {
	beforeEach(() => {
		getLineage.mockReset().mockResolvedValue({ nodes: [], edges: [] });
		getPipelineStatus.mockReset().mockResolvedValue({ today: { day: '2026-10-03', stages: [] } });
	});

	it('404s for a non-admin', async () => {
		await expect(run({ email: 'x@example.com' })).rejects.toMatchObject({ status: 404 });
		expect(getLineage).not.toHaveBeenCalled();
	});

	it('loads lineage and pipeline status for the admin', async () => {
		const data = await run(ADMIN);
		expect(data.lineage).toEqual({ nodes: [], edges: [] });
		expect(data.error).toBeNull();
		expect(data.pipeline.today.day).toBe('2026-10-03');
	});

	it('shows a lineage failure as a message', async () => {
		getLineage.mockRejectedValue(new Error('warehouse GET /monitoring/lineage failed (502)'));
		const data = await run(ADMIN);
		expect(data.lineage).toBeNull();
		expect(data.error).toContain('(502)');
	});

	it('still renders the graph when pipeline status fails', async () => {
		getPipelineStatus.mockRejectedValue(new Error('503'));
		const data = await run(ADMIN);
		expect(data.lineage).not.toBeNull();
		expect(data.pipeline).toBeNull();
	});
});
```

`src/routes/(app)/admin/lineage/schema/server.test.ts`:

```ts
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { WarehouseError } from '$lib/server/warehouse/types';

const { getTableSchema } = vi.hoisted(() => ({ getTableSchema: vi.fn() }));
vi.mock('$lib/server/warehouse', async () => ({
	...(await vi.importActual<object>('$lib/server/warehouse/types')),
	warehouseClient: () => ({ getTableSchema })
}));

const { GET } = await import('./+server');
const ADMIN = { email: 'phil.henrickson@gmail.com' };
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const call = (user: unknown, id: string | null) =>
	(GET as any)({ locals: { user }, url: new URL(`http://x/admin/lineage/schema${id ? `?id=${id}` : ''}`) });

describe('/admin/lineage/schema', () => {
	beforeEach(() => getTableSchema.mockReset().mockResolvedValue({ id: 'p.d.t', schema: [] }));

	it('404s for a non-admin', async () => {
		await expect(call({ email: 'x@example.com' }, 'p.d.t')).rejects.toMatchObject({ status: 404 });
	});

	it('400s without an id', async () => {
		await expect(call(ADMIN, null)).rejects.toMatchObject({ status: 400 });
	});

	it('returns the schema', async () => {
		const res = await call(ADMIN, 'p.d.t');
		expect(await res.json()).toEqual({ id: 'p.d.t', schema: [] });
		expect(getTableSchema).toHaveBeenCalledWith('p.d.t');
	});

	it('passes a warehouse status through', async () => {
		getTableSchema.mockRejectedValue(new WarehouseError(403, 'no access'));
		await expect(call(ADMIN, 'p.d.t')).rejects.toMatchObject({ status: 403 });
	});
});
```

- [ ] **Step 2: Run to verify they fail**

Run: `pnpm vitest run "src/routes/(app)/admin/"`
Expected: FAIL. The sections test fails, and the two new modules aren't found.

- [ ] **Step 3: Implement**

In `+layout.server.ts`, change `SECTIONS` to:

```ts
const SECTIONS = [
	{ href: '/admin/pipeline', label: 'Pipeline' },
	{ href: '/admin/lineage', label: 'Lineage' }
];
```

`src/routes/(app)/admin/lineage/+page.server.ts`:

```ts
import { error } from '@sveltejs/kit';
import { isAdmin } from '$lib/server/auth/admin';
import { warehouseClient, type Lineage, type PipelineStatus } from '$lib/server/warehouse';
import type { PageServerLoad } from './$types';

const message = (e: unknown) => (e instanceof Error ? e.message : String(e));

/**
 * Admin-only lineage graph. Pipeline status is loaded alongside for the freshness
 * reference and coverage; if it fails the graph still renders (with a fallback
 * reference). A lineage failure renders as a message, never a 500.
 */
export const load: PageServerLoad = async ({ locals }) => {
	if (!isAdmin(locals.user)) error(404, 'Not found');
	const [lineage, pipeline] = await Promise.all([
		Promise.resolve()
			.then(() => warehouseClient().getLineage())
			.then(
				(l: Lineage) => ({ lineage: l, error: null }),
				(e: unknown) => ({ lineage: null, error: message(e) })
			),
		Promise.resolve()
			.then(() => warehouseClient().getPipelineStatus(1))
			.then(
				(p: PipelineStatus) => p,
				() => null
			)
	]);
	return { ...lineage, pipeline };
};
```

`src/routes/(app)/admin/lineage/schema/+server.ts`:

```ts
import { error, json } from '@sveltejs/kit';
import { isAdmin } from '$lib/server/auth/admin';
import { warehouseClient, WarehouseError } from '$lib/server/warehouse';
import type { RequestHandler } from './$types';

/** One table's schema for the lineage detail panel, fetched when a node is clicked. Admin-only. */
export const GET: RequestHandler = async ({ locals, url }) => {
	if (!isAdmin(locals.user)) error(404, 'Not found');
	const id = url.searchParams.get('id');
	if (!id) error(400, 'id is required');
	try {
		return json(await warehouseClient().getTableSchema(id));
	} catch (e) {
		const status = e instanceof WarehouseError && e.status >= 400 && e.status < 600 ? e.status : 502;
		error(status, e instanceof Error ? e.message : String(e));
	}
};
```

- [ ] **Step 4: Run to verify they pass**

Run: `pnpm vitest run "src/routes/(app)/admin/"`
Expected: all pass. If `vi.importActual` on the types module fails, mock `$lib/server/warehouse` as `{ warehouseClient: …, WarehouseError }`, with `WarehouseError` imported in the test from `$lib/server/warehouse/types`, and note the change in the ledger.

- [ ] **Step 5: Commit**

```bash
git add "src/routes/(app)/admin"
git commit -m "feat(admin): Lineage section, page load and schema endpoint

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 10: Components and history links

These are presentation only. All their logic lives in the helpers tested in Tasks 8–9. Verify with `pnpm check`, `pnpm test`, and the end-to-end look in Task 11.

**Files:**
- Create: `src/lib/monitoring/LineageGraph.svelte`, `src/lib/monitoring/TableDetail.svelte`, `src/routes/(app)/admin/lineage/+page.svelte`
- Modify: `src/lib/monitoring/HistoryGrid.svelte`

- [ ] **Step 1: `LineageGraph.svelte`**

```svelte
<script lang="ts">
  import type { LineageNode } from '$lib/server/warehouse';
  import { layoutLineage } from './lineage-layout';
  import { nodeStatus, STATUS_GLYPH } from './display';

  let {
    nodes,
    edges,
    selected,
    reference,
    onselect
  }: {
    nodes: LineageNode[];
    edges: [string, string][];
    selected: string | null;
    reference: string;
    onselect: (id: string) => void;
  } = $props();

  const COL_W = 236, ROW_H = 54, BOX_W = 204, BOX_H = 40, PAD = 12;
  const layout = $derived(layoutLineage(nodes, edges));
  const width = $derived(layout.columns * COL_W + PAD);
  const height = $derived(layout.rows * ROW_H + PAD);
  const pos = (id: string) => {
    const p = layout.placements.get(id)!;
    return { x: PAD + p.column * COL_W, y: PAD + p.row * ROW_H };
  };
  const related = $derived(
    selected
      ? new Set([selected, ...(layout.upstream.get(selected) ?? []), ...(layout.downstream.get(selected) ?? [])])
      : null
  );
  const short = (s: string, max = 26) => (s.length > max ? `${s.slice(0, max - 1)}…` : s);
  const tone = (n: LineageNode) => {
    const s = nodeStatus(n, reference);
    return s ? s.tone : 'idle';
  };
  const key = (e: KeyboardEvent, id: string) => {
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault();
      onselect(id);
    }
  };
</script>

<div class="frame">
  <svg {width} {height} role="group" aria-label="Dataform lineage">
    {#each edges as [up, down] (`${up}>${down}`)}
      {#if layout.placements.has(up) && layout.placements.has(down)}
        {@const a = pos(up)}
        {@const b = pos(down)}
        {@const x1 = a.x + BOX_W}
        {@const y1 = a.y + BOX_H / 2}
        {@const x2 = b.x}
        {@const y2 = b.y + BOX_H / 2}
        <path
          d="M{x1} {y1} C{x1 + 40} {y1}, {x2 - 40} {y2}, {x2} {y2}"
          class="edge"
          class:hot={selected !== null && (up === selected || down === selected)}
          class:dim={related !== null && !(up === selected || down === selected)}
        />
      {/if}
    {/each}
    {#each nodes as n (n.id)}
      {@const p = pos(n.id)}
      {@const t = tone(n)}
      <g
        class="node {t}"
        class:selected={n.id === selected}
        class:dim={related !== null && !related.has(n.id)}
        transform="translate({p.x} {p.y})"
        role="button"
        tabindex="0"
        aria-label="{n.dataset}.{n.name}"
        onclick={() => onselect(n.id)}
        onkeydown={(e) => key(e, n.id)}
      >
        <title>{n.id}</title>
        <rect width={BOX_W} height={BOX_H} rx="7" />
        <circle cx="14" cy={BOX_H / 2} r="7" class="dot" />
        <text x="14" y={BOX_H / 2 + 3.5} class="glyph" text-anchor="middle"
          >{t === 'ok' ? STATUS_GLYPH.ok : t === 'warn' ? STATUS_GLYPH.warn : ''}</text
        >
        <text x="28" y="16" class="name">{short(n.name)}</text>
        <text x="28" y="31" class="sub">{n.dataset}{n.project !== 'bgg-data-warehouse' ? ` · ${n.project}` : ''}</text>
      </g>
    {/each}
  </svg>
</div>

<style>
  .frame { overflow: auto; background: var(--card); border: 1px solid var(--border); border-radius: var(--radius); max-height: 75vh; }
  svg { display: block; }
  .edge { fill: none; stroke: var(--border); stroke-width: 1.5; }
  .edge.hot { stroke: var(--foreground); stroke-width: 2; }
  .edge.dim, .node.dim { opacity: 0.25; }
  .node { cursor: pointer; }
  .node rect { fill: var(--background); stroke: var(--border); }
  .node.selected rect { stroke: var(--foreground); stroke-width: 2; }
  .node:focus-visible rect { stroke: var(--primary); stroke-width: 2; }
  .node:focus { outline: none; }
  .dot { fill: transparent; stroke: var(--muted-foreground); stroke-width: 1.5; }
  .node.ok .dot { fill: var(--status-ok); stroke: none; }
  .node.warn .dot { fill: var(--status-warn); stroke: none; }
  .glyph { font-size: 9px; font-weight: 700; fill: var(--card); }
  .node.warn .glyph { fill: oklch(0.25 0.02 260); }
  .name { font-size: 12px; font-weight: 600; fill: var(--foreground); }
  .sub { font-size: 10.5px; fill: var(--muted-foreground); }
</style>
```

- [ ] **Step 2: `TableDetail.svelte`**

```svelte
<script lang="ts">
  import type { LineageNode, PipelineTableRow, SchemaField } from '$lib/server/warehouse';
  import StatusBadge from './StatusBadge.svelte';
  import { clock, coverage, nodeStatus } from './display';

  let {
    node,
    upstream,
    downstream,
    reference,
    tableRow,
    onselect
  }: {
    node: LineageNode;
    upstream: LineageNode[];
    downstream: LineageNode[];
    reference: string;
    tableRow: PipelineTableRow | null;
    onselect: (id: string) => void;
  } = $props();

  const fmt = new Intl.NumberFormat('en-US');
  const size = (b: number | null) =>
    b == null ? '—' : b > 1e9 ? `${(b / 1e9).toFixed(1)} GB` : b > 1e6 ? `${(b / 1e6).toFixed(1)} MB` : `${(b / 1e3).toFixed(0)} KB`;
  const day = (iso: string | null) =>
    iso ? new Date(iso).toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' }) : '—';
  const status = $derived(nodeStatus(node, reference));
  const cov = $derived(tableRow ? coverage(tableRow.covered, tableRow.universe) : null);

  let schema = $state<SchemaField[] | null>(null);
  let schemaError = $state<string | null>(null);
  $effect(() => {
    const id = node.id;
    schema = null;
    schemaError = null;
    if (node.error) return;
    // Abort a slow response for a node that is no longer selected, so it can't overwrite this one.
    const ctrl = new AbortController();
    fetch(`/admin/lineage/schema?id=${encodeURIComponent(id)}`, { signal: ctrl.signal })
      .then(async (r) => {
        if (!r.ok) throw new Error((await r.json().catch(() => null))?.message ?? `HTTP ${r.status}`);
        return r.json();
      })
      .then((body) => (schema = body.schema))
      .catch((e) => {
        if (e.name !== 'AbortError') schemaError = e.message;
      });
    return () => ctrl.abort();
  });
</script>

<aside class="panel" aria-label="Table details">
  <p class="kind">{node.kind}{node.project !== 'bgg-data-warehouse' ? ` · ${node.project}` : ''}</p>
  <h3>{node.dataset}.{node.name}</h3>
  <p class="id mono">{node.id}</p>

  <dl>
    <dt>Status</dt>
    <dd>
      {#if node.error}<StatusBadge status="not_reached" label={node.error} />
      {:else if status}<StatusBadge status={status.tone === 'ok' ? 'ok' : 'warn'} label={status.label} />
      {:else}<span class="muted">—</span>{/if}
    </dd>
    <dt>Rows</dt>
    <dd class="tnum">{node.kind === 'view' ? 'view' : node.rows == null ? '—' : fmt.format(node.rows)}</dd>
    <dt>Size</dt>
    <dd class="tnum">{size(node.bytes)}</dd>
    <dt>Last modified</dt>
    <dd class="tnum">{day(node.last_modified)} · {clock(node.last_modified)} UTC</dd>
    {#if cov}
      <dt>Coverage</dt>
      <dd class="tnum">{(cov.pct * 100).toFixed(1)}%</dd>
    {/if}
  </dl>

  {#snippet links(label: string, list: LineageNode[])}
    <h4>{label}</h4>
    {#if list.length}
      <ul class="links">
        {#each list as n (n.id)}
          <li><button type="button" onclick={() => onselect(n.id)}>{n.dataset}.{n.name}</button></li>
        {/each}
      </ul>
    {:else}<p class="muted">None</p>{/if}
  {/snippet}
  {@render links('Upstream', upstream)}
  {@render links('Downstream', downstream)}

  <h4>Schema</h4>
  {#if node.error}<p class="muted">Not readable: {node.error}</p>
  {:else if schemaError}<p class="muted">{schemaError}</p>
  {:else if !schema}<p class="muted">Loading…</p>
  {:else}
    <table class="schema">
      <tbody>
        {#each schema as f (f.name)}
          <tr>
            <td class="mono">{f.name}</td>
            <td class="mono muted">{f.type}{f.mode === 'REPEATED' ? '[]' : ''}</td>
          </tr>
          {#if f.description}<tr class="desc"><td colspan="2">{f.description}</td></tr>{/if}
        {/each}
      </tbody>
    </table>
  {/if}
</aside>

<style>
  .panel { background: var(--card); border: 1px solid var(--border); border-radius: var(--radius); padding: var(--space-lg); min-width: 0; }
  .kind { margin: 0; font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.08em; color: var(--muted-foreground); }
  h3 { margin: 0.15rem 0 0; font-size: 1.05rem; overflow-wrap: anywhere; }
  .id { margin: 0.2rem 0 var(--space-md); font-size: 0.72rem; color: var(--muted-foreground); overflow-wrap: anywhere; }
  dl { display: grid; grid-template-columns: auto 1fr; gap: 0.35rem 0.75rem; margin: 0 0 var(--space-md); font-size: 0.85rem; }
  dt { color: var(--muted-foreground); }
  dd { margin: 0; }
  h4 { margin: var(--space-md) 0 0.3rem; font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.08em; color: var(--muted-foreground); }
  .links { list-style: none; margin: 0; padding: 0; display: flex; flex-wrap: wrap; gap: 0.3rem; }
  .links button { font: 0.78rem ui-monospace, Menlo, monospace; background: var(--muted); border: 1px solid var(--border); border-radius: 6px; padding: 0.15rem 0.45rem; color: var(--foreground); cursor: pointer; }
  .schema { width: 100%; border-collapse: collapse; font-size: 0.8rem; }
  .schema td { padding: 0.25rem 0.4rem 0.25rem 0; border-top: 1px solid var(--border); vertical-align: top; overflow-wrap: anywhere; }
  .schema tr.desc td { border-top: none; padding-top: 0; font-size: 0.74rem; color: var(--muted-foreground); }
  .mono { font-family: ui-monospace, Menlo, monospace; }
  .muted { color: var(--muted-foreground); font-size: 0.82rem; margin: 0; }
  .tnum { font-variant-numeric: tabular-nums; }
</style>
```

- [ ] **Step 3: The page** — `src/routes/(app)/admin/lineage/+page.svelte`:

```svelte
<script lang="ts">
  import { goto } from '$app/navigation';
  import { page } from '$app/state';
  import { Container, Stack } from '$lib/components/ui/layout';
  import LineageGraph from '$lib/monitoring/LineageGraph.svelte';
  import TableDetail from '$lib/monitoring/TableDetail.svelte';
  import { clock, freshnessReference } from '$lib/monitoring/display';
  import type { LineageNode } from '$lib/server/warehouse';
  import type { PageData } from './$types';

  let { data }: { data: PageData } = $props();

  const reference = $derived(
    data.pipeline
      ? freshnessReference(data.pipeline.today)
      : `${new Date().toISOString().slice(0, 10)}T05:00:00Z`
  );
  const byId = $derived(new Map((data.lineage?.nodes ?? []).map((n) => [n.id, n])));
  const selectedId = $derived(page.url.searchParams.get('node'));
  const selected = $derived(selectedId ? (byId.get(selectedId) ?? null) : null);
  const neighbours = (dir: 0 | 1) =>
    (data.lineage?.edges ?? [])
      .filter((e) => e[1 - dir] === selected?.id)
      .map((e) => byId.get(e[dir]))
      .filter((n): n is LineageNode => !!n);
  const tableRow = $derived(
    selected && selected.project === 'bgg-data-warehouse'
      ? (data.pipeline?.tables.find((t) => t.table === `${selected.dataset}.${selected.name}`) ?? null)
      : null
  );

  function select(id: string) {
    const url = new URL(page.url);
    url.searchParams.set('node', id);
    goto(url, { replaceState: true, noScroll: true, keepFocus: true });
  }
</script>

<svelte:head><title>Lineage · bgg-viewer</title></svelte:head>

<Container>
  <Stack>
    <header class="head">
      <h1>Lineage</h1>
      {#if data.lineage}
        <span>
          Dataform compilation {data.lineage.compilation.commit ?? ''} · {clock(data.lineage.compilation.created)} UTC ·
          {data.lineage.nodes.length} tables · may be up to 5 min old
        </span>
      {/if}
    </header>

    {#if data.error || !data.lineage}
      <div class="err" role="alert">
        <b>Couldn't load the lineage.</b>
        <p>{data.error}</p>
      </div>
    {:else}
      <div class="layout" class:with-panel={!!selected}>
        <LineageGraph
          nodes={data.lineage.nodes}
          edges={data.lineage.edges}
          selected={selected?.id ?? null}
          {reference}
          onselect={select}
        />
        {#if selected}
          <TableDetail
            node={selected}
            upstream={neighbours(0)}
            downstream={neighbours(1)}
            {reference}
            {tableRow}
            onselect={select}
          />
        {/if}
      </div>
      {#if !selected}<p class="hint">Click a table to see its status, rows and schema.</p>{/if}
    {/if}
  </Stack>
</Container>

<style>
  .head { display: flex; align-items: baseline; gap: 0.75rem; flex-wrap: wrap; }
  .head h1 { margin: 0; font-size: var(--text-heading); }
  .head span, .hint { color: var(--muted-foreground); font-size: 0.8rem; }
  .layout { display: grid; grid-template-columns: minmax(0, 1fr); gap: var(--space-lg); align-items: start; }
  .layout.with-panel { grid-template-columns: minmax(0, 1fr) 22rem; }
  @media (max-width: 900px) { .layout.with-panel { grid-template-columns: minmax(0, 1fr); } }
  .err { background: var(--card); border: 1px solid var(--border); border-left: 4px solid var(--status-fail); border-radius: var(--radius); padding: var(--space-lg); }
  .err p { margin: 0.3rem 0 0; color: var(--muted-foreground); font-family: ui-monospace, Menlo, monospace; font-size: 0.82rem; }
</style>
```

- [ ] **Step 4: History links.** In `HistoryGrid.svelte`, add `historyCell` to the `./display` import. Then replace the body cell:

```svelte
            {#each history as h (h.day)}
              {@const cell = historyCell(h.stages[s.key])}
              <td class={cell.status} title="{s.label} · {h.day} · {STATUS_WORD[cell.status]}">
                {#if cell.url}
                  <a href={cell.url} target="_blank" rel="noreferrer" aria-label="{s.label} on {h.day}: {STATUS_WORD[cell.status]}, open run">{STATUS_GLYPH[cell.status]}</a>
                {:else}{STATUS_GLYPH[cell.status]}{/if}
              </td>
            {/each}
```

Add to its `<style>`:
`td a { display: block; color: inherit; text-decoration: none; } td a:hover { outline: 2px solid var(--foreground); outline-offset: -2px; border-radius: 3px; }`

- [ ] **Step 5: Type-check and test**

Run: `pnpm check && pnpm test`
Expected: 0 errors (the existing AnalysisPanel warnings are unchanged) and all tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/lib/monitoring "src/routes/(app)/admin/lineage/+page.svelte"
git commit -m "feat(admin): lineage graph and table detail; history cells link to runs

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 11: End to end, then hand off

- [ ] **Step 1: Ask Phil to look.** He runs the servers. Locally, the viewer needs `WAREHOUSE_API_URL=http://localhost:8080` and the warehouse API from Task 6 Step 2 running, since the deployed API doesn't have `/monitoring/lineage` yet. Check with him:
  - Admin → Lineage shows the graph, with sources on the left.
  - Clicking `analytics.games_features` shows its rows, size and schema, and its upstream and downstream tables are clickable.
  - `?node=` survives a reload.
  - A `bgg-predictive-models` node shows either metadata or "no access", and the page doesn't break.
  - Pipeline's history cells open their runs.
- [ ] **Step 2: Stop.** Pushing and opening PRs for both repos is Phil's call. Deploy order: warehouse first is safe, because the viewer reads both history shapes.

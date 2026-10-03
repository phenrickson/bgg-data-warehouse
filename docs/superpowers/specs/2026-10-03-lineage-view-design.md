# Lineage View — Dataform Lineage in the Admin Panel

> Follow-up to the [pipeline monitor](2026-10-02-pipeline-monitor-design.md). Adds a
> second section to bgg-viewer's admin panel, next to Pipeline.

## Goal

See the warehouse's Dataform lineage (which tables feed which) in the admin panel, and
click any table to see its status, row count and schema.

The diagram doesn't need to be interactive: no dragging or zooming. It needs to be
readable, and clicking a node needs to show that table's details.

## What exists today

- `.github/actions/dataform-lineage/` runs after every Dataform run. It fetches the
  run's compilation result from the Dataform API, parses it with
  `parse_compilation_result()` in `generate_lineage.py`, and commits `docs/lineage.md`
  (Mermaid) and `docs/lineage.html` (vis.js).
- The compilation result is the source of truth for lineage. Checked on 2026-10-03:
  - **Fetching:** `GET .../repositories/bgg-data-warehouse/compilationResults?orderBy=create_time desc`
    lists the newest result first, and `GET {name}:query` returns its actions.
  - **Contents:** 50 actions. There are 24 tables and views (14 `TABLE`, 8
    `INCREMENTAL_TABLE`, 2 `VIEW`), 25 declared sources and 1 operation.
  - **Targets:** every action has `target: {database, schema, name}`, and tables, views
    and operations carry `dependencyTargets`.
- The parser drops `database`. So `bgg-predictive-models.raw.game_embeddings` and a
  warehouse `raw.*` table would share an ID if their names ever matched, and the
  project can't be looked up.
- The warehouse API's service account (`bgg-data-warehouse@`) already has
  `roles/dataform.editor` and `roles/bigquery.dataEditor` on `bgg-data-warehouse`. No
  IAM change is needed for the warehouse project.

## Decisions

- **The API builds the lineage on request,** from the latest compilation result on
  `main`. Nothing is committed, and nothing has to be redeployed for the graph to stay
  current.
- **One parser.** Parsing moves to `src/monitoring/lineage.py`, using the standard
  library only. The GitHub action and the API both use it, and the action's outputs
  stay the same.
- **Table details come from BigQuery metadata (`tables.get`),** not queries: row count,
  size, last modified and schema. It's free, so viewing lineage never scans data.
- **Fixed layout, computed in the viewer.** Columns run left to right by depth; nodes
  can't be moved.
- **Status colours as on Pipeline:** blue/amber/violet with glyph and word, never
  green/red.

## Node status

A node's status is based on how recently it changed, compared to today's chain:

| Status | Rule |
|---|---|
| `ok` (Fresh) | `last_modified` is on or after today's stage-1 start (fallback `<day>T05:00:00Z`), the same reference Pipeline uses |
| `warn` (N days old) | modified before that |
| no status, neutral | views (BigQuery's last-modified time on a view is when its definition changed); tables the API can't read metadata for |

Where Pipeline's table list has coverage for the same table, the panel shows it too.
The node colour stays based on freshness only.

## Components — bgg-data-warehouse

### 1. `src/monitoring/lineage.py` (new, standard library only)

- `Node(id, project, dataset, name, kind)`:
  - `id` is `"{project}.{dataset}.{name}"`.
  - `kind` is one of `table`, `incremental`, `view`, `source` (a declaration) or
    `operation`.
- `parse_compilation_result(data: dict) -> tuple[list[Node], list[tuple[str, str]]]`
  returns nodes sorted by `id`, and edges as `(upstream_id, downstream_id)` with
  duplicates removed. A dependency that has no action of its own still becomes a node,
  with `kind` `source`.
- `latest_compilation_name(results: list[dict]) -> str | None`: from a
  `compilationResults` list response, the first entry whose `gitCommitish` is `main`
  and that has no `compilationErrors`.

### 2. `.github/actions/dataform-lineage/generate_lineage.py` (modified)

- It imports `parse_compilation_result` from `src/monitoring/lineage.py`, by adding the
  checked-out repo root to `sys.path`. It no longer has its own copy.
- `generate_mermaid`/`generate_visjs_html` move to the new `Node` objects. Node IDs in
  the output change to include the project; otherwise `docs/lineage.md` and
  `docs/lineage.html` look the same.

### 3. `src/warehouse/readers/lineage.py` (new)

- `fetch_compilation(session=None) -> dict`: lists compilation results, picks the
  latest with `latest_compilation_name`, and returns `{name}:query`. By default
  `session` is a `google.auth.transport.requests.AuthorizedSession` built from ADC
  (`google.auth.default` with the cloud-platform scope); tests pass a fake. Requests go to
  `https://dataform.googleapis.com/v1beta1/projects/bgg-data-warehouse/locations/us-central1/repositories/bgg-data-warehouse`.
- `fetch_table_meta(ids, client=None) -> dict[str, dict]` makes `tables.get` calls for
  every node, in parallel (8 workers). Each node gets `rows` (`num_rows`), `bytes`,
  `last_modified` and `type`. A failed call (not found, or 403 on another project)
  gives `{"error": "<reason>"}` for that node, not an exception.
- `fetch_table_schema(id, client=None) -> list[dict]`: `name`, `type`, `mode` and
  `description` per column, with nested `RECORD` fields flattened to `parent.child`.

### 4. Two routes on the existing monitoring router

- `GET /monitoring/lineage` returns:
  ```json
  {
    "generated_at": "...",
    "compilation": {"name": "...", "created": "...", "commit": "4b04de4"},
    "nodes": [{"id": "bgg-data-warehouse.analytics.games_features",
               "project": "...", "dataset": "...", "name": "...", "kind": "table",
               "rows": 141525, "bytes": 123, "last_modified": "...", "error": null}],
    "edges": [["bgg-data-warehouse.analytics.games_active",
               "bgg-data-warehouse.analytics.games_features"]]
  }
  ```
  It's cached in-process for 5 minutes. A Dataform API error gives a `502`.
- `GET /monitoring/tables/{id}`, where `id` is `project.dataset.table`, returns
  `{"id", "schema": [...]}`.
  - It's cached for 5 minutes per id.
  - It returns a `404` if the table doesn't exist and a `403` if the API can't read it.
  - To keep the endpoint from reading arbitrary tables, `id` must be a node in the
    current lineage; otherwise it returns a `400`.

## Components — bgg-viewer

- **Section:** the `SECTIONS` list in `src/routes/(app)/admin/+layout.server.ts` gets
  `{ href: '/admin/lineage', label: 'Lineage' }`.
- **Route** `src/routes/(app)/admin/lineage/`: admin gate (`404` otherwise). It loads
  `getLineage()`, plus `getPipelineStatus()` for the freshness reference and coverage.
  A failure on either renders as a message, as on Pipeline.
- **Client:** `getLineage()` and `getTableSchema(id)` on the warehouse client, with
  types.
- **Layout** in `src/lib/monitoring/lineage-layout.ts`, pure and unit-tested:
  - **Column:** a node's depth is its longest path from a source, and the leftmost
    column is depth 0.
  - **Order within a column:** by dataset (`raw`, `core`, `staging`, `analytics`,
    `predictions`, `monitoring`, `collections`, then anything else), then by name.
  - **Cycles:** if there is one (there shouldn't be), the layout puts the remaining
    nodes in a final column rather than looping.
- **`LineageGraph.svelte`:** an SVG with one box per node. Each box shows the table
  name, its dataset and a status glyph. Edges are curved lines; the selected node's
  inputs and outputs are drawn darker and everything else dims. On narrow screens the
  page scrolls sideways inside the graph's frame, not the whole page.
- **`TableDetail.svelte`:** a side panel, or a full-width panel below the graph on
  narrow screens. It shows:
  - the full table ID;
  - its kind;
  - status, using the same freshness rule as Pipeline;
  - rows, size and last modified;
  - coverage if Pipeline has it;
  - upstream and downstream tables as buttons that select those nodes;
  - the schema, loaded on demand from `getTableSchema`.

  Views show "view" instead of a row count. Nodes the API can't read show the error.
- **URL:** the selected node is kept in `?node=<id>`, so a table can be linked
  directly.

## Validation

- **pytest:**
  - `parse_compilation_result`, against a fixture of the real 2026-10-03 compilation
    result:
    - 50 nodes plus any dependency-only nodes;
    - the `kind` counts above;
    - `bgg-predictive-models` sources keep their project;
    - no duplicate edges.
  - `latest_compilation_name`: skips non-`main` results and results with errors.
  - The action still writes Mermaid and vis.js output from the fixture.
  - The readers, with the Dataform session and the BigQuery client mocked: a failed
    `tables.get` call on one node doesn't fail the rest.
  - The routes: response shape, caching, `502`, a `400` for an id outside the lineage,
    and `404`/`403` passed through.
- **vitest:**
  - the layout: depth, ordering, and a cycle that ends;
  - the node status rule;
  - the admin gate on the route;
  - the error path.
- **End to end, locally:** your dev server against the deployed API. Open Admin →
  Lineage, click `analytics.games_features`, and check its row count and schema
  against BigQuery.

## Risks

- **Other projects' tables:** the API's service account may not be able to read
  metadata for `bgg-predictive-models` tables. Those nodes show "no access" rather
  than failing the page. Granting access would be a separate change.
- **Dataform API version:** this uses `v1beta1`, as the lineage action already does.
- **Changing the action:** changing the lineage action changes what CI commits. The
  fixture test checks that it still produces both files.

## Out of scope

- Run counts per stage on Pipeline cards. You'll use the GitHub run pages for now.
- Column-level lineage.
- Running or rerunning Dataform from the page.

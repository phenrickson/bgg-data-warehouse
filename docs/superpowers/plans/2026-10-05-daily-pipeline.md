# Daily Pipeline (one run, two Dataform pieces) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run the pipeline once a day, with Dataform invoked twice: `core` before ML and
`publish` after it, and the ML stages run by one orchestrating workflow.

**Architecture:**
- Every Dataform action is tagged `core` or `publish`, and `dataform.yml` invokes one
  tag per event.
- ML readers stop depending on Dataform's mid-chain copies. They read the ML project's
  raw tables through one shared "latest per game" SQL module.
- `ml-pipeline.yml` in bgg-predictive-models calls the six stage workflows
  (`workflow_call`, ordered by `needs:`) and ends by sending `ml_complete`.

**Tech Stack:**
- Dataform 3.0 (`.sqlx`) and the Dataform REST API v1beta1
- GitHub Actions: `repository_dispatch`, `workflow_call`
- Python 3.12 and pytest in both repos
- BigQuery

**Spec:** `docs/superpowers/specs/2026-10-05-daily-pipeline-design.md` (bgg-data-warehouse)

## Global Constraints

- **Two repos:** `bgg-data-warehouse` (warehouse) and `bgg-predictive-models` (ML).
  Each task names its repo and branch.
- **Delivery:** one PR per task, branched from that repo's `origin/main`. Conventional
  commits (`feat(dataform): …`, `feat(ci): …`, `feat(data): …`). Phil merges. All
  deploys go through Actions, never `gcloud`/`terraform` locally.
- **Merge order is the rollout order:** Task 1 → 2 → 3 → 4 → 5 → 6. Task 7 waits until
  Phil confirms bgg-dash-viewer is retired.
- **Tags:** every one of the 25 `definitions/*.sqlx` actions has exactly one tag,
  `core` or `publish`, as listed in the spec.
- **Unchanged:** pushes to `main` under `definitions/` still run the full graph. Model
  materialisation stays as it is. The Python processor still writes `core`.
- **BigQuery:** dry-run every verification query and cap it with
  `--maximum_bytes_billed`. Ask Phil before anything over 1 GB.
- **Production runs:** Phil decides when to trigger anything that writes production
  tables. A manual `publish` or ML run is one; the scheduled daily run is not.
- **Pipeline Status** (`pipeline_status.py`) only checks the three fetch workflows, so it
  is unaffected. Checked while planning.
- **Tests:** warehouse `uv run --extra test python -m pytest -m "not integration" -q`.
  ML `uv run pytest -q tests/<file>`.

## Review Focus

- **A future embedding-version bump.** The new reader keeps only the latest
  `embedding_version`. The Dataform copy merged by `game_id`, so games not yet
  re-embedded kept their old-version row. After a bump, games still waiting to be
  re-embedded drop out of complexity and scoring until they are re-embedded. Expected:
  this is deliberate (one consistent vector space) and the reader docstring says so.
  Pinned by `test_description_embeddings_sql_filters_latest_version`.
- **Complexity bound when the table is empty**, e.g. a fresh dev project. Expected: no
  `score_ts` filter, not a crash or `TIMESTAMP('None')`. Pinned by
  `test_complexity_sql_without_bound`.
- **Readers that joined the copy without deduplicating** (`load_data_with_embeddings`,
  `load_changed_games_with_embeddings`). Expected: still exactly one row per game.
  Pinned by `test_load_data_with_embeddings_uses_latest_per_game`.
- **A stage failing mid-chain.** Expected: later jobs are skipped and `ml_complete` is
  not sent. Pinned by the `needs:` chain in Task 4, checked in its YAML test.
- **The service account running collection scoring cannot read
  `bgg-predictive-models.raw.*`.** Expected: found before cutover, not in the first
  daily run. Pinned by Task 2 Step 9's dataset access check.

---

## File Structure

**bgg-data-warehouse**
- `definitions/*.sqlx` (25 files): add `tags` to each `config`
- `tests/test_dataform_tags.py`: every action has exactly one allowed tag, matching the
  spec's lists
- `.github/workflows/dataform.yml`: one tag per event, the `ml_complete` handler,
  retire the old callbacks
- `tests/test_dataform_workflow.py`: event → tag routing in `dataform.yml`
- `.github/workflows/dataform-dev.yml`: manual full graph with `schemaSuffix: dev`

**bgg-predictive-models**
- `src/data/ml_inputs.py` (new): latest-per-game SQL for description embeddings and
  complexity, plus the complexity partition bound
- `src/data/loader.py`: four readers switch to `ml_inputs`
- `src/models/embeddings/data.py`: two readers switch to `ml_inputs`
- `tests/test_ml_inputs.py`, `tests/test_loader_load_features.py`,
  `tests/test_embedding_data_query.py`
- `.github/workflows/ml-pipeline.yml` (new): the orchestrator
- The six stage workflows: add `workflow_call`, drop the old triggers and notify steps
- `tests/test_ml_pipeline_workflow.py`: orchestrator order, triggers and notifies

---

### Task 1: Tag every Dataform action `core` or `publish` (warehouse)

**Branch:** `feat/dataform-core-publish-tags`

**Files:**
- Create: `tests/test_dataform_tags.py`
- Modify: all 25 `definitions/*.sqlx` (`config` block only)

**Interfaces:**
- Produces: Dataform tags `core` and `publish`, which Tasks 3, 5 and 6 invoke by name.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_dataform_tags.py
"""Every Dataform action carries exactly one pipeline tag (core or publish)."""

import re
from pathlib import Path

DEFINITIONS = Path(__file__).resolve().parents[1] / "definitions"

CORE = {
    "games_active", "games_features", "game_features_hash", "best_player_counts",
    "player_count_recommendations", "game_product_line", "user_collections",
    "filter_categories", "filter_mechanics", "filter_designers", "filter_publishers",
    "filter_options_combined", "game_dropdown_options",
}
PUBLISH = {
    "bgg_description_embeddings", "bgg_complexity_predictions", "game_first_prediction",
    "bgg_predictions", "bgg_game_embeddings", "bgg_game_coordinates",
    "user_collection_predictions", "game_similarity_search", "similarity_profiles",
    "game_neighbors", "game_profile", "deployed_models",
}


def _tags(sqlx: Path) -> list[str]:
    config = re.search(r"^config\s*\{(.*?)^\}", sqlx.read_text(encoding="utf-8"), re.S | re.M).group(1)
    match = re.search(r"\btags:\s*\[([^\]]*)\]", config)
    return re.findall(r'"([^"]+)"', match.group(1)) if match else []


def test_every_action_is_listed_once():
    names = {p.stem for p in DEFINITIONS.glob("*.sqlx")}
    assert CORE.isdisjoint(PUBLISH)
    assert names == CORE | PUBLISH


def test_every_action_has_its_one_tag():
    wrong = {}
    for sqlx in DEFINITIONS.glob("*.sqlx"):
        expected = ["core"] if sqlx.stem in CORE else ["publish"]
        if _tags(sqlx) != expected:
            wrong[sqlx.stem] = _tags(sqlx)
    assert wrong == {}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run --extra test python -m pytest tests/test_dataform_tags.py -q`
Expected: `test_every_action_is_listed_once` passes, and
`test_every_action_has_its_one_tag` FAILS listing all 25 with `[]`.

- [ ] **Step 3: Add the tags**

In each `.sqlx`, add one line inside `config { … }` directly after the `type:` line:
`  tags: ["core"],` for the 13 CORE files, `  tags: ["publish"],` for the 12 PUBLISH
files. Example (`definitions/games_active.sqlx`):

```
config {
  type: "incremental",
  tags: ["core"],
  name: "games_active",
  uniqueKey: ["game_id"]
}
```

- [ ] **Step 4: Run the test and the compile check**

Run: `uv run --extra test python -m pytest tests/test_dataform_tags.py -q`
Expected: 2 passed.
Run: `npx -y @dataform/cli@3.0.0 compile`
Expected: compiles with no errors. That's the same check `dataform-compile.yml` runs in
CI.

- [ ] **Step 5: Commit, push and open the PR**

```bash
git add definitions tests/test_dataform_tags.py
git commit -m "feat(dataform): tag every action core or publish"
git push -u origin feat/dataform-core-publish-tags
gh pr create --title "feat(dataform): tag every action core or publish" --body "Task 1 of docs/superpowers/plans/2026-10-05-daily-pipeline.md. Tags only; dataform.yml still runs the full graph, so no behaviour change."
```

---

### Task 2: ML readers read latest-per-game from the ML project's raw tables (ML)

**Branch:** `feat/ml-inputs-from-raw`

**Files:**
- Create: `src/data/ml_inputs.py`, `tests/test_ml_inputs.py`,
  `tests/test_embedding_data_query.py`
- Modify: `src/data/loader.py` (`load_features` CTEs ~L164-192;
  `load_data_with_embeddings` ~L273-285; `load_changed_games_with_embeddings` ~L389-393),
  `src/models/embeddings/data.py:_build_query` (~L60-110),
  `tests/test_loader_load_features.py`

**Interfaces:**
- Produces:
  - `ml_inputs.ML_PROJECT_ID: str = "bgg-predictive-models"`
  - `ml_inputs.latest_description_embeddings_sql(ml_project: str = ML_PROJECT_ID) -> str`:
    a parenthesised subquery with exactly one row per `game_id` and columns `game_id`,
    `embedding`, `embedding_version`, `created_ts`, `job_id`
  - `ml_inputs.complexity_min_score_ts(client) -> str | None`
  - `ml_inputs.latest_complexity_sql(min_score_ts: str | None, ml_project: str = ML_PROJECT_ID) -> str`:
    a parenthesised subquery with one row per `game_id` and columns `game_id`,
    `predicted_complexity`, `score_ts`

- [ ] **Step 1: Write the failing tests for `ml_inputs`**

```python
# tests/test_ml_inputs.py
from unittest.mock import MagicMock

from src.data.ml_inputs import (
    complexity_min_score_ts,
    latest_complexity_sql,
    latest_description_embeddings_sql,
)


def test_description_embeddings_sql_filters_latest_version():
    sql = latest_description_embeddings_sql("p")
    assert "`p.raw.description_embeddings`" in sql
    assert "embedding_version = (SELECT MAX(embedding_version)" in sql
    assert "PARTITION BY game_id ORDER BY created_ts DESC, job_id DESC" in sql
    assert "rn = 1" in sql
    assert sql.strip().startswith("(") and sql.strip().endswith(")")


def test_complexity_sql_prunes_with_bound():
    sql = latest_complexity_sql("2026-03-12 04:00:00+00", "p")
    assert "`p.raw.complexity_predictions`" in sql
    assert "score_ts >= TIMESTAMP('2026-03-12 04:00:00+00')" in sql
    assert "PARTITION BY game_id ORDER BY score_ts DESC, job_id DESC" in sql


def test_complexity_sql_without_bound():
    sql = latest_complexity_sql(None, "p")
    assert "TIMESTAMP(" not in sql
    assert "None" not in sql


def test_complexity_min_score_ts_reads_min_of_latest_per_game():
    client = MagicMock()
    client.query.return_value.result.return_value = [{"bound": "2026-03-12 04:00:00+00"}]
    assert complexity_min_score_ts(client) == "2026-03-12 04:00:00+00"
    sql = client.query.call_args.args[0]
    assert "MAX(score_ts)" in sql and "GROUP BY game_id" in sql and "MIN(" in sql


def test_complexity_min_score_ts_empty_table():
    client = MagicMock()
    client.query.return_value.result.return_value = [{"bound": None}]
    assert complexity_min_score_ts(client) is None
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest -q tests/test_ml_inputs.py`
Expected: FAIL, `ModuleNotFoundError: No module named 'src.data.ml_inputs'`.

- [ ] **Step 3: Implement `src/data/ml_inputs.py`**

```python
"""Latest-per-game ML outputs, read straight from the ML project's raw tables.

These reproduce the warehouse Dataform models bgg_description_embeddings and
bgg_complexity_predictions, so ML stages can read the previous stage's output
mid-chain without a Dataform run in between.

Description embeddings keep only the latest embedding_version (one consistent
vector space): after a version bump, games not yet re-embedded drop out until
they are, which differs deliberately from the incremental Dataform copy.

Complexity history holds superseded model versions in old score_ts partitions.
complexity_min_score_ts finds the earliest score_ts among each game's latest
row; filtering on it gives identical results while pruning those partitions.
"""

from typing import Optional

ML_PROJECT_ID = "bgg-predictive-models"


def latest_description_embeddings_sql(ml_project: str = ML_PROJECT_ID) -> str:
    table = f"`{ml_project}.raw.description_embeddings`"
    return f"""(
  SELECT game_id, embedding, embedding_version, created_ts, job_id
  FROM (
    SELECT game_id, embedding, embedding_version, created_ts, job_id,
      ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY created_ts DESC, job_id DESC) AS rn
    FROM {table}
    WHERE embedding_version = (SELECT MAX(embedding_version) FROM {table})
  )
  WHERE rn = 1
)"""


def complexity_min_score_ts(client, ml_project: str = ML_PROJECT_ID) -> Optional[str]:
    sql = f"""
SELECT CAST(MIN(latest_ts) AS STRING) AS bound
FROM (
  SELECT MAX(score_ts) AS latest_ts
  FROM `{ml_project}.raw.complexity_predictions`
  GROUP BY game_id
)"""
    rows = list(client.query(sql).result())
    return rows[0]["bound"] if rows else None


def latest_complexity_sql(min_score_ts: Optional[str], ml_project: str = ML_PROJECT_ID) -> str:
    bound = f"WHERE score_ts >= TIMESTAMP('{min_score_ts}')" if min_score_ts else ""
    return f"""(
  SELECT game_id, predicted_complexity, score_ts
  FROM (
    SELECT game_id, predicted_complexity, score_ts,
      ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY score_ts DESC, job_id DESC) AS rn
    FROM `{ml_project}.raw.complexity_predictions`
    {bound}
  )
  WHERE rn = 1
)"""
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest -q tests/test_ml_inputs.py`
Expected: 5 passed.

- [ ] **Step 5: Write the failing loader and embedding-data tests**

Replace the `bgg_complexity_predictions` / `bgg_description_embeddings` assertions in
`tests/test_loader_load_features.py`:
- `assert "bgg_complexity_predictions" not in sql` becomes
  `assert "raw.complexity_predictions" not in sql`
- `assert "bgg_description_embeddings" not in sql` becomes
  `assert "raw.description_embeddings" not in sql`
- the corresponding `in sql` assertions get the same two substitutions.

Also stub the bound query, so that `fake_client.query` returns a job whose `.result()`
yields `[{"bound": "2026-03-12 04:00:00+00"}]` when `"MIN(latest_ts)"` is in the SQL.
Then add:

```python
def test_load_data_with_embeddings_uses_latest_per_game(fake_config):
    loader = BGGDataLoader(fake_config)
    try:
        loader.load_data_with_embeddings()
    except Exception:
        pass  # empty fake frame; only the SQL matters here
    sql = fake_config._state["last_sql"]
    assert "raw.description_embeddings" in sql
    assert "rn = 1" in sql
    assert "predictions.bgg_description_embeddings" not in sql
```

```python
# tests/test_embedding_data_query.py
from unittest.mock import MagicMock, patch

from src.models.embeddings.data import EmbeddingDataLoader


def test_build_query_reads_raw_with_bound():
    config = MagicMock()
    config.data_warehouse.project_id = "dw"
    config.data_warehouse.features_dataset = "analytics"
    config.data_warehouse.features_table = "games_features"
    with patch("src.models.embeddings.data.bigquery.Client"), \
         patch("src.models.embeddings.data.complexity_min_score_ts", return_value="2026-03-12 04:00:00+00"):
        loader = EmbeddingDataLoader(config)
        sql = loader._build_query("TRUE", use_embeddings=True)
    assert "raw.complexity_predictions" in sql
    assert "score_ts >= TIMESTAMP('2026-03-12 04:00:00+00')" in sql
    assert "raw.description_embeddings" in sql
    assert "predictions.bgg_" not in sql
```

- [ ] **Step 6: Run them to verify they fail**

Run: `uv run pytest -q tests/test_loader_load_features.py tests/test_embedding_data_query.py`
Expected: FAIL, with the SQL still referencing `predictions.bgg_*`.

- [ ] **Step 7: Switch the readers**

`src/data/loader.py`. Add `from src.data.ml_inputs import complexity_min_score_ts, latest_complexity_sql, latest_description_embeddings_sql`, then:
- In `load_features`:
  - Replace the `complexity AS (...)` CTE body with
    `complexity AS (SELECT game_id, predicted_complexity FROM {latest_complexity_sql(complexity_min_score_ts(self.client))})`.
  - Replace the `embeddings AS (...)` CTE body with
    `embeddings AS (SELECT game_id, embedding FROM {latest_description_embeddings_sql()})`.
  - Update the docstring lines naming the old tables.
- In `load_data_with_embeddings` and `load_changed_games_with_embeddings`: when
  `embeddings_table is None`, join `{latest_description_embeddings_sql()}` instead of
  the table name. Write it as `INNER JOIN {source} e` with
  `source = f"`{embeddings_table}`" if embeddings_table else latest_description_embeddings_sql()`,
  so an explicit `embeddings_table` still works as before.

`src/models/embeddings/data.py:_build_query`. Add
`from src.data.ml_inputs import complexity_min_score_ts, latest_complexity_sql, latest_description_embeddings_sql`, then:
- `latest_complexity` CTE:
  `SELECT game_id, predicted_complexity, 1 AS rn FROM {latest_complexity_sql(complexity_min_score_ts(self.client))}`
- `latest_description_emb` CTE:
  `SELECT game_id, embedding, 1 AS rn FROM {latest_description_embeddings_sql()}`

The `rn = 1` joins stay valid.

- [ ] **Step 8: Run the tests to verify they pass**

Run: `uv run pytest -q tests/test_ml_inputs.py tests/test_loader_load_features.py tests/test_embedding_data_query.py`
Expected: all pass. Then run `uv run pytest -q`. Expected: no new failures. Record any
failures that already existed before this change, by name.

- [ ] **Step 9: Verify the new reads match the old ones (BigQuery)**

Write `scripts/verify_ml_inputs.sql` (not committed if Phil prefers). It holds two
comparisons, each `EXCEPT DISTINCT` in both directions with counts:
- `SELECT game_id, embedding FROM predictions.bgg_description_embeddings` vs the
  `latest_description_embeddings_sql()` subquery
- `SELECT game_id, predicted_complexity FROM predictions.bgg_complexity_predictions` vs
  the `latest_complexity_sql(<bound>)` subquery

Get the bound by running the `complexity_min_score_ts` query (dry run, then run,
expected ≤ 0.1 GB). Dry-run the comparison and cap it. Expected: about 0.7 GB, so no
approval is needed. Expected result: 0 rows in each direction for both. Any difference
**stops the task**: report it to Phil.

Also dry-run the new `load_features` complexity CTE alone, to confirm pruning. Expected:
well under the 537 MB a full scan costs.

Then check who can read the raw dataset (metadata, free):
`bq show --format=prettyjson bgg-predictive-models:raw | jq '.access'`. Find the
service accounts the scoring, collections and embeddings Cloud Run services run as
(`gcloud run services describe <name> --region us-central1 --format 'value(spec.template.spec.serviceAccountName)'`,
read-only). Confirm each one has read access through the dataset or the project. If one
doesn't, **stop and report to Phil**: granting it is a Terraform change, made in a
separate PR.

- [ ] **Step 10: Commit, push and open the PR**

```bash
git add src/data/ml_inputs.py src/data/loader.py src/models/embeddings/data.py tests/test_ml_inputs.py tests/test_loader_load_features.py tests/test_embedding_data_query.py
git commit -m "feat(data): read ML inputs latest-per-game from raw, not Dataform copies"
git push -u origin feat/ml-inputs-from-raw
gh pr create --title "feat(data): read ML inputs latest-per-game from raw, not Dataform copies" --body "Task 2 of the daily-pipeline plan (bgg-data-warehouse docs/superpowers/plans/2026-10-05-daily-pipeline.md). Verified: new reads match the Dataform copies (0 rows each way). After merge the scoring, collections and embeddings images must be rebuilt manually: their docker-*-build workflows only trigger on services/** paths."
```

- [ ] **Step 11: After Phil merges, rebuild the three service images**

Run, in order, and wait for each to go green:
`gh workflow run docker-scoring-build.yml --ref main`,
`gh workflow run docker-collections-build.yml --ref main`,
`gh workflow run docker-embeddings-build.yml --ref main`.
These rebuild the images that import `src/data`. They aren't production data runs.
Report the run URLs.

---

### Task 3: Warehouse accepts `ml_complete` and runs `publish` (warehouse)

**Branch:** `feat/dataform-publish-on-ml-complete`

**Files:**
- Modify: `.github/workflows/dataform.yml`
- Create: `tests/test_dataform_workflow.py`

**Interfaces:**
- Consumes: the `publish` tag (Task 1).
- Produces: `repository_dispatch` type `ml_complete` → a Dataform invocation with
  `includedTags: ["publish"]`, followed by the bgg-viewer `catalog_refresh` dispatch.
  Also `workflow_dispatch` input `piece` (`full` | `core` | `publish`, default `full`).

- [ ] **Step 1: Write the failing test**

```python
# tests/test_dataform_workflow.py
"""dataform.yml routes each event to the right Dataform tag."""

from pathlib import Path

import yaml

WF = yaml.safe_load((Path(__file__).resolve().parents[1] / ".github/workflows/dataform.yml").read_text(encoding="utf-8"))
ON = WF[True] if True in WF else WF["on"]  # PyYAML parses the key `on` as True
TEXT = (Path(__file__).resolve().parents[1] / ".github/workflows/dataform.yml").read_text(encoding="utf-8")


def test_ml_complete_is_accepted():
    assert "ml_complete" in ON["repository_dispatch"]["types"]


def test_manual_runs_choose_a_piece():
    piece = ON["workflow_dispatch"]["inputs"]["piece"]
    assert piece["default"] == "full"
    assert piece["options"] == ["full", "core", "publish"]


def test_invocation_passes_included_tags():
    assert '"includedTags"' in TEXT
    assert "steps.piece.outputs.tag" in TEXT


def test_catalog_refresh_follows_ml_complete():
    assert "github.event.action == 'ml_complete'" in TEXT
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run --extra test python -m pytest tests/test_dataform_workflow.py -q`
Expected: FAIL on `ml_complete` missing.

- [ ] **Step 3: Change `dataform.yml`**

1. `repository_dispatch.types` becomes
   `[complexity_complete, text_embeddings_complete, embeddings_complete, ml_complete]`.
2. `workflow_dispatch` gains:

```yaml
  workflow_dispatch:
    inputs:
      piece:
        description: 'Which part of the graph to run'
        type: choice
        options: [full, core, publish]
        default: full
```

3. Before "Execute Workflow", add:

```yaml
      - name: Choose piece
        id: piece
        run: |
          PIECE="full"
          if [ "${{ github.event_name }}" == "workflow_dispatch" ]; then PIECE="${{ github.event.inputs.piece }}"; fi
          if [ "${{ github.event.action }}" == "ml_complete" ]; then PIECE="publish"; fi
          if [ "${PIECE}" == "full" ]; then echo "tag=" >> $GITHUB_OUTPUT; else echo "tag=${PIECE}" >> $GITHUB_OUTPUT; fi
          echo "Running piece: ${PIECE}"
```

4. In "Execute Workflow", replace the `-d '{"compilationResult": ...}'` body with:

```bash
          TAG="${{ steps.piece.outputs.tag }}"
          if [ -n "${TAG}" ]; then
            BODY=$(jq -n --arg c "${{ steps.compile.outputs.compilation_name }}" --arg t "${TAG}" \
              '{compilationResult: $c, invocationConfig: {"includedTags": [$t], transitiveDependenciesIncluded: false, transitiveDependentsIncluded: false}}')
          else
            BODY=$(jq -n --arg c "${{ steps.compile.outputs.compilation_name }}" '{compilationResult: $c}')
          fi
          EXECUTION=$(curl -s -X POST "${API_BASE}/workflowInvocations" \
            -H "Authorization: Bearer ${ACCESS_TOKEN}" \
            -H "Content-Type: application/json" \
            -d "${BODY}")
```

5. In "Notify ML Pipeline", add an `ml_complete` branch before the final `else`:
   `elif [ "${{ github.event.action }}" == "ml_complete" ]; then echo "Publish complete. No further dispatch."; exit 0`.
6. In "Notify viewer (catalog artifact)", change the `if:` condition
   `github.event.action == 'embeddings_complete'` to
   `(github.event.action == 'embeddings_complete' || github.event.action == 'ml_complete')`.

- [ ] **Step 4: Run the test and actionlint**

Run: `uv run --extra test python -m pytest tests/test_dataform_workflow.py -q`
Expected: 4 passed.
Run: `uvx --from actionlint-py actionlint .github/workflows/dataform.yml`
Expected: no errors.

- [ ] **Step 5: Commit, push and open the PR**

```bash
git add .github/workflows/dataform.yml tests/test_dataform_workflow.py
git commit -m "feat(ci): run the publish piece on ml_complete"
git push -u origin feat/dataform-publish-on-ml-complete
gh pr create --title "feat(ci): run the publish piece on ml_complete" --body "Task 3 of docs/superpowers/plans/2026-10-05-daily-pipeline.md. Old callback events still work; nothing sends ml_complete until Task 4."
```

- [ ] **Step 6: After merge, a manual `publish` test run (Phil decides when)**

Ask Phil. On a yes: `gh workflow run dataform.yml --ref main -f piece=publish`. That's a
normal production publish, the same as today's run 4 but limited to `publish`. Check in
the Dataform invocation that only the 12 `publish` actions ran.

---

### Task 4: One orchestrating ML workflow (ML)

**Branch:** `feat/ml-pipeline-orchestrator`

**Files:**
- Create: `.github/workflows/ml-pipeline.yml`, `tests/test_ml_pipeline_workflow.py`
- Modify: `run-generate-text-embeddings.yml`, `run-complexity-scoring.yml`,
  `run-scoring-service.yml`, `run-generate-embeddings.yml`, `run-collection-scoring.yml`,
  `build-collection-reports.yml`

**Interfaces:**
- Consumes: `ml_complete` handling in the warehouse (Task 3, must be merged first).
- Produces: `ml-pipeline.yml` triggered by `repository_dispatch: [dataform_complete]`;
  last job sends `ml_complete`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_ml_pipeline_workflow.py
from pathlib import Path

import yaml

WF_DIR = Path(__file__).resolve().parents[1] / ".github" / "workflows"
STAGES = [
    "run-generate-text-embeddings.yml", "run-complexity-scoring.yml", "run-scoring-service.yml",
    "run-generate-embeddings.yml", "run-collection-scoring.yml", "build-collection-reports.yml",
]
OLD_EVENTS = ["text_embeddings_complete", "complexity_complete", "embeddings_complete"]


def _load(name):
    wf = yaml.safe_load((WF_DIR / name).read_text(encoding="utf-8"))
    return wf, (wf[True] if True in wf else wf["on"])


def test_orchestrator_trigger_and_order():
    wf, on = _load("ml-pipeline.yml")
    assert on["repository_dispatch"]["types"] == ["dataform_complete"]
    jobs = wf["jobs"]
    chain = ["text-embeddings", "complexity", "scoring", "game-embeddings", "collection-scoring"]
    for prev, job in zip(chain, chain[1:]):
        assert jobs[job]["needs"] == prev
    assert jobs["collection-reports"]["needs"] == "collection-scoring"
    assert jobs["notify-warehouse"]["needs"] == "collection-scoring"
    assert "ml_complete" in str(jobs["notify-warehouse"])
    for job in chain + ["collection-reports"]:
        assert jobs[job]["uses"].startswith("./.github/workflows/")
        assert jobs[job]["secrets"] == "inherit"


def test_stages_are_callable_and_have_no_old_triggers():
    for name in STAGES:
        wf, on = _load(name)
        text = (WF_DIR / name).read_text(encoding="utf-8")
        assert "workflow_call" in on, name
        assert "workflow_dispatch" in on, name
        assert "repository_dispatch" not in on and "workflow_run" not in on and "schedule" not in on, name
        for event in OLD_EVENTS:
            assert event not in text, (name, event)
```

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run pytest -q tests/test_ml_pipeline_workflow.py`
Expected: FAIL, because `ml-pipeline.yml` doesn't exist.

- [ ] **Step 3: Create `.github/workflows/ml-pipeline.yml`**

```yaml
name: ML Pipeline

# One run per day: called by bgg-data-warehouse after the Dataform `core` piece.
# Stages run as reusable workflows in order; any failure skips the rest, so
# ml_complete (and the warehouse `publish` piece) only follow a full success.
# A single orchestrator because workflow_run cannot chain more than three levels.
on:
  repository_dispatch:
    types: [dataform_complete]
  workflow_dispatch:

permissions:
  contents: read
  id-token: write
  pages: write

jobs:
  text-embeddings:
    uses: ./.github/workflows/run-generate-text-embeddings.yml
    secrets: inherit
  complexity:
    needs: text-embeddings
    uses: ./.github/workflows/run-complexity-scoring.yml
    secrets: inherit
  scoring:
    needs: complexity
    uses: ./.github/workflows/run-scoring-service.yml
    secrets: inherit
  game-embeddings:
    needs: scoring
    uses: ./.github/workflows/run-generate-embeddings.yml
    secrets: inherit
  collection-scoring:
    needs: game-embeddings
    uses: ./.github/workflows/run-collection-scoring.yml
    secrets: inherit
  collection-reports:
    needs: collection-scoring
    uses: ./.github/workflows/build-collection-reports.yml
    secrets: inherit
  notify-warehouse:
    needs: collection-scoring
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    steps:
      - name: Send ml_complete
        env:
          GITHUB_TOKEN: ${{ secrets.CROSS_REPO_PAT }}
        run: |
          curl --fail-with-body -X POST \
            -H "Authorization: token $GITHUB_TOKEN" \
            -H "Accept: application/vnd.github.v3+json" \
            https://api.github.com/repos/phenrickson/bgg-data-warehouse/dispatches \
            -d '{"event_type": "ml_complete", "client_payload": {"run_id": "${{ github.run_id }}"}}'
```

- [ ] **Step 4: Edit the six stage workflows**

In each:
- Replace its trigger lines under `on:` with `workflow_call:` and keep the existing
  `workflow_dispatch` block unchanged:
  - `run-generate-text-embeddings.yml`: drop `repository_dispatch: types: [dataform_complete]`
  - `run-complexity-scoring.yml`: drop `repository_dispatch: types: [dataform_text_embeddings_ready]`
  - `run-scoring-service.yml`: drop `repository_dispatch: types: [dataform_complexity_ready]`
  - `run-generate-embeddings.yml`: drop the `workflow_run:` block. Change its job `if:`
    (L30) to remove the `workflow_run` clause; delete the line if nothing remains.
  - `run-collection-scoring.yml`: drop `schedule:` (`0 8 * * *`)
  - `build-collection-reports.yml`: drop `schedule:` (`0 9 * * *`). Move its top-level
    `concurrency:` block into the `deploy` job, since reusable workflows take job-level
    concurrency. Its top-level `permissions:` stays.
- Delete the cross-repo notify:
  - `run-generate-text-embeddings.yml`: the `notify-data-warehouse` job (L134-149)
  - `run-complexity-scoring.yml`: the "Notify Data Warehouse" step (L121-131)
  - `run-generate-embeddings.yml`: the `notify-data-warehouse` job (L244-258)

`github.event.inputs.*` defaults (`|| 'complexity-v2026'` and so on) already cover the
`workflow_call` case, where the inputs are empty. Leave them as they are.

- [ ] **Step 5: Run the test and actionlint**

Run: `uv run pytest -q tests/test_ml_pipeline_workflow.py`
Expected: 2 passed.
Run: `uvx --from actionlint-py actionlint .github/workflows/ml-pipeline.yml .github/workflows/run-*.yml .github/workflows/build-collection-reports.yml`
Expected: no errors.

- [ ] **Step 6: Commit, push and open the PR**

```bash
git add .github/workflows tests/test_ml_pipeline_workflow.py
git commit -m "feat(ci): one ML pipeline workflow from dataform_complete to ml_complete"
git push -u origin feat/ml-pipeline-orchestrator
gh pr create --title "feat(ci): one ML pipeline workflow from dataform_complete to ml_complete" --body "Task 4 of the daily-pipeline plan. Requires warehouse Task 3 (ml_complete handler) merged first. After merge the next daily run goes: Dataform (still full) → ML Pipeline → ml_complete → publish."
```

- [ ] **Step 7: Watch the next daily run (no manual trigger)**

After Phil merges, wait for the next scheduled run. Read `gh run list --workflow ml-pipeline.yml -L 1`, then confirm three things:
- all 7 jobs succeeded
- the warehouse `Run Dataform` shows a run with `action=ml_complete`
- no `text_embeddings_complete`, `complexity_complete` or `embeddings_complete` runs
  appeared

Report the results.

---

### Task 5: Post-fetch run is `core` only; retire the old callbacks (warehouse)

**Branch:** `feat/dataform-core-after-fetch`

**Files:**
- Modify: `.github/workflows/dataform.yml`, `tests/test_dataform_workflow.py`

**Interfaces:**
- Consumes: Task 4 is live (the old events are no longer sent).

- [ ] **Step 1: Add the failing tests**

```python
def test_old_callbacks_retired():
    assert ON["repository_dispatch"]["types"] == ["ml_complete"]
    for event in ["text_embeddings_complete", "complexity_complete", "embeddings_complete",
                  "dataform_complexity_ready", "dataform_text_embeddings_ready"]:
        assert event not in TEXT, event


def test_post_fetch_runs_core():
    assert 'github.event_name }}" == "workflow_run" ]; then PIECE="core"' in TEXT
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run --extra test python -m pytest tests/test_dataform_workflow.py -q`
Expected: the 2 new tests FAIL.

- [ ] **Step 3: Change `dataform.yml`**

1. `repository_dispatch.types` becomes `[ml_complete]`.
2. In "Choose piece", add after the `workflow_dispatch` line:
   `if [ "${{ github.event_name }}" == "workflow_run" ]; then PIECE="core"; fi`
3. In "Notify ML Pipeline", replace the whole `if repository_dispatch … else … fi`
   block with:

```bash
          if [ "${{ github.event.action }}" == "ml_complete" ]; then
            echo "Publish complete. No further dispatch."; exit 0
          fi
          if [ "${{ steps.piece.outputs.tag }}" == "publish" ]; then
            echo "Manual publish run. No ML dispatch."; exit 0
          fi
          EVENT_TYPE="dataform_complete"
```

4. In "Notify viewer (catalog artifact)", change the condition to
   `github.event.action == 'ml_complete'` only.

- [ ] **Step 4: Run all workflow tests and actionlint**

Run: `uv run --extra test python -m pytest tests/test_dataform_workflow.py tests/test_dataform_tags.py -q`
Expected: all pass. Note that `test_ml_complete_is_accepted` still holds.
Run: `uvx --from actionlint-py actionlint .github/workflows/dataform.yml`
Expected: no errors.

- [ ] **Step 5: Commit, push and open the PR**

```bash
git add .github/workflows/dataform.yml tests/test_dataform_workflow.py
git commit -m "feat(ci): run only the core piece after fetch; retire ML callbacks"
git push -u origin feat/dataform-core-after-fetch
gh pr create --title "feat(ci): run only the core piece after fetch; retire ML callbacks" --body "Task 5 of docs/superpowers/plans/2026-10-05-daily-pipeline.md. Pushes and manual runs keep the full graph by default."
```

- [ ] **Step 6: After merge, watch one daily run**

The expected sequence is `Run Dataform` (workflow_run, piece core: 13 actions), then
`ML Pipeline`, then `Run Dataform` (ml_complete, piece publish: 12 actions), then
`catalog_refresh`. Report the two invocations' action counts.

---

### Task 6: Dev job with `_dev` schema suffix (warehouse)

**Branch:** `feat/dataform-dev-job`

**Files:**
- Create: `.github/workflows/dataform-dev.yml`

**Interfaces:**
- Produces: a manual workflow that compiles `main` (or a chosen branch) with
  `schemaSuffix: "dev"` and invokes the full graph.

- [ ] **Step 1: Create `.github/workflows/dataform-dev.yml`**

```yaml
name: Run Dataform (dev)

# Full graph into <dataset>_dev datasets. Never writes production datasets.
on:
  workflow_dispatch:
    inputs:
      ref:
        description: 'Git commitish to compile'
        required: true
        default: 'main'

concurrency:
  group: dataform-dev
  cancel-in-progress: false

env:
  GCP_PROJECT_ID: bgg-data-warehouse
  GCP_REGION: us-central1
  DATAFORM_REPO: bgg-data-warehouse

jobs:
  dataform-dev:
    runs-on: ubuntu-latest
    steps:
      - uses: google-github-actions/auth@v2
        with:
          credentials_json: ${{ secrets.GCP_SA_KEY_BGG_DW }}
      - uses: google-github-actions/setup-gcloud@v2
      - name: Compile with dev suffix
        id: compile
        run: |
          ACCESS_TOKEN=$(gcloud auth print-access-token)
          API_BASE="https://dataform.googleapis.com/v1beta1/projects/${GCP_PROJECT_ID}/locations/${GCP_REGION}/repositories/${DATAFORM_REPO}"
          BODY=$(jq -n --arg r "${{ github.event.inputs.ref }}" '{gitCommitish: $r, codeCompilationConfig: {schemaSuffix: "dev"}}')
          RESULT=$(curl -s -X POST "${API_BASE}/compilationResults" -H "Authorization: Bearer ${ACCESS_TOKEN}" -H "Content-Type: application/json" -d "${BODY}")
          NAME=$(echo "${RESULT}" | jq -r '.name')
          [ -n "${NAME}" ] && [ "${NAME}" != "null" ] || { echo "${RESULT}"; exit 1; }
          echo "name=${NAME}" >> $GITHUB_OUTPUT
          # Guard: every non-declaration target must be in a *_dev dataset.
          ACTIONS=$(curl -s "https://dataform.googleapis.com/v1beta1/${NAME}:query?pageSize=1000" -H "Authorization: Bearer ${ACCESS_TOKEN}")
          BAD=$(echo "${ACTIONS}" | jq -r '.compilationResultActions[] | select(.declaration == null) | .target.schema | select(endswith("_dev") | not)')
          if [ -n "${BAD}" ]; then echo "Refusing: non-dev targets: ${BAD}"; exit 1; fi
      - name: Invoke full graph
        run: |
          ACCESS_TOKEN=$(gcloud auth print-access-token)
          API_BASE="https://dataform.googleapis.com/v1beta1/projects/${GCP_PROJECT_ID}/locations/${GCP_REGION}/repositories/${DATAFORM_REPO}"
          curl -s -X POST "${API_BASE}/workflowInvocations" -H "Authorization: Bearer ${ACCESS_TOKEN}" -H "Content-Type: application/json" \
            -d "$(jq -n --arg c "${{ steps.compile.outputs.name }}" '{compilationResult: $c}')"
```

- [ ] **Step 2: actionlint**

Run: `uvx --from actionlint-py actionlint .github/workflows/dataform-dev.yml`
Expected: no errors.

- [ ] **Step 3: Commit, push and open the PR**

```bash
git add .github/workflows/dataform-dev.yml
git commit -m "feat(ci): manual Dataform dev run into *_dev datasets"
git push -u origin feat/dataform-dev-job
gh pr create --title "feat(ci): manual Dataform dev run into *_dev datasets" --body "Task 6 of docs/superpowers/plans/2026-10-05-daily-pipeline.md. The compile step refuses to invoke if any non-declaration target lacks the _dev suffix."
```

- [ ] **Step 4: After merge, a first dev run (Phil decides when)**

It creates `analytics_dev`, `staging_dev`, `predictions_dev`, `monitoring_dev` and
`collections_dev`, at about 2.3 GB. Ask Phil, then
`gh workflow run dataform-dev.yml --ref main -f ref=main`. Confirm two things:
- the guard passed, meaning declared sources (`core.*`, cross-project) stayed unsuffixed
  and every output went to `*_dev`
- `bq ls` shows the new `_dev` datasets and no change to production table modified times

---

### Task 7: Delete the dash-viewer-only models (warehouse) — only after Phil confirms retirement

**Branch:** `chore/drop-dash-viewer-models`

**Files:**
- Delete: `definitions/filter_categories.sqlx`, `filter_mechanics.sqlx`,
  `filter_designers.sqlx`, `filter_publishers.sqlx`, `filter_options_combined.sqlx`,
  `game_dropdown_options.sqlx`
- Modify: `tests/test_dataform_tags.py` (remove the six names from `CORE`)

- [ ] **Step 1: Update the test first**

Remove the six names from `CORE` in `tests/test_dataform_tags.py`.
Run: `uv run --extra test python -m pytest tests/test_dataform_tags.py -q`
Expected: FAIL, because `names` still contains the six files.

- [ ] **Step 2: Delete the six `.sqlx` files and re-run**

Run the same test command. Expected: 2 passed. Then run
`npx -y @dataform/cli@3.0.0 compile`. Expected: no errors (nothing refs them).

- [ ] **Step 3: Commit, push and open the PR**

```bash
git add -A definitions tests/test_dataform_tags.py
git commit -m "chore(dataform): drop dash-viewer-only filter and dropdown models"
git push -u origin chore/drop-dash-viewer-models
gh pr create --title "chore(dataform): drop dash-viewer-only filter and dropdown models" --body "Task 7 of docs/superpowers/plans/2026-10-05-daily-pipeline.md. The existing analytics tables are left in place; dropping them is a separate decision."
```

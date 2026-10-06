# Pipeline Monitor v2: the daily chain after the cutover

> Follows [Pipeline Monitor](2026-10-02-pipeline-monitor-design.md) (#135) and the
> [daily pipeline](2026-10-05-daily-pipeline-design.md) cutover (#146, #147, ML #80).
> Spans bgg-data-warehouse (API, Dataform) and bgg-viewer (`/admin/pipeline`).

## Problem

The daily chain is now:

```
Fetch Thing IDs → Fetch New Games → Refresh Old Games → Dataform · core
  → ML Pipeline (one run: text embeddings → complexity → scoring → game embeddings
                 + coordinates → collection scoring → ml_complete; reports alongside)
  → Dataform · publish → Viewer Artifacts
```

#147 cut the monitor down to seven stages. The page now has three problems:

- **It shows ML Pipeline as one block.** Six stages that used to be separate runs are
  now jobs inside one run. The page can't say where inside ML Pipeline a run stopped.
- **It misreads collection failures.** The orchestrator lets a collection scoring
  failure through to publish, but GitHub still marks the whole ML Pipeline run
  `failure`. Read from the run's conclusion, a collection failure looks like a chain
  failure.
- **Its history judges old days by new stages.** Days before the cutover ran the
  twelve-stage chain, so the grid shows them as stopped at ML Pipeline.

Separately, **Deployed models asks the wrong question.** `monitoring.deployed_models`
lists every version still behind some current prediction, and flags older ones. What
matters is which model each scoring step is using now.

## Goal

The page answers the same three questions for the new chain:

1. Did today's chain run end to end, and if not, where did it stop? This includes
   which step inside ML Pipeline.
2. Is each downstream table fresh and covering what it should? Unchanged.
3. Which model is each scoring step using now? One line per game-model step, and one
   line per collection user and outcome.

## Decisions

- **Nested, not flattened.** ML Pipeline stays one chain stage, with its steps nested
  under it. Hand-offs happen between runs; inside the run, `needs:` sequences the jobs,
  so there is no hand-off to stall on.
- **Collection scoring and reports are a side branch.** If either fails, the verdict is
  an amber warning. It is not a chain failure, because publish still ran.
- **Pre-cutover days collapse to one "old chain" cell.** They are not re-judged by the
  new stages, and the old chain definition is not kept around. They fall off the
  14-day history on their own.
- **Models in use come from what ran, not from config.** For each step, the model name
  and version come from the rows its latest run wrote. Config can drift from what the
  services actually load; the rows can't.
- **`monitoring.deployed_models` is rebuilt as a Dataform table**, not computed in the
  API on each load. It changes only when the chain runs, it stays a `publish` action,
  and Dataform already declares every landing table it reads.
- **The `models` response changes shape in place.** No second shape is served while
  the viewer catches up. Phil is the only reader of an admin page.
- **Fetch jobs only when they can change the answer.** A successful ML Pipeline run
  means every step succeeded. Jobs are fetched for today's run, and for history runs
  that did not succeed.

## Status rules

### ML Pipeline steps

The jobs of the day's ML Pipeline run are grouped into steps by the caller-job prefix,
the text before ` / ` in the job name (`notify-warehouse` has none):

| Step key | Label | Jobs | Branch |
|---|---|---|---|
| `text_embeddings` | Text embeddings | `text-embeddings / *` | main |
| `complexity` | Complexity | `complexity / *` | main |
| `scoring` | Scoring | `scoring / *` | main |
| `game_embeddings` | Game embeddings | `game-embeddings / *` (embeddings, coordinates) | main |
| `ml_complete` | ml_complete sent | `notify-warehouse` | main |
| `collection_scoring` | Collection scoring | `collection-scoring / *` | side |
| `collection_reports` | Collection reports | `collection-reports / *` (discover, one per user, deploy) | side |

Step status, from its jobs:

| Status | Rule |
|---|---|
| `fail` | any job concluded `failure`, `cancelled` or `timed_out` |
| `running` | any job is `in_progress` |
| `pending` | no job has started: the step has no jobs yet, or its jobs are `queued` / `waiting` / `pending` on an upstream job |
| `ok` | every job concluded `success` or `skipped`, and at least one is `success`; note `"<job> skipped"` for each skipped job |
| `not_reached` | every job concluded `skipped` (GitHub skips dependents of a failed job) |

Rules are checked top to bottom; the first match wins.

A skipped `generate-coordinates` after a successful `generate-embeddings` is `ok`. That
is the 0-new-games case, and the condition that skips it has been in the workflow
since January.

Each step carries `started` (its earliest job start), `finished` (its latest job
completion), and `url` (its first job's `html_url`).

### ML Pipeline stage

It is judged from the **main-line steps only**:

- `fail` if any main-line step is `fail`. The note is `"failed at <label>"`, naming
  the first one.
- `ok` if `ml_complete` is `ok`.
- `running` otherwise, while the run is not completed.
- If the run completed without `ml_complete` and no main-line failure, it is `fail`
  with the note `"ml_complete not sent"`.

The run's own conclusion is ignored. A completed run with no jobs falls back to the
existing run-level rule.

The existing hand-off rule then applies to `Dataform · publish` unchanged.

### Side branch

`collection_scoring` and `collection_reports` keep their own step status. They do not
affect the ML Pipeline stage or the chain's `blocked` flag.

### Verdict

Unchanged for the main line. A main-line ML failure reads
`"ML Pipeline failed at Complexity"`. When every main-line stage is `ok` but a side step
is `fail`:

```json
{"status": "warn", "stage": "collection_scoring",
 "headline": "Chain completed · Collection scoring failed", ...}
```

`duration_minutes` is still the end-to-end time.

### History and the cutover

`CUTOVER = date(2026, 10, 7)` is the first chain day run by the new daily chain. For a
history day before it:

```json
{"day": "2026-10-05", "era": "old", "status": "ok" | "fail" | "not_reached", "url": "..."}
```

- `ok` / `fail` from that day's `dataform.yml` run titled `embeddings_complete` (the
  old chain's last pass), using its conclusion.
- `not_reached` if there was none.
- `url` is that run's link.

Days from the cutover on keep the current shape (`"era": "new"`, plus `stages`). The
`ml_pipeline` cell also carries `side: "ok" | "fail" | null`. That is `"fail"` when a
side step failed that day, and `null` when jobs were not fetched for that day (the run
succeeded, so the side branch did too).

"Today's chain" always uses the new stages. This ships after the cutover, so there is
no pre-cutover "today".

## Components: bgg-data-warehouse

### `src/monitoring/github.py`

`fetch_jobs(repo, run_id, token, session=requests)` returns `name`, `status`,
`conclusion`, `started_at`, `completed_at` and `html_url` for each job of a run. It
follows `Link: rel="next"` pagination, as `fetch_runs` does, with `per_page=100`.

### `src/monitoring/chain.py`

- `ML_STEPS`: the table above as data (`key`, `label`, `prefix`, `branch`).
- `group_steps(jobs) -> list[StepStatus]`: a pure function that applies the step rules.
- `build_chain(runs, day, now, jobs=None)`: `jobs` maps an ML Pipeline run id to its
  jobs. The `ml_pipeline` `StageStatus` gains `steps` (main and side, in table order),
  and its status follows the ML Pipeline stage rules when jobs are present.
- `verdict` gains the side-branch warning.
- `build_report` emits the old-era history cells before `CUTOVER`, and `side` on the
  `ml_pipeline` cell from it on.
- `jobs_needed(runs, days, now) -> list[run]` names the ML Pipeline runs whose jobs to
  fetch: today's, plus any non-successful run in the history window. The router fetches
  exactly those.

### `services/warehouse_api/routers/monitoring.py`

Calls `jobs_needed`, fetches those runs' jobs with `fetch_jobs`, and passes them to
`build_report`. The response's `models` takes the new table's rows.

### `definitions/deployed_models.sqlx`

A `publish`-tagged table, `monitoring.deployed_models`, with one row per step for its
latest run within 30 days. The latest run is the `job_id` of the step's newest row. Each
source filters its partition column to the last 30 days.

| Column | Type | Meaning |
|---|---|---|
| `model_category` | STRING | `game` / `collection` |
| `model_type` | STRING | `hurdle`, `rating`, `users_rated`, `geek_rating`, `complexity`, `text_embedding`, `game_embedding`; the outcome for collections |
| `username` | STRING | collection user; NULL for game models |
| `model_name` | STRING | |
| `model_version` | STRING | |
| `last_scored` | TIMESTAMP | newest row of that run |
| `games_scored` | INT64 | rows that run wrote |
| `job_id` | STRING | |

| Step | Source (declared) | Partition column | Name / version columns |
|---|---|---|---|
| hurdle, rating, users_rated, geek_rating | `bgg-predictive-models.raw.ml_predictions_landing` | `score_ts` | `<type>_model_name`, `<type>_model_version` |
| complexity | `bgg-predictive-models.raw.complexity_predictions` | `score_ts` | `complexity_model_name`, `complexity_model_version` |
| text_embedding | `bgg-predictive-models.raw.description_embeddings` | `created_ts` | `embedding_model`, `embedding_version` |
| game_embedding | `bgg-predictive-models.raw.game_embeddings` | `created_ts` | `embedding_model`, `embedding_version` |
| collections | `bgg-predictive-models.raw.collection_predictions_landing` | `score_ts` | `model_name`, `model_version`, per `username`, `outcome` |

The exact column names, and whether each source has a `job_id`, get confirmed against
the live schemas in planning. If a run used two versions, both rows appear. A step with
no rows in 30 days has no row, and the page shows it as missing.

Validated with a Dataform compile and a dry-run of the `CREATE TABLE`.

### Reader

`src/warehouse/readers/monitoring.py`: the deployed-models reader selects the new
columns, ordered by `model_category`, `model_type`, `username`.

## Components: bgg-viewer

On `/admin/pipeline`:

- **`PipelineStatus` types:** the stage `steps`, the history `era` / `side` fields, and
  the new `models` rows.
- **`Verdict`:** no logic change. The amber headline arrives from the API.
- **`ChainLanes`:** the ML Pipeline card lists its main-line steps (glyph, status word,
  duration, link to the job), then the side steps under a "doesn't block publish" rule.
  The other stages are unchanged.
- **`HistoryGrid`:**
  - An `era: "old"` day renders as one merged, muted cell down its column ("old chain"
    with its glyph and link).
  - New-era days are unchanged, except that the ML Pipeline cell shows a small amber
    corner mark when `side == "fail"`.
- **`DeployedModels`:** two tables.
  - *Scoring models*: type, model, version, last scored, games scored.
  - *Collections*: user, outcome, model, version, last scored.
  - A row whose `last_scored` is more than 48 hours before the page's
    `generated_at` shows amber "stale".
  - The section subtitle becomes "the model each scoring step used in its latest run".

Reuse the existing status tokens, glyphs and UI components (Phil's rule). There are no
new colours.

## Testing

- **Warehouse:** fixture-based unit tests for `group_steps`, `build_chain`, `verdict`
  and `build_report`.
  - ML Pipeline cases: today's run (coordinates skipped), a main-line failure, a
    collection failure with publish run, a run in progress, `ml_complete` missing.
  - History cases: a pre-cutover completed day, a failed day, a day with no run.
  - `fetch_jobs` pagination with a stubbed session.
  - `jobs_needed` with a successful history run (not fetched) and a failed one
    (fetched).
  - The reader's SQL and columns.
- **Dataform:** compile plus `CREATE TABLE` dry run.
- **Viewer:** component and load tests following the existing monitoring tests, for
  steps rendering, the side branch, old-era cells, the side mark, and the stale model
  row.

## Delivery

1. **Spec**: branch `docs/pipeline-monitor-v2` → PR.
2. **Warehouse**: branch off `main` → PR.
   - Merging redeploys the API (path filter from #150).
   - The push to `definitions/**` also triggers a full Dataform run that rebuilds
     `monitoring.deployed_models`.
   - Until the viewer releases, the Deployed models panel shows the old columns'
     blanks. Accepted.
3. **Viewer**: branch off `main` → PR, shipped by merging bgg-viewer's release PR.
   - Phil's checkout is on `feat/recommend-tool`. Ask how to handle it before starting.

Phil merges every PR.

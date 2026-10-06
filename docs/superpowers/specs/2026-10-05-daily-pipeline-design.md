# Daily pipeline: one run, two Dataform pieces — design

## Goal

Run the BGG pipeline once a day, with Dataform invoked twice: once before the ML stages
and once after them. Today Dataform runs the whole graph four times a day, plus on pushes:
233 runs in 35 days, about 470 GiB a month. Two of those runs exist only to copy one ML
output so the next ML stage can read it.

This spans `bgg-data-warehouse` and `bgg-predictive-models`.

## Current flow (verified)

| # | Step | Reads | Writes |
|---|---|---|---|
| 1 | Fetch Thing IDs (cron 06:00 UTC) | BGG | `raw.thing_ids` |
| 2 | Fetch New Games (fetch + Python processor) | BGG | `raw.raw_responses`, `fetched_responses`, `processed_responses`, `core.*` |
| 3 | Refresh Old Games (fetch + processor) | BGG, `core.games` | same as 2 |
| 4 | **Dataform run 1** (full graph) → `dataform_complete` | `core.*` | `analytics.*`, `staging.*`, `predictions.*` |
| 5 | Text embeddings → `text_embeddings_complete` | `analytics.games_features` | `bgg-predictive-models.raw.description_embeddings` |
| 6 | **Dataform run 2** (full graph) → `dataform_text_embeddings_ready` | | needed only: `predictions.bgg_description_embeddings` |
| 7 | Complexity scoring → `complexity_complete` | `games_features`, `predictions.bgg_description_embeddings`, `staging.game_features_hash` | `raw.complexity_predictions` |
| 8 | **Dataform run 3** (full graph) → `dataform_complexity_ready` | | needed only: `predictions.bgg_complexity_predictions` |
| 9 | Scoring service | `games_features`, complexity, description embeddings | `raw.ml_predictions_landing` |
| 10 | Game embeddings + coordinates (`workflow_run` after 9) → `embeddings_complete` | same as 9 | `raw.game_embeddings`, `raw.game_coordinates` |
| 11 | **Dataform run 4** (full graph) | ML landing tables | `predictions.*`, similarity, neighbors, profile |
| 12 | Notify bgg-viewer catalog build | | |

Outside the chain:
- Collection scoring, cron 08:00 → `raw.collection_predictions_landing`. Published by
  the next day's Dataform run.
- Collection reports, cron 09:00. Renders from collection scoring's parquet artifacts,
  not warehouse tables.

## Target flow

```
Fetch Thing IDs → Fetch New Games → Refresh Old Games        (unchanged)
  → Dataform: core        (includedTags: ["core"])
  → text embeddings → complexity → scoring → game embeddings + coordinates
      → collection scoring → collection reports              (ML chain, workflow_run on success)
  → Dataform: publish     (includedTags: ["publish"])
  → notify bgg-viewer
```

### Dataform tags

Every action gets exactly one of two tags. `publish` actions depend on `core` actions
that already ran earlier the same day, so a `publish` invocation does **not** include
dependencies.

- **`core`:** `games_active`, `games_features`, `game_features_hash`,
  `best_player_counts`, `player_count_recommendations`, `game_product_line`,
  `user_collections`, `filter_categories`, `filter_mechanics`, `filter_designers`,
  `filter_publishers`, `filter_options_combined`, `game_dropdown_options`.
- **`publish`:** `bgg_description_embeddings`, `bgg_complexity_predictions`,
  `game_first_prediction`, `bgg_predictions`, `bgg_game_embeddings`,
  `bgg_game_coordinates`, `user_collection_predictions`, `game_similarity_search`,
  `similarity_profiles`, `game_neighbors`, `game_profile`, `deployed_models`.

`dataform.yml` invokes `core` after the refresh, then dispatches to bgg-predictive-models.
It invokes `publish` on a single end-of-ML event, then notifies the viewer. The three
callback events (`text_embeddings_complete`, `complexity_complete`,
`embeddings_complete`) and their `dataform_*_ready` replies are retired.

### ML stages read ML outputs directly

Two ML outputs are read mid-chain today through Dataform copies. Readers switch to the
raw tables in `bgg-predictive-models` and apply **the same rule the Dataform model
applies**, so the inputs to each stage do not change:

| Data | Dataform rule (reproduced in the reader) | Readers |
|---|---|---|
| Description embeddings | latest `embedding_version` overall, then latest row per game by `created_ts`, `job_id` | `src/data/loader.py`, `src/models/embeddings/data.py` |
| Complexity | latest row per game by `score_ts`, `job_id` | `src/data/loader.py`, `src/models/embeddings/data.py` |

`loader.py` already applies latest-per-game itself (`ROW_NUMBER … ORDER BY … DESC`). Its
change is mostly the table it points at, plus the embedding-version filter.

**Complexity reads must not scan superseded model versions.**
`raw.complexity_predictions` holds 3.86M rows. 3.5M of them are versions 1–2
(Jan–Mar 2026), all in partitions before 2026-03-12. Version 3 has 362k rows covering all
129k games. The table is partitioned on `score_ts`, so a constant lower bound on
`score_ts` prunes the old versions. The plan settles where that bound comes from.

`src/models/explain.py` keeps reading the published copy. It runs on demand, not in the
chain.

### ML chain

Each stage triggers the next with `workflow_run` (`types: [completed]`, branch `main`, run
only on success). Game embeddings already follows scoring this way. Collection scoring
moves from its 08:00 cron to follow game embeddings. Collection reports moves from its
09:00 cron to follow collection scoring. The last stage dispatches the single
end-of-ML event to `bgg-data-warehouse`.

If any stage fails, the chain stops and `publish` does not run. That is the same outcome
as today.

### Dev job

A manual workflow that compiles the full graph with `schemaSuffix: "_dev"` and invokes
it. Every output lands in `<dataset>_dev`. Production datasets are never written. It is
used to test Dataform changes before merge.

### Removing dead models

The `filter_*` tables, `filter_options_combined` and `game_dropdown_options` have one
consumer, bgg-dash-viewer, which is being retired. They are deleted once it is retired.
That is a separate change, gated on the retirement.

## Rollout order

Each step is a PR that leaves the pipeline working.

1. **Tags:** add `core`/`publish` tags in `definitions/`. No behaviour change, since the
   workflow still runs the full graph.
2. **Readers:** ML readers switch to the raw tables. This works under the current chain
   too. Before switching, verify the new reader queries return the same rows as the old
   ones (a set comparison).
3. **Warehouse accepts the new event:** `dataform.yml` gains the end-of-ML event, which
   runs `publish`. The old events keep working.
4. **ML chain:** stages chain via `workflow_run`, collection scoring and reports join,
   and the last stage sends the end-of-ML event. The three old events stop being sent.
5. **Warehouse cleanup:** `dataform.yml` runs `core` after the refresh and drops the old
   callback handling.
6. **Dev job.**
7. **Later, gated on dash-viewer retirement:** delete the dead models.

## Success criteria

- Two production Dataform invocations a day (`core`, `publish`), plus pushes and dev runs.
- ML stage inputs unchanged: the reader comparison in step 2 matches.
- Collection predictions publish the same day.
- Dataform cost drops from about 470 GiB to about 65 GiB a month (`core` ~0.85 GiB per
  run, `publish` ~1.3 GiB per run, from the per-model job history).

## Unchanged by this design

- **Pushes to `main` under `definitions/`** keep triggering a full run. To be revisited
  separately.
- **Model materialisation:** incremental models stay incremental and table models stay
  tables. Incremental vs full rebuild is a separate decision.
- **The Python processor** still writes `core`. Replacing it with SQL parsing in Dataform
  is a separate cutover, made when Phil decides.

## To verify during planning

- Pipeline Status (`pipeline_status.yml`, 12:00) checks job runs. Confirm it does not
  depend on the retired events or the four-run pattern.
- That `schemaSuffix` behaves as assumed for declared sources (`core.*`,
  cross-project tables), with one test compile.

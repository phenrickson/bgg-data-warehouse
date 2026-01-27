# Pipeline Event Flow Redesign

## Problem

The current ML pipeline has a data dependency bug:

1. Complexity scoring writes to `raw.complexity_predictions`
2. Game/Text Embeddings read from `predictions.bgg_complexity_predictions` (Dataform-managed)
3. But Dataform doesn't run until AFTER embeddings complete

Result: Embeddings are generated using stale complexity predictions.

## Solution

Insert a Dataform run between complexity scoring and embeddings to materialize the complexity predictions before they're needed.

## New Pipeline Flow

```
Dataform (scheduled/after refresh)
    │ dataform_complete
    ▼
Complexity Scoring → raw.complexity_predictions
    │ complexity_complete
    ▼
Dataform (2nd run) → materializes bgg_complexity_predictions
    │ dataform_complexity_ready
    ▼
Main Scoring → raw.ml_predictions_landing
    │ workflow_run
    ▼
Text Embeddings → raw.description_embeddings
    │ workflow_run
    ▼
Game Embeddings → raw.game_embeddings
    │ embeddings_complete
    ▼
Dataform (3rd run) → materializes all predictions + embeddings
    │
    ▼ STOP (no dispatch)
```

## Event Names

### bgg-predictive-models → bgg-data-warehouse

| Event | Sent by | Meaning |
|-------|---------|---------|
| `complexity_complete` | Complexity Scoring | Complexity predictions in raw table |
| `embeddings_complete` | Game Embeddings | All embeddings in raw tables |

### bgg-data-warehouse → bgg-predictive-models

| Event | Sent by | Meaning |
|-------|---------|---------|
| `dataform_complete` | Dataform (initial) | Fresh features ready, start ML pipeline |
| `dataform_complexity_ready` | Dataform (after complexity) | Complexity materialized, continue pipeline |

## Dataform Dispatch Logic

```
if triggered by complexity_complete:
    → dispatch dataform_complexity_ready
elif triggered by embeddings_complete:
    → no dispatch (end of pipeline)
else (schedule, workflow_run, push):
    → dispatch dataform_complete
```

## File Changes

### bgg-predictive-models

1. **`run-complexity-scoring.yml`**
   - Add: send `complexity_complete` when done
   - Remove: cron schedule

2. **`run-scoring-service.yml`**
   - Change trigger: `dataform_complexity_ready` (not workflow_run after complexity)
   - Remove: cron schedule

3. **`run-generate-text-embeddings.yml`**
   - Change trigger: `workflow_run` after Scoring Service
   - Remove: `predictions_complete` dispatch
   - Remove: cron schedule

4. **`run-generate-embeddings.yml`**
   - Change trigger: `workflow_run` after Text Embeddings
   - Add: send `embeddings_complete` when done
   - Remove: cron schedule

### bgg-data-warehouse

5. **`dataform.yml`**
   - Add: `complexity_complete` to repository_dispatch types
   - Rename: `predictions_complete` to `embeddings_complete`
   - Update: dispatch logic based on trigger source

## Additional Changes

- Text Embeddings now runs BEFORE Game Embeddings (future-proofing for when game embeddings incorporates description embeddings)
- All cron schedules removed from ML workflows - pipeline is purely event-driven
- Only scheduled entry point: Dataform cron at `30 8 * * *`

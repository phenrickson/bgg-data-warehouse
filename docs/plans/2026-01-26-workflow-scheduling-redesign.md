# Workflow Scheduling Redesign

## Problem Statement

The current scheduling between `bgg-data-warehouse` and `bgg-predictive-models` has two main issues:

1. **Data Freshness**: ML scoring can run before Dataform has finished transforming warehouse data, causing predictions to use stale `games_features`
2. **Wasted Compute**: Jobs run on fixed schedules regardless of whether upstream data changed

## Current State

### Timeline (UTC)

| Time | Repo | Job | Reads From | Writes To |
|------|------|-----|------------|-----------|
| 6:00 | warehouse | Fetch New Games | BGG API | `raw.raw_responses` |
| 6:00 | predictive | Complexity Scoring | `analytics.games_features` | `raw.complexity_predictions` |
| 6:30 | warehouse | Dataform | `raw.*`, `core.*`, `predictive.raw.*` | `analytics.*`, `staging.game_features_hash` |
| 7:00 | warehouse | Refresh Old Games | BGG API | `raw.raw_responses` |
| 7:00 | predictive | Main Scoring Service | `analytics.games_features` | `raw.ml_predictions_landing` |
| 7:00 | predictive | Game Embeddings | `analytics.games_features` | `raw.game_embeddings` |
| 7:30 | warehouse | Dataform | (same) | (same) |
| 8:00 | predictive | Text Embeddings | `analytics.games_features` | `raw.description_embeddings` |
| 8:30 | warehouse | Dataform | (same) | (same) |

### Problems

1. **Complexity scoring at 6:00 AM** reads `games_features` before the 6:30 AM Dataform run updates it with today's new games
2. **Main scoring at 7:00 AM** may start before 6:30 AM Dataform finishes
3. **Dataform runs 3x daily** regardless of whether ML predictions have landed
4. **No skip logic** - all jobs run even when upstream data hasn't changed

### What's Already Working

- `game_features_hash` table tracks feature changes via `FARM_FINGERPRINT`
- Complexity scoring already uses change detection (lines 282-307 in `scoring_service/main.py`)
- Dataform triggers on `workflow_run` completion from fetch/refresh jobs

---

## Proposed Design: Event-Driven Pipeline

### Architecture

```
PHASE 1: Data Collection (6:00 AM UTC trigger)
┌─────────────────────────────────────────────────────────────┐
│  warehouse: Fetch New Games ──┬──> warehouse: Refresh Old   │
│                               │    Games (parallel)         │
│                               │                             │
│  Both use workflow_run to trigger Phase 2                   │
└───────────────────────────────┼─────────────────────────────┘
                                │
                                ▼
PHASE 2: Warehouse Transform
┌─────────────────────────────────────────────────────────────┐
│  warehouse: Dataform                                        │
│    - Transforms raw data to analytics tables                │
│    - Updates game_features_hash with change timestamps      │
│                                                             │
│  On success: repository_dispatch to bgg-predictive-models   │
│              event_type: "dataform_complete"                │
└───────────────────────────────┼─────────────────────────────┘
                                │
                                ▼
PHASE 3: ML Predictions (sequential, event-driven)
┌─────────────────────────────────────────────────────────────┐
│  predictive: Complexity Scoring                             │
│    - Uses game_features_hash for change detection           │
│    - Only scores games with updated features                │
│                                                             │
│  On success: triggers Main Scoring via workflow_run         │
└───────────────────────────────┼─────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────┐
│  predictive: Main Scoring Service                           │
│    - NEW: Uses game_features_hash for change detection      │
│    - Only scores games needing predictions                  │
│                                                             │
│  On success: triggers Embeddings via workflow_run           │
└───────────────────────────────┼─────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────┐
│  predictive: Game Embeddings                                │
│    - NEW: Uses change detection                             │
│    - Only embeds games with updated features                │
│                                                             │
│  On success: triggers Text Embeddings via workflow_run      │
└───────────────────────────────┼─────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────┐
│  predictive: Text Embeddings                                │
│    - NEW: Uses change detection                             │
│                                                             │
│  On success: repository_dispatch to bgg-data-warehouse      │
│              event_type: "predictions_complete"             │
└───────────────────────────────┼─────────────────────────────┘
                                │
                                ▼
PHASE 4: Final Sync
┌─────────────────────────────────────────────────────────────┐
│  warehouse: Dataform (predictions sync only)                │
│    - Syncs prediction tables to analytics                   │
│    - Single run after all ML completes                      │
└─────────────────────────────────────────────────────────────┘
```

---

## Implementation Details

### 1. Cross-Repository Dispatch

**Requirement**: GitHub Personal Access Token with `repo` scope, stored as `CROSS_REPO_PAT` secret in both repos.

**bgg-data-warehouse/dataform.yml** - Add dispatch after success:

```yaml
- name: Notify ML Pipeline
  if: success()
  env:
    GITHUB_TOKEN: ${{ secrets.CROSS_REPO_PAT }}
  run: |
    curl -X POST \
      -H "Authorization: token $GITHUB_TOKEN" \
      -H "Accept: application/vnd.github.v3+json" \
      https://api.github.com/repos/phenrickson/bgg-predictive-models/dispatches \
      -d '{"event_type": "dataform_complete", "client_payload": {"run_id": "${{ github.run_id }}"}}'
```

**bgg-predictive-models** - Final workflow dispatches back:

```yaml
- name: Notify Data Warehouse
  if: success()
  env:
    GITHUB_TOKEN: ${{ secrets.CROSS_REPO_PAT }}
  run: |
    curl -X POST \
      -H "Authorization: token $GITHUB_TOKEN" \
      -H "Accept: application/vnd.github.v3+json" \
      https://api.github.com/repos/phenrickson/bgg-data-warehouse/dispatches \
      -d '{"event_type": "predictions_complete"}'
```

---

### 2. Workflow Trigger Changes

**bgg-predictive-models/run-complexity-scoring.yml**:

```yaml
on:
  repository_dispatch:
    types: [dataform_complete]
  workflow_dispatch:
  # REMOVE: schedule cron
```

**bgg-predictive-models/run-scoring-service.yml**:

```yaml
on:
  workflow_run:
    workflows: ["Score Complexity Predictions"]
    types: [completed]
    branches: [main]
  workflow_dispatch:
  # REMOVE: schedule cron
```

**bgg-predictive-models/run-generate-embeddings.yml**:

```yaml
on:
  workflow_run:
    workflows: ["Run Scoring Service"]
    types: [completed]
    branches: [main]
  workflow_dispatch:
  # REMOVE: schedule cron
```

**bgg-predictive-models/run-generate-text-embeddings.yml**:

```yaml
on:
  workflow_run:
    workflows: ["Run Game Embeddings"]
    types: [completed]
    branches: [main]
  workflow_dispatch:
  # REMOVE: schedule cron
```

**bgg-data-warehouse/dataform.yml**:

```yaml
on:
  workflow_run:
    workflows: ["Run Fetch New Games", "Run Refresh Old Games"]
    types: [completed]

  repository_dispatch:
    types: [predictions_complete]

  push:
    branches: [main]
    paths:
      - 'definitions/**'
      - 'workflow_settings.yaml'

  workflow_dispatch:

  # REMOVE: All cron schedules
```

---

### 3. Change Detection for Main Scoring Service

Add a new function to `scoring_service/main.py`:

```python
def load_games_for_main_scoring(
    start_year: int,
    end_year: int,
    max_games: int = 50000
) -> pd.DataFrame:
    """
    Load games that need main predictions (hurdle, rating, users_rated).

    Returns games that are:
    - In the year range AND
    - Either never scored OR have changed features since last scoring
    """
    config = load_config()
    data_warehouse_config = config.get_data_warehouse_config()
    loader = BGGDataLoader(data_warehouse_config)

    where_clause = f"""
    game_id IN (
      SELECT gf.game_id
      FROM `bgg-data-warehouse.analytics.games_features` gf
      LEFT JOIN `bgg-data-warehouse.staging.game_features_hash` fh
        ON gf.game_id = fh.game_id
      LEFT JOIN (
        SELECT
          game_id,
          score_ts,
          ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY score_ts DESC) as rn
        FROM `bgg-predictive-models.raw.ml_predictions_landing`
      ) lp ON gf.game_id = lp.game_id AND lp.rn = 1
      WHERE
        gf.year_published IS NOT NULL
        AND gf.year_published >= {start_year}
        AND gf.year_published < {end_year}
        AND (
          lp.game_id IS NULL  -- Never scored
          OR fh.last_updated > lp.score_ts  -- Features changed since last score
        )
      LIMIT {max_games}
    )
    """

    df = loader.load_data(where_clause=where_clause, preprocessor=None)
    logger.info(f"Found {len(df)} games needing main predictions")
    return df.to_pandas()
```

Update `predict_games_endpoint` to use this:

```python
@app.post("/predict_games", response_model=PredictGamesResponse)
async def predict_games_endpoint(request: PredictGamesRequest):
    # ... existing model loading code ...

    # Load game data with change detection
    if request.game_ids:
        # Specific games requested - load directly
        df_pandas = load_game_data(game_ids=request.game_ids)
    elif request.use_change_detection:  # NEW PARAMETER
        # Use change detection to find games needing scoring
        df_pandas = load_games_for_main_scoring(
            request.start_year,
            request.end_year,
            max_games=request.max_games or 50000
        )
        if len(df_pandas) == 0:
            logger.info("No games need scoring - all features unchanged")
            return PredictGamesResponse(
                job_id=str(uuid.uuid4()),
                model_details={},
                scoring_parameters={},
                games_scored=0,
                skipped_reason="no_changes"
            )
    else:
        # Original behavior - load by year range
        df_pandas = load_game_data(request.start_year, request.end_year)

    # ... rest of existing code ...
```

Add to `PredictGamesRequest`:

```python
class PredictGamesRequest(BaseModel):
    # ... existing fields ...
    use_change_detection: bool = True  # NEW: Default to using change detection
    max_games: Optional[int] = 50000   # NEW: Limit for change detection mode
```

---

### 4. Change Detection for Embeddings

**embeddings_service/main.py** - Add similar change detection:

```python
def load_games_for_embeddings(max_games: int = 25000) -> pd.DataFrame:
    """
    Load games that need new embeddings.

    Returns games where:
    - No embedding exists, OR
    - game_features_hash.last_updated > game_embeddings.created_ts
    """
    query = f"""
    SELECT gf.*
    FROM `bgg-data-warehouse.analytics.games_features` gf
    LEFT JOIN `bgg-data-warehouse.staging.game_features_hash` fh
      ON gf.game_id = fh.game_id
    LEFT JOIN (
      SELECT game_id, created_ts,
             ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY created_ts DESC) as rn
      FROM `bgg-predictive-models.raw.game_embeddings`
    ) ge ON gf.game_id = ge.game_id AND ge.rn = 1
    WHERE
      gf.year_published IS NOT NULL
      AND (
        ge.game_id IS NULL  -- No embedding exists
        OR fh.last_updated > ge.created_ts  -- Features changed
      )
    ORDER BY gf.users_rated DESC  -- Prioritize popular games
    LIMIT {max_games}
    """
    # ... execute and return ...
```

**text_embeddings_service/main.py** - Similar pattern:

```python
def load_games_for_text_embeddings(max_games: int = 25000) -> pd.DataFrame:
    """
    Load games that need new text embeddings.

    Returns games where description has changed or no embedding exists.
    """
    query = f"""
    SELECT gf.game_id, gf.name, gf.description
    FROM `bgg-data-warehouse.analytics.games_features` gf
    LEFT JOIN `bgg-data-warehouse.staging.game_features_hash` fh
      ON gf.game_id = fh.game_id
    LEFT JOIN (
      SELECT game_id, created_ts,
             ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY created_ts DESC) as rn
      FROM `bgg-predictive-models.raw.description_embeddings`
    ) de ON gf.game_id = de.game_id AND de.rn = 1
    WHERE
      gf.year_published IS NOT NULL
      AND gf.description IS NOT NULL
      AND (
        de.game_id IS NULL  -- No embedding exists
        OR fh.last_updated > de.created_ts  -- Features changed
      )
    ORDER BY gf.users_rated DESC
    LIMIT {max_games}
    """
    # ... execute and return ...
```

---

### 5. Dataform Selective Execution

**bgg-data-warehouse/dataform.yml** - Add conditional logic:

```yaml
- name: Execute Workflow
  run: |
    ACCESS_TOKEN=$(gcloud auth print-access-token)
    API_BASE="https://dataform.googleapis.com/v1beta1/projects/${GCP_PROJECT_ID}/locations/${GCP_REGION}/repositories/${DATAFORM_REPO}"

    # Determine which tables to run based on trigger
    if [[ "${{ github.event_name }}" == "repository_dispatch" && "${{ github.event.action }}" == "predictions_complete" ]]; then
      # Only sync prediction tables
      INCLUDED_TAGS='["predictions"]'
    else
      # Run all transformations
      INCLUDED_TAGS='[]'
    fi

    EXECUTION=$(curl -s -X POST "${API_BASE}/workflowInvocations" \
      -H "Authorization: Bearer ${ACCESS_TOKEN}" \
      -H "Content-Type: application/json" \
      -d '{
        "compilationResult": "${{ steps.compile.outputs.compilation_name }}",
        "invocationConfig": {
          "includedTags": '"$INCLUDED_TAGS"'
        }
      }')
```

Add tags to prediction-related Dataform tables:

```sql
-- definitions/predictions/bgg_predictions.sqlx
config {
  type: "table",
  tags: ["predictions"]
}
```

---

## Migration Plan

### Phase 1: Add Change Detection (Low Risk)

1. Add `load_games_for_main_scoring()` to scoring service
2. Add `use_change_detection` parameter (default: false initially)
3. Test manually with `workflow_dispatch`
4. Monitor games scored vs. total games

### Phase 2: Add Cross-Repo Dispatch (Medium Risk)

1. Create `CROSS_REPO_PAT` secret in both repos
2. Add dispatch step to warehouse Dataform workflow
3. Add `repository_dispatch` trigger to complexity scoring
4. Keep cron schedules as backup

### Phase 3: Chain ML Workflows (Medium Risk)

1. Add `workflow_run` triggers between ML workflows
2. Test full chain with manual dispatch
3. Monitor execution times and failure handling

### Phase 4: Remove Cron Schedules (Higher Risk)

1. Remove cron from ML workflows (rely on events)
2. Remove extra cron runs from Dataform (keep `workflow_run` trigger)
3. Monitor for missed runs
4. Add alerting for pipeline failures

### Rollback

Each phase can be rolled back by:
- Re-adding cron schedules
- Setting `use_change_detection: false`
- Removing `repository_dispatch` triggers

---

## Expected Outcomes

| Metric | Before | After |
|--------|--------|-------|
| Dataform runs/day | 3+ | 2 (collection + sync) |
| Games scored/day | ~150k (all) | ~1-5k (changed only) |
| Embeddings generated/day | ~250k (all) | ~1-5k (changed only) |
| Data freshness guarantee | None | Yes (event-driven) |
| Typical daily compute | ~4 hours | ~30 min |

---

## Open Questions

1. **Failure handling**: What happens if complexity scoring fails? Should main scoring still run?
   - Recommendation: No, fail the chain. Use `if: success()` conditions.

2. **Timeout for dispatch**: How long should warehouse wait for ML pipeline?
   - Recommendation: Don't wait. Fire-and-forget dispatch, ML will dispatch back when done.

3. **Manual reruns**: How to trigger a full rescore of all games?
   - Recommendation: Keep `workflow_dispatch` with `use_change_detection: false` option.

4. **Monitoring**: How to track pipeline health?
   - Recommendation: Add GitHub Action job summaries showing games processed, and alerting on zero-game runs when changes were expected.

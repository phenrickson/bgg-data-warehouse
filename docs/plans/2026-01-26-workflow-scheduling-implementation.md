# Workflow Scheduling Redesign Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Convert cron-based pipelines to event-driven orchestration with change detection for main scoring service.

**Architecture:** Cross-repository coordination via GitHub `repository_dispatch` events; intra-repo chaining via `workflow_run` triggers; change detection queries against `game_features_hash` table to skip unchanged games.

**Tech Stack:** GitHub Actions, BigQuery, Python/FastAPI, Pydantic

---

## Phase 1: Add Change Detection to Main Scoring Service

### Task 1: Add `use_change_detection` field to PredictGamesRequest

**Files:**
- Modify: `/Users/phenrickson/Documents/projects/bgg-predictive-models/scoring_service/main.py:79-94`

**Step 1: Add new fields to PredictGamesRequest**

Open `scoring_service/main.py` and add two new fields to the `PredictGamesRequest` class:

```python
class PredictGamesRequest(BaseModel):
    hurdle_model_name: str
    complexity_model_name: str
    rating_model_name: str
    users_rated_model_name: str
    hurdle_model_version: Optional[int] = None
    complexity_model_version: Optional[int] = None
    rating_model_version: Optional[int] = None
    users_rated_model_version: Optional[int] = None
    start_year: Optional[int] = 2024
    end_year: Optional[int] = 2029
    prior_rating: float = 5.5
    prior_weight: float = 2000
    output_path: Optional[str] = "data/predictions/game_predictions.parquet"
    upload_to_data_warehouse: bool = True
    game_ids: Optional[List[int]] = None
    use_change_detection: bool = False  # NEW: Enable incremental scoring
    max_games: Optional[int] = 50000    # NEW: Limit for change detection mode
```

**Step 2: Verify the change compiles**

Run: `cd /Users/phenrickson/Documents/projects/bgg-predictive-models && uv run python -c "from scoring_service.main import PredictGamesRequest; print(PredictGamesRequest.model_fields.keys())"`

Expected: Output includes `use_change_detection` and `max_games`

**Step 3: Commit**

```bash
cd /Users/phenrickson/Documents/projects/bgg-predictive-models
git add scoring_service/main.py
git commit -m "feat(scoring): add use_change_detection and max_games fields to PredictGamesRequest"
```

---

### Task 2: Add `games_scored` and `skipped_reason` to PredictGamesResponse

**Files:**
- Modify: `/Users/phenrickson/Documents/projects/bgg-predictive-models/scoring_service/main.py:97-104`

**Step 1: Add new fields to PredictGamesResponse**

Update the `PredictGamesResponse` class:

```python
class PredictGamesResponse(BaseModel):
    job_id: str
    model_details: Dict[str, Any]
    scoring_parameters: Dict[str, Any]
    output_location: Optional[str] = None
    data_warehouse_job_id: Optional[str] = None
    data_warehouse_table: Optional[str] = None
    predictions: Optional[List[Dict[str, Any]]] = None
    games_scored: Optional[int] = None      # NEW: Number of games actually scored
    skipped_reason: Optional[str] = None    # NEW: Why scoring was skipped (e.g., "no_changes")
```

**Step 2: Verify the change compiles**

Run: `cd /Users/phenrickson/Documents/projects/bgg-predictive-models && uv run python -c "from scoring_service.main import PredictGamesResponse; print(PredictGamesResponse.model_fields.keys())"`

Expected: Output includes `games_scored` and `skipped_reason`

**Step 3: Commit**

```bash
git add scoring_service/main.py
git commit -m "feat(scoring): add games_scored and skipped_reason to PredictGamesResponse"
```

---

### Task 3: Implement `load_games_for_main_scoring` function

**Files:**
- Modify: `/Users/phenrickson/Documents/projects/bgg-predictive-models/scoring_service/main.py` (add after `load_game_data` function around line 208)

**Step 1: Add the change detection function**

Add this function after `load_game_data`:

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

    Args:
        start_year: Start year for predictions (inclusive)
        end_year: End year for predictions (exclusive)
        max_games: Maximum number of games to load

    Returns:
        DataFrame with game features for scoring
    """
    config = load_config()
    data_warehouse_config = config.get_data_warehouse_config()
    loader = BGGDataLoader(data_warehouse_config)

    ml_project = config.ml_project_id
    dw_project = config.data_warehouse.project_id

    where_clause = f"""
    game_id IN (
      SELECT gf.game_id
      FROM `{dw_project}.analytics.games_features` gf
      LEFT JOIN `{dw_project}.staging.game_features_hash` fh
        ON gf.game_id = fh.game_id
      LEFT JOIN (
        SELECT
          game_id,
          score_ts,
          ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY score_ts DESC) as rn
        FROM `{ml_project}.raw.ml_predictions_landing`
      ) lp ON gf.game_id = lp.game_id AND lp.rn = 1
      WHERE
        gf.year_published IS NOT NULL
        AND gf.year_published >= {start_year}
        AND gf.year_published < {end_year}
        AND (
          lp.game_id IS NULL
          OR fh.last_updated > lp.score_ts
        )
      LIMIT {max_games}
    )
    """

    logger.info(f"Loading games needing main predictions (years {start_year}-{end_year}, max {max_games})...")
    df = loader.load_data(where_clause=where_clause, preprocessor=None)
    logger.info(f"Found {len(df)} games needing main predictions")
    return df.to_pandas()
```

**Step 2: Verify syntax**

Run: `cd /Users/phenrickson/Documents/projects/bgg-predictive-models && uv run python -c "from scoring_service.main import load_games_for_main_scoring; print('OK')"`

Expected: `OK`

**Step 3: Commit**

```bash
git add scoring_service/main.py
git commit -m "feat(scoring): add load_games_for_main_scoring with change detection"
```

---

### Task 4: Update `predict_games_endpoint` to use change detection

**Files:**
- Modify: `/Users/phenrickson/Documents/projects/bgg-predictive-models/scoring_service/main.py:370-421` (the endpoint function)

**Step 1: Update the data loading logic**

Find the section in `predict_games_endpoint` that loads game data (around line 418-421):

```python
        # Load game data
        df_pandas = load_game_data(
            request.start_year, request.end_year, game_ids=request.game_ids
        )
```

Replace it with:

```python
        # Load game data
        if request.game_ids:
            # Specific games requested - load directly
            logger.info(f"Loading {len(request.game_ids)} specific games")
            df_pandas = load_game_data(game_ids=request.game_ids)
        elif request.use_change_detection:
            # Use change detection to find games needing scoring
            logger.info("Using change detection to find games needing scoring")
            df_pandas = load_games_for_main_scoring(
                request.start_year or 2024,
                request.end_year or 2029,
                max_games=request.max_games or 50000
            )
            if len(df_pandas) == 0:
                logger.info("No games need scoring - all features unchanged")
                return PredictGamesResponse(
                    job_id=job_id,
                    model_details={
                        "hurdle": {"name": request.hurdle_model_name},
                        "complexity": {"name": request.complexity_model_name},
                        "rating": {"name": request.rating_model_name},
                        "users_rated": {"name": request.users_rated_model_name},
                    },
                    scoring_parameters={
                        "start_year": request.start_year,
                        "end_year": request.end_year,
                        "prior_rating": request.prior_rating,
                        "prior_weight": request.prior_weight,
                    },
                    games_scored=0,
                    skipped_reason="no_changes"
                )
        else:
            # Original behavior - load by year range
            logger.info(f"Loading all games for years {request.start_year}-{request.end_year}")
            df_pandas = load_game_data(request.start_year, request.end_year)
```

**Step 2: Add games_scored to the successful response**

Find the return statement at the end of the function and add `games_scored=len(results)`. The response construction starts around line 530+. Add the field to the response:

```python
        return PredictGamesResponse(
            job_id=job_id,
            model_details={...},
            scoring_parameters={...},
            output_location=output_path,
            data_warehouse_job_id=data_warehouse_job_id,
            data_warehouse_table=data_warehouse_table,
            predictions=predictions_list,
            games_scored=len(results),  # ADD THIS LINE
        )
```

**Step 3: Verify syntax**

Run: `cd /Users/phenrickson/Documents/projects/bgg-predictive-models && uv run python -c "from scoring_service.main import predict_games_endpoint; print('OK')"`

Expected: `OK`

**Step 4: Commit**

```bash
git add scoring_service/main.py
git commit -m "feat(scoring): update predict_games_endpoint to use change detection"
```

---

### Task 5: Update workflow to enable change detection

**Files:**
- Modify: `/Users/phenrickson/Documents/projects/bgg-predictive-models/.github/workflows/run-scoring-service.yml`

**Step 1: Add use_change_detection input**

Add a new input to the workflow_dispatch section:

```yaml
on:
  workflow_dispatch:
    inputs:
      start_year:
        description: 'Start year for predictions'
        required: false
        default: '2025'
      end_year:
        description: 'End year for predictions'
        required: false
        default: '2030'
      output_path:
        description: 'Cloud storage path for predictions (optional, determined by environment if not provided)'
        required: false
        default: ''
      use_change_detection:
        description: 'Only score games with changed features (recommended for daily runs)'
        required: false
        default: 'true'
        type: choice
        options:
          - 'true'
          - 'false'

  schedule:
    # Run daily at 7 AM UTC
    - cron: '0 7 * * *'
```

**Step 2: Update score.py invocation to pass flag**

The workflow calls `scoring_service.score` module. We need to update the Trigger Predictions step. However, `score.py` doesn't currently support this flag. We have two options:

Option A: Update `score.py` to accept `--use-change-detection` flag
Option B: Call the service directly with curl

For now, let's use Option A. First update the workflow step:

Find the "Trigger Predictions using score.py" step and update it:

```yaml
    - name: Trigger Predictions using score.py
      run: |
        USE_CHANGE_DETECTION="${{ github.event.inputs.use_change_detection || 'true' }}"

        CHANGE_DETECTION_FLAG=""
        if [ "$USE_CHANGE_DETECTION" = "true" ]; then
          CHANGE_DETECTION_FLAG="--use-change-detection"
        fi

        uv run -m scoring_service.score \
          --service-url "${{ steps.get-url.outputs.service_url }}" \
          --start-year ${{ github.event.inputs.start_year || 2024 }} \
          --end-year ${{ github.event.inputs.end_year || 2029 }} \
          --upload-to-bigquery \
          $CHANGE_DETECTION_FLAG \
          ${{ github.event.inputs.output_path != '' && format('--output-path "{0}"', github.event.inputs.output_path) || '' }}
```

**Step 3: Commit (we'll update score.py in the next task)**

```bash
git add .github/workflows/run-scoring-service.yml
git commit -m "feat(workflow): add use_change_detection option to scoring workflow"
```

---

### Task 6: Update score.py to support change detection flag

**Files:**
- Modify: `/Users/phenrickson/Documents/projects/bgg-predictive-models/scoring_service/score.py`

**Step 1: Add argument to parser**

Find the argument parser section and add:

```python
    parser.add_argument(
        "--use-change-detection",
        action="store_true",
        default=False,
        help="Only score games with changed features (recommended for daily runs)",
    )
```

**Step 2: Add to payload construction**

Find the `submit_scoring_request` function and add `use_change_detection` parameter:

```python
def submit_scoring_request(
    service_url: str,
    start_year: int,
    end_year: int,
    hurdle_model: Optional[str] = None,
    complexity_model: Optional[str] = None,
    rating_model: Optional[str] = None,
    users_rated_model: Optional[str] = None,
    output_path: Optional[str] = None,
    prior_rating: Optional[float] = None,
    prior_weight: Optional[float] = None,
    upload_to_data_warehouse: bool = True,
    use_change_detection: bool = False,  # ADD THIS
) -> dict:
```

Then add it to the payload construction (around line 76):

```python
            payload = {
                "hurdle_model_name": hurdle_model or model_config.get("hurdle"),
                "complexity_model_name": complexity_model or model_config.get("complexity"),
                "rating_model_name": rating_model or model_config.get("rating"),
                "users_rated_model_name": users_rated_model
                or model_config.get("users_rated"),
                "start_year": start_year,
                "end_year": end_year,
                "prior_rating": prior_rating or param_config.get("prior_rating", 5.5),
                "prior_weight": prior_weight or param_config.get("prior_weight", 2000),
                "upload_to_data_warehouse": upload_to_data_warehouse,
                "use_change_detection": use_change_detection,  # ADD THIS
            }
```

**Step 3: Pass argument in main()**

Update the `submit_scoring_request` call in `main()`:

```python
        response = submit_scoring_request(
            service_url=args.service_url,
            start_year=args.start_year,
            end_year=args.end_year,
            hurdle_model=args.hurdle_model,
            complexity_model=args.complexity_model,
            rating_model=args.rating_model,
            users_rated_model=args.users_rated_model,
            output_path=args.output_path,
            prior_rating=args.prior_rating,
            prior_weight=args.prior_weight,
            upload_to_data_warehouse=upload_to_data_warehouse,
            use_change_detection=args.use_change_detection,  # ADD THIS
        )
```

**Step 4: Add logging for skipped runs**

After the response, add handling for skipped runs:

```python
        # Check if run was skipped due to no changes
        if response.get("skipped_reason") == "no_changes":
            logger.info("Scoring skipped - no games have changed features")
            logger.info(f"Job ID: {response['job_id']}")
            return

        # Log job details (existing code)
        logger.info(f"Scoring Job ID: {response['job_id']}")
```

**Step 5: Verify syntax**

Run: `cd /Users/phenrickson/Documents/projects/bgg-predictive-models && uv run python -c "from scoring_service.score import submit_scoring_request; print('OK')"`

Expected: `OK`

**Step 6: Commit**

```bash
git add scoring_service/score.py
git commit -m "feat(scoring): add --use-change-detection flag to score.py CLI"
```

---

## Phase 2: Event-Driven Workflow Orchestration

### Task 7: Create GitHub PAT secret (Manual Step)

**This is a manual step - document for user:**

1. Go to GitHub Settings > Developer Settings > Personal Access Tokens > Fine-grained tokens
2. Create token with:
   - Name: `BGG-CROSS-REPO-DISPATCH`
   - Repository access: Select `phenrickson/bgg-data-warehouse` and `phenrickson/bgg-predictive-models`
   - Permissions: Contents (read), Actions (write)
3. Copy the token
4. Add as secret `CROSS_REPO_PAT` to both repositories:
   - `phenrickson/bgg-data-warehouse` > Settings > Secrets > Actions
   - `phenrickson/bgg-predictive-models` > Settings > Secrets > Actions

---

### Task 8: Add repository_dispatch trigger to complexity scoring

**Files:**
- Modify: `/Users/phenrickson/Documents/projects/bgg-predictive-models/.github/workflows/run-complexity-scoring.yml`

**Step 1: Update trigger section**

Replace the `on:` section:

```yaml
name: Score Complexity Predictions

on:
  repository_dispatch:
    types: [dataform_complete]

  workflow_dispatch:
    inputs:
      model_name:
        description: 'Model name to use'
        required: false
        default: 'complexity-v2026'

  # Keep schedule as fallback during transition
  schedule:
    - cron: '0 6 * * *'  # Daily at 6 AM UTC
```

**Step 2: Commit**

```bash
git add .github/workflows/run-complexity-scoring.yml
git commit -m "feat(workflow): add repository_dispatch trigger to complexity scoring"
```

---

### Task 9: Chain main scoring to complexity via workflow_run

**Files:**
- Modify: `/Users/phenrickson/Documents/projects/bgg-predictive-models/.github/workflows/run-scoring-service.yml`

**Step 1: Update trigger section**

Replace the `on:` section:

```yaml
name: Run Scoring Service

on:
  workflow_run:
    workflows: ["Score Complexity Predictions"]
    types: [completed]
    branches: [main]

  workflow_dispatch:
    inputs:
      start_year:
        description: 'Start year for predictions'
        required: false
        default: '2025'
      end_year:
        description: 'End year for predictions'
        required: false
        default: '2030'
      output_path:
        description: 'Cloud storage path for predictions (optional)'
        required: false
        default: ''
      use_change_detection:
        description: 'Only score games with changed features'
        required: false
        default: 'true'
        type: choice
        options:
          - 'true'
          - 'false'

  # Keep schedule as fallback during transition
  schedule:
    - cron: '0 7 * * *'
```

**Step 2: Add condition to skip on upstream failure**

Add this condition to the `trigger-predictions` job:

```yaml
  trigger-predictions:
    needs: [setup]
    runs-on: ubuntu-latest
    environment: ${{ github.ref == 'refs/heads/main' && 'PROD' || 'DEV' }}
    if: ${{ github.event_name != 'workflow_run' || github.event.workflow_run.conclusion == 'success' }}
```

**Step 3: Commit**

```bash
git add .github/workflows/run-scoring-service.yml
git commit -m "feat(workflow): chain main scoring to complexity via workflow_run"
```

---

### Task 10: Chain embeddings to main scoring

**Files:**
- Modify: `/Users/phenrickson/Documents/projects/bgg-predictive-models/.github/workflows/run-generate-embeddings.yml`

**Step 1: Update trigger section**

Replace the `on:` section:

```yaml
name: Run Game Embeddings

on:
  workflow_run:
    workflows: ["Run Scoring Service"]
    types: [completed]
    branches: [main]

  workflow_dispatch:
    inputs:
      model_name:
        description: 'Model name to use'
        required: false
        default: 'embeddings-v2026'
      max_games:
        description: 'Maximum games to process'
        required: false
        default: '50000'

  # Keep schedule as fallback during transition
  schedule:
    - cron: '0 7 * * *'
```

**Step 2: Add skip condition for upstream failure**

Add to the `generate-embeddings` job:

```yaml
  generate-embeddings:
    runs-on: ubuntu-latest
    environment: ${{ github.ref == 'refs/heads/main' && 'PROD' || 'DEV' }}
    if: ${{ github.event_name != 'workflow_run' || github.event.workflow_run.conclusion == 'success' }}
```

**Step 3: Commit**

```bash
git add .github/workflows/run-generate-embeddings.yml
git commit -m "feat(workflow): chain embeddings to main scoring via workflow_run"
```

---

### Task 11: Chain text embeddings to game embeddings

**Files:**
- Modify: `/Users/phenrickson/Documents/projects/bgg-predictive-models/.github/workflows/run-generate-text-embeddings.yml`

**Step 1: Update trigger section**

Replace the `on:` section:

```yaml
name: Run Text Embeddings

on:
  workflow_run:
    workflows: ["Run Game Embeddings"]
    types: [completed]
    branches: [main]

  workflow_dispatch:
    inputs:
      model_name:
        description: 'Model name to use'
        required: false
        default: 'text-embeddings-v2026'
      max_games:
        description: 'Maximum games to process per batch'
        required: false
        default: '25000'

  # Keep schedule as fallback during transition
  schedule:
    - cron: '0 8 * * *'
```

**Step 2: Add skip condition**

Add to the job:

```yaml
  generate-text-embeddings:
    runs-on: ubuntu-latest
    environment: ${{ github.ref == 'refs/heads/main' && 'PROD' || 'DEV' }}
    if: ${{ github.event_name != 'workflow_run' || github.event.workflow_run.conclusion == 'success' }}
```

**Step 3: Add dispatch back to data warehouse at end**

Add a new step at the end of the job (after "Job Summary"):

```yaml
    - name: Notify Data Warehouse
      if: success() && github.ref == 'refs/heads/main'
      env:
        GITHUB_TOKEN: ${{ secrets.CROSS_REPO_PAT }}
      run: |
        curl -X POST \
          -H "Authorization: token $GITHUB_TOKEN" \
          -H "Accept: application/vnd.github.v3+json" \
          https://api.github.com/repos/phenrickson/bgg-data-warehouse/dispatches \
          -d '{"event_type": "predictions_complete", "client_payload": {"run_id": "${{ github.run_id }}", "total_games": "${{ steps.generate.outputs.total_games }}"}}'
```

**Step 4: Commit**

```bash
git add .github/workflows/run-generate-text-embeddings.yml
git commit -m "feat(workflow): chain text embeddings and add dispatch back to warehouse"
```

---

### Task 12: Update warehouse Dataform workflow for bidirectional dispatch

**Files:**
- Modify: `/Users/phenrickson/Documents/projects/bgg-data-warehouse/.github/workflows/dataform.yml`

**Step 1: Add repository_dispatch trigger**

Update the `on:` section:

```yaml
name: Run Dataform

on:
  workflow_run:
    workflows: ["Run Fetch New Games", "Run Refresh Old Games"]
    types: [completed]

  repository_dispatch:
    types: [predictions_complete]

  push:
    branches:
      - main
    paths:
      - 'definitions/**'
      - 'workflow_settings.yaml'
      - '.github/workflows/dataform.yml'

  workflow_dispatch:

  # Remove the triple cron schedules - event-driven now
  # Keep one as safety fallback
  schedule:
    - cron: '30 8 * * *'  # Single daily fallback at 8:30 AM UTC
```

**Step 2: Add dispatch to ML pipeline after Dataform completes**

Add a new step after "Execute Workflow":

```yaml
      - name: Notify ML Pipeline
        if: success() && github.ref == 'refs/heads/main' && github.event_name != 'repository_dispatch'
        env:
          GITHUB_TOKEN: ${{ secrets.CROSS_REPO_PAT }}
        run: |
          curl -X POST \
            -H "Authorization: token $GITHUB_TOKEN" \
            -H "Accept: application/vnd.github.v3+json" \
            https://api.github.com/repos/phenrickson/bgg-predictive-models/dispatches \
            -d '{"event_type": "dataform_complete", "client_payload": {"run_id": "${{ github.run_id }}"}}'
```

Note: The `github.event_name != 'repository_dispatch'` condition prevents infinite loops - we don't dispatch to ML when we're running because ML told us predictions are complete.

**Step 3: Commit**

```bash
cd /Users/phenrickson/Documents/projects/bgg-data-warehouse
git add .github/workflows/dataform.yml
git commit -m "feat(workflow): add bidirectional dispatch for ML pipeline coordination"
```

---

## Phase 3: Testing & Validation

### Task 13: Manual test of change detection

**This is a manual verification step:**

1. Trigger the scoring service manually with change detection:
   ```bash
   cd /Users/phenrickson/Documents/projects/bgg-predictive-models
   uv run -m scoring_service.score \
     --service-url "$(gcloud run services describe bgg-model-scoring --region us-central1 --format 'value(status.url)')" \
     --start-year 2024 \
     --end-year 2029 \
     --use-change-detection \
     --no-upload
   ```

2. Check the output:
   - If games exist: Should see "Found X games needing main predictions"
   - If no games: Should see "Scoring skipped - no games have changed features"

---

### Task 14: Manual test of workflow chain

**This is a manual verification step:**

1. Go to GitHub Actions for `bgg-data-warehouse`
2. Manually trigger "Run Dataform" workflow
3. Watch for:
   - Dataform completes
   - Check `bgg-predictive-models` Actions - "Score Complexity Predictions" should start
   - Then "Run Scoring Service" should start
   - Then "Run Game Embeddings" should start
   - Then "Run Text Embeddings" should start
   - Finally, `bgg-data-warehouse` "Run Dataform" should trigger again (for predictions sync)

---

## Summary

**Phase 1 (Tasks 1-6):** Adds change detection to main scoring service
- Low risk, can be tested independently
- Immediate compute savings

**Phase 2 (Tasks 7-12):** Event-driven orchestration
- Requires PAT setup
- Keeps cron schedules as fallback

**Phase 3 (Tasks 13-14):** Manual validation

**Rollback:** Remove `repository_dispatch` triggers and restore cron schedules

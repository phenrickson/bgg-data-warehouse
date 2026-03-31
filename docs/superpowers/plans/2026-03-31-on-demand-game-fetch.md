# On-Demand Game Fetch Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a GitHub Actions workflow that fetches specific games from BGG on demand, processes them into BigQuery, and triggers the downstream Dataform + ML pipeline.

**Architecture:** A new pipeline script (`fetch_games.py`) reads game IDs from the `GAME_IDS` environment variable, delegates to the existing `ResponseFetcher` (which already supports a `game_ids` parameter) and `ResponseProcessor`. A new Cloud Run job (`bgg-fetch-games`) runs this script. A new GitHub Actions workflow (`fetch_games.yml`) triggers the Cloud Run job with user-provided game IDs, and Dataform is updated to trigger after it completes.

**Tech Stack:** Python 3.12, Google Cloud Run Jobs, GitHub Actions, BigQuery

**Spec:** `docs/superpowers/specs/2026-03-31-on-demand-game-fetch-design.md`

---

## File Structure

| Action | File | Responsibility |
|--------|------|----------------|
| Create | `src/pipeline/fetch_games.py` | Parse GAME_IDS env var, orchestrate fetch + process |
| Create | `tests/test_fetch_games.py` | Unit tests for the pipeline script |
| Modify | `config/cloudbuild.yaml` | Add `bgg-fetch-games` Cloud Run job definition |
| Create | `.github/workflows/fetch_games.yml` | workflow_dispatch workflow for on-demand fetch |
| Modify | `.github/workflows/dataform.yml:5-6` | Add new workflow to `workflow_run` trigger list |

---

### Task 1: Pipeline Script

**Files:**
- Create: `src/pipeline/fetch_games.py`
- Create: `tests/test_fetch_games.py`

- [ ] **Step 1: Write the test for parsing game IDs from environment variable**

Create `tests/test_fetch_games.py`:

```python
"""Tests for fetch_games pipeline script."""

import os
from unittest.mock import patch, MagicMock

import pytest

from src.pipeline.fetch_games import parse_game_ids, main


class TestParseGameIds:
    def test_single_id(self):
        assert parse_game_ids("467694") == [467694]

    def test_multiple_ids(self):
        assert parse_game_ids("467694,12345,99999") == [467694, 12345, 99999]

    def test_whitespace_handling(self):
        assert parse_game_ids(" 467694 , 12345 ") == [467694, 12345]

    def test_deduplication(self):
        assert parse_game_ids("467694,467694,12345") == [467694, 12345]

    def test_empty_string_raises(self):
        with pytest.raises(ValueError, match="No game IDs provided"):
            parse_game_ids("")

    def test_none_raises(self):
        with pytest.raises(ValueError, match="No game IDs provided"):
            parse_game_ids(None)

    def test_invalid_id_raises(self):
        with pytest.raises(ValueError, match="Invalid game ID"):
            parse_game_ids("abc,123")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/phenrickson/Documents/projects/bgg-data-warehouse && uv run pytest tests/test_fetch_games.py -v`
Expected: FAIL with `ModuleNotFoundError` or `ImportError` (module doesn't exist yet)

- [ ] **Step 3: Write the pipeline script**

Create `src/pipeline/fetch_games.py`:

```python
"""Pipeline script for fetching and processing specific games on demand.

Reads game IDs from the GAME_IDS environment variable (comma-separated),
fetches their data from the BGG API, and processes responses into
normalized BigQuery tables.

Usage:
    GAME_IDS=467694,12345 python -m src.pipeline.fetch_games
"""

import logging
import os
from typing import List, Optional

from dotenv import load_dotenv

from ..modules.response_fetcher import ResponseFetcher
from ..modules.response_processor import ResponseProcessor
from ..utils.logging_config import setup_logging

# Load environment variables
load_dotenv()

# Set up logging
logger = logging.getLogger(__name__)
setup_logging()


def parse_game_ids(game_ids_str: Optional[str]) -> List[int]:
    """Parse comma-separated game IDs string into a list of integers.

    Args:
        game_ids_str: Comma-separated string of game IDs (e.g., "467694,12345")

    Returns:
        Deduplicated list of integer game IDs

    Raises:
        ValueError: If input is empty or contains non-integer values
    """
    if not game_ids_str or not game_ids_str.strip():
        raise ValueError("No game IDs provided. Set the GAME_IDS environment variable.")

    ids = []
    for part in game_ids_str.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            ids.append(int(part))
        except ValueError:
            raise ValueError(f"Invalid game ID: '{part}'. Game IDs must be integers.")

    if not ids:
        raise ValueError("No game IDs provided. Set the GAME_IDS environment variable.")

    return list(dict.fromkeys(ids))


def main() -> None:
    """Main entry point for on-demand game fetching."""
    game_ids_str = os.environ.get("GAME_IDS", "")
    game_ids = parse_game_ids(game_ids_str)

    logger.info(f"Starting on-demand fetch for {len(game_ids)} game(s): {game_ids}")

    # Step 1: Fetch responses from BGG API
    logger.info("=" * 80)
    logger.info("Step 1: Fetching responses from BGG API")
    logger.info("=" * 80)
    response_fetcher = ResponseFetcher(
        batch_size=len(game_ids),
        chunk_size=20,
    )
    responses_fetched = response_fetcher.run(game_ids=game_ids)

    if responses_fetched:
        logger.info("Responses fetched - proceeding to process them")
    else:
        logger.info("No responses fetched - checking for unprocessed responses anyway")

    # Step 2: Process responses into normalized tables
    logger.info("=" * 80)
    logger.info("Step 2: Processing responses into normalized tables")
    logger.info("=" * 80)
    response_processor = ResponseProcessor(
        batch_size=100,
    )
    responses_processed = response_processor.run()

    # Summary
    logger.info("=" * 80)
    logger.info("On-demand fetch pipeline completed")
    logger.info("=" * 80)
    logger.info(f"Summary:")
    logger.info(f"  - Game IDs requested: {game_ids}")
    logger.info(f"  - Responses fetched: {'Yes' if responses_fetched else 'No'}")
    logger.info(f"  - Responses processed: {'Yes' if responses_processed else 'No'}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/phenrickson/Documents/projects/bgg-data-warehouse && uv run pytest tests/test_fetch_games.py -v`
Expected: All 7 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/pipeline/fetch_games.py tests/test_fetch_games.py
git commit -m "feat: add on-demand game fetch pipeline script"
```

---

### Task 2: Cloud Run Job Definition

**Files:**
- Modify: `config/cloudbuild.yaml`

- [ ] **Step 1: Add the bgg-fetch-games Cloud Run job to cloudbuild.yaml**

Add the following block after the existing `bgg-refresh-old-games` job definition (after line 109 in `config/cloudbuild.yaml`):

```yaml
  # Deploy Fetch Games (on-demand) Cloud Run Job - Create if not exists, otherwise update
  - name: 'gcr.io/cloud-builders/gcloud'
    entrypoint: 'bash'
    args:
      - '-c'
      - |
        if gcloud run jobs describe bgg-fetch-games --region=us-central1 2>/dev/null; then
          echo "Updating existing job: bgg-fetch-games"
          gcloud run jobs update bgg-fetch-games \
            --image=gcr.io/$PROJECT_ID/bgg-processor:latest \
            --args=src.pipeline.fetch_games \
            --tasks=1 \
            --max-retries=1 \
            --task-timeout=30m \
            --memory=2Gi \
            --cpu=1 \
            --region=us-central1 \
            --service-account=bgg-data-warehouse@$PROJECT_ID.iam.gserviceaccount.com \
            --set-env-vars=BGG_API_TOKEN=$_BGG_API_TOKEN
        else
          echo "Creating new job: bgg-fetch-games"
          gcloud run jobs create bgg-fetch-games \
            --image=gcr.io/$PROJECT_ID/bgg-processor:latest \
            --args=src.pipeline.fetch_games \
            --tasks=1 \
            --max-retries=1 \
            --task-timeout=30m \
            --memory=2Gi \
            --cpu=1 \
            --region=us-central1 \
            --service-account=bgg-data-warehouse@$PROJECT_ID.iam.gserviceaccount.com \
            --set-env-vars=BGG_API_TOKEN=$_BGG_API_TOKEN
        fi
```

- [ ] **Step 2: Verify the YAML is valid**

Run: `cd /Users/phenrickson/Documents/projects/bgg-data-warehouse && python -c "import yaml; yaml.safe_load(open('config/cloudbuild.yaml'))" && echo "YAML valid"`
Expected: `YAML valid`

- [ ] **Step 3: Commit**

```bash
git add config/cloudbuild.yaml
git commit -m "feat: add bgg-fetch-games Cloud Run job to cloudbuild"
```

---

### Task 3: GitHub Actions Workflow

**Files:**
- Create: `.github/workflows/fetch_games.yml`

- [ ] **Step 1: Create the workflow file**

Create `.github/workflows/fetch_games.yml`:

```yaml
name: Run Fetch Games

on:
  workflow_dispatch:
    inputs:
      game_ids:
        description: 'Comma-separated game IDs to fetch (e.g., 467694,12345)'
        required: true
        type: string

env:
  GCP_PROJECT_ID: bgg-data-warehouse
  GCP_REGION: us-central1

jobs:
  fetch-games:
    name: Fetch Specific Games
    runs-on: ubuntu-latest

    steps:
      - name: Validate input
        run: |
          if [ -z "${{ github.event.inputs.game_ids }}" ]; then
            echo "Error: No game IDs provided"
            exit 1
          fi
          echo "Fetching games: ${{ github.event.inputs.game_ids }}"

      - name: Google Cloud Auth
        uses: google-github-actions/auth@v2
        with:
          credentials_json: ${{ secrets.GCP_SA_KEY_BGG_DW }}

      - name: Set up Cloud SDK
        uses: google-github-actions/setup-gcloud@v2

      - name: Execute Fetch Games Job
        run: |
          gcloud run jobs execute bgg-fetch-games \
            --project=${GCP_PROJECT_ID} \
            --region=${GCP_REGION} \
            --update-env-vars=GAME_IDS=${{ github.event.inputs.game_ids }} \
            --wait

      - name: Write job summary
        if: always()
        run: |
          echo "## Fetch Games Summary" >> $GITHUB_STEP_SUMMARY
          echo "" >> $GITHUB_STEP_SUMMARY
          echo "| Detail | Value |" >> $GITHUB_STEP_SUMMARY
          echo "|--------|-------|" >> $GITHUB_STEP_SUMMARY
          echo "| Game IDs requested | ${{ github.event.inputs.game_ids }} |" >> $GITHUB_STEP_SUMMARY
          echo "| Triggered by | @${{ github.actor }} |" >> $GITHUB_STEP_SUMMARY
```

- [ ] **Step 2: Validate workflow YAML syntax**

Run: `cd /Users/phenrickson/Documents/projects/bgg-data-warehouse && python -c "import yaml; yaml.safe_load(open('.github/workflows/fetch_games.yml'))" && echo "YAML valid"`
Expected: `YAML valid`

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/fetch_games.yml
git commit -m "feat: add on-demand fetch games GitHub Actions workflow"
```

---

### Task 4: Dataform Integration

**Files:**
- Modify: `.github/workflows/dataform.yml:5-6`

- [ ] **Step 1: Add the new workflow to the Dataform trigger list**

In `.github/workflows/dataform.yml`, change line 6 from:

```yaml
    workflows: ["Run Fetch New Games", "Run Refresh Old Games"]
```

to:

```yaml
    workflows: ["Run Fetch New Games", "Run Refresh Old Games", "Run Fetch Games"]
```

- [ ] **Step 2: Validate workflow YAML syntax**

Run: `cd /Users/phenrickson/Documents/projects/bgg-data-warehouse && python -c "import yaml; yaml.safe_load(open('.github/workflows/dataform.yml'))" && echo "YAML valid"`
Expected: `YAML valid`

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/dataform.yml
git commit -m "feat: trigger Dataform after on-demand game fetch"
```

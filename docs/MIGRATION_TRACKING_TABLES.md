# Migration: Tracking Tables for Response Processing

## Overview

This migration addresses BigQuery streaming buffer limitations by replacing UPDATE operations with INSERT-only tracking tables. This eliminates the error: "UPDATE or DELETE statement over table would affect rows in the streaming buffer, which is not supported."

**Date:** 2025-12-03
**Status:** ✅ Completed
**Environment:** Test

---

## Problem Statement

### Original Error
```
ERROR:src.pipeline.process_responses:Failed to mark responses as processed using record_id:
400 GET https://bigquery.googleapis.com/bigquery/v2/projects/.../queries/...:
UPDATE or DELETE statement over table gcp-demos-411520.bgg_raw_test.raw_responses
would affect rows in the streaming buffer, which is not supported
```

### Root Cause
- `raw_responses` table received streaming inserts from API fetching
- Processing pipeline attempted to UPDATE `processed = TRUE` on recently streamed data
- BigQuery does not allow UPDATE/DELETE on data in streaming buffer (30-90 minutes after insert)
- This caused processing to fail and records to remain unprocessed indefinitely

---

## Solution Architecture

### New Table Structure

#### 1. `raw_responses` (Modified)
**Purpose:** Store raw API response data (append-only)

| Column | Type | Description |
|--------|------|-------------|
| `record_id` | STRING | UUID identifying this response record |
| `game_id` | INTEGER | BGG game ID |
| `response_data` | STRING | Raw XML/JSON response |
| `fetch_timestamp` | TIMESTAMP | When response was fetched |

**Removed Columns:** `processed`, `process_timestamp`, `process_status`, `process_attempt`

#### 2. `fetched_responses` (New)
**Purpose:** Track which responses have been fetched (append-only)

| Column | Type | Mode | Description |
|--------|------|------|-------------|
| `record_id` | STRING | REQUIRED | FK to raw_responses |
| `game_id` | INTEGER | REQUIRED | BGG game ID |
| `fetch_timestamp` | TIMESTAMP | REQUIRED | When fetched |
| `fetch_status` | STRING | REQUIRED | 'success', 'no_response', 'parse_error' |

**Partitioning:** Daily by `fetch_timestamp`
**Clustering:** `record_id`, `game_id`

#### 3. `processed_responses` (New)
**Purpose:** Track which responses have been processed (append-only)

| Column | Type | Mode | Description |
|--------|------|------|-------------|
| `record_id` | STRING | REQUIRED | FK to raw_responses |
| `process_timestamp` | TIMESTAMP | REQUIRED | When processed |
| `process_status` | STRING | REQUIRED | 'success', 'failed', 'error', 'no_response', 'parse_error' |
| `process_attempt` | INTEGER | REQUIRED | Attempt number |
| `error_message` | STRING | NULLABLE | Error details if failed |

**Partitioning:** Daily by `process_timestamp`
**Clustering:** `record_id`

---

## Code Changes

### 1. BGGResponseFetcher (`src/pipeline/fetch_responses.py`)

#### Changes to `store_response()`
**Before:** Only inserted into `raw_responses`
**After:** Inserts into both `raw_responses` AND `fetched_responses`

```python
# After storing in raw_responses, also track in fetched_responses
fetched_tracking_rows.append({
    "record_id": record_row.record_id,
    "game_id": row["game_id"],
    "fetch_timestamp": row["fetch_timestamp"],
    "fetch_status": "success" | "no_response" | "parse_error"
})
self.bq_client.insert_rows_json(fetched_table_id, fetched_tracking_rows)
```

#### Changes to `get_unfetched_ids()`
**Before:** Checked `raw_responses.process_status` to determine what to fetch
**After:** Checks `fetched_responses` to determine what to fetch

```sql
-- Before
WHERE NOT EXISTS (
    SELECT 1 FROM raw_responses r
    WHERE t.game_id = r.game_id
    AND r.process_status NOT IN ('no_response', 'parse_error')
)

-- After
WHERE NOT EXISTS (
    SELECT 1 FROM fetched_responses f
    WHERE t.game_id = f.game_id
    AND f.fetch_status = 'success'
)
```

### 2. BGGResponseProcessor (`src/pipeline/process_responses.py`)

#### Changes to `get_unprocessed_responses()`
**Before:** `WHERE processed = FALSE`
**After:** Uses JOIN pattern with tracking tables

```sql
-- Before
SELECT * FROM raw_responses
WHERE processed = FALSE

-- After
SELECT r.*
FROM raw_responses r
INNER JOIN fetched_responses f ON r.record_id = f.record_id
LEFT JOIN processed_responses p ON r.record_id = p.record_id
WHERE p.record_id IS NULL  -- Not yet processed
  AND f.fetch_status = 'success'  -- Only process successful fetches
```

#### Changes to `mark_responses_as_processed()`
**Before:** UPDATE statement (failed on streaming buffer)
**After:** INSERT into tracking table

```python
# Before (FAILED)
UPDATE raw_responses
SET processed = TRUE,
    process_status = 'success',
    process_timestamp = CURRENT_TIMESTAMP()
WHERE record_id IN (...)

# After (WORKS)
INSERT INTO processed_responses
VALUES (record_id, CURRENT_TIMESTAMP(), 'success', 1, NULL)
```

#### Changes to error handling
All error cases (`failed`, `error`, `no_response`, `parse_error`) now INSERT into `processed_responses` instead of UPDATE `raw_responses`.

### 3. BGGGameRefresher (`src/pipeline/refresh_games.py`)

#### Removed duplicate code
- Removed `store_response()` method entirely
- Now uses `BGGResponseFetcher.store_response()` instead

#### Changes to `get_games_to_refresh()`
**Before:** Checked `games.load_timestamp` to decide what to refresh
**After:** Checks `fetched_responses.fetch_timestamp` to decide what to refresh

```sql
-- Before
LEFT JOIN (
    SELECT game_id, MAX(load_timestamp) as last_load_timestamp
    FROM games
    GROUP BY game_id
) last_refresh ON game_data.game_id = last_refresh.game_id
WHERE last_refresh.last_load_timestamp < TIMESTAMP_SUB(...)

-- After
LEFT JOIN (
    SELECT game_id, MAX(fetch_timestamp) as last_fetch_timestamp
    FROM fetched_responses
    GROUP BY game_id
) last_fetch ON game_data.game_id = last_fetch.game_id
WHERE last_fetch.last_fetch_timestamp < TIMESTAMP_SUB(...)
```

**Rationale:** Refresh decision should be based on when we last *fetched* data, not when we last *processed* it. This prevents re-fetching data that's been fetched but not yet processed.

---

## Migration Steps

### Step 1: Create Tracking Tables
**Script:** `src/warehouse/migration_scripts/create_tracking_tables.py`

```bash
uv run python -m src.warehouse.migration_scripts.create_tracking_tables
```

**Result:**
- Created `fetched_responses` table with partitioning and clustering
- Created `processed_responses` table with partitioning and clustering

### Step 2: Backfill Historical Data
**Script:** `src/warehouse/migration_scripts/backfill_tracking_tables.py`

```bash
uv run python -m src.warehouse.migration_scripts.backfill_tracking_tables
```

**Result:**
- Backfilled 141,373 records into `fetched_responses`
- Backfilled 140,379 records into `processed_responses`
- Verified all records migrated successfully

### Step 3: Deploy Code Changes
- Updated `fetch_responses.py`, `process_responses.py`, `refresh_games.py`
- No downtime required (old and new code compatible during transition)

### Step 4: Test Migration
**Test:** Run processor with small batch

```bash
uv run python -m src.pipeline.process_responses --batch-size 10
```

**Result:**
```
INFO:__main__:Found 994 unprocessed responses
INFO:__main__:Marking 10 records as processed using processed_responses
INFO:__main__:Successfully marked 10 records as processed
INFO:__main__:Verified 10 records were marked as processed
```

✅ No streaming buffer errors!

### Step 5: Clean Up Old Columns
**Script:** `src/warehouse/migration_scripts/remove_processed_columns.py`

```bash
uv run python -m src.warehouse.migration_scripts.remove_processed_columns
```

**Result:**
- Dropped `processed`, `process_timestamp`, `process_status`, `process_attempt` columns
- `raw_responses` now has only: `record_id`, `game_id`, `response_data`, `fetch_timestamp`

---

## Benefits

### 1. No More Streaming Buffer Errors
- All operations are INSERT-only
- No UPDATE/DELETE on recently streamed data
- Processing continues without interruption

### 2. Better Separation of Concerns
- Fetch tracking separate from process tracking
- Clear audit trail of both operations
- Easier to debug issues

### 3. Improved Refresh Logic
- Refresh based on fetch time, not process time
- Prevents duplicate fetches of unprocessed data
- More accurate refresh intervals

### 4. Code Deduplication
- `refresh_games.py` now uses shared `BGGResponseFetcher`
- Single source of truth for storing responses
- Easier maintenance

### 5. Performance Improvements
- INSERT is faster than UPDATE
- Partitioned tables enable efficient queries
- Clustered on `record_id` for fast lookups

---

## Query Patterns

### Find Unprocessed Responses
```sql
SELECT r.*
FROM raw_responses r
INNER JOIN fetched_responses f ON r.record_id = f.record_id
LEFT JOIN processed_responses p ON r.record_id = p.record_id
WHERE p.record_id IS NULL
  AND f.fetch_status = 'success'
```

### Find Failed Processing Attempts
```sql
SELECT r.game_id, p.error_message, p.process_attempt
FROM raw_responses r
INNER JOIN processed_responses p ON r.record_id = p.record_id
WHERE p.process_status IN ('failed', 'error')
ORDER BY p.process_timestamp DESC
```

### Check Fetch vs Process Gap
```sql
SELECT
    COUNT(DISTINCT f.record_id) as fetched_count,
    COUNT(DISTINCT p.record_id) as processed_count,
    COUNT(DISTINCT f.record_id) - COUNT(DISTINCT p.record_id) as pending_count
FROM fetched_responses f
LEFT JOIN processed_responses p ON f.record_id = p.record_id
WHERE f.fetch_status = 'success'
```

---

## Rollback Plan (if needed)

If issues are discovered, rollback is straightforward:

1. **Keep tracking tables** - They don't interfere with anything
2. **Revert code changes** - Git revert the commits
3. **Re-add processed columns** to `raw_responses`:
```sql
ALTER TABLE raw_responses
ADD COLUMN processed BOOLEAN DEFAULT FALSE,
ADD COLUMN process_timestamp TIMESTAMP,
ADD COLUMN process_status STRING,
ADD COLUMN process_attempt INTEGER DEFAULT 0;
```

However, rollback should not be necessary as the new system is strictly better.

---

## Monitoring

### Key Metrics to Watch

1. **Processing lag:** Time between fetch and process
```sql
SELECT
    AVG(TIMESTAMP_DIFF(p.process_timestamp, f.fetch_timestamp, MINUTE)) as avg_lag_minutes
FROM fetched_responses f
INNER JOIN processed_responses p ON f.record_id = p.record_id
WHERE f.fetch_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
```

2. **Unprocessed backlog:**
```sql
SELECT COUNT(*) as unprocessed_count
FROM fetched_responses f
LEFT JOIN processed_responses p ON f.record_id = p.record_id
WHERE p.record_id IS NULL
  AND f.fetch_status = 'success'
```

3. **Error rate:**
```sql
SELECT
    p.process_status,
    COUNT(*) as count,
    COUNT(*) / SUM(COUNT(*)) OVER() as percentage
FROM processed_responses p
WHERE p.process_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
GROUP BY p.process_status
```

---

## Future Improvements

1. **Add retry logic** for failed processing attempts
2. **Add alerting** when processing lag exceeds threshold
3. **Archive old tracking data** to reduce table size
4. **Add processing duration** metrics to `processed_responses`

---

## Files Modified

### Created
- `src/warehouse/migration_scripts/create_tracking_tables.py`
- `src/warehouse/migration_scripts/backfill_tracking_tables.py`
- `src/warehouse/migration_scripts/remove_processed_columns.py`

### Modified
- `src/pipeline/fetch_responses.py`
  - Updated `store_response()` method
  - Updated `get_unfetched_ids()` method
- `src/pipeline/process_responses.py`
  - Updated `get_unprocessed_responses()` method
  - Updated `get_unprocessed_count()` method
  - Updated `mark_responses_as_processed()` method
  - Updated all error handling to use tracking tables
- `src/pipeline/refresh_games.py`
  - Removed duplicate `store_response()` method
  - Updated `get_games_to_refresh()` method
  - Updated `count_games_needing_refresh()` method
  - Now uses `BGGResponseFetcher` for storing responses

---

## Testing Checklist

- [x] Migration scripts run successfully
- [x] Backfill verified correct record counts
- [x] Processor runs without streaming buffer errors
- [x] Unprocessed responses query returns expected results
- [x] Failed records properly tracked in `processed_responses`
- [x] Refresh logic uses fetch timestamps correctly
- [x] Old columns removed from `raw_responses`

---

## Conclusion

This migration successfully addresses the BigQuery streaming buffer limitation by adopting an append-only architecture with separate tracking tables. The new system is more robust, maintainable, and performant than the previous UPDATE-based approach.

**Status:** ✅ Production Ready (tested in test environment)

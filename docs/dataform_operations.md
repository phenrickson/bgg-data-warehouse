# Dataform Operations

## Incremental Tables and Schema Changes

Dataform's incremental mode only MERGEs into existing tables — it does **not** ALTER the schema. This means:

- Adding a new column to an incremental `.sqlx` definition will not add the column to the existing BigQuery table
- Converting a table from `type: "table"` to `type: "incremental"` will not add columns needed for incremental filtering (e.g., `created_ts`, `load_timestamp`)
- The incremental WHERE clause will silently fail or be skipped if the column it references doesn't exist in the target table

**After any schema change to an incremental table, you must force a full refresh.**

### Prior incidents

- `games_features`: Converted to incremental with a new `load_timestamp` column. The existing BigQuery table never got the column, causing every run to take 4+ hours instead of 8 seconds.
- `game_similarity_search`: Converted to incremental with `created_ts`. Missing column meant new embeddings (e.g., newly added games) were never picked up by the incremental filter.

## Targeted Full Refresh via REST API

You can full-refresh a single Dataform table without affecting other tables. This requires two API calls:

### 1. Create a compilation result

```bash
ACCESS_TOKEN=$(gcloud auth print-access-token)
API_BASE="https://dataform.googleapis.com/v1beta1/projects/bgg-data-warehouse/locations/us-central1/repositories/bgg-data-warehouse"

COMPILATION=$(curl -s -X POST "${API_BASE}/compilationResults" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"gitCommitish": "main"}')

COMPILATION_NAME=$(echo "${COMPILATION}" | uv run python -c "import sys,json; print(json.load(sys.stdin)['name'])")
```

### 2. Run with full refresh on a specific table

```bash
curl -s -X POST "${API_BASE}/workflowInvocations" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  -H "Content-Type: application/json" \
  -d "{
    \"compilationResult\": \"${COMPILATION_NAME}\",
    \"invocationConfig\": {
      \"includedTargets\": [{
        \"database\": \"bgg-data-warehouse\",
        \"schema\": \"analytics\",
        \"name\": \"game_similarity_search\"
      }],
      \"fullyRefreshIncrementalTablesEnabled\": true
    }
  }"
```

**Important:** The `database` field is required in `includedTargets`. Without it, the API returns `FAILED_PRECONDITION: Requested target does not exist`.

### Checking invocation status

```bash
curl -s "https://dataform.googleapis.com/v1beta1/${INVOCATION_NAME}" \
  -H "Authorization: Bearer ${ACCESS_TOKEN}"
```

The response includes `state` (`RUNNING`, `SUCCEEDED`, `FAILED`) and `invocationTiming` with start/end timestamps.

# Dataform Incremental Table Schema Drift

## Problem

When a Dataform table is converted from `type: "table"` to `type: "incremental"`, or when columns are added to an existing incremental table's SQL, Dataform does not alter the target BigQuery table schema. It continues to MERGE against the old schema.

This caused `games_features` to run for 4+ hours on every Dataform invocation instead of seconds, because the incremental filter referenced a column (`load_timestamp`) that didn't exist in the target table.

## What happened

1. `games_features` was originally created as `type: "table"` — Dataform did a full `CREATE OR REPLACE` each run
2. Commit `b807678` converted it to `type: "incremental"` and added `g.load_timestamp` to the SELECT
3. Dataform switched to MERGE mode against the existing table, which didn't have the `load_timestamp` column
4. The incremental filter `WHERE load_timestamp > (SELECT MAX(load_timestamp) FROM ${self()})` silently broke
5. Every run took 4+ hours instead of seconds

## How it was fixed

Ran two BigQuery commands directly:
```sql
ALTER TABLE `bgg-data-warehouse.analytics.games_features`
ADD COLUMN IF NOT EXISTS load_timestamp TIMESTAMP;

UPDATE `bgg-data-warehouse.analytics.games_features` gf
SET load_timestamp = ga.load_timestamp
FROM `bgg-data-warehouse.analytics.games_active` ga
WHERE gf.game_id = ga.game_id;
```

## Rule going forward

**When changing the schema of a Dataform incremental table (adding/removing columns, or converting from `table` to `incremental`), you must force a full refresh on the next Dataform run.**

Options:
- Run Dataform with `--full-refresh` flag for that specific table
- Temporarily set the table to `type: "table"` for one run, then switch back to `incremental`
- Manually drop the target table before the next run (Dataform will recreate it)

Dataform will not alter existing table schemas on its own during incremental mode.

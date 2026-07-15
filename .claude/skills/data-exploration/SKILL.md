---
name: data-exploration
description: Answer questions about the warehouse data with safe, cost-aware BigQuery. Use when the user wants to inspect table contents, check row counts/freshness/nulls, profile a column, validate a pipeline's output, or write an ad-hoc analytical query. Emphasizes dry-run-first and cost control.
---

# Data exploration

Query the BigQuery warehouse to answer data questions — cost-consciously. The project
is `bgg-data-warehouse` (location `US`). Datasets: `raw` (landed data, e.g.
`raw.thing_ids`), `core`, `predictions`, `analytics` (Dataform outputs).

## Rules of engagement

1. **Dry-run first on anything non-trivial.** Know what it scans before you run it:
   ```
   bq query --dry-run --nouse_legacy_sql '<SQL>'
   ```
   It reports bytes scanned without spending. For a big table, that number is the cost.

2. **Never `SELECT *` on a large table.** Select only the columns you need; the fewer
   columns, the fewer bytes scanned (BigQuery is columnar).

3. **Prune with the partition/cluster keys**, and `LIMIT` while exploring — note that
   `LIMIT` does **not** reduce bytes scanned (only column and partition pruning does).

4. **Start small, then widen.** Peek at schema and a few rows before aggregating:
   ```
   bq show --schema --format=prettyjson bgg-data-warehouse:raw.thing_ids
   bq query --nouse_legacy_sql 'SELECT * FROM `bgg-data-warehouse.raw.thing_ids` LIMIT 20'
   ```

## Common questions → queries

- **Row counts by type:**
  `SELECT type, COUNT(*) c FROM \`bgg-data-warehouse.raw.thing_ids\` GROUP BY type ORDER BY type`
- **Freshness / did the last run land data:** `SELECT MAX(load_timestamp) FROM ...`
  (the ID pipeline stamps `load_timestamp`; predictions models stamp `score_ts`).
- **Unprocessed backlog:** `... WHERE processed = false`.
- **Null/quality check:** `SELECT COUNTIF(col IS NULL) nulls, COUNT(*) total FROM ...`.
- **Dedup sanity (grain):** `SELECT key, COUNT(*) c FROM ... GROUP BY key HAVING c > 1`.

## Guardrails

- Read-only by default. **Do not** run `CREATE`/`DELETE`/`UPDATE`/`MERGE`/`TRUNCATE`
  against warehouse tables from an exploration — data changes go through pipelines and
  Dataform, not ad-hoc SQL. Confirm explicitly before any mutating statement.
- Prefer backtick-quoted `\`project.dataset.table\`` and `--nouse_legacy_sql`.
- If a query would scan many GB, say so and confirm before running it.

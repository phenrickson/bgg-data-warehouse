# Processor narrow scan — implementation plan

**Goal:** stop the response processor re-reading all of `raw.raw_responses` (including the
`response_data` blob) on every batch, and cut the number of batches. No change to *which*
rows are processed or in what order.

**Why:** a 2026-10 BigQuery cost review (`region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`)
found `get_unprocessed_responses` was the single largest cost in the project: 937 GiB over
35 days, 382 runs at ~2.5 GiB each, and growing month over month with the table. The query
selects `response_data` before filtering, never filters on the `fetch_timestamp` partition
column, and runs once per 100-game batch (~10×/day).

**Success criteria**
- Per-batch selection reads only narrow columns (no `response_data`).
- `response_data` is fetched only for the selected batch, partition- and cluster-pruned.
- Same rows, same order, same downstream behaviour; `core.*` output unchanged.
- Daily processor bytes drop from ~25 GiB/day to well under 1 GiB/day (checked post-deploy).

## Affected files

- `src/modules/response_processor.py` — `get_unprocessed_responses()`
- `src/pipeline/fetch_new_games.py`, `src/pipeline/refresh_old_games.py`,
  `src/pipeline/fetch_games.py` — `batch_size`
- `tests/test_response_processor.py` (new)

## Steps

1. **Branch** `perf/processor-narrow-scan` from `origin/main`.
2. **Split the selection query** in `get_unprocessed_responses()`:
   - *Select:* the existing CTE (joins to `fetched_responses` / anti-join to
     `processed_responses`, latest-per-game `ROW_NUMBER`, ordering, `LIMIT`) minus
     `response_data`.
   - *Fetch:* `SELECT record_id, response_data FROM raw_responses WHERE record_id IN
     UNNEST(@record_ids) AND game_id IN UNNEST(@game_ids) AND fetch_timestamp BETWEEN
     @min_ts AND @max_ts` — parameterised; the timestamp range prunes partitions, the
     `game_id` filter uses clustering.
   - Merge in Python preserving the selection order; everything downstream is unchanged.
   - *Verify:* unit tests (mocked `bq_client`) — selection SQL has no `response_data`;
     fetch passes the parameters; rows and order match. `bq query --dry_run` of both
     queries against the real tables to confirm bytes.
3. **Raise `batch_size` 100 → 1000** in the three pipeline callers: 10× fewer batches, so
   10× fewer count queries, verify queries and loader DML statements (each billed at the
   10 MiB DML minimum). Bounded rather than one-pass so memory stays capped on a large
   backlog (~5 KB per response → ~5 MB per batch).
   - *Verify:* `uv run --extra test python -m pytest -m "not integration"`.
4. **PR** `perf(processor): …`; deploy via the existing Cloud Build workflow on merge.
5. **Post-deploy:** one `JOBS_BY_PROJECT` query comparing a day of processor bytes against
   the baseline.

## Risks / rollback

- Partition pruning with query parameters — confirmed by the step-2 dry run.
- `IN UNNEST` of 1000 ids, and the loader's `DELETE … IN (…)` of 1000 ids — within limits.
- Rollback: revert the PR. No schema change, no backfill.

## Out of scope

- Correctness issues found in review (an older unprocessed response can be processed after
  a newer one; an all-bad batch halts `run()`; errors in the count/select are swallowed as
  "nothing to do").
- The loader's per-table DELETE/MERGE pattern itself.
- Moving response parsing into BigQuery/Dataform.

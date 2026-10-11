# Refresh policy: fair share across buckets

**Goal:** refresh 5,000 games/day across non-overlapping age buckets, most-overdue first,
in a job that runs ~20 min.

**Branch / delivery:** `feat/refresh-policy-fair-share` off `main`, one PR, Phil merges.
Cloud Build redeploys the job images on merge.

## Why

Staleness on 2026-10-11 (days since last fetch, median):

| bucket | games | median | schedule |
|---|---|---|---|
| recent (2024+) | 17,991 | 9 | 7d |
| established (2021–23) | 16,150 | 307 | 30d |
| classic (2016–20) | 24,305 | 305 | 90d |
| vintage (≤2015) | 70,504 | 482 | 180d |
| unpublished (NULL) | 12,064 | 481 | 14d |
| future (2027+) | 702 | 86 | never |

- The schedule needs ~4,700 games/day against a 1,000 batch. Selection is ordered
  `year_published DESC`, so recent eats every slot and older buckets never refresh
  (the 2026-10-11 run: 1,000/1,000 recent). NULL years sort last.
- `BETWEEN` ranges overlap at boundary years (games selected twice per run);
  `year_published > current_year` is in no bucket.
- Fetching takes ~15s per 20-game chunk because `store_response` runs one BigQuery
  query per game to read back the server-generated `record_id`.

## Steps

0. **Client-side `record_id`.** `store_response` sets `record_id = str(uuid4())` on each
   `raw_responses` row (overrides the `GENERATE_UUID()` default) and reuses it for
   `fetched_responses`; the per-game lookup goes. Rows the raw insert rejects get no
   tracking row. Shared with Fetch New Games.
   *Verify:* unit test — same `record_id` in both inserts, no lookup query.
1. **Config.** Ages are half-open `[min_age_years, max_age_years)`, age =
   `current_year - year_published`, a missing bound is unbounded, first match wins.
   future <0 / 7d · recent 0–2 / 7d · established 3–5 / 60d · classic 6–10 / 180d ·
   vintage 11+ / 365d · unpublished NULL / 60d. `batch_size: 5000`.
2. **Refresher.** One `CASE` (`_bucket_case_sql`) tags each game with a bucket; one
   scan replaces the per-interval `UNION ALL`. Order by
   `days_since_fetch / refresh_days DESC`, never-fetched first. The count query reuses
   the `CASE` with `GROUP BY`.
   *Verify:* unit tests — each boundary year lands in exactly one bucket, 2027 → future,
   NULL → unpublished, query orders by the overdue ratio.
3. **Pre-PR checks (with approval).** Dry-run the selection query for bytes; run it
   read-only `LIMIT 5000` and check the bucket mix.

## Expected

- New and upcoming games are the priority: they get the shortest intervals, so their
  overdue ratio grows fastest and they win whenever the budget is short.
- Steady-state demand ≈ 3,500 games/day; the ~1,500/day headroom clears the ~125k
  backlog in ~7–8 weeks.
- The first batches go to ~8k recent games ~290 days stale (≈40× overdue); older
  buckets follow in order unpublished → established → classic → vintage.
- Job ≈ 12 min fetch + ~7 min processing at 5,000 (estimate until the first run).

## Risks / rollback

Step 0 is shared with Fetch New Games. No schema or data changes; rollback is a revert.

## Out of scope

The 30-min `fetch_in_progress` lock; batching the per-chunk in-progress cleanup;
popularity-weighted refresh; the integration tests that hard-code 4 intervals.

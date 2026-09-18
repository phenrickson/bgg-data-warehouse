# ID Probe — Look Below the Frontier

> Follow-up to #117 (API-probe discovery). The daily and weekly probes described
> here share one code path; what differs is the window and the schedule.

## Problem

`fetch_thing_ids --source bgg_api_probe` walks the ID space upward from
`MAX(game_id)+1` and stops after 500 consecutive misses. That finds games whose
IDs sit above the known frontier — and nothing else.

BGG allocates an ID when an item is submitted but publishes it only after
approval, often weeks later. By then the probe has walked past it. Measured on
the 3,911 IDs the sitemap scrape found Jun–Sep 2026, relative to the frontier at
the time each was found:

| Position | Share |
|---|---|
| above the frontier | 7% |
| 0–500 below | 70% |
| 500–2,000 below | 14% |
| 2,000–5,000 below | 5% |
| >5,000 below | 4% |

The probe covers the 7% slice. Daily finds dropped from ~40 (sitemap era) to
1–16 (probe era). The sitemap is no longer an option: BGG has blocked the home
box's IP, and datacenter egress was already blocked.

## Two probes, two jobs

**Daily — new and upcoming games.** IDs within a few hundred of the frontier were
reserved in the last few weeks; those are the upcoming releases the predictions
pipeline exists for. Daily = frontier walk + a short trailing window.

**Weekly — catch-up.** Late approvals and older-year additions land further back.
They matter less and don't need to be found the day they appear. Weekly = a deep
trailing window, run on Sundays, not gating anything.

## Solution

One probe, parameterised by a `lookback`:

```
range = [max_id − lookback, …)          # walk upward
skip  = { game_id ∈ raw.thing_ids : game_id ≥ max_id − lookback }
```

- Below `max_id` every ID not in `skip` is probed; consecutive misses are
  expected there and do **not** stop the walk.
- At and above `max_id` the existing rule applies: stop after 500 consecutive
  misses.
- Batches stay 20 IDs per request at the existing 2 s throttle. A failed request
  still raises rather than counting as a miss (a 429 must never read as "nothing
  new").

| Run | `--lookback` | Requests | Wall time | Covers |
|---|---|---|---|---|
| daily 06:00 UTC | 500 | ≤ 25 + frontier | ~1–2 min | ~77% of new IDs the day they're approved |
| weekly Sun 06:00 UTC | 5000 | ≤ 250 + frontier | ~8 min | ~96% within a week |

Known IDs are skipped, so the request counts above are worst-case (a sparse
window costs fewer). Upserts are the existing `MERGE`, so overlap between the two
runs is harmless.

Why 5,000 and not deeper: going to 20,000 costs ~750 more requests and ~27 more
minutes of GitHub-runner time a week for ~3% more coverage, and without
known-absent tracking it re-asks the same empty IDs every Sunday. A deeper
one-off sweep is a manual dispatch with `lookback: 20000`.

## Components

### 1. `src/modules/id_probe_fetcher.py`

`ProbeIDFetcher.probe(start_id, *, frontier_id, skip=frozenset(), max_ids=None)`

- `start_id` — first ID to consider (`max_id − lookback`).
- `frontier_id` — `max_id`; the miss counter is only active for IDs ≥ this.
- `skip` — IDs already in `raw.thing_ids`; never requested.
- Batches are built by taking the next 20 IDs from the range that are not in
  `skip`, so a batch may span a gap.
- Returns the same `[{game_id, type}]` shape as today.

### 2. `src/modules/id_fetcher.py`

`_fetch_via_probe(lookback: int)`:

1. `max_id = get_max_game_id()`
2. `skip = SELECT game_id FROM raw.thing_ids WHERE game_id >= max_id − lookback`
   (≤ ~5k rows; trivial scan).
3. `ProbeIDFetcher().probe(max_id − lookback, frontier_id=max_id, skip=skip)`

`run(source, lookback=0)` threads it through. `lookback=0` is exactly today's
behaviour.

### 3. `src/pipeline/fetch_thing_ids.py`

`--lookback N` (int, default `0`). The workflow, not the code, owns the daily and
weekly values so they're tunable without a release.

### 4. `.github/workflows/fetch_thing_ids.yml`

```yaml
schedule:
  - cron: '0 6 * * 1-6'   # daily: frontier + 500 back
  - cron: '0 6 * * 0'     # Sunday: frontier + 5,000 back
workflow_dispatch:
  inputs:
    source: …             # unchanged
    lookback: { type: number, default: 500 }
```

The run step picks the lookback: `inputs.lookback` if dispatched, else `5000`
on Sundays (`date +%u == 7`), else `500`. Both crons fire the same
`workflow_run` chain into Fetch New Games → Refresh Old Games → Dataform.

The job summary gains three rows: `Lookback`, `IDs examined`, `IDs found` — so
"is the probe finding things?" is answerable from the run page.

### 5. Tests

- `probe()` never requests an ID in `skip`; batches are still ≤ 20.
- Misses below `frontier_id` don't stop the walk; 500 misses above it do.
- `_fetch_via_probe(500)` asks the client for exactly `[max−500, …] − known`.
- `lookback=0` reproduces the current requested-ID sequence (regression guard).

## Validation

- Local: `uv run python -m src.pipeline.fetch_thing_ids --source bgg_api_probe --lookback 200`
  against prod — ~10 requests, idempotent MERGE, safe.
- First daily run: step summary shows `lookback=500`, examined ≈ 500 + frontier,
  found > 0 on a typical day.
- First Sunday run: completes without 429 failures; summary shows the deep
  sweep's found count.
- Over the following week, `raw.thing_ids` daily new-ID counts should return to
  the sitemap-era range (~20–70/day).

## Risks

- **Rate limiting on the weekly sweep.** Calibration point: `refresh_old_games`
  already fetches ~1,000 games/day (~50 requests at 20/batch) at the same 2 s
  cadence, daily, without 429 failures. The sweep is the same rate for ~5× the
  duration (~250 requests, ~8 min), and on Sundays it runs immediately ahead
  of Fetch New Games and the refresh in the same chain, so the morning's request
  stream is ~30 min continuous at 0.5 req/s. The rate never rises; only the
  length does. #117 saw 429s only at 2 req/s. The client retries 429 three times
  with backoff, then raises — the run fails visibly rather than recording false
  misses. If the first Sunday fails, drop to `2000` or add a pause every N
  batches; both are one-line tunables.
- **Cost.** One small BigQuery lookup per run. No new tables, no backfill, no
  Dataform changes.
- **Rollback.** Revert the workflow to `--lookback 0`; the code path is
  identical to today's.

## What this doesn't change

- `raw.thing_ids` schema, the `MERGE` upsert, Dataform, or the downstream chain.
- The sitemap/browser code path (kept, unused).
- The heartbeat — separate follow-up once the summary exposes found-count.

## Deferred

- **Known-absent tracking with decay.** Record probed-and-absent IDs with a
  probe count; the weekly sweep skips IDs already checked N times. Cuts the
  weekly request count toward the true gap population, which is what would make
  a 20,000 window affordable. Needs a small `raw.probe_misses` table. Do it
  after a few Sundays show the real miss volume.
- **Reserved vs nonexistent.** If the API distinguishes them, decay can be
  smarter. Unknown; not needed for this slice.

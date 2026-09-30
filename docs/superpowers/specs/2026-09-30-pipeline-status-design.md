# Pipeline Status — Replace the Scrape Heartbeat

> Replaces `.github/workflows/scrape_heartbeat.yml`. Follow-up to #117 (API-probe
> discovery), #118 and #128 (heartbeat retargeting).

## Problem

The heartbeat asks one question: *did "Run Fetch New Games" finish green on
GitHub in the last 26h?* That is a proxy, and it is the wrong one now.

- **It false-alarms.** On 2026-09-18, 09-22 and 09-30 the runs listing
  (`/actions/workflows/fetch_new_games.yml/runs?status=success`) returned a page
  whose newest entry was `2026-09-08T06:01:30Z`, although the chain had
  succeeded that morning. The same request minutes later was correct. #128 took
  the max over 20 runs, which can't help when the whole page is stale.
- **It misses what matters.** A green run says nothing about whether discovery
  searched, whether new IDs landed, whether they were fetched, or whether old
  games were refreshed. A run that processed nothing still passes, and a
  *skipped* run (09-16: Fetch Thing IDs failed, so Fetch New Games was skipped)
  looks the same as one that never fired.
- **Its premise is gone.** It checks Fetch New Games because the home box used
  to trigger it by `repository_dispatch` without a Fetch Thing IDs run. Since
  #117 discovery runs on Actions (`fetch_thing_ids.yml`, 06:00 UTC), so a Fetch
  Thing IDs run *is* the record that we searched.

### What September looked like

From `raw.thing_ids`, `raw.fetched_responses` and the Actions run history:

| Date | What happened | New IDs | New games fetched | Refreshed |
|---|---|---|---|---|
| 09-07, 09-12, 09-13 | home box never dispatched; no Fetch New Games run | 0 | 0 | 1,000 |
| 09-14 | #117 API probe; manual catch-up | 125 | 81 | 1,000 |
| 09-15, 09-16 | probe walks the frontier only | 1 | 0–1 | ~1,000 |
| 09-16 | Fetch Thing IDs failed (token newline, #121); Fetch New Games **skipped**; manual rerun | — | — | — |
| 09-18 → | lookback probe (#127) | 6–101/day | 3–72/day | 1,000/day |

Every zero day for new games this month was an outage, not a quiet day.
Refresh is exactly the batch size (1,000) every day. Only `boardgame` IDs are
fetched; expansions and accessories are recorded but not fetched. The
`raw.thing_ids.processed` column is 0 on every row, so it can't serve as a
fetched signal.

## Goal

One daily status that answers:

1. Did the jobs run?
2. Did we search for games?
3. Did we add new game IDs, and were they fetched?
4. Did we refresh old games?

It is a **status update, not a failure**. Every day's answers go to the job
summary. When something doesn't pass, the answers reach Phil as a GitHub issue
comment (emailed). The workflow itself goes red only if the check can't run.

## Checks

Window: the last `WINDOW_HOURS` (default 26) before the run. The run is at 12:00
UTC, about 6h after the chain, so the window holds exactly one daily chain.

| # | Question | Source | Flag when |
|---|---|---|---|
| 1 | Jobs ran | Actions: newest run of `fetch_thing_ids.yml`, `fetch_new_games.yml`, `refresh.yml` created in the window | any of the three has no run with `conclusion == success` (skipped/failed/missing all flag) |
| 2 | Searched | Fetch Thing IDs from check 1 | covered by check 1; reported on its own line |
| 3a | New IDs found | `raw.thing_ids` with `load_timestamp` in window, by `type` | total is 0 ("worth a look": it has only happened in outages, but can happen legitimately) |
| 3b | New games fetched | window boardgame IDs joined to `raw.fetched_responses` | any boardgame ID has **no fetch row at all** (never attempted). Attempted but not yet successful is reported, not flagged, because the fetcher retries those |
| 4 | Old games refreshed | `raw.fetched_responses` rows in window whose `game_id` has an earlier fetch | 0 |
| — | Failed fetches | `fetch_status != 'success'` in window | never, count only (3–19/day is normal) |

Check 1 queries `.../actions/workflows/<file>/runs?created=>=<window start>`.
The date in the URL changes every day, so the cached listing behind the
false alarms can't be reused. A stale response would come back empty and read
as "missing", which still flags; see Risks.

## Components

### 1. `.github/workflows/pipeline_status.yml` (new)

Follows the repo's existing style: bash plus `bq` plus `gh`, as in
`fetch_new_games.yml`.

- **Triggers:** `schedule: '0 12 * * *'`, plus `workflow_dispatch` with a
  `window_hours` input (default 26).
- **Permissions:** `actions: read`, `issues: write`.
- **Auth:** `google-github-actions/auth@v2` with `GCP_SA_KEY_BGG_DW`, then
  `setup-gcloud`, as the other workflows do.
- **Steps:**
  1. *Job runs:* for each of the three workflow files, `gh api` the runs
     listing with the `created` filter, then record the newest run's
     `created_at`, `event` and `conclusion` and whether any run in the window
     succeeded.
  2. *Warehouse counts:* one `bq query` returning a single row. The counts are
     IDs found by type, boardgames found, boardgames fetched successfully,
     boardgames attempted but not successful, boardgames never attempted,
     refreshed, and failed fetches.
  3. *Evaluate and render:* build a markdown table (question · answer · ✅/⚠️)
     and write it to `$GITHUB_STEP_SUMMARY` and to a body file. Set
     `flagged=true|false`.
  4. *Notify:* look up the open issue labelled `pipeline-status`
     (`gh label create pipeline-status --force` first so the label exists).
     - flagged, no open issue → `gh issue create` with the body.
     - flagged, issue open → `gh issue comment` with the body.
     - not flagged, issue open → comment "All clear" with the body, then
       `gh issue close`.
     - not flagged, no issue → nothing.
- A step exits non-zero only when a query or API call itself errors.

### 2. `.github/workflows/scrape_heartbeat.yml` (deleted)

### 3. README

Replace the heartbeat paragraph with a short description of Pipeline Status and
the `pipeline-status` issue.

## Validation

- **SQL:** `bq query --dry_run` on the counts query, then one real run with the
  window set to 2026-09-12 → 09-13 (flagged), 2026-09-16 (partial) and a
  recent day (clean). The counts should match the September table above.
- **Workflow:** `workflow_dispatch` only works once the file is on `main`. After
  merge, run it twice:
  - `window_hours: 26` → clean status, no issue opened.
  - `window_hours: 1` → no job runs in the window, so it flags and opens the
    issue. Re-running with 26 comments "All clear" and closes the issue. This
    exercises the whole notify path without faking data.

## Risks

- **The Actions listing stays stale even with the date filter.** It would show
  as "no run" and open an issue while the warehouse counts look healthy, which
  is visible in the same comment. If it recurs, look each run up by its ID
  instead of the listing, but only then.
- **Issue noise during a multi-day outage:** one comment a day on a single
  open issue, which is the intended behaviour.
- **The service account can't read a table:** the step errors and the workflow
  goes red. That is correct, because the check couldn't run.

## What this doesn't change

The pipelines, their workflows and their job summaries. No new tables, and no
changes to `raw.*` writes.

## Deferred

- A per-run log table written by each pipeline (so "searched and found 0" is
  recorded in the warehouse rather than inferred from Actions). Not needed while
  discovery runs on Actions.
- Dataform run status and freshness of downstream tables.

## Delivery

Branch `feat/pipeline-status` off `main`, one PR (workflow + heartbeat
removal + README), merged by Phil. This spec lands on `docs/pipeline-status-design`.

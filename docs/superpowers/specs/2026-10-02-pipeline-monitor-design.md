# Pipeline Monitor — Whole-Chain Status for bgg-viewer

> Follow-up to [Pipeline Status](2026-09-30-pipeline-status-design.md) (#133), which
> covers discovery → fetch → refresh and defers "Dataform run status and freshness of
> downstream tables". This spec covers the rest of the chain and serves it to an
> admin page in bgg-viewer. Mockup:
> `bgg-viewer/docs/design/pipeline-monitor-mockup.html`.

## Problem

The daily chain crosses three repos and twelve workflow runs:

| # | Stage | Repo | Workflow file | Run title (`display_title`) | Lane |
|---|---|---|---|---|---|
| 1 | Fetch Thing IDs | warehouse | `fetch_thing_ids.yml` | Fetch Thing IDs | warehouse |
| 2 | Fetch New Games | warehouse | `fetch_new_games.yml` | Run Fetch New Games | warehouse |
| 3 | Refresh Old Games | warehouse | `refresh.yml` | Run Refresh Old Games | warehouse |
| 4 | Dataform · pass 1 | warehouse | `dataform.yml` | Run Dataform (event `workflow_run`) | warehouse |
| 5 | Text Embeddings | models | `run-generate-text-embeddings.yml` | `dataform_complete` | models |
| 6 | Dataform · pass 2 | warehouse | `dataform.yml` | `text_embeddings_complete` | warehouse |
| 7 | Score Complexity | models | `run-complexity-scoring.yml` | `dataform_text_embeddings_ready` | models |
| 8 | Dataform · pass 3 | warehouse | `dataform.yml` | `complexity_complete` | warehouse |
| 9 | Score Games | models | `run-scoring-service.yml` | `dataform_complexity_ready` | models |
| 10 | Game Embeddings + Coords | models | `run-generate-embeddings.yml` | Run Game Embeddings | models |
| 11 | Dataform · pass 4 | warehouse | `dataform.yml` | `embeddings_complete` | warehouse |
| 12 | Viewer Artifacts | viewer | `viewer-artifacts.yml` | `catalog_refresh` | viewer |

Off-chain (cron): Collection Scoring (`run-collection-scoring.yml`, 08:00 UTC) and
Collection Reports (`build-collection-reports.yml`, 09:00 UTC) in bgg-predictive-models,
and Pipeline Status (`pipeline_status.yml`, 12:00 UTC) here.

Repos: `phenrickson/bgg-data-warehouse`, `phenrickson/bgg-predictive-models`,
`phenrickson/bgg-viewer`. Verified on 2026-10-02 that a `repository_dispatch` run's
`display_title` is its event type, so the four Dataform passes and every dispatched ML
stage can be told apart from run history alone. On a normal day the chain runs
~06:26 → ~07:30 UTC.

What nobody can see today:

- **A stall with every run green.** `dataform.yml` only warns when an invocation isn't
  `SUCCEEDED` after polling, and then skips its dispatch. Nothing downstream starts and
  nothing is red. Pipeline Status watches stages 1–3 only.
- **Collection scoring racing the chain.** It runs on a fixed clock. On a slow day it
  scores against yesterday's predictions without error.
- **Downstream freshness and coverage.** Nothing compares the prediction, embedding and
  coordinate tables against the games they should cover, or says when each last moved.
- **Which models are live.** `monitoring.deployed_models` exists but nothing in
  bgg-viewer reads it. It is also the wrong shape: a view over the raw landing history
  (~360MB per query; `raw.game_embeddings` alone is 3.7M rows) that lists every version
  ever landed, not the versions actually serving predictions.

## Goal

One admin-only page in bgg-viewer, `/admin/pipeline`, that answers:

1. Did today's chain run end to end, and if not, where did it stop?
2. Is each downstream table fresh, and does it cover the games it should?
3. Which model versions are live, and is an older one still serving some games?

## Decisions

- **Observe, don't fix.** This makes the gaps visible. Fixes (Dataform failing on a
  non-`SUCCEEDED` invocation; chaining collection scoring off `embeddings_complete`) are
  separate PRs.
- **No service health pings.** Pinging five Cloud Run services on each page load wakes
  each from a cold start (see #131/#132). Cut.
- **No new notifications.** The `pipeline-status` GitHub issue stays the push channel.
  Widening it to the whole chain is a follow-up that can reuse `build_chain`.
- **No new Dataform view for freshness.** That SQL lives in a Python reader, as
  `pipeline_status` and `/new-games` already do.
- **Fix `monitoring.deployed_models` rather than work around it.** Rebuild it as a
  *table* on the serving tables (see Components). Only the retired bgg-dash-viewer read
  it, and BigQuery job history shows no queries against it in the 30 days to 2026-10-02,
  so changing its meaning and dropping `embedding_dim`/`document_method` is safe.
- **Status colours avoid green/red.** Blue = ok, amber = needs attention, violet =
  failed; every state also carries a glyph and a word.

## Status rules

`build_chain(runs, day)` is a pure function. `day` is a UTC date; the chain window is
`[day 05:00 UTC, day+1 05:00 UTC)`. For each stage it takes the **latest** matching run
created in the window (a manual rerun supersedes an earlier failure).

| Status | Rule |
|---|---|
| `ok` | run `conclusion == success`, and the next chain stage has a run created after this one |
| `warn` (no hand-off) | run succeeded, but no run of the next stage was created within **30 minutes** of this run's `updated_at` |
| `fail` | run `conclusion` is `failure`, `cancelled` or `timed_out` |
| `running` | run `status` is `queued` or `in_progress` |
| `not_reached` | no run, and an upstream stage is not `ok` |
| `fail` (missing) | stage 1 has no run and `now` is past 07:00 UTC on `day` |
| `pending` | no run, upstream is `ok`/`running`, and the 30-minute hand-off window hasn't closed |

Stage 12 has no next stage; success is `ok`. A `skipped` conclusion is `not_reached`.

Off-chain:
- Collection Scoring is `warn` ("ran before today's predictions") if its run started
  before stage 11's `updated_at`, or if stage 11 has no successful run that day.
  Otherwise its own conclusion maps as above.
- Collection Reports and Pipeline Status map from their own conclusion only.

**Verdict:** the first chain stage that isn't `ok`. If it's `warn`, the headline names
the stage and the time since its `updated_at` ("Stalled after Dataform · pass 3 —
4h 52m"). All `ok` → "Chain completed" with end-to-end duration (stage 1 `created_at` →
stage 12 `updated_at`).

## Components — bgg-data-warehouse

### 1. `src/monitoring/github.py` (new)

- `fetch_runs(repo, workflow_file, start, end, token, session=requests)` — moved from
  `pipeline_status.fetch_job_runs`. Returns `created_at`, `updated_at`, `event`,
  `status`, `conclusion`, `display_title`, `html_url`. Follows `Link: rel="next"`
  pagination.
- `pipeline_status.py` imports it instead of defining its own. Its behaviour doesn't
  change. Its tests are updated for the move.

### 2. `src/monitoring/chain.py` (new)

- `STAGES` and `OFF_CHAIN`: the tables above as data (`key`, `label`, `repo`,
  `workflow_file`, `title`, `event`, `lane`).
- `match_stage(run, stage)`: a run matches when its workflow file matches and, for
  `dataform.yml`, its `display_title` (dispatch passes) or `event == "workflow_run"`
  (pass 1) matches.
- `build_chain(runs_by_workflow, day, now) -> Chain`: the status rules above. Pure.
- `build_history(runs_by_workflow, days, now) -> list[Chain]`: one chain per day.

### 3. `src/warehouse/readers/pipeline.py` (new)

- `fetch_table_status(client=None) -> list[dict]`: one parameterised BigQuery query
  that returns `table`, `last_updated`, `games`, `covered`, `universe` (nullable) and
  `users` (collection predictions only) per row:

| Table | `last_updated` from | Universe (coverage denominator) |
|---|---|---|
| `raw.thing_ids` | `MAX(load_timestamp)` | — |
| `raw.fetched_responses` | `MAX(fetch_timestamp)` | distinct `boardgame` IDs in `raw.thing_ids` |
| `analytics.games_features` | `MAX(load_timestamp)` | — |
| `predictions.bgg_description_embeddings` | `MAX(created_ts)` | `games_features` |
| `predictions.bgg_complexity_predictions` | `MAX(score_ts)` | `games_features` |
| `predictions.bgg_predictions` | `MAX(score_ts)` | `games_features` with `year_published` 2025–2030 |
| `predictions.bgg_game_embeddings` | `MAX(created_ts)` | `games_features` |
| `predictions.bgg_game_coordinates` | `MAX(created_ts)` | `games_features` |
| `predictions.user_collection_predictions` | `MAX(score_ts)` | — (also returns distinct users) |

  The 2025–2030 range is the scoring workflow's default (`run-scoring-service.yml`)
  and lives in one constant. Column names verified against the live schemas on
  2026-10-02.
- `fetch_deployed_models(client=None) -> list[dict]`: every row of the
  `monitoring.deployed_models` table (a dozen rows), newest first within each type.

### 3b. `definitions/deployed_models.sqlx` (rewritten)

- `type: "table"`, not a view. Dataform rebuilds it on each of the four daily passes
  (~26MB each), so the API's read is a few KB.
- Built from the deduped serving tables via `ref()`: the model name, version and
  experiment columns of `bgg_predictions` (hurdle, rating, users_rated, geek_rating),
  `bgg_complexity_predictions`, and the `embedding_model`/`embedding_version`/`algorithm`
  columns of `bgg_game_embeddings` and `bgg_description_embeddings`.
- One row per (`model_type`, `model_name`, `model_version`) still behind at least one
  game, with `games_count` and `last_updated`. Columns: `model_category`, `model_type`,
  `model_name`, `model_version` (STRING, so `3` and `2027.0.1` both fit), `experiment`, `algorithm`, `games_count`,
  `last_updated`. More than one row for a type means an older version still serves some
  games (on 2026-10-02: v1 hurdle, rating and users_rated still served 4,319 games).
- Ships with the warehouse PR; `dataform.yml` rebuilds it on merge (push to
  `definitions/**`). Until then the reader's columns also exist on the old view.

### 4. `GET /monitoring/pipeline` on the warehouse API

Added to `services/warehouse_api/routers/monitoring.py`.

- Query: `days` (1–30, default 14).
- Fetches the runs for every `STAGES` + `OFF_CHAIN` workflow in parallel, then
  `build_chain` for today, `build_history` for the history grid, plus
  `fetch_table_status` and `fetch_deployed_models`.
- Response:
  ```json
  {
    "generated_at": "…",
    "verdict": {"status": "warn", "stage": "dataform_pass_3", "headline": "…", "since": "…"},
    "today": {"day": "2026-10-02", "stages": [{"key": "…", "label": "…", "lane": "warehouse",
              "status": "ok", "started": "…", "finished": "…", "url": "…", "event": "…"}],
              "off_chain": [ … ]},
    "history": [{"day": "…", "stages": {"fetch_thing_ids": "ok", …}}],
    "tables": [{"table": "…", "last_updated": "…", "games": 0, "covered": 0, "universe": 0,
                "users": null}],
    "models": [{"model_type": "…", "model_name": "…", "model_version": "…",
                "games_count": 0, "last_updated": "…"}]
  }
  ```
- Cached in-process for 5 minutes, keyed by `days`, like `/new-games`.
- `GH_TOKEN` from the environment. Missing → `503` with a clear message. A GitHub or
  BigQuery error → `502`; the page shows the error, not a partial status.
- Deploy: `GH_TOKEN` is added as a Secret Manager secret mounted on the Cloud Run
  service. The token needs read access to Actions on the three repos (all public, so a
  fine-grained token with no extra scopes is enough; it only lifts the rate limit).

## Components — bgg-viewer

- **Route** `src/routes/(app)/admin/pipeline/+page.server.ts`: `error(404)` unless
  `isAdmin(locals.user)`. Loads `warehouseClient().getPipelineStatus(days)` and the
  catalog artifact pointer (`builtAt`, `stale`) the viewer already reads.
- **Client:** `getPipelineStatus(days)` in `src/lib/server/warehouse/client.ts`, with
  types in `types.ts`.
- **Components** in `src/lib/monitoring/`, following the mockup:
  - `Verdict.svelte`
  - `ChainLanes.svelte`: three lanes; falls back to a single column with a lane tag
    under 760px
  - `HistoryGrid.svelte`: healthy cells pale, problems saturated
  - `TableStatus.svelte`: includes the catalog artifact row from the viewer's own
    pointer
  - `DeployedModels.svelte`
  - `StatusBadge.svelte`: glyph + word, never colour alone
  - A `running` state is added to the mockup's set (neutral, animated ring) and a
    `pending` state (dashed, like not reached, labelled "Waiting").
- **Tokens** in `src/app.css`: `--status-ok`, `--status-warn`, `--status-fail`, light
  and dark, as in the mockup.
- **Admin panel:** an "Admin" link beside Settings (and in the mobile menu), rendered
  only for the admin, opens `/admin`: an admin-only panel (404 otherwise) with its own
  sub-nav. It opens straight onto Pipeline, its only section so far; later admin tools
  join the sub-nav.
- **Freshness status** in `TableStatus` (viewer-side, display only): "Fresh" if
  `last_updated` is on or after today's stage 1 start; otherwise "A day old" / "N days
  old" in amber. Coverage bar amber below 99.5%.

## Validation

- **pytest, warehouse**
  - `tests/test_chain.py`: `build_chain` against a fixture of the real runs from all
    three repos for 2026-10-02 (all `ok`), plus synthetic days: stalled after pass 3
    (`warn` then `not_reached`), Fetch Thing IDs failed then manually rerun (latest run
    wins), no stage-1 run by 07:00 (`fail` missing), collection scoring started before
    stage 11 finished (`warn`), a run `in_progress` (`running`).
  - `tests/test_pipeline_reader.py`: query parameters and row mapping, BigQuery mocked.
  - `tests/test_monitoring_router.py`: the new route with readers mocked; cache; 503
    without `GH_TOKEN`.
  - `tests/test_pipeline_status.py` still passes after the move.
- **Local replay:** `uv run python -c` (or a small `__main__` in `chain.py`) prints
  `build_chain` for 2026-09-16; Fetch Thing IDs shows `fail` then the rerun.
- **vitest, viewer:** the freshness and coverage display rules; the admin gate.
- **End to end, locally:**
  ```sh
  # bgg-data-warehouse
  GH_TOKEN=$(gh auth token) uv run python -m services.warehouse_api.main   # :8080
  # bgg-viewer .env
  WAREHOUSE_API_URL=http://localhost:8080
  DEV_AUTH_EMAIL=<admin email>
  ```
  then `just dev` and open `/admin/pipeline`.

## Risks

- **Stale Actions listing** (seen by the old heartbeat): the `created` date filter
  changes daily, which Pipeline Status relies on too. A stale page would show stages as
  `pending`/`not_reached` while tables look fresh — visible on the same page.
- **GitHub rate limit:** ~16 workflow listings per refresh, cached 5 minutes. Well
  under the 5,000/hour authenticated limit.
- **A renamed workflow or dispatch event** silently drops a stage to `not_reached`.
  `STAGES` is the one place to update; the fixture test pins today's names.
- **BigQuery cost:** one query over nine tables per 5 minutes while the page is open.
  It reads only the timestamp and game ID columns.

## Out of scope

- Fixing the gaps it reveals (separate PRs).
- Widening the `pipeline-status` issue to the whole chain.
- Experiment / training history (the GCS experiments bucket).
- Service health checks.

## Delivery

1. bgg-data-warehouse, branch `feat/pipeline-monitor`: this spec, the plan, `src/monitoring/`,
   the reader, the route, tests. Merge deploys the API; add the `GH_TOKEN` secret.
2. bgg-viewer, branch `feat/pipeline-monitor`: the mockup, the page, tokens, nav, tests.
   Works locally against a local API before step 1 is deployed.

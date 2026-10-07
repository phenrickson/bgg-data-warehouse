# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.9.0](https://github.com/phenrickson/bgg-data-warehouse/compare/v0.8.0...v0.9.0) (2026-10-07)


### Features

* **api:** pooled similarity on /games/{id}/similar, with a POST for id pools ([#158](https://github.com/phenrickson/bgg-data-warehouse/issues/158)) ([8a9b21b](https://github.com/phenrickson/bgg-data-warehouse/commit/8a9b21b7673a617ec0536264891d630b70ef491d))
* **monitor:** games served by each step's current model; always fetch today's ML steps ([#156](https://github.com/phenrickson/bgg-data-warehouse/issues/156)) ([9bcdba2](https://github.com/phenrickson/bgg-data-warehouse/commit/9bcdba24944a1320bf3265d3129b639fd6bbaa95))

## [0.8.0](https://github.com/phenrickson/bgg-data-warehouse/compare/v0.7.0...v0.8.0) (2026-10-06)


### Features

* **monitor:** pipeline monitor v2 — ML steps, cutover history, models in use ([#153](https://github.com/phenrickson/bgg-data-warehouse/issues/153)) ([b5a08e6](https://github.com/phenrickson/bgg-data-warehouse/commit/b5a08e690f7a9c4fdb659035e09bf526d5f7b14c))

## [0.7.0](https://github.com/phenrickson/bgg-data-warehouse/compare/v0.6.7...v0.7.0) (2026-10-06)


### Features

* **ci:** core-only run after fetch, retire ML callbacks; monitor the new chain ([#147](https://github.com/phenrickson/bgg-data-warehouse/issues/147)) ([e63c498](https://github.com/phenrickson/bgg-data-warehouse/commit/e63c498226a0d25c328d055888cf49d35504975d))
* **ci:** pipeline status replaces the scrape heartbeat ([#133](https://github.com/phenrickson/bgg-data-warehouse/issues/133)) ([ceb89c9](https://github.com/phenrickson/bgg-data-warehouse/commit/ceb89c908b2be18b547809ce4a2e63a0b503a9a0))
* **dataform:** core/publish pieces, ml_complete handler and dev run ([#146](https://github.com/phenrickson/bgg-data-warehouse/issues/146)) ([51a16f5](https://github.com/phenrickson/bgg-data-warehouse/commit/51a16f54d9ec6b1fb754bca8c53fc5d4f400965d))
* **dataform:** materialize the similarity profiles as a table ([#130](https://github.com/phenrickson/bgg-data-warehouse/issues/130)) ([650a176](https://github.com/phenrickson/bgg-data-warehouse/commit/650a17658aced3a2b0a00fc7b5713eb9b1accf55))
* **deploy:** mount GH_TOKEN on the warehouse API ([#137](https://github.com/phenrickson/bgg-data-warehouse/issues/137)) ([6e16016](https://github.com/phenrickson/bgg-data-warehouse/commit/6e160161126063a99a60499cda4d18e8dcd92881))
* **monitor:** Dataform lineage endpoints for the admin Lineage view ([#139](https://github.com/phenrickson/bgg-data-warehouse/issues/139)) ([e128b09](https://github.com/phenrickson/bgg-data-warehouse/commit/e128b0983b10ca13f1b7bcdc91b9c13d1af9f99c))
* **monitor:** GET /monitoring/pipeline for the whole daily chain ([#135](https://github.com/phenrickson/bgg-data-warehouse/issues/135)) ([83e10cb](https://github.com/phenrickson/bgg-data-warehouse/commit/83e10cb0f9852f92754cb910e17412be296ae768))
* **probe:** look below the ID frontier (daily 500, Sunday 5,000) ([#127](https://github.com/phenrickson/bgg-data-warehouse/issues/127)) ([82408f2](https://github.com/phenrickson/bgg-data-warehouse/commit/82408f27d5f34e4a237bcd5042972acdf8c71b94))
* **release:** release-please for warehouse releases ([#148](https://github.com/phenrickson/bgg-data-warehouse/issues/148)) ([37a9158](https://github.com/phenrickson/bgg-data-warehouse/commit/37a9158775a349f65b939c5ae05535e2abbcbd2a))
* **terraform:** secret for the warehouse API's GitHub token ([#136](https://github.com/phenrickson/bgg-data-warehouse/issues/136)) ([7f71673](https://github.com/phenrickson/bgg-data-warehouse/commit/7f71673115ef74cefe4b2feaa06b8065c6a0bcdc))


### Bug Fixes

* **api:** strip BGG_API_TOKEN so a pasted trailing newline doesn't break the header ([#121](https://github.com/phenrickson/bgg-data-warehouse/issues/121)) ([6afaa4d](https://github.com/phenrickson/bgg-data-warehouse/commit/6afaa4d5b1bb10294b73a192d89ac039b5c5bded))
* **ci:** chain Refresh Old Games off Fetch New Games so Dataform cascades once a day ([#123](https://github.com/phenrickson/bgg-data-warehouse/issues/123)) ([a5919e1](https://github.com/phenrickson/bgg-data-warehouse/commit/a5919e1fcc4eb6b2d080b5e7ee8c080cefe6e0ab))
* **ci:** heartbeat checks Fetch New Games, which every discovery path reaches ([#118](https://github.com/phenrickson/bgg-data-warehouse/issues/118)) ([2aafb40](https://github.com/phenrickson/bgg-data-warehouse/commit/2aafb40939dcb0f53d48cb466e7d891f4232b2da))
* **ci:** let Fetch Games accept more than one game id ([#120](https://github.com/phenrickson/bgg-data-warehouse/issues/120)) ([fefdc21](https://github.com/phenrickson/bgg-data-warehouse/commit/fefdc21fa0f66bd75baa93dfa445e4d90a9ef14d))
* **ci:** redeploy the warehouse API when any module it imports changes ([#150](https://github.com/phenrickson/bgg-data-warehouse/issues/150)) ([d15eba7](https://github.com/phenrickson/bgg-data-warehouse/commit/d15eba7b3428f313b15eee36b8b3ea07451102a7))
* **ci:** take max run time over a window in the scrape heartbeat ([#128](https://github.com/phenrickson/bgg-data-warehouse/issues/128)) ([e4649ae](https://github.com/phenrickson/bgg-data-warehouse/commit/e4649ae4f89971de4352cc541b4c90ade669e21e))
* **monitor:** measure ML table coverage against games with a year_published ([#138](https://github.com/phenrickson/bgg-data-warehouse/issues/138)) ([4b04de4](https://github.com/phenrickson/bgg-data-warehouse/commit/4b04de42f410cf6b2585adf0f4c3052e8de9a7d5))
* **processor:** keep negative (BC) publication years instead of nulling them ([#119](https://github.com/phenrickson/bgg-data-warehouse/issues/119)) ([23b912e](https://github.com/phenrickson/bgg-data-warehouse/commit/23b912eee1fd3c768fabcec66f7eab2c8bebdc7a))


### Performance

* **api:** slim the image and compile bytecode to cut cold-start imports ([#132](https://github.com/phenrickson/bgg-data-warehouse/issues/132)) ([7489ba3](https://github.com/phenrickson/bgg-data-warehouse/commit/7489ba31c122206ab7ac468b57b64c81ed0fc63c))
* **processor:** select the batch without response_data; batches of 1000 ([#140](https://github.com/phenrickson/bgg-data-warehouse/issues/140)) ([9a17cf1](https://github.com/phenrickson/bgg-data-warehouse/commit/9a17cf1ff86b8d1c25bdfc5c37fe993c4c23da28))

## [0.6.7] - 2026-09-14

### Added

- **ID discovery via the XML API** (`src/modules/id_probe_fetcher.py`): new IDs are found by walking the numeric ID space above the highest known ID instead of crawling BGG's sitemaps. BGG publishes no endpoint that enumerates the catalog or reports new items, so IDs still have to be discovered — but the API has no bot-protection surface, where the sitemap path does. `fetch_thing_ids` takes `--source bgg_api_probe` (default) or `bgg_sitemap`, and rows record which method found them in the `source` column. Replaying the probe over a known ID range recovered 100% of its items plus 16 the sitemap had missed, since the sitemap is regenerated periodically while the API is live.

### Changed

- **Scheduled ID discovery moved back to GitHub Actions** from the residential-IP home box. Cloudflare blocks datacenter egress on BGG's HTML/sitemap paths, but not on the authenticated API, so the residential IP is no longer required. `Fetch Thing IDs` runs on a schedule again and the existing `workflow_run` trigger resumes the `Fetch New Games → Dataform` chain. The home box and its `repository_dispatch` remain functional and are not yet retired; the box job now runs the probe too.
- **`BGGAPIClient` takes `throttle_delay` and `log_requests`.** BGG throttles sustained traffic well before the documented 2 req/s — a 0.5s cadence drew `429` after ~48 consecutive requests — so bulk callers pace themselves separately from the detail pipeline, which is unaffected at ~50 requests/day.
- **`get_thing()` accepts `type_filter=None`** to omit the `type` parameter. It previously hardcoded `type=boardgame`, which would have hidden expansions and accessories from the probe.
- **Scrape Heartbeat now checks for a recent successful `Fetch Thing IDs` run** rather than a home-box `repository_dispatch`, which no longer indicates health now that the schedule is back in Actions.

### Fixed

- **Home-box `git pull` permanently blocked by a stale `uv.lock`**: the 0.6.6 release bumped `pyproject.toml`'s version but not `uv.lock`, so every `uv run` on the box silently regenerated a locally-modified lockfile that then aborted the nightly `git pull --ff-only`. This had been failing since 2026-07-15, leaving the box **36 commits behind `main`** and running two-month-old code; the sitemap scrape kept working, so nothing surfaced it. Regenerated `uv.lock` to match.
- **Cloudflare hard-block page treated as a retryable challenge**: `_wait_for_cloudflare` only checked for the "Just a moment" JS-challenge title; a terminal `Attention Required! | Cloudflare` block page (a static "you have been blocked" response, not a solvable challenge) matched the same wait condition, so the scrape burned its full timeout and all retries for nothing before failing. It now raises `CloudflareBlockedError` immediately on a block page.

### Documentation

- Corrected `docs/bgg_api.md`: the XML API now **requires** authentication (BGG made this change in September 2026; the doc claimed it was public), documented the real rate-limit behavior, and recorded how item discovery works and why.

## [0.6.6] - 2026-07-15

### Removed

- **Retired the in-repo Streamlit dashboard**: the consumer-facing app is now the separate `bgg-dash-viewer` project, which reads the `analytics`/`predictions` datasets directly. Removed `src/visualization/`, `docker/Dockerfile.dashboard`, the `deploy-dashboard.yml` workflow, and the now-unused `streamlit`/`plotly` dependencies.

### Documentation

- Refreshed `README.md` and `docs/architecture.md` to the current pipeline (home-box scrape, event-driven `repository_dispatch` chain, four pipelines, ML/predictions integration), fixing stale commands, Cloud Run job names, and secret names. Corrected the `docs/bgg_api.md` authentication note (BGG's XML API2 is public) and removed the obsolete `docs/dashboard_deployment.md`.

## [0.6.5] - 2026-07-15

### Changed

- **Scheduled ID scraping moved to a residential-IP "home box"**: BGG's Cloudflare protection blocks datacenter egress, so `fetch_thing_ids` no longer runs on a schedule in GitHub Actions. A residential-IP box runs the scrape via Task Scheduler and, on success, fires a `repository_dispatch` (`thing_ids_fetched`) that resumes the downstream `Fetch New Games → Run Dataform` chain. `fetch_thing_ids.yml` remains as a manual `workflow_dispatch` fallback. (#75, #76)
- **Scrape Heartbeat workflow**: warns if no successful home-box dispatch lands within ~26h (box offline, scrape error, etc.), replacing the daily red-X that the removed schedule used to provide. (#76)

### Fixed

- **Cloudflare bot detection on sitemaps**: fetch the sitemap index and pages through a stealth browser session instead of plain HTTP. (#75)
- **Home-box wrapper reliability**: run native commands via `cmd` so PowerShell 5.1 doesn't treat native stderr as a terminating error (#77); write the run log as consistent UTF-8 (#78).
- **Scrape starved by Modern Standby**: the home mini PC was dropping into low-power standby mid-run, killing network connectivity and failing the fetch. The wrapper now holds a `SetThreadExecutionState` wake-lock for the duration of the run, and `fetch_sitemap_index` retries with exponential backoff and uses `wait_until="domcontentloaded"` instead of hanging on `load`. (#79)

### Added

- **Claude Code skills** under `.claude/skills/` (developer tooling, no runtime change): `brainstorming`, `planning`, `debugging`, `write-tests`, `explain-codebase`, `data-exploration`, `dataform-model`, `release`. (#80)

## [0.6.4] - 2026-05-04

### Added

- **`predictions.user_collection_predictions` table**: New incremental Dataform model exposing per-user collection-model predictions to downstream consumers (e.g. bgg-dash-viewer). Joins `bgg-predictive-models.raw.collection_predictions_landing` to `collection_models_registry` filtered to `status = 'active'`, deduped to the latest `score_ts` per `(username, game_id, outcome)`. Two new cross-project source declarations in `definitions/sources.js`.

## [0.6.3] - 2026-03-31

### Added

- **On-demand game fetch workflow**: New GitHub Actions `workflow_dispatch` workflow (`Run Fetch Games`) to fetch specific games by ID on demand. Enter comma-separated game IDs to trigger a fetch, process responses, and run the full Dataform + ML scoring pipeline. Works for both new games and refreshing existing ones.
  - New pipeline script: `src/pipeline/fetch_games.py`
  - New Cloud Run job: `bgg-fetch-games`
  - New workflow: `.github/workflows/fetch_games.yml`
  - Dataform now triggers after on-demand fetches

## [0.6.2] - 2026-03-31

### Fixed

- **Sitemap fetcher blocked by Cloudflare**: BGG now blocks plain HTTP requests to individual sitemap pages with 403 Forbidden. Switched from `requests` to fetching all sitemaps through the Playwright browser session, which bypasses bot detection
- **Retry logic for sitemap fetches**: Added retry with exponential backoff (3 attempts, 5s/10s/20s) for transient failures when fetching individual sitemaps

## [0.6.1] - 2026-02-23

### Fixed

- **Sitemap scraper stability**: Individual sitemaps are now fetched via plain HTTP instead of the browser, eliminating memory crashes that caused incomplete fetches
- **Type misclassification prevention**: Errors during sitemap fetching now abort the entire run instead of silently uploading partial results. Previously, a crash mid-run would upload only boardgame sitemaps (missing expansion/accessory overrides), causing expansions to be misclassified as boardgames
- **Sitemap processing order**: Explicitly sort sitemaps (boardgame → expansion → accessory) to ensure correct last-write-wins type assignment, matching the activityclub.org Perl script behavior

### Data Cleanup

- Deleted 29,321 misclassified `thing_ids` rows from broken Feb 23 run
- Deleted ~44K incorrectly fetched responses from `fetched_responses` and `raw_responses`

## [0.6.0] - 2026-02-19

### Added

- **Browser-based BGG ID discovery**: New pipeline to scrape game IDs directly from BGG sitemaps using Playwright
  - Replaces dependency on `bgg.activityclub.org/bggdata/thingids.txt` which stopped updating
  - New `fetch_thing_ids` pipeline (`src/pipeline/fetch_thing_ids.py`)
  - New `BrowserIDFetcher` module (`src/modules/id_fetcher_browser.py`) using Playwright/Chromium
  - Bypasses Cloudflare protection on BGG sitemaps
- **New Cloud Run job**: `bgg-fetch-thing-ids` for ID discovery
- **Playwright dependency**: Added for browser automation

### Changed

- **Pipeline architecture**: Split ID fetching from response processing
  - `fetch_thing_ids`: Discovers new game IDs from BGG sitemaps → uploads to `thing_ids`
  - `fetch_new_games`: Fetches API responses for unfetched IDs → processes into normalized tables
- **GitHub Actions workflow**: Now runs two jobs in sequence (`fetch-thing-ids` → `fetch-new-games`)
- **Docker image**: Updated to include Playwright and Chromium dependencies
- **ID source field**: New IDs now have `source = 'bgg_sitemap'` instead of `'bgg.activityclub.org'`

### Removed

- Dependency on `bgg.activityclub.org` for game ID discovery
- ID fetching step from `fetch_new_games` pipeline (now handled by `fetch_thing_ids`)

## [0.5.0] - 2026-02-16

### Changed

- **Incremental Dataform tables**: Converted 7 tables from full rebuilds to incremental processing to reduce BigQuery costs
  - `bgg_game_embeddings`: Only processes new embeddings since last run
  - `bgg_description_embeddings`: Only processes new embeddings since last run
  - `bgg_predictions`: Only processes new predictions since last run
  - `bgg_complexity_predictions`: Only processes new predictions since last run
  - `game_similarity_search`: Only processes games with new embeddings
  - `games_active`: Only processes newly loaded/updated games
  - `games_features`: Only processes newly loaded/updated games with optimized aggregation CTEs
- **Cost optimization**: Expected to reduce monthly BigQuery analysis costs by ~90% by staying within 1TB free tier
  - Previous: ~120-150 GB/day scanned (~1.76 TB/month, exceeding free tier)
  - Expected: Only new records scanned per run (KB-MB instead of GB)

### Migration Notes

- First run after deployment will do a full table rebuild (normal incremental behavior)
- Use `--full-refresh` flag in Dataform to force a complete rebuild if needed
- Tables use `uniqueKey: ["game_id"]` for MERGE operations (upsert on game_id)

## [0.4.4] - 2026-01-29

### Added

- **Deployed models monitoring view**: New `monitoring.deployed_models` view consolidating ML model metadata
  - Extracts metadata from prediction and embedding tables for dashboard monitoring
  - Tracks model name, version, experiment, algorithm, dimensions, and game counts
  - Covers all 6 model types: hurdle, complexity, rating, users_rated, game_embedding, text_embedding

## [0.4.3] - 2026-01-27

### Changed

- **Pipeline event flow redesign**: Fixed data dependency bug where embeddings used stale complexity predictions
  - Dataform now runs after complexity scoring to materialize predictions before embeddings
  - New event naming: `complexity_complete`, `embeddings_complete`, `dataform_complexity_ready`
  - Replaced generic `predictions_complete` with specific events for better observability
  - Pipeline is now purely event-driven (removed cron schedules from ML workflows)
  - Text embeddings now runs before game embeddings (future-proofing for dependency)
  - See `docs/plans/2026-01-27-pipeline-event-flow-design.md` for full design

## [0.4.2] - 2026-01-26

### Added

- **Description embeddings Dataform integration**: New analytics table for text description embeddings from bgg-predictive-models
  - Added `bgg_description_embeddings.sqlx` transformation with version-aware deduplication
  - Outputs to `predictions.bgg_description_embeddings` with latest embeddings per game
  - Added external source declaration in `sources.js`
  - Added 8:30 AM UTC scheduled Dataform run to sync after text embeddings workflow

## [0.4.1] - 2026-01-20

### Added

- **Unpublished games refresh**: Games with `year_published IS NULL` are now included in the refresh pipeline
  - New `unpublished` interval in refresh policy (bi-weekly refresh)
  - Catches games that were announced/pre-release when first fetched and have since been published
  - Ensures these games get added to `game_features_hash` and receive embeddings once updated

## [0.4.0] - 2026-01-06

### Added
- **Terraform-managed infrastructure**: Complete infrastructure-as-code setup
  - GCP project and authentication, BigQuery datasets and schemas managed via Terraform
  - Service account with required IAM permissions
  - Cloud Run jobs and schedulers
- **Dataform integration**: Analytics transformations via Google Dataform loading to `analytics datasets`
  - `games_active` view for latest game data
  - `games_features` as table for predictive modeling
  -  tables used for Dash application (`filter_publishers`, `filter_designers`)
  - GitHub Actions workflow for automated Dataform runs
- **New computed columns in `games_features`**:
  - `hurdle`: Binary flag (1 if users_rated >= 25, else 0)
  - `geek_rating`: Alias for bayes_average
  - `complexity`: Alias for average_weight
  - `rating`: Alias for average_rating
  - `log_users_rated`: Natural log of (users_rated + 1)
- Migration documentation for moving from old GCP project

### Changed
- **GCP project migration**: Moved from `gcp-demos-411520` to dedicated `bgg-data-warehouse` project
- **Simplified dataset naming**: `raw`, `core`, `analytics` instead of `bgg_raw_{env}`, `bgg_data_{env}`
- **Hardcoded table names**: Removed multi-environment configuration in favor of single-project setup
- Changed `year_published` column type from INTEGER to FLOAT64 to support ancient games with BCE publication dates
- Removed environment separation from configuration

### Removed
- Multi-environment configuration (`dev`/`prod` suffix on datasets)
- Dynamic table name resolution from config

## [0.2.0] - 2025-06-24

### Added
- UV package manager integration replacing pip
- Automated hourly pipeline runs via Cloud Run jobs
- Streamlined deployment process with Cloud Build
- Enhanced environment configuration handling
- Comprehensive GitHub Actions workflows:
  - Deployment workflow for automated Cloud Build updates
  - Pipeline workflow for scheduled job execution
- Cloud Run job execution improvements:
  - bgg-fetch-responses job for data collection
  - bgg-process-responses job for data transformation

### Changed
- Migrated to UV for package management and virtual environments
- Improved response fetching and processing pipeline
- Enhanced error handling for API response parsing
- Added robust tracking for game IDs with no response or parsing errors
- Optimized Cloud Run job configurations
- Streamlined deployment process

### Deprecated
- None

### Removed
- Pip-based package management
- Manual job execution processes

### Fixed
- Resolved issues with handling game IDs that no longer exist or return no response
- Improved logging and status tracking for API response processing
- Added graceful handling of empty or problematic API responses
- Enhanced Cloud Run job error handling

### Security
- Enhanced data integrity checks in response processing pipeline
- Improved error logging to prevent potential data leakage
- Secured GitHub Actions secret handling
- Enhanced Cloud Run job security configurations

## [0.3.11] - 2026-01-05

### Changed
- Reduced Cloud Run job resources from 4Gi/2vCPU to 2Gi/1vCPU for cost optimization

## [0.3.1]

### Added
- New fetch_in_progress table for tracking and locking game fetches
- Parallel fetching support with distributed locking mechanism
- Automated cleanup of stale in-progress entries after 30 minutes

### Changed
- Pipeline now runs every 3 hours instead of hourly for better resource utilization
- ID fetcher now runs in both prod and dev environments (previously prod-only)
- Enhanced response processing with better error handling
- Improved logging for fetch operations and error cases

### Fixed
- Prevented duplicate game fetches in parallel execution
- Added robust handling of API response parsing errors
- Improved cleanup of orphaned in-progress entries

### Security
- Added safeguards against race conditions in parallel fetching

## [0.1.0] - 2025-06-09

### Added
- Initial release
- Basic project structure
- Core functionality for BGG data pipeline
- Documentation and setup instructions

[0.6.1]: https://github.com/phenrickson/bgg-data-warehouse/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/phenrickson/bgg-data-warehouse/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/phenrickson/bgg-data-warehouse/compare/v0.4.4...v0.5.0
[0.4.4]: https://github.com/phenrickson/bgg-data-warehouse/compare/v0.4.3...v0.4.4
[0.4.3]: https://github.com/phenrickson/bgg-data-warehouse/compare/v0.4.2...v0.4.3
[0.4.2]: https://github.com/phenrickson/bgg-data-warehouse/compare/v0.4.1...v0.4.2
[0.4.1]: https://github.com/phenrickson/bgg-data-warehouse/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/phenrickson/bgg-data-warehouse/compare/v0.3.11...v0.4.0
[0.3.11]: https://github.com/phenrickson/bgg-data-warehouse/compare/v0.3.1...v0.3.11
[0.3.1]: https://github.com/phenrickson/bgg-data-warehouse/compare/v0.2.0...v0.3.1
[0.2.0]: https://github.com/phenrickson/bgg-data-warehouse/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/phenrickson/bgg-data-warehouse/releases/tag/v0.1.0

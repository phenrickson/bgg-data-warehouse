# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

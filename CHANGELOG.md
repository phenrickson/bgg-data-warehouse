# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[0.4.0]: https://github.com/phenrickson/bgg-data-warehouse/compare/v0.3.11...v0.4.0
[0.3.11]: https://github.com/phenrickson/bgg-data-warehouse/compare/v0.3.1...v0.3.11
[0.3.1]: https://github.com/phenrickson/bgg-data-warehouse/compare/v0.2.0...v0.3.1
[0.2.0]: https://github.com/phenrickson/bgg-data-warehouse/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/phenrickson/bgg-data-warehouse/releases/tag/v0.1.0

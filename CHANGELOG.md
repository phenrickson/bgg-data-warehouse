# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

## [Unreleased]

### Added
- New fetch_in_progress table for tracking and locking game fetches
- Parallel fetching support with distributed locking mechanism
- Automated cleanup of stale in-progress entries after 30 minutes

### Changed
- Pipeline now runs every 3 hours instead of hourly for better resource utilization
- ID fetcher now runs in both prod and dev environments (previously prod-only)
- Enhanced response processing with better error handling
- Improved logging for fetch operations and error cases

### Deprecated
- None

### Removed
- None

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

[Unreleased]: https://github.com/yourusername/bgg-data-warehouse/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/yourusername/bgg-data-warehouse/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/yourusername/bgg-data-warehouse/releases/tag/v0.1.0

# Manual Testing Scripts

This directory contains scripts for manually testing and validating BGG data warehouse functionality.

## Refresh Strategy Testing

### Dry Run Script

The `refresh_strategy_dry_run.py` script allows you to test the refresh strategy implementation without making any changes to your database. It shows which games would be selected for refresh based on the exponential decay algorithm.

#### Usage

```bash
# Basic usage (uses dev environment by default)
uv run python tests/manual/refresh_strategy_dry_run.py

# Use production environment
uv run python tests/manual/refresh_strategy_dry_run.py --environment prod

# Export results to CSV files for further analysis
uv run python tests/manual/refresh_strategy_dry_run.py --export-csv

# Analyze more games (default is 100)
uv run python tests/manual/refresh_strategy_dry_run.py --limit 500
```

#### What It Does

The script:

1. Identifies games that have never been fetched
2. Identifies games that are due for refresh based on the exponential decay formula
3. Analyzes the distribution of refresh candidates by year and category
4. Simulates which games would be selected in a batch operation
5. Optionally exports the results to CSV files

#### Output Example

```
=== Refresh Configuration ===
Base interval (current year): 7 days
Upcoming interval: 3 days
Decay factor: 2.0
Maximum interval: 90 days
Refresh batch size: 200

Identifying unfetched games...
Found 1250 unfetched games

Identifying refresh candidates...
Found 325 games due for refresh

=== Refresh Distribution by Year ===
Year 2025: 45 games
Year 2024: 78 games
Year 2023: 92 games
Year 2022: 65 games
Year 2021: 45 games

=== Refresh Distribution by Category ===
Current Year: 45 games
Last Year: 78 games
Older: 202 games

=== Average Refresh Interval by Year (days) ===
Year 2025: 7.0 days
Year 2024: 14.0 days
Year 2023: 28.0 days
Year 2022: 56.0 days
Year 2021: 90.0 days

=== Average Hours Overdue by Year ===
Year 2025: 12.5 hours
Year 2024: 24.3 hours
Year 2023: 36.7 hours
Year 2022: 48.2 hours
Year 2021: 72.1 hours

=== Batch Selection Simulation ===
Batch composition:
  - Unfetched games: 800
  - Refresh games: 200
  - Total batch size: 1000

Sample of 5 unfetched games that would be included:
  - Game ID: 12345
  - Game ID: 23456
  - Game ID: 34567
  - Game ID: 45678
  - Game ID: 56789

Sample of 5 refresh games that would be included:
  - Game ID: 98765, Name: Newest Game, Year: 2025
  - Game ID: 87654, Name: Popular Game, Year: 2024
  - Game ID: 76543, Name: Another Game, Year: 2024
  - Game ID: 65432, Name: Older Game, Year: 2023
  - Game ID: 54321, Name: Classic Game, Year: 2022
```

### Full Implementation Test

The `test_refresh_implementation.py` script provides a more comprehensive test of the refresh strategy, including:

1. Setting up test data with controlled refresh timestamps
2. Verifying refresh candidate selection
3. Checking data consistency before and after refresh
4. Running a limited refresh operation
5. Verifying refresh tracking updates
6. Checking monitoring views

**Note:** This script makes actual changes to the database and should only be used in a test environment.

```bash
# Run in dev environment
uv run python tests/manual/test_refresh_implementation.py

# Run in test environment
uv run python tests/manual/test_refresh_implementation.py --environment test

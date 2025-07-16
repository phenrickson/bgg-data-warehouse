# BGG Data Refresh Strategy

This document describes the implementation of the exponential decay refresh strategy for keeping BGG game data current.

## Overview

The refresh strategy automatically updates game data based on how recently the games were published, with newer games refreshed more frequently than older games. This ensures that:

- Upcoming releases get the most frequent updates (every 3-4 days)
- Current year games stay very current (weekly)
- Recent games are refreshed regularly (bi-weekly to monthly)
- Older games are refreshed periodically (quarterly)

## Refresh Frequency Formula

The refresh interval is calculated using exponential decay:

```
Refresh Interval = base_interval * decay_factor^(current_year - year_published)
```

**Default Configuration:**
- `base_interval`: 7 days (for current year games)
- `decay_factor`: 2.0
- `max_interval`: 90 days (quarterly cap)
- `upcoming_interval`: 3 days (for future releases)

**Example refresh frequencies (for 2025):**
- **2026+ games (upcoming):** Every 3 days
- **2025 games (current):** Every 7 days  
- **2024 games:** Every 14 days
- **2023 games:** Every 28 days
- **2022 games:** Every 56 days
- **2021 and older:** Every 90 days (capped)

## Implementation Details

### Database Schema Changes

Three new columns were added to the `raw_responses` table:

```sql
ALTER TABLE raw_responses 
ADD COLUMN last_refresh_timestamp TIMESTAMP,
ADD COLUMN refresh_count INTEGER DEFAULT 0,
ADD COLUMN next_refresh_due TIMESTAMP;
```

### Core Components

#### 1. Enhanced `get_unfetched_ids()` Method

The method now returns both truly unfetched games and games due for refresh:

```python
def get_unfetched_ids(self, game_ids=None, include_refresh=True):
    # Get unfetched games (highest priority)
    unfetched_games = self._get_unfetched_games(game_ids)
    
    # Add refresh candidates if space available
    if include_refresh and len(unfetched_games) < batch_size:
        refresh_candidates = self._get_refresh_candidates(remaining_slots)
    
    return combined_results
```

#### 2. Refresh Candidate Selection

Games are selected for refresh using a complex SQL query that:

- Calculates refresh intervals based on publication year
- Identifies overdue games
- Prioritizes by year (newer first) and overdue duration

#### 3. Refresh Tracking

When games are refreshed, the system:

- Updates `last_refresh_timestamp` to current time
- Increments `refresh_count`
- Logs the refresh operation

### Configuration

Refresh settings are managed in `src/config.py`:

```python
def get_refresh_config():
    return {
        "enabled": True,
        "base_interval_days": 7,
        "upcoming_interval_days": 3,
        "decay_factor": 2.0,
        "max_interval_days": 90,
        "refresh_batch_size": 200
    }
```

## Monitoring

### BigQuery Views

Three monitoring views are created to track refresh performance:

#### 1. `monitoring.refresh_queue`
Shows refresh queue depth by publication year:

```sql
SELECT 
  year_published,
  total_games,
  games_due_for_refresh,
  avg_refresh_interval_days,
  avg_hours_overdue
FROM monitoring.refresh_queue
ORDER BY year_published DESC;
```

#### 2. `monitoring.refresh_activity`
Tracks daily refresh activity:

```sql
SELECT 
  fetch_date,
  year_published,
  responses_fetched,
  refresh_responses,
  initial_responses,
  refresh_ratio
FROM monitoring.refresh_activity
WHERE fetch_date >= CURRENT_DATE() - 7
ORDER BY fetch_date DESC;
```

#### 3. `monitoring.games_overdue_for_refresh`
Lists games that are overdue for refresh:

```sql
SELECT 
  game_id,
  primary_name,
  year_published,
  hours_overdue,
  overdue_category
FROM monitoring.games_overdue_for_refresh
WHERE overdue_category = 'Very Overdue (< 1 month)'
LIMIT 100;
```

## Deployment Steps

### 1. Schema Migration

Run the migration script to add refresh columns:

```bash
uv run python src/warehouse/migration_scripts/add_refresh_columns.py
```

### 2. Create Monitoring Views

Set up monitoring infrastructure:

```bash
uv run python src/warehouse/create_refresh_monitoring_views.py
```

### 3. Deploy Updated Code

The refresh logic is automatically enabled when the updated `fetch_responses.py` is deployed.

### 4. Verify Operation

Monitor the refresh queue and activity:

```sql
-- Check refresh queue status
SELECT * FROM monitoring.refresh_queue;

-- Monitor recent activity
SELECT * FROM monitoring.refresh_activity 
WHERE fetch_date >= CURRENT_DATE() - 1;
```

## Performance Considerations

### API Rate Limiting

The refresh strategy respects BGG API limits by:

- Mixing refresh games with new fetches in the same batches
- Using the same rate-limited API client
- Processing refreshes in chunks of 20 games per API call

### Database Performance

- Refresh candidate queries use indexes on `game_id` and `year_published`
- Batch updates minimize database round trips
- Monitoring views are optimized for common queries

### Resource Usage

- Refresh operations reuse existing infrastructure
- No additional API quota required
- Minimal additional storage (3 columns per game)

## Testing

Comprehensive tests verify:

- Exponential decay calculation accuracy
- Refresh candidate selection logic
- Priority handling (unfetched vs. refresh)
- Tracking updates
- Configuration loading

Run tests with:

```bash
uv run pytest tests/test_refresh_strategy.py -v
```

## Troubleshooting

### Common Issues

1. **No games being refreshed**
   - Check `refresh_config["enabled"]` is `True`
   - Verify `last_refresh_timestamp` is populated for existing games
   - Check monitoring views for queue depth

2. **Refresh tracking not updating**
   - Verify `is_refresh` flag is passed correctly
   - Check BigQuery permissions for UPDATE operations
   - Review logs for refresh tracking errors

3. **Unexpected refresh frequencies**
   - Verify exponential decay calculation in SQL
   - Check `year_published` data quality
   - Review configuration parameters

### Monitoring Queries

```sql
-- Check refresh configuration effectiveness
SELECT 
  year_published,
  COUNT(*) as total_games,
  AVG(refresh_count) as avg_refreshes,
  MAX(last_refresh_timestamp) as most_recent_refresh
FROM raw.raw_responses r
JOIN bgg_data.games g ON r.game_id = g.game_id
WHERE r.processed = TRUE
GROUP BY year_published
ORDER BY year_published DESC;

-- Identify stale games
SELECT 
  g.game_id,
  g.primary_name,
  g.year_published,
  r.last_refresh_timestamp,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), r.last_refresh_timestamp, DAY) as days_since_refresh
FROM raw.raw_responses r
JOIN bgg_data.games g ON r.game_id = g.game_id
WHERE r.processed = TRUE
  AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), r.last_refresh_timestamp, DAY) > 100
ORDER BY days_since_refresh DESC
LIMIT 50;
```

## Future Enhancements

### Adaptive Refresh Intervals

Consider implementing adaptive intervals based on:
- Rating volatility (games with changing ratings refresh more often)
- User activity (highly viewed games refresh more frequently)
- Data completeness (incomplete games refresh until complete)

### Smart Prioritization

Enhance prioritization with:
- BGG ranking changes
- Recent comment activity
- Kickstarter campaign status
- Publisher update notifications

### Performance Optimization

Potential optimizations:
- Materialized views for refresh candidates
- Partitioned tables by publication year
- Cached refresh interval calculations
- Parallel refresh processing

## Configuration Tuning

### Adjusting Refresh Frequencies

To modify refresh behavior, update the configuration:

```python
# More aggressive refresh for recent games
"base_interval_days": 5,        # Current year: every 5 days
"upcoming_interval_days": 2,    # Upcoming: every 2 days
"decay_factor": 1.5,           # Slower decay (more frequent refresh)

# More conservative refresh
"base_interval_days": 14,       # Current year: bi-weekly
"decay_factor": 3.0,           # Faster decay (less frequent refresh)
"max_interval_days": 180,      # Semi-annual cap
```

### Environment-Specific Settings

Different environments can have different refresh strategies:

```python
# Development: faster refresh for testing
if environment == "dev":
    config["base_interval_days"] = 1
    config["max_interval_days"] = 7

# Production: balanced approach
elif environment == "prod":
    config["base_interval_days"] = 7
    config["max_interval_days"] = 90
```

This refresh strategy ensures your BGG data warehouse stays current while efficiently managing API resources and database performance.

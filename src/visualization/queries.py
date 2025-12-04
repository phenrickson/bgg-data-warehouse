"""SQL queries for the monitoring dashboard."""

# New Games vs Refreshed Games Queries

NEW_GAMES_FETCHED_QUERY = """
WITH first_fetches AS (
    SELECT
        game_id,
        MIN(fetch_timestamp) as first_fetch_timestamp
    FROM `${project_id}.${raw_dataset}.fetched_responses`
    WHERE fetch_status = 'success'
    GROUP BY game_id
)
SELECT COUNT(DISTINCT game_id) as new_games_count
FROM first_fetches
WHERE first_fetch_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY);
"""

NEW_GAMES_PROCESSED_QUERY = """
WITH first_fetches AS (
    SELECT
        f.record_id,
        f.game_id,
        f.fetch_timestamp,
        ROW_NUMBER() OVER (PARTITION BY f.game_id ORDER BY f.fetch_timestamp) as fetch_order
    FROM `${project_id}.${raw_dataset}.fetched_responses` f
    WHERE f.fetch_status = 'success'
)
SELECT COUNT(DISTINCT f.game_id) as new_games_processed
FROM first_fetches f
INNER JOIN `${project_id}.${raw_dataset}.processed_responses` p
    ON f.record_id = p.record_id
WHERE f.fetch_order = 1
  AND f.fetch_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  AND p.process_status = 'success';
"""

REFRESHED_GAMES_FETCHED_QUERY = """
WITH fetch_counts AS (
    SELECT
        game_id,
        fetch_timestamp,
        ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY fetch_timestamp) as fetch_order
    FROM `${project_id}.${raw_dataset}.fetched_responses`
    WHERE fetch_status = 'success'
)
SELECT COUNT(DISTINCT game_id) as refreshed_games_count
FROM fetch_counts
WHERE fetch_order > 1
  AND fetch_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY);
"""

REFRESHED_GAMES_PROCESSED_QUERY = """
WITH fetch_counts AS (
    SELECT
        f.record_id,
        f.game_id,
        f.fetch_timestamp,
        ROW_NUMBER() OVER (PARTITION BY f.game_id ORDER BY f.fetch_timestamp) as fetch_order
    FROM `${project_id}.${raw_dataset}.fetched_responses` f
    WHERE f.fetch_status = 'success'
)
SELECT COUNT(DISTINCT f.game_id) as refreshed_games_processed
FROM fetch_counts f
INNER JOIN `${project_id}.${raw_dataset}.processed_responses` p
    ON f.record_id = p.record_id
WHERE f.fetch_order > 1
  AND f.fetch_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  AND p.process_status = 'success';
"""

# Daily trends for new vs refreshed games
DAILY_NEW_GAMES_FETCHED = """
WITH first_fetches AS (
    SELECT
        game_id,
        MIN(fetch_timestamp) as first_fetch_timestamp
    FROM `${project_id}.${raw_dataset}.fetched_responses`
    WHERE fetch_status = 'success'
    GROUP BY game_id
)
SELECT
    DATE(first_fetch_timestamp) as date,
    COUNT(DISTINCT game_id) as new_games_count
FROM first_fetches
WHERE first_fetch_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
GROUP BY DATE(first_fetch_timestamp)
ORDER BY date;
"""

DAILY_REFRESHED_GAMES_FETCHED = """
WITH refresh_fetches AS (
    SELECT
        f.game_id,
        f.fetch_timestamp
    FROM `${project_id}.${raw_dataset}.fetched_responses` f
    WHERE f.fetch_status = 'success'
      AND f.fetch_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
      AND EXISTS (
          SELECT 1
          FROM `${project_id}.${raw_dataset}.fetched_responses` f2
          WHERE f2.game_id = f.game_id
            AND f2.fetch_status = 'success'
            AND f2.fetch_timestamp < f.fetch_timestamp
      )
)
SELECT
    DATE(fetch_timestamp) as date,
    COUNT(*) as refreshed_games_count
FROM refresh_fetches
GROUP BY DATE(fetch_timestamp)
ORDER BY date;
"""

TOTAL_GAMES_QUERY = """
SELECT COUNT(DISTINCT game_id) as total_games
FROM `${project_id}.${dataset}.games_active`;
"""

GAMES_WITH_BAYESAVERAGE_QUERY = """
SELECT COUNT(DISTINCT game_id) as games_with_bayesaverage
FROM `${project_id}.${dataset}.games_active`
WHERE bayes_average IS NOT NULL 
  AND bayes_average > 0
  AND type = 'boardgame';
"""

UNPROCESSED_RESPONSES_QUERY = """
SELECT COUNT(*) as unprocessed_count
FROM `${project_id}.${raw_dataset}.fetched_responses` f
LEFT JOIN `${project_id}.${raw_dataset}.processed_responses` p ON f.record_id = p.record_id
WHERE p.record_id IS NULL
  AND f.fetch_status = 'success';
"""

# Combined entity counts query
ALL_ENTITY_COUNTS_QUERY = """
SELECT
  (SELECT COUNT(DISTINCT category_id) FROM `${project_id}.${dataset}.categories`) as category_count,
  (SELECT COUNT(DISTINCT mechanic_id) FROM `${project_id}.${dataset}.mechanics`) as mechanic_count,
  (SELECT COUNT(DISTINCT family_id) FROM `${project_id}.${dataset}.families`) as family_count,
  (SELECT COUNT(DISTINCT designer_id) FROM `${project_id}.${dataset}.designers`) as designer_count,
  (SELECT COUNT(DISTINCT artist_id) FROM `${project_id}.${dataset}.artists`) as artist_count,
  (SELECT COUNT(DISTINCT publisher_id) FROM `${project_id}.${dataset}.publishers`) as publisher_count
FROM (SELECT 1) -- Dummy table to make the query valid
"""

RECENT_FETCH_ACTIVITY = """
SELECT
    DATE(fetch_timestamp) as date,
    COUNT(*) as responses_fetched
FROM `${project_id}.${raw_dataset}.fetched_responses`
WHERE fetch_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  AND fetch_status = 'success'
GROUP BY date
ORDER BY date;
"""

PROCESSING_STATUS = """
WITH recent_fetches AS (
    SELECT f.*
    FROM `${project_id}.${raw_dataset}.fetched_responses` f
    WHERE f.fetch_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
      AND f.fetch_status = 'success'
)
SELECT
    COUNT(*) as total_responses,
    COUNTIF(p.record_id IS NOT NULL) as processed_count,
    COUNTIF(p.record_id IS NULL) as unprocessed_count,
    ROUND(COUNTIF(p.record_id IS NOT NULL) / COUNT(*) * 100, 2) as success_rate
FROM recent_fetches f
LEFT JOIN `${project_id}.${raw_dataset}.processed_responses` p ON f.record_id = p.record_id;
"""

RECENT_ERRORS = """
SELECT
    f.game_id,
    p.process_status as error,
    p.process_attempt,
    f.fetch_timestamp,
    p.process_timestamp,
    p.error_message
FROM `${project_id}.${raw_dataset}.processed_responses` p
INNER JOIN `${project_id}.${raw_dataset}.fetched_responses` f ON p.record_id = f.record_id
WHERE p.process_status IN ('failed', 'error')
ORDER BY p.process_timestamp DESC
LIMIT 10;
"""

LATEST_GAMES = """
WITH first_fetches AS (
    SELECT
        game_id,
        MIN(fetch_timestamp) as first_fetch_timestamp
    FROM `${project_id}.${raw_dataset}.fetched_responses`
    WHERE fetch_status = 'success'
    GROUP BY game_id
)
SELECT
    g.game_id,
    g.name,
    g.year_published,
    g.average_rating,
    g.users_rated,
    g.load_timestamp,
    f.first_fetch_timestamp
FROM `${project_id}.${dataset}.games_active` g
INNER JOIN first_fetches f ON g.game_id = f.game_id
ORDER BY f.first_fetch_timestamp DESC
LIMIT 100;
"""

DAILY_PROCESSING_COUNTS = """
SELECT
    DATE(process_timestamp) as date,
    COUNT(*) as processed_count
FROM `${project_id}.${raw_dataset}.processed_responses`
WHERE process_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  AND process_status = 'success'
GROUP BY date
ORDER BY date;
"""

PROCESSING_ERROR_TRENDS = """
SELECT
    DATE(process_timestamp) as date,
    COUNT(*) as error_count
FROM `${project_id}.${raw_dataset}.processed_responses`
WHERE process_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  AND process_status IN ('failed', 'error')
GROUP BY date
ORDER BY date;
"""

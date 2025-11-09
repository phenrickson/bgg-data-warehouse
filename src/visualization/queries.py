"""SQL queries for the monitoring dashboard."""

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
FROM `${project_id}.bgg_raw_dev.raw_responses`
WHERE processed = FALSE;
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
FROM `${project_id}.bgg_raw_dev.raw_responses`
WHERE fetch_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
GROUP BY date
ORDER BY date;
"""

PROCESSING_STATUS = """
SELECT
    COUNT(*) as total_responses,
    COUNTIF(processed) as processed_count,
    COUNTIF(NOT processed) as unprocessed_count,
    ROUND(COUNTIF(processed) / COUNT(*) * 100, 2) as success_rate
FROM `${project_id}.bgg_raw_dev.raw_responses`
WHERE fetch_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY);
"""

RECENT_ERRORS = """
SELECT
    game_id,
    process_status as error,
    process_attempt,
    fetch_timestamp,
    process_timestamp
FROM `${project_id}.bgg_raw_dev.raw_responses`
WHERE NOT processed
AND process_status IS NOT NULL
ORDER BY process_timestamp DESC
LIMIT 10;
"""

LATEST_GAMES = """
SELECT 
    g.game_id,
    g.name,
    g.year_published,
    g.average_rating,
    g.users_rated,
    g.load_timestamp
FROM `${project_id}.${dataset}.games_active` g
ORDER BY g.load_timestamp DESC
LIMIT 100;
"""

DAILY_PROCESSING_COUNTS = """
SELECT
    DATE(process_timestamp) as date,
    COUNT(*) as processed_count
FROM `${project_id}.bgg_raw_dev.raw_responses`
WHERE process_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
AND processed = TRUE
GROUP BY date
ORDER BY date;
"""

PROCESSING_ERROR_TRENDS = """
SELECT
    DATE(process_timestamp) as date,
    COUNT(*) as error_count
FROM `${project_id}.bgg_raw_dev.raw_responses`
WHERE process_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
AND NOT processed
AND process_status IS NOT NULL
GROUP BY date
ORDER BY date;
"""

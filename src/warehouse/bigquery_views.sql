-- BGG Data Warehouse Views
-- Most recent game data with per-game timestamp tracking
CREATE OR REPLACE VIEW `${project_id}.${dataset}.games_active` AS
WITH game_latest_timestamps AS (
  SELECT 
    game_id,
    MAX(load_timestamp) AS latest_game_timestamp
  FROM `${project_id}.${dataset}.games`
  GROUP BY game_id
),
latest_game_data AS (
  SELECT g.*
  FROM `${project_id}.${dataset}.games` g
  JOIN game_latest_timestamps lt 
    ON g.game_id = lt.game_id 
    AND g.load_timestamp = lt.latest_game_timestamp
)
SELECT DISTINCT
    game_id,
    type,
    primary_name AS name,
    year_published,
    average_rating,
    average_weight,
    bayes_average,
    users_rated,
    owned_count,
    trading_count,
    wanting_count,
    wishing_count,
    num_comments,
    num_weights,
    min_players,
    max_players,
    playing_time,
    min_playtime,
    max_playtime,
    min_age,
    description,
    thumbnail,
    image,
    load_timestamp
FROM latest_game_data;

-- Player count recommendations with total votes and filtering
CREATE OR REPLACE VIEW `${project_id}.${dataset}.player_count_recommendations` AS
WITH player_count_stats AS (
  SELECT 
    game_id,
    player_count,
    best_votes,
    recommended_votes,
    not_recommended_votes,
    best_votes + recommended_votes + not_recommended_votes AS total_votes,
    CASE 
        WHEN (best_votes + recommended_votes + not_recommended_votes) = 0 
        THEN 0 
        ELSE ROUND(best_votes / (best_votes + recommended_votes + not_recommended_votes) * 100, 2) 
    END as best_percentage,
    CASE 
        WHEN (best_votes + recommended_votes + not_recommended_votes) = 0 
        THEN 0 
        ELSE ROUND(recommended_votes / (best_votes + recommended_votes + not_recommended_votes) * 100, 2) 
    END as recommended_percentage
  FROM `${project_id}.${dataset}.player_counts`
)
SELECT 
    g.game_id,
    g.name,
    pc.player_count,
    pc.best_votes,
    pc.recommended_votes,
    pc.not_recommended_votes,
    pc.total_votes,
    pc.best_percentage,
    pc.recommended_percentage
FROM `${project_id}.${dataset}.games_active` g
JOIN player_count_stats pc ON g.game_id = pc.game_id
WHERE pc.best_votes IS NOT NULL 
  AND pc.recommended_votes IS NOT NULL 
  AND pc.not_recommended_votes IS NOT NULL
ORDER BY pc.total_votes DESC, pc.best_percentage DESC;

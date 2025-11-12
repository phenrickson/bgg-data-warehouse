-- BGG Data Warehouse Views
-- Note: The games_active view has been replaced by the games_active_table
-- which is refreshed daily via a scheduled query for better performance and lower query costs.
-- See src/warehouse/create_scheduled_tables.py for implementation details.

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
FROM `${project_id}.${dataset}.games_active_table` g
JOIN player_count_stats pc ON g.game_id = pc.game_id
WHERE pc.best_votes IS NOT NULL 
  AND pc.recommended_votes IS NOT NULL 
  AND pc.not_recommended_votes IS NOT NULL
ORDER BY pc.total_votes DESC, pc.best_percentage DESC;

-- sql/input_sample.sql  (5,000 random games, every response for each)
CREATE OR REPLACE VIEW `bgg-data-warehouse.scratch_parsing.responses_input` AS
SELECT r.*
FROM `bgg-data-warehouse.scratch_parsing.responses_json` r
WHERE r.game_id IN (
  SELECT game_id FROM `bgg-data-warehouse.core.games`
  GROUP BY game_id
  ORDER BY FARM_FINGERPRINT(CAST(game_id AS STRING))
  LIMIT 5000
)

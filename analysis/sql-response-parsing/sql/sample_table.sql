-- 5,000 random games, every response for each, materialised once so sample
-- iterations scan the sample rather than all of responses_json.
CREATE OR REPLACE TABLE `bgg-data-warehouse.scratch_parsing.responses_sample`
CLUSTER BY game_id, record_id AS
SELECT r.*
FROM `bgg-data-warehouse.scratch_parsing.responses_json` r
WHERE r.game_id IN (
  SELECT game_id FROM `bgg-data-warehouse.core.games`
  GROUP BY game_id
  ORDER BY FARM_FINGERPRINT(CAST(game_id AS STRING))
  LIMIT 5000
)

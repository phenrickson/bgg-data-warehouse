-- The exact response current core.* was built from, per game. core.games is
-- append-only and its load_timestamp is the source response's fetch_timestamp.
-- Processing status is only a tie-breaker: core can hold rows built from
-- responses recorded as 'failed' (see FINDINGS.md, game 1).
CREATE OR REPLACE TABLE `bgg-data-warehouse.scratch_parsing.core_inputs` AS
WITH latest AS (
  SELECT game_id, MAX(load_timestamp) AS fetch_timestamp
  FROM `bgg-data-warehouse.core.games`
  GROUP BY game_id
)
SELECT l.game_id, r.record_id, l.fetch_timestamp
FROM latest l
JOIN `bgg-data-warehouse.raw.raw_responses` r
  ON r.game_id = l.game_id AND r.fetch_timestamp = l.fetch_timestamp
LEFT JOIN `bgg-data-warehouse.raw.processed_responses` p
  ON p.record_id = r.record_id AND p.process_status = 'success'
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY l.game_id ORDER BY p.record_id IS NOT NULL DESC, r.record_id DESC) = 1

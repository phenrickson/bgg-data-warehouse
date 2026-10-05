-- Latest core.games row per game, materialised once so compares don't rescan core.games.
CREATE OR REPLACE TABLE `bgg-data-warehouse.scratch_parsing.core_games_latest`
CLUSTER BY game_id AS
SELECT * EXCEPT(rn) FROM (
  SELECT g.*, ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY load_timestamp DESC) rn
  FROM `bgg-data-warehouse.core.games` g
) WHERE rn = 1

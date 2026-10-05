-- Today's snapshot (core_inputs records) vs the latest core.games row per game.
WITH core_latest AS (
  SELECT * EXCEPT(rn) FROM (
    SELECT g.*, ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY load_timestamp DESC) rn
    FROM `bgg-data-warehouse.core.games` g
    WHERE game_id IN (SELECT game_id FROM `bgg-data-warehouse.scratch_parsing.parsed_games`)
  ) WHERE rn = 1
),
parsed AS (
  SELECT p.* EXCEPT(record_id, item_type)
  FROM `bgg-data-warehouse.scratch_parsing.parsed_games` p
  JOIN `bgg-data-warehouse.scratch_parsing.core_inputs` c USING (record_id)
),
only_core AS (SELECT * FROM core_latest EXCEPT DISTINCT SELECT * FROM parsed),
only_parsed AS (SELECT * FROM parsed EXCEPT DISTINCT SELECT * FROM core_latest)
SELECT 'games' AS tbl,
  (SELECT COUNT(*) FROM core_latest) AS core_rows,
  (SELECT COUNT(*) FROM parsed) AS parsed_rows,
  (SELECT COUNT(*) FROM only_core) AS only_in_core,
  (SELECT COUNT(*) FROM only_parsed) AS only_in_parsed,
  ARRAY(SELECT AS STRUCT * FROM only_core ORDER BY game_id LIMIT 20) AS sample_core,
  ARRAY(SELECT AS STRUCT * FROM only_parsed ORDER BY game_id LIMIT 20) AS sample_parsed

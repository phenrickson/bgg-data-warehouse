-- sql/compare_suggested_ages.sql
WITH games AS (SELECT game_id, record_id FROM `bgg-data-warehouse.scratch_parsing.core_inputs`
               WHERE game_id IN (SELECT game_id FROM `bgg-data-warehouse.scratch_parsing.parsed_items`)),
core_rows AS (SELECT game_id, age, votes
              FROM `bgg-data-warehouse.core.suggested_ages` WHERE game_id IN (SELECT game_id FROM games)),
parsed AS (SELECT p.game_id, p.age, p.votes
           FROM `bgg-data-warehouse.scratch_parsing.parsed_suggested_ages` p JOIN games g USING (record_id)),
only_core AS (SELECT * FROM core_rows EXCEPT DISTINCT SELECT * FROM parsed),
only_parsed AS (SELECT * FROM parsed EXCEPT DISTINCT SELECT * FROM core_rows)
SELECT 'suggested_ages' AS tbl,
  (SELECT COUNT(*) FROM core_rows) AS core_rows, (SELECT COUNT(*) FROM parsed) AS parsed_rows,
  (SELECT COUNT(*) FROM only_core) AS only_in_core, (SELECT COUNT(*) FROM only_parsed) AS only_in_parsed,
  ARRAY(SELECT AS STRUCT * FROM only_core ORDER BY game_id LIMIT 20) AS sample_core,
  ARRAY(SELECT AS STRUCT * FROM only_parsed ORDER BY game_id LIMIT 20) AS sample_parsed

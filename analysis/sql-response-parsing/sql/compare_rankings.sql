WITH games AS (SELECT game_id, record_id, fetch_timestamp FROM `bgg-data-warehouse.scratch_parsing.core_inputs`
               WHERE game_id IN (SELECT game_id FROM `bgg-data-warehouse.scratch_parsing.parsed_items`)),
core_rows AS (SELECT r.game_id, r.ranking_type, r.ranking_name, r.friendly_name, r.value, r.bayes_average, r.load_timestamp
              FROM `bgg-data-warehouse.core.rankings` r
              JOIN games g ON g.game_id = r.game_id AND g.fetch_timestamp = r.load_timestamp),
parsed AS (SELECT p.game_id, p.ranking_type, p.ranking_name, p.friendly_name, p.value, p.bayes_average, p.load_timestamp
           FROM `bgg-data-warehouse.scratch_parsing.parsed_rankings` p JOIN games g USING (record_id)),
only_core AS (SELECT * FROM core_rows EXCEPT DISTINCT SELECT * FROM parsed),
only_parsed AS (SELECT * FROM parsed EXCEPT DISTINCT SELECT * FROM core_rows)
SELECT 'rankings' AS tbl,
  (SELECT COUNT(*) FROM core_rows) AS core_rows, (SELECT COUNT(*) FROM parsed) AS parsed_rows,
  (SELECT COUNT(*) FROM only_core) AS only_in_core, (SELECT COUNT(*) FROM only_parsed) AS only_in_parsed,
  ARRAY(SELECT AS STRUCT * FROM only_core ORDER BY game_id LIMIT 20) AS sample_core,
  ARRAY(SELECT AS STRUCT * FROM only_parsed ORDER BY game_id LIMIT 20) AS sample_parsed

-- games-table mismatches per edge tag and per fetch quarter. A response mismatches
-- when its parsed games row differs in any column from the latest core.games row.
WITH parsed AS (
  SELECT p.record_id, TO_JSON_STRING((SELECT AS STRUCT p.* EXCEPT(record_id, item_type))) AS row_json
  FROM `bgg-data-warehouse.scratch_parsing.parsed_games` p
  JOIN `bgg-data-warehouse.scratch_parsing.core_inputs` c USING (record_id)),
core_json AS (
  SELECT c.record_id, TO_JSON_STRING(g) AS row_json
  FROM `bgg-data-warehouse.scratch_parsing.core_games_latest` g
  JOIN `bgg-data-warehouse.scratch_parsing.core_inputs` c USING (game_id)),
mismatched AS (
  SELECT p.record_id FROM parsed p
  LEFT JOIN core_json k ON k.record_id = p.record_id AND k.row_json = p.row_json
  WHERE k.record_id IS NULL),
tagged AS (
  SELECT e.record_id, e.fetch_quarter, e.tags
  FROM `bgg-data-warehouse.scratch_parsing.edge_tags` e
  JOIN `bgg-data-warehouse.scratch_parsing.core_inputs` c USING (record_id))
SELECT tag AS bucket, COUNT(*) AS responses, COUNTIF(m.record_id IS NOT NULL) AS games_mismatched
FROM tagged t, UNNEST(t.tags) tag
LEFT JOIN mismatched m ON m.record_id = t.record_id
GROUP BY tag
UNION ALL
SELECT CONCAT('era ', t.fetch_quarter), COUNT(*), COUNTIF(m.record_id IS NOT NULL)
FROM tagged t
LEFT JOIN mismatched m ON m.record_id = t.record_id
GROUP BY t.fetch_quarter
ORDER BY bucket

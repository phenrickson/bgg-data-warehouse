-- Why is game 1 missing from core_inputs?
SELECT 'core.games' AS src, CAST(load_timestamp AS STRING) AS ts, NULL AS record_id, NULL AS status
FROM `bgg-data-warehouse.core.games` WHERE game_id = 1
UNION ALL
SELECT 'raw_responses', CAST(r.fetch_timestamp AS STRING), r.record_id,
       CONCAT(IFNULL(f.fetch_status, 'no fetch row'), ' / ', IFNULL(p.process_status, 'not processed'))
FROM `bgg-data-warehouse.raw.raw_responses` r
LEFT JOIN `bgg-data-warehouse.raw.fetched_responses` f USING (record_id)
LEFT JOIN `bgg-data-warehouse.raw.processed_responses` p USING (record_id)
WHERE r.game_id = 1
ORDER BY ts

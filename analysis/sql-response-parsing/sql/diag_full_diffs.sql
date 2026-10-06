-- Games behind the remaining full-population differences: load vs fetch history.
WITH ids AS (SELECT id FROM UNNEST([461749, 448639, 448655, 448724, 296100, 420498]) id)
SELECT game_id, 'core.games' AS src, CAST(load_timestamp AS STRING) AS ts, CAST(NULL AS STRING) AS record_id, CAST(NULL AS STRING) AS status
FROM `bgg-data-warehouse.core.games` WHERE game_id IN (SELECT id FROM ids)
UNION ALL
SELECT r.game_id, 'raw_responses', CAST(r.fetch_timestamp AS STRING), r.record_id,
       CONCAT(IFNULL(f.fetch_status, 'no fetch row'), ' / ', IFNULL(STRING_AGG(p.process_status, ','), 'not processed'))
FROM `bgg-data-warehouse.raw.raw_responses` r
LEFT JOIN `bgg-data-warehouse.raw.fetched_responses` f USING (record_id)
LEFT JOIN `bgg-data-warehouse.raw.processed_responses` p USING (record_id)
WHERE r.game_id IN (SELECT id FROM ids)
GROUP BY r.game_id, r.fetch_timestamp, r.record_id, f.fetch_status
ORDER BY game_id, ts, src

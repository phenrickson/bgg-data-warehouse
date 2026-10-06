SELECT 'link @id' AS field, COUNT(*) AS n, ARRAY_AGG(STRUCT(record_id, id_raw) LIMIT 10) AS sample
FROM `bgg-data-warehouse.scratch_parsing.parsed_links`
WHERE id_raw IS NOT NULL AND SAFE_CAST(id_raw AS INT64) IS NULL
UNION ALL
SELECT 'language @level', COUNT(*), ARRAY_AGG(STRUCT(record_id, level_raw AS id_raw) LIMIT 10)
FROM `bgg-data-warehouse.scratch_parsing.parsed_language_dependence`
WHERE level_raw IS NOT NULL AND SAFE_CAST(level_raw AS INT64) IS NULL
UNION ALL
SELECT 'language @numvotes', COUNT(*), ARRAY_AGG(STRUCT(record_id, votes_raw AS id_raw) LIMIT 10)
FROM `bgg-data-warehouse.scratch_parsing.parsed_language_dependence`
WHERE votes_raw IS NOT NULL AND SAFE_CAST(votes_raw AS INT64) IS NULL
UNION ALL
SELECT 'age @numvotes', COUNT(*), ARRAY_AGG(STRUCT(record_id, votes_raw AS id_raw) LIMIT 10)
FROM `bgg-data-warehouse.scratch_parsing.parsed_suggested_ages`
WHERE votes_raw IS NOT NULL AND SAFE_CAST(votes_raw AS INT64) IS NULL

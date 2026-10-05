SELECT convert_status, JSON_TYPE(response_json) AS json_type, COUNT(*) AS n
FROM `bgg-data-warehouse.scratch_parsing.responses_json`
GROUP BY 1, 2

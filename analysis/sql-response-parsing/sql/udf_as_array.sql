-- sql/udf_as_array.sql
-- xmltodict gives one child as an object and several as a list; null/missing -> [].
CREATE OR REPLACE FUNCTION `bgg-data-warehouse.scratch_parsing.as_array`(j JSON)
RETURNS ARRAY<JSON> AS (
  CASE
    WHEN j IS NULL OR JSON_TYPE(j) = 'null' THEN []
    WHEN JSON_TYPE(j) = 'array' THEN JSON_QUERY_ARRAY(j)
    ELSE [j]
  END
)

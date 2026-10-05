-- sql/udf_safe_float.sql
-- processor._safe_float on a string: float(), failure -> 0.0.
CREATE OR REPLACE FUNCTION `bgg-data-warehouse.scratch_parsing.safe_float`(s STRING)
RETURNS FLOAT64 AS (
  IFNULL(SAFE_CAST(TRIM(s) AS FLOAT64), 0.0)
)

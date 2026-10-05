-- sql/udf_safe_int.sql
-- processor._safe_int on a string: int(), negatives -> 0, failure -> 0.
CREATE OR REPLACE FUNCTION `bgg-data-warehouse.scratch_parsing.safe_int`(s STRING)
RETURNS INT64 AS (
  IFNULL(GREATEST(SAFE_CAST(TRIM(s) AS INT64), 0), 0)
)

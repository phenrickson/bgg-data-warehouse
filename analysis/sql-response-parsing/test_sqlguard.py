# analysis/sql-response-parsing/test_sqlguard.py
import pytest

from sqlguard import check_sql

OK = """
CREATE OR REPLACE TABLE `bgg-data-warehouse.scratch_parsing.parsed_games` AS
SELECT * FROM `bgg-data-warehouse.raw.raw_responses`
"""


def test_scratch_create_reading_raw_is_allowed():
    check_sql(OK)


def test_create_outside_scratch_is_refused():
    with pytest.raises(ValueError, match="outside scratch_parsing"):
        check_sql("CREATE OR REPLACE TABLE `bgg-data-warehouse.core.games` AS SELECT 1")


def test_unqualified_create_is_refused():
    with pytest.raises(ValueError, match="outside scratch_parsing"):
        check_sql("CREATE TABLE games AS SELECT 1")


@pytest.mark.parametrize("stmt", ["INSERT INTO", "UPDATE ", "DELETE FROM", "MERGE ", "TRUNCATE TABLE", "DROP TABLE", "ALTER TABLE"])
def test_dml_and_drops_are_refused(stmt):
    with pytest.raises(ValueError, match="not allowed"):
        check_sql(f"{stmt} `bgg-data-warehouse.scratch_parsing.x` SELECT 1")


def test_keywords_inside_comments_and_strings_do_not_count():
    check_sql(OK + "\n-- we never DELETE FROM anything\nSELECT 'MERGE ' AS s")


def test_functions_and_views_in_scratch_are_allowed():
    check_sql("CREATE OR REPLACE FUNCTION `bgg-data-warehouse.scratch_parsing.as_array`(j JSON) AS (j)")
    check_sql("CREATE OR REPLACE VIEW `bgg-data-warehouse.scratch_parsing.responses_input` AS SELECT 1")

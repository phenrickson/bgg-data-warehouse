# analysis/sql-response-parsing/test_verify_conversion.py
from convert import content_hash
from verify_conversion import check_row


def test_dict_matching_hash_passes():
    parsed = {"items": {"item": {"@id": "13"}}}
    assert check_row(parsed, content_hash(parsed))


def test_json_string_value_is_parsed_first():
    assert check_row('{"a": "1"}', content_hash({"a": "1"}))


def test_changed_value_fails():
    assert not check_row({"a": "2"}, content_hash({"a": "1"}))


def test_null_with_no_hash_passes_and_mismatched_null_fails():
    assert check_row(None, None)
    assert not check_row(None, content_hash({"a": "1"}))

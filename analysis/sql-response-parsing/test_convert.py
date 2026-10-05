# analysis/sql-response-parsing/test_convert.py
import pytest

from convert import assert_scratch, content_hash, convert_response

REPR = "{'items': {'item': {'@id': '13', 'name': {'@type': 'primary', '@value': \"Catan's\"}, 'description': None, 'flag': True}}}"


def test_repr_converts_and_round_trips():
    status, parsed = convert_response(REPR)
    assert status == "repr"
    assert parsed["items"]["item"]["name"]["@value"] == "Catan's"
    assert parsed["items"]["item"]["description"] is None
    assert parsed["items"]["item"]["flag"] is True


def test_json_input_is_accepted_as_json():
    status, parsed = convert_response('{"items": {"item": {"@id": "13"}}}')
    assert (status, parsed) == ("json", {"items": {"item": {"@id": "13"}}})


@pytest.mark.parametrize("blank", [None, "", "   \n"])
def test_blank_is_empty(blank):
    assert convert_response(blank) == ("empty", None)


def test_garbage_is_unparseable():
    assert convert_response("{'items': ") == ("unparseable", None)


def test_non_dict_is_unparseable():
    assert convert_response("[1, 2]") == ("unparseable", None)


def test_lossy_round_trip_raises():
    # A tuple becomes a JSON list and would not compare equal on the way back
    with pytest.raises(ValueError, match="round trip"):
        convert_response("{'items': ('a', 'b')}")


def test_unicode_and_entities_survive():
    status, parsed = convert_response("{'d': 'Café &amp; ☃ &#10;'}")
    assert parsed == {"d": "Café &amp; ☃ &#10;"}


def test_content_hash_ignores_key_order():
    assert content_hash({"a": "1", "b": ["x"]}) == content_hash({"b": ["x"], "a": "1"})
    assert content_hash({"a": "1"}) != content_hash({"a": "2"})


def test_assert_scratch():
    assert_scratch("bgg-data-warehouse.scratch_parsing.responses_json")
    with pytest.raises(ValueError):
        assert_scratch("bgg-data-warehouse.raw.raw_responses")

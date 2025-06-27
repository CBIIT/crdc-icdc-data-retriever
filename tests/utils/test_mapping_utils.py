import pytest

from utils.mapping_utils import normalize_metadata_groups, extract_first_valid_match


@pytest.mark.parametrize(
    "input_data, expected",
    [
        ([], []),
        ([{"a": 1}, {"b": 2}], [[{"a": 1}, {"b": 2}]]),
        (
            [[{"a": 1}], [{"b": 2}]],
            [[{"a": 1}], [{"b": 2}]],
        ),
        (
            [{"a": 1}],
            [[{"a": 1}]],
        ),
    ],
)
def test_normalize_metadata_groups(input_data, expected):
    assert normalize_metadata_groups(input_data) == expected


@pytest.mark.parametrize(
    "metadata_group, match_key, expected",
    [
        ([{"x": ""}, {"x": "value"}, {"x": "ignored"}], "x", "value"),
        ([{"x": ""}, {"y": "none"}, {}], "x", ""),
        ([{"id": "123"}, {"id": "456"}], "id", "123"),
        ([{"id": None}, {"id": "OK"}], "id", "OK"),
        ([], "id", ""),
        ([{"id": 0}, {"id": "nonzero"}], "id", "nonzero"),
    ],
)
def test_extract_first_valid_match(metadata_group, match_key, expected):
    assert extract_first_valid_match(metadata_group, match_key) == expected

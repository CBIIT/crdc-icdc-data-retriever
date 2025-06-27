import pytest

from utils.match_utils import is_fuzzy_match


@pytest.mark.parametrize(
    "str1, str2, threshold, expected",
    [
        ("GLIOMA01", "icdc_glioma", 75, True),
        ("GLIOMA01", "canine", 75, False),
        ("cat", "dog", 80, False),
        ("canine", "pediatric", 75, False),
    ],
)
def test_is_fuzzy_match_cases(str1, str2, threshold, expected):
    assert is_fuzzy_match(str1, str2, threshold) is expected


def test_exact_match():
    assert is_fuzzy_match("test", "test") is True


@pytest.mark.parametrize(
    "str1, str2, threshold, expected",
    [
        ("", "icdc_glioma", 75, False),
        ("GLIOMA01", "", 75, False),
        ("", "", 80, False),
    ],
)
def test_empty_string_match(str1, str2, threshold, expected):
    assert is_fuzzy_match(str1, str2, threshold) is expected

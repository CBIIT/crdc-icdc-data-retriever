import pytest

from utils.post_processor_utils import deep_merge_additive


@pytest.mark.parametrize(
    "base, override, expected",
    [
        (
            {"a": {"count": 1, "items": [1, 2]}},
            {"a": {"count": 4, "items": [2, 3]}},
            {"a": {"count": 5, "items": [1, 2, 3]}},
        ),
        (
            {"stats": {"views": 10}, "tags": ["A"]},
            {"stats": {"views": 15}, "tags": ["B"]},
            {"stats": {"views": 25}, "tags": ["A", "B"]},
        ),
        (
            {"nested": {"level1": {"level2": {"count": 2}}}},
            {"nested": {"level1": {"level2": {"count": 3}}}},
            {"nested": {"level1": {"level2": {"count": 5}}}},
        ),
        (
            {"x": {"y": 1}},
            {"x": {"z": 2}},
            {"x": {"y": 1, "z": 2}},
        ),
    ],
)
def test_deep_merge_additive(base, override, expected):
    result = deep_merge_additive(base, override)
    assert result == expected

import pytest

from utils.notification_utils import build_notification_message


@pytest.mark.parametrize(
    "success, mappings, project, expected_contains_success",
    [
        (True, [], "ICDC", True),
        (False, [], "ICDC", False),
    ],
)
def test_build_notification_message(
    success, mappings, project, expected_contains_success
):
    message = build_notification_message(success, mappings, project)
    assert ("Success" in message) == expected_contains_success

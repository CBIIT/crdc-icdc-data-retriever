def normalize_metadata_groups(matched_source_data: list) -> list:
    """Normalizes matched source data into a consistent list-of-lists format.

    Args:
        matched_source_data (list): Source metadata (either a list of dicts
        or a list of a list of dicts).

    Returns:
        list: A list where each item is a list of metadata dicts.
    """

    if not matched_source_data:
        return []
    if isinstance(matched_source_data[0], dict):
        return [matched_source_data]
    return matched_source_data


def extract_first_valid_match(metadata_group: list, match_key: str) -> str:
    """Extracts first non-empty match_key value from a metadata group.

    Args:
        metadata_group (list): List of metadata dicts.
        match_key (str): Key to look up in each metadata dict.

    Returns:
        str: The first non-empty value found for match_key, or an empty string.
    """

    for metadata in metadata_group:
        value = metadata.get(match_key)
        if value:
            return value
    return ""

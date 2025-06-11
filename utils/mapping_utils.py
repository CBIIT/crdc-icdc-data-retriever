def normalize_metadata_groups(matched_source_data):
    if not matched_source_data:
        return []
    if isinstance(matched_source_data[0], dict):
        return [matched_source_data]
    return matched_source_data


def extract_first_valid_match(metadata_group, match_key):
    for metadata in metadata_group:
        value = metadata.get(match_key)
        if value:
            return value
    return ""

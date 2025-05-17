from utils.match_utils import is_fuzzy_match


def map_matches_to_study(
    study,
    matched_source_data,
    dataset_base_url,
    dataset_base_url_param,
    repository_name,
    match_key,
    post_processor=None,
) -> list:
    crdc_links = []
    study_name = study.get("clinical_study_designation", "")

    for metadata in matched_source_data:
        candidate = metadata.get(match_key, "")
        if not is_fuzzy_match(study_name, candidate):
            continue

        if post_processor:
            metadata = post_processor(metadata)

        match_id = metadata.get(match_key)
        url = dataset_base_url.format(**{dataset_base_url_param: match_id})
        crdc_links.append(
            {"repository": repository_name, "url": url, "metadata": metadata}
        )

    return crdc_links

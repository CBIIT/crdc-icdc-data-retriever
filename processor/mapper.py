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

    for metadata in matched_source_data:
        if post_processor:
            metadata = post_processor(metadata)

        match_id = metadata.get(match_key)
        url = dataset_base_url.format(**{dataset_base_url_param: match_id})

        crdc_links.append(
            {"repository": repository_name, "url": url, "metadata": metadata}
        )

    return crdc_links

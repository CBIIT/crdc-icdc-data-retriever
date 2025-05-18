from utils.match_utils import is_fuzzy_match


def map_matches_to_entity(
    entity,
    matched_source_data,
    dataset_base_url,
    dataset_base_url_param,
    repository_name,
    match_key,
    post_processor=None,
) -> list:
    crdc_links = []
    entity_name = entity.get("clinical_study_designation", "")

    for metadata in matched_source_data:
        candidate = metadata.get(match_key, "")
        # log warning if match key not present in metadata
        if not is_fuzzy_match(entity_name, candidate):
            continue

        if post_processor:
            metadata = post_processor(metadata)

        match_id = metadata.get(match_key)
        url = dataset_base_url.format(**{dataset_base_url_param: match_id})
        crdc_links.append(
            {"repository": repository_name, "url": url, "metadata": metadata}
        )

    return crdc_links


def collect_mappings(
    entities,
    matched_source_data,
    dataset_base_url,
    dataset_base_url_param,
    repository_name,
    match_key,
    post_processor=None,
) -> list:
    crdc_mappings = []

    for entity in entities:
        mappings = map_matches_to_entity(
            entity=entity,
            matched_source_data=matched_source_data,
            dataset_base_url=dataset_base_url,
            dataset_base_url_param=dataset_base_url_param,
            repository_name=repository_name,
            match_key=match_key,
            post_processor=post_processor,
        )

        if mappings:
            crdc_mappings.append(
                {
                    "CRDCLinks": mappings,
                    "number_of_crdc_nodes": entity.get(
                        "numberOfCRDCNodes", len({i["repository"] for i in mappings})
                    ),
                    "number_of_collections": entity.get(
                        "numberOfImageCollections", len(mappings)
                    ),
                    "entity_id": entity.get("clinical_study_designation"),
                }
            )

    return crdc_mappings

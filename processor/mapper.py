from utils.match_utils import is_fuzzy_match


def map_matches_to_entity(
    entity,
    source_config,
    matched_source_data,
    dataset_base_url,
    dataset_base_url_param,
    repository_name,
    match_key,
    post_processor=None,
) -> list:
    crdc_links = []
    entity_id_key = source_config["output"]["entity_id_key"]
    entity_id = entity.get(entity_id_key, "")

    for metadata in matched_source_data:
        candidate = metadata.get(match_key, "")
        # log warning if match key not present in metadata
        if not is_fuzzy_match(entity_id, candidate):
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
    source_config,
    matched_source_data,
    dataset_base_url,
    dataset_base_url_param,
    repository_name,
    match_key,
    post_processor=None,
) -> list:
    crdc_mappings = []
    entity_id_key = source_config["output"]["entity_id_key"]

    for entity in entities:
        mappings = map_matches_to_entity(
            entity=entity,
            source_config=source_config,
            matched_source_data=matched_source_data,
            dataset_base_url=dataset_base_url,
            dataset_base_url_param=dataset_base_url_param,
            repository_name=repository_name,
            match_key=match_key,
            post_processor=post_processor,
        )

        if mappings:
            entity_id = entity.get(entity_id_key)
            crdc_mappings.append(
                {
                    "CRDCLinks": mappings,
                    "number_of_crdc_nodes": len({i["repository"] for i in mappings}),
                    "number_of_collections": len(mappings),
                    "entity_id": entity_id,
                }
            )

    return crdc_mappings

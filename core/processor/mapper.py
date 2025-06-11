import logging

from core.processor.post_processor_registry import apply_post_processor
from utils.mapping_utils import normalize_metadata_groups, extract_first_valid_match
from utils.match_utils import is_fuzzy_match

logger = logging.getLogger(__name__)


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
    entity_id_key = source_config["entity_id_key"]
    entity_id = entity.get(entity_id_key, "")

    for metadata in normalize_metadata_groups(matched_source_data):
        candidate = extract_first_valid_match(
            metadata_group=metadata, match_key=match_key
        )
        if not candidate:
            logger.warning(f"Match key '{match_key}' not present in metadata.")
            continue

        if not is_fuzzy_match(entity_id, candidate):
            logger.debug(f"Entity '{entity_id}' did not match candidate '{candidate}'")
            continue

        match_id = extract_first_valid_match(
            metadata_group=metadata, match_key=match_key
        )
        context = {
            "entity": entity,
            "collection_id": match_id,
            "entity_id_key": entity_id_key,
        }

        if post_processor:
            metadata = apply_post_processor(post_processor, metadata, **context)
            logger.info(f"Applied post-processor: {post_processor.__name__}")

        url = dataset_base_url.format(**{dataset_base_url_param: match_id})
        crdc_links.append(
            {"repository": repository_name, "url": url, "metadata": metadata}
        )

    logger.debug(f"{len(crdc_links)} links mapped for entity '{entity_id}'")

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
    entity_id_key = source_config["entity_id_key"]

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
                    "entity_id": entity_id,
                    "CRDCLinks": mappings,
                }
            )
            logger.info(f"Mapped {len(mappings)} collections to entity '{entity_id}'")
        else:
            logger.debug(
                f"No mappings found for entity '{entity.get(entity_id_key, "<unknown>")}'"
            )

    return crdc_mappings

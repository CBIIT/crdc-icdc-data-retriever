import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from core.fetcher import fetch_from_source
from core.processor.mapper import collect_mappings
from core.processor.post_processor_registry import get_post_processor

logger = logging.getLogger(__name__)


def run_dispatcher(config, parallel: bool = False):
    logger.info("Starting dispatcher run...")
    entity_source_name = config["entity_source"]
    sources = config["sources"]

    logger.info("Fetching all source data...")
    fetched_data = fetch_all_parallel(sources) if parallel else fetch_all(sources)
    logger.info("Fetching complete - beginning entity matching...")

    entities = fetched_data[entity_source_name]
    if not entities:
        logger.error(f"No data found for entity source: {entity_source_name}")
        return []

    return match_all(entities, sources, fetched_data, entity_source_name)


def fetch_all(sources: list) -> dict:
    logger.info(f"Fetching from {len(sources)} sources sequentially...")
    results = {}
    for source in sources:
        name = source.get("name", "<unknown>")
        try:
            results[name] = fetch_from_source(source)
            logger.info(f"Fetched data from source: {name}")
        except Exception as e:
            logger.error(f"Failed to fetch from source '{name}': {e}")
            results[name] = None
    return results


def fetch_all_parallel(sources: list, max_workers: int = 8) -> dict:
    logger.info(
        f"Fetching from {len(sources)} sources in parallel using {max_workers} workers..."
    )
    results = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_source = {
            executor.submit(fetch_from_source, source): source for source in sources
        }

    for future in as_completed(future_to_source):
        source = future_to_source[future]
        name = source.get("name", "<unknown>")
        try:
            results[name] = future.result()
            logger.info(f"Fetched data from source: {name}")
        except Exception as e:
            logger.error(f"Parallel fetch failed for source '{name}': {e}")
            results[name] = None

    return results


def match_all(entities, sources, fetched_data, entity_source_name) -> list:
    logger.info("Beginning mapping of source data to entities...")
    results = []

    for source in sources:
        name = source.get("name")
        if name == entity_source_name:
            continue

        logger.debug(f"Processing source for mapping: {name}")
        source_data = fetched_data[source["name"]]
        if source_data is None:
            logger.warning(f"No data to map for source: {name}")
            continue

        post_processor = get_post_processor(source.get("post_processor"))

        mappings = collect_mappings(
            entities=entities,
            source_config=source,
            matched_source_data=source_data,
            dataset_base_url=source["dataset_base_url"],
            dataset_base_url_param=source["dataset_base_url_param"],
            repository_name=source["name"],
            match_key=source["match_key"],
            post_processor=post_processor,
        )

        if mappings:
            logger.info(f"{len(mappings)} mappings created from source: {name}")
            results.extend(mappings)
        else:
            logger.info(f"No mappings created from source: {name}")

    logger.info(f"Total mappings created: {len(results)}")

    return results

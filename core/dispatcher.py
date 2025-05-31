from concurrent.futures import ThreadPoolExecutor, as_completed

from core.fetcher import fetch_from_source
from processor.mapper import collect_mappings
from processor.post_processor_registry import get_post_processor


def run_dispatcher(config):
    entity_source_name = config["entity_source_name"]
    sources = config["sources"]

    fetched_data = fetch_all(sources)
    entities = fetched_data[entity_source_name]
    return match_all(entities, sources, fetched_data, entity_source_name)


def fetch_all(sources: list) -> dict:
    return {source["name"]: fetch_from_source(source) for source in sources}


def fetch_all_parallel(sources: list, max_workers: int = 8) -> dict:
    results = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_source = {
            executor.submit(fetch_from_source, source): source for source in sources
        }

    for future in as_completed(future_to_source):
        source = future_to_source[future]
        name = source.get("name", "")
        try:
            data = future.result()
            results[name] = data
        except Exception as e:
            results[name] = None

    return results


def match_all(entities, sources, fetched_data, entity_source_name) -> list:
    results = []

    for source in sources:
        if source["name"] == entity_source_name:
            continue

        source_data = fetched_data[source["name"]]
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
            results.extend(mappings)

    return results

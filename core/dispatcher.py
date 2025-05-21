from fetcher import fetch_from_source
from processor.mapper import collect_mappings
from processor.post_processor_registry import get_post_processor


def run_dispatcher(config):
    entity_source_name = config["entity_source"]
    sources = config["sources"]

    # fetch phase
    fetched_data = {}
    for source in sources:
        source_name = source["name"]
        fetched_data[source_name] = fetch_from_source(config)

    # entity_source = next(src for src in sources if src["name"] == entity_source_name)
    entities = fetched_data[entity_source_name]

    # match phase
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

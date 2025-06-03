import logging
import requests


logger = logging.getLogger(__name__)


def fetch_from_source(source):
    source_name = source.get("name", "<unknown>")
    source_type = source.get("type", "<unknown>")

    logger.info(f"Starting fetch from source: {source_name} (type: {source_type})")

    try:
        if source_type == "rest":
            if "discovery" in source:
                logger.debug(f"Using two-part fetch for source: {source_name}")
                data = do_discovery_then_fetch(source)
            else:
                logger.debug(f"Using direct fetch for source: {source_name}")
                data = fetch_direct(source)
        elif source["type"] == "graphql":
            logger.debug(f"Using GraphQL fetch for source: {source_name}")
            data = fetch_graphql(source)
        else:
            logger.warning(
                f"Unknown source type '{source_type}' for source: {source_name}"
            )
            return None

        if data is None:
            logger.warning(f"No data returned from source: {source_name}")
        else:
            logger.info(
                f"Successfully fetched data from source: {source_name} (records: {len(data) if isinstance(data, list) else 'n/a'})"
            )
        return data
    except Exception as e:
        logger.error(
            f"Failed to fetch data from source: {source_name}. Error: {e}",
            exc_info=True,
        )
        return None


def fetch_direct(source):
    # expand to allow for authentication or additional headers, etc.
    try:
        source_url = f"{source['api_base_url']}{source['endpoint']}"
        response = requests.get(source_url)
        if not response.ok:
            raise RuntimeError(
                f"Fetch failed: {response.status_code} {response.reason}"
            )
        data = extract_response_data(source, response.json())
        if (
            isinstance(data, list)
            and "filter_prefix" in source
            and "match_key" in source
        ):
            filter_prefix = source["filter_prefix"]
            match_key = source["match_key"]
            data = [item for item in data if filter_prefix in item.get(match_key, "")]
        return data
    except requests.exceptions.RequestException as e:
        # specific error logging
        raise RuntimeError(f"Request failed for source {source['name']}: {e}")
    except ValueError as e:
        # specific error logging
        raise RuntimeError(f"Invalid JSON response for source {source['name']}: {e}")


def do_discovery_then_fetch(source):
    discovery = source["discovery"]
    match_key = discovery["match_key"]
    filter_prefix = discovery["filter_prefix"]
    discovery_url = f"{source['api_base_url']}{discovery['endpoint']}"
    try:
        # discovery phase
        discovery_res = requests.get(discovery_url)
        if not discovery_res.ok:
            raise RuntimeError(
                f"Fetch failed: {discovery_res.status_code} {discovery_res.reason}"
            )
        discovery_data = extract_response_data(source, discovery_res.json())
        filtered_discovery_data = [
            item[match_key]
            for item in discovery_data
            if filter_prefix in item[match_key]
        ]
        # fetch phase
        fetch_data = []
        for match in filtered_discovery_data:
            try:
                param = source["fetch"]["key_param"]
                fetch_url = source["fetch"]["endpoint_template"].format(
                    **{param: match}
                )
            except KeyError as e:
                raise ValueError(f"Missing key in 'endpoint_template': {e}")
            res = requests.get(fetch_url)
            data = extract_response_data(source, res.json())
            fetch_data.append(data)
        return fetch_data
    except requests.exceptions.RequestException as e:
        # specific error logging
        raise RuntimeError(f"Request failed for source {source['name']}: {e}")
    except ValueError as e:
        # specific error logging
        raise RuntimeError(f"Invalid JSON response for source {source['name']}: {e}")


def fetch_graphql(source):
    try:
        source_url = f"{source['api_base_url']}{source['endpoint']}"
        response = requests.post(url=source_url, json={"query": source["query"]})
        if not response.ok:
            raise RuntimeError(
                f"Fetch failed: {response.status_code} {response.reason}"
            )
        data = response.json()
        return extract_response_data(source, data)
    except requests.exceptions.RequestException as e:
        # specific error logging
        raise RuntimeError(f"Request failed for source {source['name']}: {e}")
    except ValueError as e:
        # specific error logging
        raise RuntimeError(f"Invalid JSON response for source {source['name']}: {e}")


def extract_response_data(source, response_json):
    key = source.get("response_data_key")
    if not key:
        return response_json
    for part in key.split("."):
        response_json = response_json.get(part, {})
    return response_json

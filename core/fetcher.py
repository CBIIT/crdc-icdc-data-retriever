import logging
import requests
from typing import Optional, Union

logger = logging.getLogger(__name__)


REQUEST_TIMEOUT = (5, 30)  # (connect_timeout, read_timeout)


def fetch_from_source(source: dict) -> Optional[list]:
    """Routes external data source fetching to appropriate fetching function
    based on source config.

    Args:
        source (dict): Config for external data source.

    Returns:
        Optional[list]: Data fetched from the source, or None if no data was retrieved.
    """
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


def fetch_direct(source: dict) -> list:
    """Performs direct fetch (i.e. GET request) using the endpoint specified
    in the source config.

    Args:
        source (dict): Config for external data source.

    Returns:
        list: Data fetched from the source.

    Raises:
        RuntimeError: If the request fails or response is invalid.
    """
    source_name = source.get("name", "<unknown>")

    logger.info(f"Starting direct fetch for source: {source_name}")

    try:
        source_url = f"{source['api_base_url']}{source['endpoint']}"
        logger.debug(f"Request URL: {source_url}")
        response = requests.get(source_url, timeout=REQUEST_TIMEOUT)
        if not response.ok:
            logger.error(
                f"Direct fetch failed for source '{source_name}': {response.status_code} {response.reason}"
            )
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
            logger.debug(
                f"Filtering fetched data for prefix '{filter_prefix}' on key '{match_key}'"
            )
            data = [
                item
                for item in data
                if item.get(match_key, "").startswith(filter_prefix)
            ]
        logger.info(f"Fetched {len(data)} records from source: {source_name}")
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"RequestException for source {source_name}: {e}")
        raise RuntimeError(f"Request failed for source {source_name}: {e}")
    except requests.exceptions.Timeout as e:
        logger.warning(
            f"Request timed out for source {source['name']} (url={source_url})"
        )
    except ValueError as e:
        logger.error(f"Invalid JSON response for source {source_name}: {e}")
        raise RuntimeError(f"Invalid JSON response for source {source_name}: {e}")


def do_discovery_then_fetch(source: dict) -> list:
    """Performs a two-step fetch process, where data from a 'discovery'
    endpoint is used to generate one or more follow-up fetch requests.

    Args:
        source (dict): Config for external data source.

    Returns:
        list: Data fetched from the source.
    """
    discovery = source["discovery"]
    match_key = discovery["match_key"]
    filter_prefix = discovery["filter_prefix"]
    api_base_url = source["api_base_url"]
    discovery_url = f"{api_base_url}{discovery['endpoint']}"

    try:
        logger.debug(f"Starting fetch from discovery URL: {discovery_url}")
        discovery_res = requests.get(discovery_url, timeout=REQUEST_TIMEOUT)
        if not discovery_res.ok:
            logger.error(
                f"Discovery fetch failed for source '{source['name']}': {discovery_res.status_code} {discovery_res.reason}"
            )
            raise RuntimeError(
                f"Fetch failed: {discovery_res.status_code} {discovery_res.reason}"
            )
        discovery_data = extract_response_data(source, discovery_res.json())
        logger.debug(
            f"Filtering fetched data for prefix '{filter_prefix}' on key '{match_key}'"
        )
        filtered_discovery_data = [
            item[match_key]
            for item in discovery_data
            if item[match_key].startswith(filter_prefix)
        ]
        logger.debug("Starting fetch phase...")
        fetch_data = []
        for match in filtered_discovery_data:
            try:
                param = source["fetch"]["key_param"]
                endpoint = source["fetch"]["endpoint_template"].format(**{param: match})
                fetch_url = f"{api_base_url}{endpoint}"
            except KeyError as e:
                logger.error(f"Missing key in 'endpoint_template': {e}")
                raise ValueError(f"Missing key in 'endpoint_template': {e}")
            res = requests.get(fetch_url, timeout=REQUEST_TIMEOUT)
            if not res.ok:
                logger.error(
                    f"Fetch phase failed for {fetch_url}: {res.status_code} {res.reason}"
                )
                raise RuntimeError(f"Fetch failed: {res.status_code} {res.reason}")
            data = extract_response_data(source, res.json())
            logger.debug(
                f"Successfully fetched data for match '{match}' from URL: {fetch_url}"
            )
            fetch_data.append(data)
        logger.info(
            f"Fetched {len(fetch_data)} batches of data from source: {source['name']}"
        )
        return fetch_data
    except requests.exceptions.RequestException as e:
        logger.error(f"RequestException for source {source['name']}: {e}")
        raise RuntimeError(f"Request failed for source {source['name']}: {e}")
    except requests.exceptions.Timeout as e:
        logger.warning(
            f"Request timed out for source {source['name']} (url={source_url})"
        )
    except ValueError as e:
        logger.error(f"Invalid JSON response for source {source['name']}: {e}")
        raise RuntimeError(f"Invalid JSON response for source {source['name']}: {e}")


def fetch_graphql(source: dict) -> list:
    """Performs data fetch from a GraphQL endpoint via a POST request.

    Args:
        source (dict): Config for external data source.

    Returns:
        list: Data fetched from the GraphQL source.

    Raises:
        RuntimeError: If the GraphQL query fails or returns an error.
    """
    logger.info(f"Starting GraphQL fetch for source: {source['name']}")

    try:
        source_url = f"{source['api_base_url']}{source['endpoint']}"
        logger.debug(f"GraphQL fetch URL: {source_url}")
        response = requests.post(
            url=source_url, json={"query": source["query"]}, timeout=REQUEST_TIMEOUT
        )
        if not response.ok:
            logger.error(
                f"GraphQL fetch failed for {source_url}: {response.status_code} {response.reason}"
            )
            raise RuntimeError(
                f"GraphQL fetch failed: {response.status_code} {response.reason}"
            )
        data = response.json()
        logger.info(f"GraphQL fetch successful for source: {source['name']}")
        return extract_response_data(source, data)
    except requests.exceptions.RequestException as e:
        logger.error(f"RequestException for source {source['name']}: {e}")
        raise RuntimeError(f"Request failed for source {source['name']}: {e}")
    except requests.exceptions.Timeout as e:
        logger.warning(
            f"Request timed out for source {source['name']} (url={source_url})"
        )
    except ValueError as e:
        logger.error(f"Invalid JSON response for source {source['name']}: {e}")
        raise RuntimeError(f"Invalid JSON response for source {source['name']}: {e}")


def extract_response_data(source: dict, response_json: dict) -> Union[list, dict]:
    """Extracts relevant data from the full JSON response using the 'response_data_key'
    provided in source config.

    Args:
        source (dict): Config for external data source.
        response_json (dict): JSON object returned from data fetch.

    Returns:
        Union[list, dict]: Portion of response containing relevant data.
    """
    key = source.get("response_data_key")
    if not key:
        return response_json
    for part in key.split("."):
        if not isinstance(response_json, dict):
            return {}
        response_json = response_json.get(part, {})
    return response_json

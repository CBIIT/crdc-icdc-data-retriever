import requests


def fetch_from_source(source_config):
    # higher level logging messages
    try:
        if source_config["type"] == "rest":
            if "discovery" in source_config:
                return do_discovery_then_fetch(source_config)
            else:
                return fetch_direct(source_config)
        elif source_config["type"] == "graphql":
            return fetch_graphql(source_config)
    except Exception:
        return None


def fetch_direct(source):
    # expand to allow for authentication or additional headers, etc.
    try:
        source_url = f"{source["api_base_url"]}{source["endpoint"]}"
        response = requests.get(source_url)
        if not response.ok:
            raise RuntimeError(f"Fetch failed: {response.status_code} {response.reason}")
        data = response.json()
        if "response_data_key" in source:
            data = data.get(source["response_data_key"], {})
        return data
    except requests.exceptions.RequestException as e:
        # specific error logging
        raise RuntimeError(f"Request failed for source {source["name"]}: {e}")
    except ValueError as e:
        # specific error logging
        raise RuntimeError(f"Invalid JSON response for source {source["name"]}: {e}")


def do_discovery_then_fetch(source):
    discovery = source["discovery"]
    match_key = discovery["match_key"]
    filter_prefix = discovery["filter_prefix"]
    discovery_url = f"{source["api_base_url"]}{discovery["endpoint"]}"
    try:
        # discovery phase
        discovery_res = requests.get(discovery_url)
        if not discovery_res.ok:
            raise RuntimeError(f"Fetch failed: {discovery_res.status_code} {discovery_res.reason}")
        discovery_data = discovery_res.json()
        if "response_data_key" in source:
            discovery_data = discovery_data.get(source["response_data_key"], {})
        filtered_discovery_data = [
            item[match_key] for item in discovery_data if filter_prefix in item[match_key]
        ]
        # iterate filtered results, passing each to the fetch phase
        # fetch phase
        fetch_data = []
        for match in filtered_discovery_data:
            try:
                fetch_url = source["fetch"]["endpoint_template"].format(collection_id=source["fetch"]["key_param"])
            except KeyError as e:
                raise ValueError(f"Missing key in 'endpoint_template': {e}")
            res = requests.get(fetch_url)
            data = res.json()
            if "response_data_key" in source:
                data = data.get(source["response_data_key"], {})
            fetch_data.append(data)
        return fetch_data
    except requests.exceptions.RequestException as e:
        # specific error logging
        raise RuntimeError(f"Request failed for source {source["name"]}: {e}")
    except ValueError as e:
        # specific error logging
        raise RuntimeError(f"Invalid JSON response for source {source["name"]}: {e}")


def fetch_graphql(source):
    try:
        source_url = f"{source["api_base_url"]}{source["endpoint"]}"
        response = requests.post(url=source_url, json={"query": source["query"]})
        if not response.ok:
            raise RuntimeError(f"Fetch failed: {response.status_code} {response.reason}")
        data = response.json()
        if "response_data_key" in source:
            data = data.get(source["response_data_key"], {})
        return data
    except requests.exceptions.RequestException as e:
        # specific error logging
        raise RuntimeError(f"Request failed for source {source["name"]}: {e}")
    except ValueError as e:
        # specific error logging
        raise RuntimeError(f"Invalid JSON response for source {source["name"]}: {e}")

import requests


def fetch_from_source(source_config):
    if source_config["type"] == "rest":
        if "discovery" in source_config:
            do_discovery_then_fetch(source_config)
        else:
            fetch_direct(source_config)
    elif source_config["type"] == "graphql":
        fetch_graphql(source_config)

def fetch_direct(source):
    # expand to allow for authentication or additional headers, etc.
    # add try/catch blocks for better error handling
    source_url = f"{source["api_base_url"]}{source["endpoint"]}"
    response = requests.get(source_url)
    return response.json()

def do_discovery_then_fetch(source):
    # discovery phase
    discovery = source["discovery"]
    match_key = discovery["match_key"]
    filter_prefix = discovery["filter_prefix"]

    discovery_url = f"{source["api_base_url"]}{discovery["endpoint"]}"
    discovery_response = requests.get(discovery_url)
    discovery_json = discovery_response.json()
    filtered_discovery_json = [
        item[match_key] for item in discovery_json if filter_prefix in item[match_key]
    ]
    # iterate filtered results, passing each to the fetch phase
    pass

def fetch_graphql(source):
    pass

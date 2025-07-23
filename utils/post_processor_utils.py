def deep_merge_additive(d1: dict, d2: dict) -> dict:
    """Recursively merges two dictionaries (d2 -> d1).

    Args:
        d1 (dict): The first dictionary.
        d2 (dict): The second dictionary.

    Returns:
        dict: Modifies d1 dict in place and returns it.
    """
    for k, v in d2.items():
        if k in d1:
            if isinstance(d1[k], dict) and isinstance(v, dict):
                deep_merge_additive(d1[k], v)
            elif isinstance(d1[k], list) and isinstance(v, list):
                d1[k].extend(v)
            elif isinstance(d1[k], int) and isinstance(v, int):
                d1[k] += v
            else:
                d1[k] = v
        else:
            d1[k] = v
    return d1

def deep_merge_additive(base: dict, override: dict) -> dict:
    """Recursively merges two dictionaries (override -> base).

    Args:
        base (dict): The base dictionary.
        override (dict): The override dictionary.

    Returns:
        dict: Modifies base dict in place and returns it.
    """
    for k, v in override.items():
        if k in base:
            if isinstance(base[k], dict) and isinstance(v, dict):
                deep_merge_additive(base[k], v)
            elif isinstance(base[k], list) and isinstance(v, list):
                base[k] = sorted(set(base[k] + v))
            elif isinstance(base[k], int) and isinstance(v, int):
                base[k] += v
            else:
                base[k] = v
        else:
            base[k] = v
    return base

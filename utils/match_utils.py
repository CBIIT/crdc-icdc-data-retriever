from rapidfuzz import fuzz


def is_fuzzy_match(str1: str, str2: str, threshold: int = 75) -> bool:
    """Checks if two strings are a fuzzy match based on a similarity threshold.

    Args:
        str1 (str): First string to compare.
        str2 (str): Second string to compare.
        threshold (int): Similarity threshold (0-100) for fuzzy matching.

    Returns:
        bool: True if string comparison meets fuzzy match threshold; otherwise False.
    """

    return fuzz.partial_ratio(str1.lower(), str2.lower()) >= threshold

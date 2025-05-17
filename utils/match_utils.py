from rapidfuzz import fuzz


def is_fuzzy_match(str1: str, str2: str, threshold: int = 75) -> bool:
    return fuzz.partial_ratio(str1.lower(), str2.lower()) >= threshold

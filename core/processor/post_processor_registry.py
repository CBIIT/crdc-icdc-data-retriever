import inspect
from typing import Callable, Optional, Any

import core.processor.post_processor as post_processor

POST_PROCESSOR_MAP = {
    name: fn
    for name, fn in inspect.getmembers(post_processor, inspect.isfunction)
    if getattr(fn, "_is_post_processor", False)
}


def get_post_processor(name: str) -> Optional[Callable[..., Any]]:
    """Maps a post-processor name to its corresponding function.

    Args:
        name (str): Post-processor name.

    Returns:
        Optional[Callable[..., Any]]: A corresponding post-processor function
        or None if not found.
    """

    return POST_PROCESSOR_MAP.get(name)


def apply_post_processor(fn: Callable[..., Any], metadata: dict, **kwargs: dict) -> Any:
    """Applies a post-processor function to supplied metadata.

    Args:
        fn (Callable[..., Any]): Post-processor function.
        metadata (dict): Metadata undergoing post-processing.
        kwargs (dict): Additional post-processor kwargs.
    Returns:
        Any: The result of the post-processor function call.
    """

    try:
        return fn(metadata, **kwargs)
    except TypeError:
        return fn(metadata)

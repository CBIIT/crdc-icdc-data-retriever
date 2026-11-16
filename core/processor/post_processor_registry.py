import inspect
from typing import Callable, Any

import core.processor.post_processor as post_processor

POST_PROCESSOR_MAP = {
    name: fn
    for name, fn in inspect.getmembers(post_processor, inspect.isfunction)
    if getattr(fn, "_is_post_processor", False)
}


def get_post_processors(names: str | list | None) -> list[Callable[..., Any]]:
    """Maps post-processor names to their corresponding functions.

    Args:
        names (str | list | None): Post-processor name or list of names.

    Returns:
        list[Callable[..., Any]]: A list of corresponding post-processor functions.
    """
    if names is None:
        names = []
    elif isinstance(names, str):
        names = [names]

    pp_functions = []
    for pp_name in names:
        if pp_name not in POST_PROCESSOR_MAP:
            raise ValueError(
                f"Unknown post-processor '{pp_name}'. Available: {sorted(POST_PROCESSOR_MAP)}"
            )
        pp_functions.append(POST_PROCESSOR_MAP[pp_name])
    return pp_functions


def apply_post_processors(
    fns: list[Callable[..., Any]], data: Any, **kwargs: Any
) -> Any:
    """Applies post-processor functions to supplied metadata.

    Args:
        fns (list[Callable[..., Any]]): List of post-processor functions.
        data (Any): Data undergoing post-processing.
        kwargs (dict): Additional post-processor kwargs.
    Returns:
        Any: The result of the last post-processor function call.
    """
    result = data
    for fn in fns:
        params = inspect.signature(fn).parameters
        if any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values()):
            accepted = kwargs
        else:
            accepted = {k: v for k, v in kwargs.items() if k in params}
        result = fn(result, **accepted)
    return result

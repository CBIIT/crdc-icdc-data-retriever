import inspect

import processor.post_processor as post_processor

POST_PROCESSOR_MAP = {
    name: fn
    for name, fn in inspect.getmembers(post_processor, inspect.isfunction)
    if getattr(fn, "_is_post_processor", False)
}


def get_post_processor(name):
    return POST_PROCESSOR_MAP.get(name)


def apply_post_processor(fn, metadata, **kwargs):
    try:
        return fn(metadata, **kwargs)
    except TypeError:
        return fn(metadata)

import pytest

from core.processor.post_processor_registry import (
    get_post_processor,
    apply_post_processor,
)


def dummy_processor(metadata: dict, suffix: str = "") -> dict:
    return {**metadata, "extra": f"value_{suffix}"}


def dummy_processor_no_kwargs(metadata: dict) -> dict:
    return {**metadata, "static": True}


def test_get_nonexistent_post_processor():
    assert get_post_processor("nonexistent_post_processor") is None


def test_apply_post_processor_with_kwargs():
    metadata = {"field": "data"}
    result = apply_post_processor(dummy_processor, metadata, suffix="123")
    assert result == {"field": "data", "extra": "value_123"}


def test_apply_post_processor_no_kwargs():
    metadata = {"field": "data"}
    result = apply_post_processor(dummy_processor_no_kwargs, metadata)
    assert result == {"field": "data", "static": True}


def test_apply_post_processor_without_kwargs():
    metadata = {"field": "data"}
    result = apply_post_processor(
        dummy_processor_no_kwargs, metadata, ignore_kwarg=True
    )
    assert result == {"field": "data", "static": True}

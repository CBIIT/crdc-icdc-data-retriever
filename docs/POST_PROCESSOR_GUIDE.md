# Creating and Adding a Post-processor Plug-in

Post-processors are functions that are intended to be modular external dataset metadata transformers. While the only required input argument to a post-processor is a metadata object of type `list[dict]`, post-processors can return different types of outputs depending on the specific use case or downstream needs.

## General Requirements
### Each post-processor must:
- Be a function accepting a `list[dict]` as its first argument
- Exist in `core/processor/post_processor.py`
- Be decorated with the `@post_processor` decorator (so it can be detected via `post_processor_registry.py`)
- Return a data structure compatible with the user's downstream expectations

**ex:**
```python
@post_processor
def my_post_processor(metadata: list[dict], **kwargs) -> Union[list[dict], dict]:
    ...

```

## How Post-processor Output is Used
Post-processor output is passed directly to the `metadata` field of the corresponding `CRDCLinks` entry. Here is the relevant logic in `core/processor/mapper.py`:

```python
        if post_processor:
            metadata = apply_post_processor(post_processor, metadata, **context)
            logger.info(f"Applied post-processor: {post_processor.__name__}")

        url = dataset_base_url.format(**{dataset_base_url_param: match_id})
        crdc_links.append(
            {"repository": repository_name, "url": url, "metadata": metadata}
        )
```

### What this means:
- The post-processor must return something suitable for the `metadata` field in OpenSearch.
- This is typically a `list[dict]` or `dict`, but could technically be any JSON-serializable structure.
- If uncertain about return data type/structure, inspect how the data will be used in `mapper.py` and shape the return value accordingly.


## Existing Post-processors
| Post-processor       | Input Type   | Output Type  | Description |
| -------------------  | ------------ | ------------ | ----------- |
| `clean_idc_metadata` | `list[dict]` | `list[dict]` | Cleans and returns input dataset metadata |
| `aggregate_tcia_series_data`| `list[dict]` | `dict` | Aggregates metadata into a single record |
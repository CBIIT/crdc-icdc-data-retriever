# Post-processor Plug-in Guide

Post-processors are modular transformation functions that can be applied to data at two distinct points in the pipeline:

- **Source-level** — applied per source to matched metadata before entity mapping is finalized
- **Output-level** — applied to the full set of documents just before they are written to OpenSearch

Both levels use the same function registry and decorator; placement in the config determines when they run.

---

## Source-level vs. Output-level

### Source-level post-processors

Configured on a source via the `post_processor` field. Called once per matched metadata collection (i.e., per `CRDCLinks` entry) with that source's metadata; a single entity may trigger multiple calls if it matches multiple collections. The return value replaces the `metadata` field in the corresponding `CRDCLinks` entry.

```yaml
sources:
  - name: IDC
    type: rest
    ...
    post_processor: clean_idc_metadata
```

Relevant logic in `core/processor/mapper.py`:

```python
if post_processor:
    metadata = apply_post_processor(post_processor, metadata, **context)

crdc_links.append(
    {"repository": repository_name, "url": url, "metadata": metadata}
)
```

The `context` dict passed as `**kwargs` contains:
- `entity` — the current entity dict
- `collection_id` — the matched external ID
- `entity_id_key` — the key used to identify entities

### Output-level post-processors

Configured under `output.config.post_processor`. Called once with the full list of documents before bulk writing to OpenSearch. Typically used to reshape or reformat the entire result set (e.g. adding timestamps, restructuring fields for a specific downstream schema).

```yaml
output:
  destination: opensearch
  config:
    ...
    post_processor: format_for_icdc
```

---

## Implementing a Post-processor

### Requirements

Every post-processor must:

1. Be a function whose **first argument** accepts a `list[dict]`
2. Be defined in `core/processor/post_processor.py`
3. Be decorated with `@post_processor` (enables auto-discovery by the registry)
4. Return a JSON-serializable value compatible with its intended use (see below)

```python
from typing import Union

@post_processor
def my_post_processor(metadata: list[dict], **kwargs) -> Union[list[dict], dict]:
    # transform and return
    ...
```

The `**kwargs` signature is recommended so that source-level context (e.g. `entity`, `collection_id`) can be accepted or ignored gracefully without raising errors.

### Return values

| Context | Expected return type | Notes |
| ------- | -------------------- | ----- |
| Source-level | `list[dict]` or `dict` | Replaces the `metadata` field for a single `CRDCLinks` entry |
| Output-level | `list[dict]` | Replaces the entire document list passed to the OpenSearch writer |

---

## Built-in Post-processors

### `clean_idc_metadata`
**Level:** source  
**Input:** `list[dict]` — IDC collection metadata  
**Output:** `list[dict]`

Converts any HTML in `description` fields to plain text using `html2text`.

---

### `aggregate_tcia_series_data`
**Level:** source  
**Input:** `list[dict]` — TCIA series metadata records  
**Output:** `dict`

Aggregates per-series TCIA records into a single summary dict for a collection. Computed fields:

- `Collection` — collection ID
- `Aggregate_PatientID` — number of unique patients
- `Aggregate_Modality` — list of unique modalities
- `Aggregate_BodyPartExamined` — list of unique body parts
- `Aggregate_ImageCount` — total image count

Supports per-entity override data for known edge cases (e.g. `GLIOMA01`).

Requires source-level context — must accept `entity`, `collection_id`, and `entity_id_key` kwargs:

```python
@post_processor
def aggregate_tcia_series_data(
    data: list, entity: dict, collection_id: str, entity_id_key: str
) -> dict:
    ...
```

---

### `format_for_icdc`
**Level:** output  
**Input:** `list[dict]` — entity-mapped documents  
**Output:** `list[dict]`

Reshapes entity-mapped documents for ICDC ingestion. Each output document includes:

- `timestamp` — ISO 8601 UTC timestamp (millisecond precision)
- `clinical_study_designation` — entity ID from the source document
- `CRDCLinks` — array of external dataset links
- `numberOfImageCollections` — count of `CRDCLinks` entries
- `numberOfCRDCNodes` — count of unique repositories across all links

---

### `format_for_ccdi`
**Level:** output  
**Input:** `list[dict]` — raw fetched documents  
**Output:** `list[dict]`

Wraps each raw document in a CCDI-compatible envelope:

```json
{
  "timestamp": "2026-03-11T14:00:00.000Z",
  "repository": "IDC",
  "data": { "...": "..." }
}
```

---

## Post-processor Registry

The registry (`core/processor/post_processor_registry.py`) auto-discovers all functions in `post_processor.py` that have the `_is_post_processor` attribute set by the `@post_processor` decorator. No manual registration is required — adding a new decorated function to the module makes it immediately available by name in the config.

```python
POST_PROCESSOR_MAP = {
    name: fn
    for name, fn in inspect.getmembers(post_processor, inspect.isfunction)
    if getattr(fn, "_is_post_processor", False)
}
```

The registry also handles graceful kwarg forwarding: if a post-processor does not accept the context kwargs, they are silently dropped rather than raising a `TypeError`.

---

## Summary

| Post-processor | Level | Input | Output | Description |
| -------------- | ----- | ----- | ------ | ----------- |
| `clean_idc_metadata` | source | `list[dict]` | `list[dict]` | Converts HTML `description` fields to plain text |
| `aggregate_tcia_series_data` | source | `list[dict]` | `dict` | Aggregates TCIA series records into a collection summary |
| `format_for_icdc` | output | `list[dict]` | `list[dict]` | Reshapes entity-mapped documents for ICDC ingestion |
| `format_for_ccdi` | output | `list[dict]` | `list[dict]` | Wraps raw documents in a CCDI-compatible envelope |

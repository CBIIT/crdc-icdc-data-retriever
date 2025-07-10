## YAML Configuration Guide

This guide explains how to customize a configuration YAML for the data retriever service. The service uses this file to orchestrate data fetching, mapping and output.

### File Structure Overview

A config YAML contains the following fields:

```yaml
project: ICDC
entity_source: source_A
output:
  destination: opensearch
  config:
    host: http://opensearch-host
    index: external_data
    use_ssl: true
    verify_certs: true
sources:
  - name: source_A
    type: graphql
    api_base_url: https://some-api.org
    endpoint: /graphql
    query: "{ query { field } }"
    response_data_key: data.records
    match_key: external_id
    post_processor: clean_idc_metadata

  - name: source_B
    type: rest
    api_base_url: https://another-api.org
    endpoint: /records
    response_data_key: items
    match_key: external_id
    filter_prefix: ABC
```
---

### Top-level Fields

| Field | Type   | Required | Description |
| ----  | ------ | -------- | ----------- |
| project | str | **yes** | Name of project or context |
| entity_source | str | **yes** | Name of source providing core entity list |
| output | object | **yes** | Output configuration (OpenSearch settings) |
| sources | list | **yes** | List of data source configs to fetch and map |

---

### Output

```yaml
output:
  destination: opensearch
  config:
    host: http://opensearch-host
    index: external_data
    use_ssl: true
    verify_certs: true
```

| Field | Type   | Required | Description |
| ----  | ------ | -------- | ----------- |
| destination | str | **yes** | Output target (OpenSearch) |
| host | str | **yes** | OpenSearch endpoint |
| index | str | **yes** | Index to write data to |
| use_ssl | boolean | **no** | Create client with SSL enabled |
| verify_certs | boolean | **no** | Validate certificates |

---

### Sources

Each item in the sources list defines a data source to fetch from and potentially map to relevant entities.

| Field | Type   | Required | Description |
| ----  | ------ | -------- | ----------- |
| name | str | **yes** | Unique identifier for the source |
| type | str | **yes** | One of: `rest`, `graphql` |
| api_base_url | str | **yes** | Root URL of the external API |
| response_data_key | str | **no** | Dot-path to relevant JSON payload (ex: `data.records`) |
| match_key | str | **yes** | Field used to match to core entity IDs |
| filter_prefix | str | **no** | Used to filter records by prefix on the match key |
| post_processor | str | **no** | Name of a registered post-processor function |

#### Source configuration examples:

- REST source - direct fetch
```yaml
- name: source_B
  type: rest
  api_base_url: https://some-api.org
  endpoint: /records
  response_data_key: items
  match_key: external_id
  filter_prefix: ABC
```

- REST source - discovery + fetch phases
```yaml
- name: source_C
  type: rest
  api_base_url: https://some-api.org
  discovery:
    endpoint: /discovery
    match_key: id
    filter_prefix: X
  fetch:
    endpoint_template: /lookup/{id}
    key_param: id
  response_data_key: results
```

- GraphQL source
```yaml
- name: source_A
  type: graphql
  api_base_url: https://graphql-api.org
  endpoint: /graphql
  query: "{ data { id name } }"
  response_data_key: data
  match_key: external_id
```

#### Post-processor plug-ins

Post-processors can be applied per source to modify or transform the external dataset metadata before it is written to OpenSearch. Specify the function name in the `post_processor` field. See [POST_PROCESSOR_GUIDE.md](docs/POST_PROCESSOR_GUIDE.md) for implementation and registration details.

---

### Notifications (optional)

The data retriever supports sending notifications (AWS SNS) after data retrieval/ingestion success or failure.

```yaml
notifications:
  destination: sns
  config:
    topic_arn: ${SNS_TOPIC_ARN}
    region: ${SNS_REGION:-us-east-1}
```

| Field | Type   | Required | Description |
| ----  | ------ | -------- | ----------- |
| destination | str | **yes** | Output target (only SNS supported) |
| topic_arn | str | **yes** | Full ARN of SNS topic |
| region | str | **yes** | AWS region |

---

### Notes
- Every source `name` field must be unique.
- The `entity_source` should return the master entity list.
- The configuration is strict - malformed YAML or missing keys will cause startup errors.
- For more examples, see the [config](config/) directory.

---
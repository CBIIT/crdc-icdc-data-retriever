## YAML Configuration Guide

This guide explains how to configure a YAML file for the data retriever service. The service uses this file to orchestrate data fetching, optional entity matching, post-processing, and output.

---

### File Structure Overview

```yaml
project: ICDC
entity_source: ICDC        # required in entity-mapped mode; omit when all sources are rest_raw

output:
  destination: opensearch
  config:
    host: http://opensearch-host   # or use 'hosts' for a list
    index: external_data
    use_ssl: false
    verify_certs: false
    post_processor: format_for_icdc  # optional output-level post-processor

sources:
  - name: source_A
    type: graphql
    api_base_url: https://some-api.org
    endpoint: /graphql
    query: "{ query { field } }"
    response_data_key: data.records
    match_key: external_id
    entity_id_key: study_id
    dataset_base_url: https://some-api.org/data?Collection={collection_id}
    dataset_base_url_param: collection_id
    post_processor: clean_idc_metadata

  - name: source_B
    type: rest
    api_base_url: https://another-api.org
    endpoint: /records
    response_data_key: items
    match_key: external_id
    entity_id_key: study_id
    dataset_base_url: https://another-api.org/data?dataset={dataset_id}
    dataset_base_url_param: dataset_id
    filter_prefix: ABC

notifications:
  destination: sns
  config:
    topic_arn: ${SNS_TOPIC_ARN}
    region: ${SNS_REGION:-us-east-1}
```

---

### Environment Variable Substitution

String values in the config support inline environment variable substitution with an optional fallback:

```yaml
host: ${OPENSEARCH_HOST:-http://localhost:9200}
topic_arn: ${SNS_TOPIC_ARN}
```

- `${VAR}` — resolves to the value of `VAR`, or an empty string if unset
- `${VAR:-fallback}` — resolves to the value of `VAR`, or `fallback` if unset

---

### Top-level Fields

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| `project` | str | **yes** | Unique project identifier |
| `entity_source` | str | conditional | Name of source providing the entity list. Required unless all sources are `rest_raw` |
| `output` | object | **yes** | Output configuration (OpenSearch settings) |
| `sources` | list | **yes** | List of data source configs |
| `notifications` | object | no | AWS SNS notification settings |

---

### Output

```yaml
output:
  destination: opensearch
  config:
    host: http://opensearch-host     # single host
    # hosts:                         # OR multiple hosts
    #   - http://host-1:9200
    #   - http://host-2:9200
    index: external_data
    use_ssl: false
    verify_certs: false
    post_processor: format_for_icdc  # optional
    # username: ${OPENSEARCH_USERNAME}
    # password: ${OPENSEARCH_PASSWORD}
```

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| `destination` | str | **yes** | Output target — only `opensearch` is supported |
| `host` | str | conditional | Single OpenSearch host URL. Use `host` or `hosts`, not both |
| `hosts` | list | conditional | List of OpenSearch host URLs for multi-host writes |
| `index` | str | **yes** | Index name to write documents to |
| `use_ssl` | bool | no | Enable SSL on the client connection |
| `verify_certs` | bool | no | Validate SSL certificates |
| `post_processor` | str | no | Output-level post-processor applied to all documents before writing |
| `username` | str | no | OpenSearch username (overridden by `OPENSEARCH_USERNAME` env var) |
| `password` | str | no | OpenSearch password (overridden by `OPENSEARCH_PASSWORD` env var) |

> **Note:** When `hosts` is a list, the writer will attempt to connect to and write to each host. The pipeline reports success if at least one write succeeds.

---

### Sources

Each item in the `sources` list defines a data source. The service supports three source types: `rest`, `graphql`, and `rest_raw`.

#### Common Fields

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| `name` | str | **yes** | Unique identifier for the source |
| `type` | str | **yes** | One of: `rest`, `graphql`, `rest_raw` |
| `api_base_url` | str | **yes** | Root URL of the external API |
| `response_data_key` | str | no | Dot-path into the JSON response to the relevant data (e.g. `data.records`) |
| `post_processor` | str | no | Source-level post-processor applied to matched metadata before mapping |

#### Entity-Mapped Source Fields (`rest`, `graphql`)

These fields are required for sources participating in entity matching (i.e., all non-`rest_raw` sources):

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| `entity_id_key` | str | **yes** | Key used to identify entity IDs in the source response |
| `match_key` | str | **yes** | Field in the fetched records used to match against entity IDs |
| `dataset_base_url` | str | **yes** | URL template for linking to the external dataset (uses `dataset_base_url_param`) |
| `dataset_base_url_param` | str | **yes** | Named placeholder in `dataset_base_url` to substitute the matched ID |
| `filter_prefix` | str | no | If set, only records whose `match_key` value begins with this prefix are retained |

> **Fuzzy matching:** Entity IDs are compared to `match_key` values using fuzzy string matching (via `rapidfuzz`) with a default similarity threshold of 75. This tolerates minor differences in naming conventions between sources.

---

#### Source Type Examples

**REST — direct fetch**

Performs a single GET request to `api_base_url` + `endpoint`.

```yaml
- name: IDC
  type: rest
  api_base_url: https://api.imaging.datacommons.cancer.gov/v2
  endpoint: /collections
  response_data_key: collections
  match_key: collection_id
  filter_prefix: icdc_
  entity_id_key: clinical_study_designation
  dataset_base_url: https://portal.imaging.datacommons.cancer.gov/explore/filters/?collection_id={collection_id}
  dataset_base_url_param: collection_id
  post_processor: clean_idc_metadata
```

**REST — two-phase (discovery + fetch)**

First fetches a list of IDs from a discovery endpoint, then issues individual requests for each matching ID.

```yaml
- name: TCIA
  type: rest
  api_base_url: https://nbia.cancerimagingarchive.net/nbia-api/services/v4
  entity_id_key: clinical_study_designation
  dataset_base_url: https://nbia.cancerimagingarchive.net/nbia-search/?CollectionCriteria={collection_id}
  dataset_base_url_param: collection_id
  discovery:
    endpoint: /getCollectionValues
    match_key: Collection
    filter_prefix: ICDC-
  fetch:
    endpoint_template: /getSeries?format=json&Collection={collection_id}
    key_param: collection_id
  post_processor: aggregate_tcia_series_data
```

The `discovery` block fields:

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| `endpoint` | str | **yes** | Endpoint to fetch the discovery list from |
| `match_key` | str | **yes** | Field in discovery records used to filter and identify IDs |
| `filter_prefix` | str | **yes** | Only discovery records whose `match_key` starts with this prefix are used |

The `fetch` block fields:

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| `endpoint_template` | str | **yes** | URL path template for individual fetch requests (uses `key_param` as placeholder) |
| `key_param` | str | **yes** | Named placeholder in `endpoint_template` to substitute matching IDs |

**GraphQL**

Sends a POST request with the given query and extracts the response using `response_data_key`.

```yaml
- name: ICDC
  type: graphql
  api_base_url: https://caninecommons.cancer.gov/v1
  endpoint: /graphql/
  query: |
    {
      studiesByProgram {
        clinical_study_designation
      }
    }
  response_data_key: data.studiesByProgram
  entity_id_key: clinical_study_designation
```

**REST Raw (`rest_raw`)**

Fetches data from a REST endpoint without entity matching. Supports automatic pagination via `Link` response headers and `X-Wp-TotalPages`. Data is written directly to OpenSearch. When **all** sources are `rest_raw`, entity matching is bypassed entirely and `entity_source` is not required.

```yaml
- name: IDC
  type: rest_raw
  api_base_url: https://api.imaging.datacommons.cancer.gov/v1
  endpoint: /collections
  response_data_key: collections
```

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| `endpoint` | str | **yes** | Endpoint path to fetch from |
| `response_data_key` | str | no | Dot-path to relevant data in the JSON response |

---

### Notifications (optional)

When configured, an SNS notification is published after every pipeline run (success or failure). Notifications are skipped in `--dry-run` mode.

```yaml
notifications:
  destination: sns
  config:
    topic_arn: ${SNS_TOPIC_ARN}
    region: ${SNS_REGION:-us-east-1}
```

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| `destination` | str | **yes** | Notification target — only `sns` is supported |
| `topic_arn` | str | **yes** | Full ARN of the SNS topic |
| `region` | str | **yes** | AWS region for the SNS client |

AWS credentials must be available as environment variables: `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.

---

### Notes

- Every source `name` must be unique.
- The `entity_source` name must match a `name` in the `sources` list.
- Use `host` or `hosts` in `output.config`, not both.
- Malformed YAML or missing required keys will cause a startup validation error.
- For complete working examples, see the [config/](../config/) directory.

---
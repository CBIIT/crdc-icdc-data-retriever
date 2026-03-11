# Data Retriever Service

A YAML-configurable service that fetches data from multiple external sources, optionally maps records to internal project entities using fuzzy matching, applies post-processing transformations, and writes the results to OpenSearch. Supports two distinct operating modes: **entity-mapped** and **raw fetch**.

---

## Operating Modes

### Entity-Mapped Mode
The default mode. One source acts as the **entity source**, providing the canonical list of project entities (e.g. ICDC studies). All other sources are fetched and matched to those entities using configurable match keys and fuzzy matching. Results are written as one document per entity, with all matching external dataset links grouped under a `CRDCLinks` array.

### Raw Fetch Mode
Activated automatically when **all** configured sources have `type: rest_raw`. In this mode the service bypasses entity matching entirely — data is fetched from each source as-is, optionally post-processed, and written directly to OpenSearch. The `entity_source` field is not required in this mode.

---

## Configuration

All behavior is driven by a YAML config file. See [docs/CONFIG_GUIDE.md](docs/CONFIG_GUIDE.md) for a full reference.

The config file must define:

- `project` — Unique project identifier (e.g. `"ICDC"`)
- `entity_source` — Name of the source that provides the entity list *(required in entity-mapped mode)*
- `sources` — One or more external data sources to fetch from
- `output` — OpenSearch connection and index settings
- `notifications` — *(optional)* AWS SNS topic for pipeline completion alerts

### Environment Variables

Config values support inline environment variable substitution with optional fallbacks using the `${VAR:-fallback}` syntax.

For OpenSearch authentication (credentials may also be set directly in the config):

```bash
export OPENSEARCH_USERNAME=<opensearch-username>
export OPENSEARCH_PASSWORD=<opensearch-password>
```

For SNS notifications:

```bash
export AWS_ACCESS_KEY_ID=<access-key>
export AWS_SECRET_ACCESS_KEY=<secret-access-key>
```

---

## Running the Service

```bash
python main.py --config path/to/config.yaml
```

### CLI Flags

| Flag | Description |
| ---- | ----------- |
| `--config` | Path to YAML config file (default: `config.yaml`) |
| `--dry-run` | Fetch and map data without writing to OpenSearch or sending notifications |
| `--parallel-fetch` | Fetch from all sources concurrently using threads |
| `--log-level` | Log verbosity: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` (default: `INFO`) |

### Logging

Logs are written to both the console and a rotating file at `./logs/app.log` (max 5 MB per file, 3 backups retained).

---

## Output Format

### Entity-Mapped Mode

One document is written per matched entity:

```json
{
  "entity_id": "GLIOMA01",
  "CRDCLinks": [
    {
      "repository": "IDC",
      "url": "https://portal.imaging.datacommons.cancer.gov/explore/filters/?collection_id=icdc_glioma",
      "metadata": { "...": "..." }
    },
    {
      "repository": "TCIA",
      "url": "https://nbia.cancerimagingarchive.net/nbia-search/?CollectionCriteria=ICDC-GLIOMA",
      "metadata": { "...": "..." }
    }
  ]
}
```

An optional output-level post-processor (e.g. `format_for_icdc`) can reshape this structure before it is written to OpenSearch.

### Raw Fetch Mode

Documents are written as fetched, after any configured output-level post-processor is applied. Example output using `format_for_ccdi`:

```json
{
  "timestamp": "2026-03-11T14:00:00.000Z",
  "repository": "IDC",
  "data": { "...": "..." }
}
```

---

## Project Structure

```
.
├── main.py                         # Entry point and pipeline orchestration
├── config_loader.py                # YAML config loader with env var substitution and validation
├── core/
│   ├── dispatcher.py               # Fetch coordination and mode routing (entity-mapped vs. raw)
│   ├── fetcher.py                  # Source-type fetch logic (REST, GraphQL, raw, two-phase)
│   ├── sns_notifier.py             # AWS SNS notification integration
│   ├── processor/
│   │   ├── mapper.py               # Entity-to-source mapping with fuzzy match
│   │   ├── post_processor.py       # Built-in post-processor functions
│   │   └── post_processor_registry.py  # Auto-discovery and invocation of post-processors
│   └── writer/
│       └── opensearch_writer.py    # Bulk document writer with multi-host support
├── utils/
│   ├── logging_utils.py            # Rotating file + console logger setup
│   ├── mapping_utils.py            # Metadata normalization helpers
│   ├── match_utils.py              # Fuzzy string matching (rapidfuzz)
│   ├── notification_utils.py       # SNS message builder
│   └── post_processor_utils.py     # Deep-merge utility for post-processors
└── config/                         # Example configuration files
```

---

## Additional Guides

- [Configuration YAML Guide](docs/CONFIG_GUIDE.md)
- [Post-processor Plug-in Guide](docs/POST_PROCESSOR_GUIDE.md)

---

# Data Retriever Service

A YAML-configurable service that fetches data from multiple external sources, maps them to internal entities (ex: ICDC study), provides optional post-processing and stores the results in OpenSearch.

---

### Config File

Ensure the following sections are configured:

- `project`: Unique project identifier (e.g. `"ICDC"`)
- `entity_source`: Source that provides internal entities
- `sources`: List of external data sources
- `output`: OpenSearch settings
- `notifications`: (optional) SNS topic for alerting

### Environment Variables

For OpenSearch authentication, make sure the following are defined:

```bash
export OPENSEARCH_USERNAME=<opensearch-username>
export OPENSEARCH_PASSWORD=<opensearch-password>
```

If sending notifications, ensure the following are available:

```bash
export AWS_ACCESS_KEY_ID=<access-key>
export AWS_SECRET_ACCESS_KEY=<secret-access-key>
```

### Run the Service

```bash
python main.py --config path/to/config/yaml
```

### Optional Flags

- `--dry-run`: Skip writing to OpenSearch and sending notifications  
- `--parallel-fetch`: Use threads to fetch from all sources in parallel  
- `--log-level`: Set log verbosity (`DEBUG`, `INFO`, `WARNING`, etc.)  

---

### Output Format

Documents are written to OpenSearch with one document per `entity_id` per source:

```json
{
  "entity_id": "GLIOMA01",
  "CRDCLinks": [
    {
      "repository": "IDC",
      "url": "https://.../icdc_glioma",
      "metadata": {...}
    },
    {
      "repository": "TCIA",
      "url": "https://.../collection=ICDC-GLIOMA",
      "metadata": {...}
    }
  ]
}
```

---

### Project Structure

```
.
├── main.py                  # Entry point
├── config_loader.py         # YAML config parser + env var support
├── core/
│   ├── dispatcher.py        # Fetch + match coordination
│   ├── fetcher.py           # Fetch config + logic
│   ├── writer/              # OpenSearch writer
│   └── sns_notifier.py      # SNS integration
├── processor/
│   ├── mapper.py            # Entity-to-source mapping logic
│   └── post_processor.py    # Optional transformation hooks
│   └── post_processor_registry.py # Post-processor mapping
├── utils/
│   ├── logging_utils.py     # Logger setup
│   └── mapping_utils.py     # Mapping helpers
│   └── match_utils.py       # Matching helpers
│   └── notification_utils.py # Notification message builder
```

---

### TODO / Enhancements

- [x] Unit tests 
- [ ] Integration tests 
- [ ] Prefect integration
- [ ] Add retry/backoff logic for unstable endpoints
- [ ] More detailed type hints for improved IDE integration

---

### Additional Guides:

- [Configuration YAML Setup Guide](docs/CONFIG_GUIDE.md)
- [Post-processor Plug-in Guide](docs/POSTPROCESSOR_GUIDE.md)

---
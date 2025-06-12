# Data Retriever Service

A lightweight, YAML-configurable service that fetches data from multiple external sources (REST, GraphQL), maps them to internal entities, and stores the results in OpenSearch. Optional post-processing and SNS notification support included.

## 📦 Features

- ✅ Config-driven data fetching and transformation  
- 🔁 Supports REST (with discovery/fetch) and GraphQL sources  
- 🧠 Pluggable post-processing  
- 💥 Optional multithreaded fetch for performance  
- 🔍 Outputs to OpenSearch  
- 🔔 SNS notifications on pipeline success/failure  
- 📄 YAML config validation and environment variable substitution  

---

## 🚀 Quick Start

### 1. Clone the Repo

```bash
git clone https://github.com/your-org/data-retriever.git
cd data-retriever
```

### 2. Create a Config File

Copy and edit the provided config template:

```bash
cp config.example.yaml config.yaml
```

Ensure the following sections are configured:

- `project`: Unique project identifier (e.g. `"ICDC"`)
- `entity_source`: Source that provides internal entities
- `sources`: List of external data sources
- `output`: OpenSearch settings
- `notifications`: (optional) SNS topic for alerting

### 3. Set Environment Variables

Ensure the following are available:

```bash
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
```

You may also use `.env` or your orchestration tool (Docker/Kubernetes/etc.) to set these.

### 4. Run the Service

```bash
python main.py --config config.yaml
```

### Optional Flags

- `--dry-run`: Skip writing to OpenSearch and sending notifications  
- `--parallel-fetch`: Use threads to fetch from all sources in parallel  
- `--log-level`: Set log verbosity (`DEBUG`, `INFO`, `WARNING`, etc.)  

---

## 🐳 Docker Usage

Build and run the service inside a container:

```bash
docker build -t data-retriever .
docker run -e AWS_ACCESS_KEY_ID=... -e AWS_SECRET_ACCESS_KEY=... -v $(pwd)/config.yaml:/app/config.yaml data-retriever --config config.yaml
```

> ✅ Ensure `config.yaml` is mounted into the container if not baked into the image.

---

## ⚙️ Output Format

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

## 🧪 Testing

Start with config validation:

```bash
python main.py --config config.yaml --dry-run --log-level DEBUG
```

Then test with real writes and notifications disabled:

```bash
python main.py --config config.yaml --dry-run
```

---

## 📬 Notifications

If configured, the app will send SNS messages (e.g., to a Slack integration) on success or failure.  
Customize the message format in `utils/notification_utils.py`.

---

## 🧱 Project Structure

```
.
├── main.py                  # Entry point
├── config_loader.py         # YAML config parser + env var support
├── core/
│   ├── dispatcher.py        # Fetch + match coordination
│   ├── fetcher.py           # REST + GraphQL fetch logic
│   ├── writer/              # OpenSearch writer
│   └── sns_notifier.py      # SNS integration
├── processor/
│   ├── mapper.py            # Entity-to-source mapping logic
│   └── post_processors.py   # Optional transformation hooks
├── utils/
│   ├── logging_utils.py     # Logger setup
│   └── notification_utils.py # Slack-style message builder
```

---

## 📝 TODO / Enhancements

- [ ] Add retry/backoff logic for flaky endpoints  
- [ ] Expand unit tests  
- [ ] Template-based config generation (e.g. Jinja)

---

## 📄 License

MIT License — see `LICENSE` file for details.

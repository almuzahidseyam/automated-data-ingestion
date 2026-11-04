# Local telemetry ingestion

A small, working Python pipeline for collecting local CPU/memory measurements and importing validated JSONL events into SQLite. It provides persistent source checkpoints and idempotent event insertion.

This is a local tool, not a distributed telemetry service. It does not create branches, pull requests, auto-merge logs or upload data anywhere. The previous “enterprise-grade autonomous ingestion” description referred to functionality that was not implemented.

## Collect real metrics

Use Python 3.12 or newer:

```sh
python -m venv .venv
# Windows: .venv\Scripts\activate
# Linux/macOS: source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

python ingest.py collect
python ingest.py --db data/metrics.sqlite collect --count 60 --interval 1
```

The default command records one sample. `--count` is bounded to 1–100,000 and `--interval` to 0.1–3,600 seconds. CPU usage is measured over 0.1 seconds using psutil. Memory usage means **total minus available**, in bytes; it is not the process’s RSS or a random simulated value. No health/anomaly classification is inferred from the measurements.

A random node ID is stored in the database and reused on later runs. Use `--node-id edge-01` to choose a stable label explicitly. The tool does not record the machine’s hostname or credentials. Each sample has a fresh UUID and UTC timestamp.

## Ingest an existing event stream

```sh
python ingest.py --db data/metrics.sqlite ingest events.jsonl --source-id edge-01-log
```

Each UTF-8 line must be a complete JSON object ending in a newline, containing exactly these fields:

```json
{"schema_version":1,"event_id":"11111111-1111-4111-8111-111111111111","node_id":"example-node","observed_at":"2026-01-01T00:00:00Z","cpu_percent":12.5,"memory_used_bytes":512,"memory_total_bytes":1024}
```

This example is synthetic and only illustrates the schema. CPU must be finite and between 0 and 100. Memory fields are integer bytes with `0 <= used <= total`, total greater than zero and at most a signed 64-bit integer. Timestamps require a timezone and are normalized to UTC. Unknown or duplicate JSON fields, invalid UUIDs, nonfinite values and invalid node labels are rejected.

## Checkpoints and failure behavior

- `event_id` is the deduplication key. Replaying identical normalized content is a no-op. Reusing an ID with different content stops ingestion rather than silently overwriting a measurement.
- Each event and its source checkpoint commit in **one SQLite transaction**. A failed checkpoint write also rolls back its event. Previously committed records remain available.
- Checkpoints store a byte offset and SHA-256 hash of the processed prefix. On resume the prefix is re-read and checked, so truncating or changing already committed input is detected. The source ID defaults to the resolved input path; use an explicit ID when moving an append-only source.
- A malformed/incomplete line stops at that line. Correct only the unprocessed tail, then rerun. Changed or replacement files require a new source ID; existing event UUIDs still deduplicate across sources.
- Concurrent insertions serialize through SQLite. A second importer that encounters an advanced checkpoint stops and asks you to rerun; it never rewinds another importer’s progress.
- Lock contention has a 2-second SQLite wait per attempt and at most three transaction attempts, with short backoff. Validation/conflict errors are never retried.
- Inputs are limited to **256 MiB per file** and **16 KiB per line**. Prefix verification is linear in the processed byte count. Files should be trusted, append-only local inputs; this is not a queue consumer or an operating-system isolation boundary.

Successful commands print a compact JSON count summary. Failures return a nonzero exit status. Ctrl+C preserves committed records/checkpoints and exits with status 130. A continuous scheduler, remote transport, monitoring, retention policy and database backups are the operator’s responsibility; the tool does not claim exactly-once delivery across an external source system.

## Stored data

`events` stores normalized event JSON with indexed observation timestamps. `checkpoints` stores ingestion positions, and `metadata` stores the local node ID. The default `data/` directory and SQLite files are ignored by Git. Keep the database and its source logs together when backing up; deleting the database loses its deduplication history and checkpoints. Database size grows as records are added.

The old `system_logs.csv` is preserved as [`examples/legacy_activity_log.csv`](examples/legacy_activity_log.csv). It is an unverified historical activity log, not evidence of measured CPU/memory telemetry, and does not match the ingestion schema.

## Verification

```sh
python -m unittest discover -s tests -v
python -m pip install pip-audit
python -m pip_audit -r requirements.txt
```

Windows and Linux CI exercise schema rejection, timezone normalization, stable node IDs, duplicate/conflicting events, append/resume behavior, modified-prefix detection, atomic checkpoint rollback, bounded lock retries, concurrent connections and CLI failure status. CI also captures two real local samples into a temporary runner database; it sends no telemetry to external services.

[MIT license](LICENSE)

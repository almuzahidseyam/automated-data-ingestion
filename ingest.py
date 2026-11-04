"""Local telemetry collection and resumable JSONL ingestion. No GitHub writes or uploads."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import re
import sqlite3
import sys
import time
import uuid

MAX_LINE = 16384
MAX_INPUT = 256 * 1024 * 1024
FIELDS = {"schema_version", "event_id", "node_id", "observed_at", "cpu_percent", "memory_used_bytes", "memory_total_bytes"}


def normalize(raw):
    if not isinstance(raw, dict) or set(raw) != FIELDS or type(raw["schema_version"]) is not int or raw["schema_version"] != 1:
        raise ValueError("Event must contain exactly the schema-v1 fields")
    try:
        event_id = str(uuid.UUID(raw["event_id"]))
        observed = datetime.fromisoformat(raw["observed_at"].replace("Z", "+00:00"))
        if observed.tzinfo is None:
            raise ValueError("Timestamp requires a timezone")
        timestamp = observed.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
    except (ValueError, TypeError, AttributeError, OverflowError) as error:
        raise ValueError("Invalid UUID or timezone-aware timestamp") from error
    node = raw["node_id"]
    if not isinstance(node, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", node):
        raise ValueError("Node ID must be 1..64 letters, digits, dots, underscores or hyphens")
    cpu = raw["cpu_percent"]
    if isinstance(cpu, bool) or not isinstance(cpu, (int, float)) or not 0 <= cpu <= 100 or not math.isfinite(cpu):
        raise ValueError("CPU percent must be finite and within 0..100")
    used, total = raw["memory_used_bytes"], raw["memory_total_bytes"]
    if type(used) is not int or type(total) is not int or not 0 <= used <= total <= 2**63-1 or total == 0:
        raise ValueError("Memory values must be integer bytes with 0 <= used <= total")
    return {"schema_version": 1, "event_id": event_id, "node_id": node, "observed_at": timestamp,
            "cpu_percent": float(cpu), "memory_used_bytes": used, "memory_total_bytes": total}


def collect_system_metrics(node_id, sampler=None):
    if sampler is None:
        import psutil
        sampler = psutil
    cpu = sampler.cpu_percent(interval=.1)
    memory = sampler.virtual_memory()
    return normalize({"schema_version": 1, "event_id": str(uuid.uuid4()), "node_id": node_id,
                      "observed_at": datetime.now(timezone.utc).isoformat(), "cpu_percent": cpu,
                      "memory_used_bytes": memory.total - memory.available, "memory_total_bytes": memory.total})


def open_database(path):
    location = Path(path)
    location.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(location, timeout=2, isolation_level=None)
    try:
        version = db.execute("PRAGMA user_version").fetchone()[0]
        if version not in (0, 1):
            raise ValueError("Unsupported database schema version")
        db.executescript("""
            CREATE TABLE IF NOT EXISTS events (
                event_id TEXT PRIMARY KEY, observed_at TEXT NOT NULL,
                node_id TEXT NOT NULL, payload TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS events_observed ON events(observed_at);
            CREATE TABLE IF NOT EXISTS checkpoints (
                source TEXT PRIMARY KEY, offset INTEGER NOT NULL, prefix_hash TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            PRAGMA user_version=1;
        """)
        db.execute("INSERT OR IGNORE INTO metadata VALUES('node_id', ?)", (str(uuid.uuid4()),))
        return db
    except BaseException:
        db.close()
        raise


def transaction(db, operation):
    for attempt in range(3):
        try:
            db.execute("BEGIN IMMEDIATE")
            result = operation()
            db.execute("COMMIT")
            return result
        except BaseException as error:
            if db.in_transaction:
                db.execute("ROLLBACK")
            if not isinstance(error, sqlite3.OperationalError) or not any(word in str(error).lower() for word in ("locked", "busy")) or attempt == 2:
                raise
            time.sleep(.1 * (2**attempt))


def insert_event(db, raw):
    event = normalize(raw)
    payload = json.dumps(event, sort_keys=True, separators=(",", ":"), allow_nan=False)
    existing = db.execute("SELECT payload FROM events WHERE event_id=?", (event["event_id"],)).fetchone()
    if existing:
        if existing[0] != payload:
            raise ValueError("Event ID already exists with different content")
        return False
    db.execute("INSERT INTO events VALUES(?,?,?,?)", (event["event_id"], event["observed_at"], event["node_id"], payload))
    return True


def save_event(db, event):
    return transaction(db, lambda: insert_event(db, event))


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON field")
        result[key] = value
    return result


def ingest_file(db, path, source_id=None):
    source_path = Path(path).resolve(strict=True)
    source = source_id or str(source_path)
    if not isinstance(source, str) or not source or len(source) > 1024:
        raise ValueError("Source ID must be nonempty and at most 1024 characters")
    if not source_path.is_file() or source_path.stat().st_size > MAX_INPUT:
        raise ValueError("Input must be a regular file no larger than 256 MiB")
    checkpoint = db.execute("SELECT offset,prefix_hash FROM checkpoints WHERE source=?", (source,)).fetchone()
    offset, expected_hash = checkpoint or (0, hashlib.sha256(b"").hexdigest())
    if not 0 <= offset <= MAX_INPUT:
        raise ValueError("Invalid checkpoint")
    digest = hashlib.sha256()
    report = {"inserted": 0, "duplicates": 0, "offset": offset}
    with source_path.open("rb") as stream:
        remaining = offset
        while remaining:
            block = stream.read(min(65536, remaining))
            if not block:
                raise ValueError("Input was truncated before its saved checkpoint")
            digest.update(block)
            remaining -= len(block)
        if digest.hexdigest() != expected_hash:
            raise ValueError("Previously ingested input changed; use a new source ID for a new file")
        while line := stream.readline(MAX_LINE + 1):
            if len(line) > MAX_LINE or stream.tell() > MAX_INPUT:
                raise ValueError("JSONL record or input exceeds its byte limit")
            if not line.endswith(b"\n"):
                raise ValueError("Incomplete final line: append a newline before retrying")
            try:
                event = normalize(json.loads(line.decode("utf-8"), object_pairs_hook=unique_object))
            except (ValueError, UnicodeError) as error:
                raise ValueError(f"Invalid event at byte offset {offset}; checkpoint has not advanced past it") from error
            digest.update(line)
            new_offset, new_hash = stream.tell(), digest.hexdigest()

            def apply():
                current = db.execute("SELECT offset,prefix_hash FROM checkpoints WHERE source=?", (source,)).fetchone()
                if (current or (0, hashlib.sha256(b"").hexdigest())) != (offset, expected_hash):
                    raise ValueError("Another importer advanced this source; rerun to resume its checkpoint")
                inserted = insert_event(db, event)
                db.execute("INSERT INTO checkpoints VALUES(?,?,?) ON CONFLICT(source) DO UPDATE SET offset=excluded.offset,prefix_hash=excluded.prefix_hash", (source, new_offset, new_hash))
                return inserted

            inserted = transaction(db, apply)
            report["inserted" if inserted else "duplicates"] += 1
            offset, expected_hash = new_offset, new_hash
            report["offset"] = offset
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="data/metrics.sqlite")
    commands = parser.add_subparsers(dest="command", required=True)
    collect = commands.add_parser("collect", help="Collect real local CPU and memory samples")
    collect.add_argument("--count", type=int, default=1)
    collect.add_argument("--interval", type=float, default=1)
    collect.add_argument("--node-id")
    ingest = commands.add_parser("ingest", help="Resume a newline-delimited JSON input")
    ingest.add_argument("input", type=Path)
    ingest.add_argument("--source-id")
    args = parser.parse_args(argv)
    if args.command == "collect" and (not 1 <= args.count <= 100000 or not math.isfinite(args.interval) or not .1 <= args.interval <= 3600):
        parser.error("Count must be 1..100000 and interval 0.1..3600 seconds")
    db = None
    try:
        db = open_database(args.db)
        if args.command == "ingest":
            result = ingest_file(db, args.input, args.source_id)
        else:
            node = args.node_id or db.execute("SELECT value FROM metadata WHERE key='node_id'").fetchone()[0]
            inserted = 0
            for number in range(args.count):
                start = time.monotonic()
                inserted += save_event(db, collect_system_metrics(node))
                if number + 1 < args.count:
                    time.sleep(max(0, args.interval - (time.monotonic() - start)))
            result = {"inserted": inserted, "source": "local-system"}
        print(json.dumps(result))
        return 0
    except (OSError, ValueError, sqlite3.Error) as error:
        print(f"Ingestion stopped: {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("Stopped. Committed records and checkpoints are preserved.", file=sys.stderr)
        return 130
    finally:
        if db is not None:
            db.close()


if __name__ == "__main__":
    raise SystemExit(main())

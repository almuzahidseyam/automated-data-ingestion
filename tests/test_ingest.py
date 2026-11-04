import contextlib
import hashlib
import io
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid

import ingest


def event(**changes):
    return {"schema_version":1,"event_id":str(uuid.uuid4()),"node_id":"fixture-node","observed_at":"2026-01-01T00:00:00Z","cpu_percent":12.5,"memory_used_bytes":512,"memory_total_bytes":1024,**changes}


class IngestTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix="ingest-test-")
        self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        self.db=ingest.open_database(self.root/"data.sqlite")
        self.addCleanup(self.db.close)
        self.path=self.root/"events.jsonl"

    def write(self, records):
        self.path.write_text("".join(json.dumps(record)+"\n" for record in records),encoding="utf-8")

    def count(self):
        return self.db.execute("SELECT count(*) FROM events").fetchone()[0]

    def test_collector_uses_real_sampler_contract_and_stable_node(self):
        sampler=SimpleNamespace(cpu_percent=lambda interval: 25, virtual_memory=lambda: SimpleNamespace(total=4096,available=1024))
        first=ingest.collect_system_metrics("fixture",sampler)
        second=ingest.collect_system_metrics("fixture",sampler)
        self.assertEqual(first["cpu_percent"],25)
        self.assertEqual(first["memory_used_bytes"],3072)
        self.assertEqual(first["node_id"],second["node_id"])
        self.assertNotEqual(first["event_id"],second["event_id"])

    def test_database_node_id_survives_reopen(self):
        node=self.db.execute("SELECT value FROM metadata WHERE key='node_id'").fetchone()[0]
        with contextlib.closing(ingest.open_database(self.root/"data.sqlite")) as reopened:
            self.assertEqual(reopened.execute("SELECT value FROM metadata WHERE key='node_id'").fetchone()[0],node)

    def test_schema_validation_rejects_nonfinite_wrong_units_and_unknown_fields(self):
        for changes in [{"cpu_percent":float("nan")},{"cpu_percent":101},{"cpu_percent":True},{"cpu_percent":10**1000},{"memory_used_bytes":1025},{"memory_total_bytes":0},{"memory_used_bytes":1.5},{"observed_at":"2026-01-01"},{"node_id":"bad node"},{"event_id":"not-a-uuid"},{"unknown":1}]:
            with self.subTest(changes=changes),self.assertRaises(ValueError): ingest.normalize(event(**changes))

    def test_timezone_normalization(self):
        value=ingest.normalize(event(observed_at="2026-01-01T06:00:00+06:00"))
        self.assertEqual(value["observed_at"],"2026-01-01T00:00:00.000000Z")

    def test_idempotent_replay_and_conflicting_id_rejected(self):
        item=event()
        self.assertTrue(ingest.save_event(self.db,item))
        self.assertFalse(ingest.save_event(self.db,item))
        with self.assertRaises(ValueError): ingest.save_event(self.db,{**item,"cpu_percent":15})
        self.assertEqual(self.count(),1)

    def test_checkpoints_resume_and_append_without_duplicates(self):
        records=[event(),event()]
        self.write(records)
        report=ingest.ingest_file(self.db,self.path)
        self.assertEqual(report["inserted"],2)
        self.assertEqual(ingest.ingest_file(self.db,self.path)["inserted"],0)
        with self.path.open("a",encoding="utf-8") as handle: handle.write(json.dumps(event())+"\n")
        self.assertEqual(ingest.ingest_file(self.db,self.path)["inserted"],1)
        self.assertEqual(self.count(),3)

    def test_duplicate_event_in_different_sources_is_not_inserted_twice(self):
        item=event();self.write([item,item])
        report=ingest.ingest_file(self.db,self.path,"source-a")
        self.assertEqual((report["inserted"],report["duplicates"]),(1,1))
        report=ingest.ingest_file(self.db,self.path,"source-b")
        self.assertEqual((report["inserted"],report["duplicates"]),(0,2))
        self.assertEqual(self.count(),1)

    def test_invalid_record_does_not_advance_checkpoint_past_it(self):
        good=event();self.write([good,{"bad":1}])
        first_line=len(self.path.read_bytes().splitlines(keepends=True)[0])
        with self.assertRaises(ValueError): ingest.ingest_file(self.db,self.path)
        self.assertEqual(self.count(),1)
        self.assertEqual(self.db.execute("SELECT offset FROM checkpoints").fetchone()[0],first_line)
        self.write([good,event()])
        self.assertEqual(ingest.ingest_file(self.db,self.path)["inserted"],1)

    def test_committed_prefix_replacement_and_truncation_rejected(self):
        item=event();self.write([item]);ingest.ingest_file(self.db,self.path)
        self.write([{**item,"cpu_percent":99}])
        with self.assertRaises(ValueError): ingest.ingest_file(self.db,self.path)
        self.path.write_bytes(b"")
        with self.assertRaises(ValueError): ingest.ingest_file(self.db,self.path)

    def test_checkpoint_failure_rolls_back_event_in_same_transaction(self):
        self.write([event()])
        self.db.execute("CREATE TRIGGER fail_checkpoint BEFORE INSERT ON checkpoints BEGIN SELECT RAISE(ABORT,'fixture failure'); END")
        with self.assertRaises(sqlite3.IntegrityError): ingest.ingest_file(self.db,self.path)
        self.assertEqual(self.count(),0)
        self.assertEqual(self.db.execute("SELECT count(*) FROM checkpoints").fetchone()[0],0)

    def test_partial_and_oversized_lines_and_duplicate_fields_rejected(self):
        for data in [json.dumps(event()).encode(),b"x"*(ingest.MAX_LINE+1)+b"\n",b'{"schema_version":1,"schema_version":2}\n']:
            self.path.write_bytes(data)
            with self.assertRaises(ValueError): ingest.ingest_file(self.db,self.path)
        self.assertEqual(self.count(),0)

    def test_database_lock_retries_are_bounded_and_eventually_succeed(self):
        blocked=sqlite3.connect(self.root/"data.sqlite",isolation_level=None)
        blocked.execute("BEGIN IMMEDIATE")
        self.db.execute("PRAGMA busy_timeout=1")
        sleeps=[]
        def unlock(delay):
            sleeps.append(delay)
            blocked.execute("ROLLBACK")
        try:
            with patch("ingest.time.sleep",side_effect=unlock): self.assertTrue(ingest.save_event(self.db,event()))
            self.assertEqual(sleeps,[.1])
            blocked.execute("BEGIN IMMEDIATE")
            with patch("ingest.time.sleep") as sleep,self.assertRaises(sqlite3.OperationalError): ingest.save_event(self.db,event())
            self.assertEqual(sleep.call_count,2)
            blocked.execute("ROLLBACK")
        finally: blocked.close()

    def test_competing_connections_insert_one_event(self):
        item=event();results=[];errors=[];barrier=threading.Barrier(2)
        def save():
            try:
                with contextlib.closing(ingest.open_database(self.root/"data.sqlite")) as db:
                    barrier.wait(timeout=5)
                    results.append(ingest.save_event(db,item))
            except BaseException as error: errors.append(error)
        threads=[threading.Thread(target=save) for _ in range(2)]
        for thread in threads: thread.start()
        for thread in threads: thread.join(timeout=8)
        self.assertFalse(errors)
        self.assertEqual(sorted(results),[False,True])
        self.assertEqual(self.count(),1)

    def test_cli_failure_returns_nonzero_and_preserves_checkpoint(self):
        self.path.write_text("invalid\n",encoding="utf-8")
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(ingest.main(["--db",str(self.root/"data.sqlite"),"ingest",str(self.path)]),1)


if __name__=="__main__": unittest.main()

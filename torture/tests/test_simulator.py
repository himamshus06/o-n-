import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

from adapter import AdapterAmbiguousError, AdapterFailedError, AdapterRequestTimeoutError
from process import ProcessConfig, SubprocessDatabaseAdapter
from protocol import (
    Acknowledgement,
    BatchRequest,
    DeleteRequest,
    DumpRequest,
    DumpResponse,
    GetRequest,
    MAX_BATCH_OPERATIONS,
    NotFoundResponse,
    PutRequest,
    ValueResponse,
)


SIMULATOR_PATH = Path(__file__).resolve().parents[1] / "simulator.py"


class SimulatorIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database_dir = Path(self.temp_dir.name) / "db"

    def tearDown(self):
        self.temp_dir.cleanup()

    def make_adapter(self, *extra_args: str, timeout: float = 1.0):
        command = (sys.executable, "-u", str(SIMULATOR_PATH), "--database-dir", str(self.database_dir), *extra_args)
        return SubprocessDatabaseAdapter(ProcessConfig(command, self.database_dir, response_timeout_seconds=timeout))

    def test_crud_and_missing_key(self):
        adapter = self.make_adapter()
        adapter.start()
        try:
            self.assertEqual(adapter.send_request(PutRequest(1, b"name", b"Ada")), Acknowledgement(1))
            self.assertEqual(adapter.send_request(GetRequest(2, b"name")), ValueResponse(2, b"Ada"))
            self.assertEqual(adapter.send_request(DeleteRequest(3, b"name")), Acknowledgement(3))
            self.assertEqual(adapter.send_request(GetRequest(4, b"name")), NotFoundResponse(4))
        finally:
            adapter.graceful_shutdown()

    def test_batch_is_atomic_and_durable(self):
        adapter = self.make_adapter()
        adapter.start()
        try:
            adapter.send_request(PutRequest(1, b"old", b"value"))
            response = adapter.send_request(BatchRequest(2, (PutRequest(20, b"new", b"42"), DeleteRequest(21, b"old"))))
            self.assertEqual(response, Acknowledgement(2))
            self.assertEqual(adapter.send_request(GetRequest(3, b"new")), ValueResponse(3, b"42"))
            self.assertEqual(adapter.send_request(GetRequest(4, b"old")), NotFoundResponse(4))
        finally:
            adapter.graceful_shutdown()

    def test_batch_of_10000_operations(self):
        adapter = self.make_adapter(timeout=10)
        adapter.start()
        try:
            operations = tuple(PutRequest(i, f"k{i}".encode(), b"v") for i in range(MAX_BATCH_OPERATIONS))
            self.assertEqual(adapter.send_request(BatchRequest("large", operations)), Acknowledgement("large"))
        finally:
            adapter.graceful_shutdown()

    def test_dump_returns_complete_consistent_snapshot(self):
        adapter = self.make_adapter()
        adapter.start()
        try:
            adapter.send_request(BatchRequest(1, (PutRequest(10, b"a", b"1"), PutRequest(11, b"b", b"2"))))
            dump = adapter.send_request(DumpRequest(2))
            self.assertEqual(dump, DumpResponse(2, ((b"a", b"1"), (b"b", b"2"))))
        finally:
            adapter.graceful_shutdown()

    def test_state_survives_restart(self):
        first = self.make_adapter()
        first.start()
        first.send_request(PutRequest(1, b"persisted", b"value"))
        first.graceful_shutdown()
        second = self.make_adapter()
        second.start()
        try:
            self.assertEqual(second.send_request(GetRequest(2, b"persisted")), ValueResponse(2, b"value"))
        finally:
            second.graceful_shutdown()

    def test_crash_after_persist_and_restart(self):
        adapter = self.make_adapter("--crash-after-persist")
        adapter.start()
        with self.assertRaises(AdapterAmbiguousError):
            adapter.send_request(BatchRequest(1, (PutRequest(10, b"a", b"1"), PutRequest(11, b"b", b"2"))))
        adapter.restart()
        try:
            self.assertEqual(adapter.send_request(GetRequest(2, b"a")), ValueResponse(2, b"1"))
            self.assertEqual(adapter.send_request(GetRequest(3, b"b")), ValueResponse(3, b"2"))
        finally:
            adapter.graceful_shutdown()

    def test_unknown_outcome_fail_stops_adapter(self):
        adapter = self.make_adapter("--unknown-after-persist")
        adapter.start()
        try:
            response = adapter.send_request(PutRequest(1, b"x", b"1"))
            self.assertEqual(response.outcome, "UNKNOWN")
            with self.assertRaises(AdapterFailedError):
                adapter.send_request(GetRequest(2, b"x"))
        finally:
            adapter.crash()

    def test_delay_can_trigger_timeout(self):
        adapter = self.make_adapter("--delay-ms", "200", timeout=0.05)
        adapter.start()
        try:
            with self.assertRaises(AdapterRequestTimeoutError):
                adapter.send_request(PutRequest(1, b"slow", b"1"))
        finally:
            adapter.crash()

    def test_ready_file_remains_available_as_optional_test_signal(self):
        ready_file = self.database_dir / "ready.signal"
        adapter = self.make_adapter("--ready-file", str(ready_file))
        adapter.start()
        try:
            deadline = time.monotonic() + 1
            while time.monotonic() < deadline and not ready_file.exists():
                time.sleep(0.01)
            self.assertTrue(ready_file.exists())
        finally:
            adapter.graceful_shutdown()

    def test_malformed_requests_return_errors(self):
        command = [sys.executable, "-u", str(SIMULATOR_PATH), "--database-dir", str(self.database_dir)]
        process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding="utf-8")
        try:
            self.assertEqual(process.stderr.readline().strip(), "READY")
            process.stdin.write('{"id": 1, "op": "NOPE"}\nnot-json\n')
            process.stdin.flush()
            first = json.loads(process.stdout.readline())
            second = json.loads(process.stdout.readline())
            self.assertEqual(first["status"], "ERROR")
            self.assertEqual(second["status"], "ERROR")
        finally:
            process.stdin.close()
            process.wait(timeout=2)
            process.stdout.close()
            process.stderr.close()


if __name__ == "__main__":
    unittest.main()

import os
from pathlib import Path
import tempfile
import unittest

from oracle import Oracle
from process import ProcessConfig, SubprocessDatabaseAdapter
from protocol import BatchRequest, DeleteRequest, DumpRequest, GetRequest, NotFoundResponse, PutRequest, ValueResponse, Acknowledgement


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def go_executable() -> Path:
    configured = os.environ.get("KVDB_EXECUTABLE")
    return Path(configured) if configured else REPOSITORY_ROOT / "kvdb.exe"


class GoDatabaseIntegrationTests(unittest.TestCase):
    def setUp(self):
        executable = go_executable()
        if not executable.exists():
            self.skipTest(f"Go executable not found: {executable}; build kvdb.exe first")
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database_path = Path(self.temp_dir.name) / "database.kv"
        self.adapter = None

    def tearDown(self):
        if self.adapter is not None:
            self.adapter.graceful_shutdown()
        self.temp_dir.cleanup()

    def make_adapter(self):
        command = (str(go_executable()), "serve", str(self.database_path))
        return SubprocessDatabaseAdapter(ProcessConfig(command, Path(self.temp_dir.name), response_timeout_seconds=10.0))

    def test_crud_batch_dump_and_restart_against_oracle(self):
        oracle = Oracle()
        self.adapter = self.make_adapter()
        self.adapter.start()
        self.assertEqual(self.adapter.send_request(PutRequest("put", b"name", b"Ada")), Acknowledgement("put"))
        oracle.apply_put(b"name", b"Ada")
        self.assertEqual(self.adapter.send_request(GetRequest(2, b"name")), ValueResponse(2, b"Ada"))
        delete = DeleteRequest(3, b"name")
        self.assertEqual(self.adapter.send_request(delete), Acknowledgement(3))
        oracle.apply_delete(b"name")
        self.assertEqual(self.adapter.send_request(GetRequest(4, b"name")), NotFoundResponse(4))

        batch = BatchRequest("batch", (PutRequest("a", b"a", b"1"), PutRequest("b", b"b", b"2"), DeleteRequest("c", b"c")))
        self.assertEqual(self.adapter.send_request(batch), Acknowledgement("batch"))
        oracle.apply_batch(batch)
        dump = self.adapter.send_request(DumpRequest(5))
        oracle.verify_dump(dump)
        self.adapter.graceful_shutdown()
        self.adapter = self.make_adapter()
        self.adapter.start()
        oracle.verify_dump(self.adapter.send_request(DumpRequest(6)))

    def test_crash_and_restart_preserves_confirmed_state(self):
        self.adapter = self.make_adapter()
        self.adapter.start()
        self.assertEqual(self.adapter.send_request(PutRequest(1, b"persisted", b"value")), Acknowledgement(1))
        self.adapter.crash()
        self.adapter.restart()
        self.assertEqual(self.adapter.send_request(GetRequest(2, b"persisted")), ValueResponse(2, b"value"))


if __name__ == "__main__":
    unittest.main()

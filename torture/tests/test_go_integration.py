
import tempfile
import unittest
from pathlib import Path

from adapter import AdapterConfig
from controller import DatabaseController
from process import ProcessConfig, SubprocessDatabaseAdapter
from protocol import (
    PutRequest,
    GetRequest,
    DumpRequest,
    ValueResponse,
    NotFoundResponse,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
GO_BINARY = (PROJECT_ROOT / "kvdb").resolve()


class TestGoIntegration(unittest.TestCase):

    def setUp(self):
        if not GO_BINARY.exists():
            self.fail(
                f"Go database executable not found: {GO_BINARY}\n"
                "Build it first with: go build -o kvdb.exe ./src/cmd/kvdb"
            )

        self.temp_dir = tempfile.TemporaryDirectory()
        self.database_dir = Path(self.temp_dir.name)

        database_file = self.database_dir / "database.db"

        command = (
            str(GO_BINARY),
            "serve",
            str(database_file),
        )

        config = ProcessConfig(
            command=command,
            database_dir=self.database_dir,
            response_timeout_seconds=5.0,
            startup_timeout_seconds=5.0,
        )

        self.adapter = SubprocessDatabaseAdapter(config)
        self.controller = DatabaseController(self.adapter)
        self.controller.start()

    def tearDown(self):
        try:
            if self.controller.is_ready():
                self.controller.stop()
            else:
                self.adapter.stop()
        finally:
            self.temp_dir.cleanup()

    def test_go_server_starts(self):
        self.assertTrue(self.controller.is_ready())

    def test_put_and_get(self):
        put_response = self.adapter.execute(
            PutRequest("put-1", b"apple", b"red")
        )

        self.assertTrue(put_response.is_durable)

        get_response = self.adapter.execute(
            GetRequest("get-1", b"apple")
        )

        self.assertIsInstance(get_response, ValueResponse)
        self.assertEqual(get_response.value, b"red")

    def test_missing_key_returns_not_found(self):
        response = self.adapter.execute(
            GetRequest("missing-1", b"unknown")
        )

        self.assertIsInstance(response, NotFoundResponse)

    def test_data_survives_crash_and_restart(self):
        self.adapter.execute(
            PutRequest("put-before-crash", b"survivor", b"still-here")
        )

        self.controller.crash()
        self.controller.restart()

        response = self.adapter.execute(
            GetRequest("get-after-restart", b"survivor")
        )

        self.assertIsInstance(response, ValueResponse)
        self.assertEqual(response.value, b"still-here")

    def test_dump_returns_database_entries(self):
        self.adapter.execute(
            PutRequest("put-dump-1", b"alpha", b"one")
        )
        self.adapter.execute(
            PutRequest("put-dump-2", b"beta", b"two")
        )

        response = self.adapter.execute(DumpRequest("dump-1"))

        entries = {key: value for key, value in response.entries}

        self.assertEqual(entries[b"alpha"], b"one")
        self.assertEqual(entries[b"beta"], b"two")


if __name__ == "__main__":
    unittest.main()

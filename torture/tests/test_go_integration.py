
import tempfile
import unittest
import os
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


def go_executable() -> Path:
    configured = os.environ.get("KVDB_EXECUTABLE")
    candidates = [Path(configured)] if configured else []
    if not configured:
        names = ("kvdb.exe", "kvdb") if os.name == "nt" else ("kvdb", "kvdb.exe")
        candidates.extend(PROJECT_ROOT / name for name in names)

    for candidate in candidates:
        resolved = candidate.expanduser().resolve()
        if resolved.is_file():
            return resolved

    searched = ", ".join(str(path.expanduser().resolve()) for path in candidates)
    raise FileNotFoundError(
        "Go database executable not found. Set KVDB_EXECUTABLE or build one of: "
        f"{searched}"
    )


class TestGoIntegration(unittest.TestCase):

    def setUp(self):
        try:
            go_binary = go_executable()
        except FileNotFoundError as exc:
            self.fail(str(exc))

        self.temp_dir = tempfile.TemporaryDirectory()
        self.database_dir = Path(self.temp_dir.name)

        database_file = self.database_dir / "database.db"

        command = (
            str(go_binary),
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

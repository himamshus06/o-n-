
from pathlib import Path
import sys
import tempfile
import unittest

from controller import DatabaseController
from process import ProcessConfig, SubprocessDatabaseAdapter
from protocol import (
    DumpRequest,
    DumpResponse,
    GetRequest,
    NotFoundResponse,
    PutRequest,
    ValueResponse,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SIMULATOR_PATH = PROJECT_ROOT / "torture" / "simulator.py"


class TestSimulatorIntegration(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        database_dir = Path(self.temp_dir.name)

        command = (
            sys.executable,
            str(SIMULATOR_PATH),
            "--database-dir",
            str(database_dir),
        )

        config = ProcessConfig(
            command=command,
            database_dir=database_dir,
            startup_timeout_seconds=5.0,
            response_timeout_seconds=5.0,
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
        except Exception:
            # Make a best-effort cleanup if a test fails.
            try:
                self.adapter.crash()
            except Exception:
                pass

        self.temp_dir.cleanup()

    def test_simulator_starts_successfully(self):
        self.assertTrue(self.controller.is_ready())

    def test_put_and_get(self):
        put_response = self.adapter.execute(
            PutRequest("put-1", b"apple", b"red")
        )

        self.assertEqual(put_response.request_id, "put-1")
        self.assertTrue(put_response.is_durable)

        get_response = self.adapter.execute(
            GetRequest("get-1", b"apple")
        )

        self.assertIsInstance(get_response, ValueResponse)
        self.assertEqual(get_response.value, b"red")

    def test_missing_key_returns_not_found(self):
        response = self.adapter.execute(
            GetRequest("get-missing", b"unknown")
        )

        self.assertIsInstance(response, NotFoundResponse)

    def test_data_survives_crash_and_restart(self):
        # Write data and wait for its durable acknowledgement.
        self.adapter.execute(
            PutRequest("put-persistent", b"fruit", b"mango")
        )

        # Abruptly terminate the simulator.
        self.controller.crash()

        self.assertFalse(self.controller.is_ready())

        # Start a new process using the same database directory.
        self.controller.start()

        self.assertTrue(self.controller.is_ready())

        # The new process must recover the previously persisted value.
        response = self.adapter.execute(
            GetRequest("get-persistent", b"fruit")
        )

        self.assertIsInstance(response, ValueResponse)
        self.assertEqual(response.value, b"mango")

    def test_dump_returns_database_entries(self):
        self.adapter.execute(
            PutRequest("put-a", b"apple", b"red")
        )

        self.adapter.execute(
            PutRequest("put-b", b"banana", b"yellow")
        )

        response = self.adapter.execute(
            DumpRequest("dump-1")
        )

        self.assertIsInstance(response, DumpResponse)

        self.assertEqual(
            response.entries,
            (
                (b"apple", b"red"),
                (b"banana", b"yellow"),
            ),
        )


if __name__ == "__main__":
    unittest.main()


import unittest

from controller import (
    ControllerError,
    DatabaseController,
    DatabaseState,
)


class FakeAdapter:
    """A tiny database adapter for testing lifecycle behaviour."""

    def __init__(self):
        self.running = False
        self.start_count = 0
        self.stop_count = 0
        self.crash_count = 0

    def start(self):
        self.running = True
        self.start_count += 1

    def stop(self):
        self.running = False
        self.stop_count += 1

    def crash(self):
        self.running = False
        self.crash_count += 1

    def readiness(self):
        return self.running

    def execute(self, request):
        if not self.running:
            raise RuntimeError("Database is not running")
        return None


class TestDatabaseController(unittest.TestCase):

    def setUp(self):
        self.adapter = FakeAdapter()
        self.controller = DatabaseController(self.adapter)

    def test_initial_state_is_stopped(self):
        self.assertEqual(
            self.controller.state,
            DatabaseState.STOPPED,
        )

    def test_start_database(self):
        self.controller.start()

        self.assertEqual(
            self.controller.state,
            DatabaseState.READY,
        )
        self.assertTrue(self.controller.is_ready())
        self.assertEqual(self.adapter.start_count, 1)

    def test_stop_database(self):
        self.controller.start()
        self.controller.stop()

        self.assertEqual(
            self.controller.state,
            DatabaseState.STOPPED,
        )
        self.assertFalse(self.controller.is_ready())
        self.assertEqual(self.adapter.stop_count, 1)

    def test_crash_database(self):
        self.controller.start()
        self.controller.crash()

        self.assertEqual(
            self.controller.state,
            DatabaseState.CRASHED,
        )
        self.assertEqual(self.adapter.crash_count, 1)

    def test_restart_after_crash(self):
        self.controller.start()
        self.controller.crash()
        self.controller.restart()

        self.assertEqual(
            self.controller.state,
            DatabaseState.READY,
        )
        self.assertTrue(self.controller.is_ready())
        self.assertEqual(self.adapter.start_count, 2)

    def test_restart_while_running(self):
        self.controller.start()
        self.controller.restart()

        self.assertEqual(
            self.controller.state,
            DatabaseState.READY,
        )
        self.assertEqual(self.adapter.crash_count, 1)
        self.assertEqual(self.adapter.start_count, 2)

    def test_cannot_crash_stopped_database(self):
        with self.assertRaises(ControllerError):
            self.controller.crash()

    def test_cannot_start_already_running_database(self):
        self.controller.start()

        with self.assertRaises(ControllerError):
            self.controller.start()

    def test_context_manager_starts_and_stops(self):
        with DatabaseController(self.adapter) as controller:
            self.assertTrue(controller.is_ready())

        self.assertEqual(
            controller.state,
            DatabaseState.STOPPED,
        )
        self.assertEqual(self.adapter.stop_count, 1)


if __name__ == "__main__":
    unittest.main()

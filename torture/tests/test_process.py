import json
from pathlib import Path
import sys
import tempfile
import textwrap
import time
import unittest

from adapter import AdapterAmbiguousError, AdapterCommunicationError, AdapterRequestTimeoutError
from process import ProcessConfig, SubprocessDatabaseAdapter
from protocol import Acknowledgement, Durability, PutRequest


SIMULATOR = textwrap.dedent(
    r'''
    import json, os, sys, time
    mode = os.environ.get("SIM_MODE", "normal")
    if mode == "exit-before-ready":
        sys.exit(9)
    if mode == "diagnostic":
        print("recovery diagnostic", file=sys.stderr, flush=True)
    if mode != "no-ready":
        print("READY", file=sys.stderr, flush=True)
    for line in sys.stdin:
        request = json.loads(line)
        if mode == "timeout":
            time.sleep(2)
        elif mode == "no-ready" and os.environ.get("REQUEST_SENTINEL"):
            with open(os.environ["REQUEST_SENTINEL"], "w") as marker:
                marker.write("request received")
        elif mode == "malformed":
            print("this is not json", flush=True)
            continue
        elif mode == "exit":
            sys.exit(3)
        elif mode == "unknown":
            print(json.dumps({"id": request["id"], "status": "ERROR", "outcome": "UNKNOWN", "error": "uncertain"}), flush=True)
            continue
        elif mode == "stderr":
            for _ in range(5000):
                print("noise", file=sys.stderr, flush=True)
        elif mode == "durable":
            pass
        print(json.dumps({"id": request["id"], "status": "ACK"}), flush=True)
    '''
)


class ProcessAdapterTests(unittest.TestCase):
    def make_adapter(self, mode="normal", timeout=0.5):
        self.temp_dir = tempfile.TemporaryDirectory()
        command = (sys.executable, "-u", "-c", SIMULATOR)
        config = ProcessConfig(command, Path(self.temp_dir.name), response_timeout_seconds=timeout)
        adapter = SubprocessDatabaseAdapter(config)
        adapter._test_env = mode
        return adapter

    def start_adapter(self, mode="normal", timeout=0.5):
        adapter = self.make_adapter(mode, timeout)
        # The simulator reads this environment variable from the inherited environment.
        # Use a command wrapper to keep ProcessConfig itself platform-neutral.
        command = (sys.executable, "-u", "-c", SIMULATOR)
        adapter.process_config = ProcessConfig(command, adapter.process_config.database_dir, timeout)
        import os
        self._old_mode = os.environ.get("SIM_MODE")
        os.environ["SIM_MODE"] = mode
        adapter.start()
        return adapter

    def tearDown(self):
        import os
        if hasattr(self, "_old_mode"):
            if self._old_mode is None:
                os.environ.pop("SIM_MODE", None)
            else:
                os.environ["SIM_MODE"] = self._old_mode
        if hasattr(self, "temp_dir"):
            self.temp_dir.cleanup()

    def test_normal_request_and_durable_ack(self):
        adapter = self.start_adapter("durable")
        try:
            response = adapter.send_request(PutRequest(1, b"x", b"value"))
            self.assertIsInstance(response, Acknowledgement)
            self.assertEqual(response.durability, Durability.DURABLE)
        finally:
            adapter.graceful_shutdown()

    def test_stderr_is_drained(self):
        adapter = self.start_adapter("stderr")
        try:
            adapter.send_request(PutRequest(1, b"x", b"value"))
            self.assertGreater(len(adapter.stderr_lines), 0)
        finally:
            adapter.graceful_shutdown()

    def test_timeout_is_ambiguous(self):
        adapter = self.start_adapter("timeout", timeout=0.1)
        try:
            with self.assertRaises(AdapterRequestTimeoutError):
                adapter.send_request(PutRequest(1, b"x", b"value"))
        finally:
            adapter.crash()

    def test_malformed_response_is_communication_error(self):
        adapter = self.start_adapter("malformed")
        try:
            with self.assertRaises(AdapterCommunicationError):
                adapter.send_request(PutRequest(1, b"x", b"value"))
        finally:
            adapter.crash()

    def test_unexpected_exit_makes_request_ambiguous(self):
        adapter = self.start_adapter("exit")
        try:
            with self.assertRaises(AdapterAmbiguousError):
                adapter.send_request(PutRequest(1, b"x", b"value"))
        finally:
            adapter.crash()

    def test_crash_and_restart_reuses_directory(self):
        adapter = self.start_adapter("normal")
        adapter.crash()
        self.assertFalse(adapter.is_running())
        adapter.restart()
        try:
            response = adapter.send_request(PutRequest(2, b"x", b"value"))
            self.assertIsInstance(response, Acknowledgement)
        finally:
            adapter.graceful_shutdown()

    def test_startup_timeout_when_ready_never_arrives(self):
        adapter = self.make_adapter(timeout=0.05)
        import os
        old_mode = os.environ.get("SIM_MODE")
        sentinel = str(Path(self.temp_dir.name) / "request-received")
        old_sentinel = os.environ.get("REQUEST_SENTINEL")
        os.environ["SIM_MODE"] = "no-ready"
        os.environ["REQUEST_SENTINEL"] = sentinel
        try:
            with self.assertRaises(Exception):
                adapter.start()
            self.assertFalse(Path(sentinel).exists())
        finally:
            if old_mode is None:
                os.environ.pop("SIM_MODE", None)
            else:
                os.environ["SIM_MODE"] = old_mode
            if old_sentinel is None:
                os.environ.pop("REQUEST_SENTINEL", None)
            else:
                os.environ["REQUEST_SENTINEL"] = old_sentinel

    def test_process_exit_before_ready_is_reported(self):
        adapter = self.make_adapter("exit-before-ready", timeout=0.2)
        import os
        old_mode = os.environ.get("SIM_MODE")
        os.environ["SIM_MODE"] = "exit-before-ready"
        try:
            with self.assertRaises(Exception):
                adapter.start()
        finally:
            if old_mode is None:
                os.environ.pop("SIM_MODE", None)
            else:
                os.environ["SIM_MODE"] = old_mode

    def test_stderr_diagnostics_are_preserved_while_waiting(self):
        adapter = self.start_adapter("diagnostic")
        try:
            self.assertIn("recovery diagnostic", adapter.stderr_lines)
        finally:
            adapter.graceful_shutdown()

    def test_unknown_outcome_fails_adapter_until_restart(self):
        from adapter import AdapterFailedError
        from protocol import ErrorResponse
        adapter = self.start_adapter("unknown")
        try:
            response = adapter.send_request(PutRequest(99, b"x", b"v"))
            self.assertIsInstance(response, ErrorResponse)
            self.assertEqual(response.outcome, "UNKNOWN")
            with self.assertRaises(AdapterFailedError):
                adapter.send_request(PutRequest(100, b"x", b"v"))
        finally:
            adapter.crash()


if __name__ == "__main__":
    unittest.main()

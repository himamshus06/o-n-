import unittest

from adapter import (
    AdapterConfig,
    AdapterNotReadyError,
    DatabaseAdapter,
    response_is_durable_ack,
)
from protocol import Acknowledgement, GetRequest, Response


class FakeAdapter(DatabaseAdapter):
    def start(self):
        self._ready = True

    def stop(self):
        self._ready = False

    def crash(self):
        self._ready = False

    def execute(self, request) -> Response:
        self.require_ready()
        return Acknowledgement(request.request_id)

    def readiness(self):
        return self._ready


class AdapterTests(unittest.TestCase):
    def test_config_validates_timeout(self):
        with self.assertRaises(ValueError):
            AdapterConfig("db", request_timeout_seconds=0)

    def test_adapter_boundary_requires_readiness(self):
        adapter = FakeAdapter(AdapterConfig("simulator"))
        with self.assertRaises(AdapterNotReadyError):
            adapter.execute(GetRequest(1, b"x"))
        adapter.start()
        response = adapter.execute(GetRequest(1, b"x"))
        self.assertTrue(response_is_durable_ack(response))
        adapter.crash()
        self.assertFalse(adapter.readiness())

    def test_durable_predicate_does_not_accept_other_responses(self):
        self.assertFalse(response_is_durable_ack(GetRequest(1, b"x")))


if __name__ == "__main__":
    unittest.main()

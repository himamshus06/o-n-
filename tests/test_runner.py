
import unittest

from adapter import AdapterAmbiguousError
from oracle import Oracle
from protocol import (
    Acknowledgement,
    ErrorResponse,
    GetRequest,
    NotFoundResponse,
    PutRequest,
    ValueResponse,
)

from runner import WorkloadRunner


class FakeAdapter:
    """A small adapter that returns predefined responses."""

    def __init__(self, responses=None, ready=True):
        self.responses = list(responses or [])
        self.is_ready = ready
        self.requests_received = []

    def readiness(self):
        return self.is_ready

    def execute(self, request):
        self.requests_received.append(request)

        response = self.responses.pop(0)

        if isinstance(response, Exception):
            raise response

        return response


class TestWorkloadRunner(unittest.TestCase):

    def test_successful_put_updates_oracle(self):
        request = PutRequest(
            request_id="put-1",
            key=b"apple",
            value=b"red",
        )

        adapter = FakeAdapter([
            Acknowledgement(request_id="put-1")
        ])

        oracle = Oracle()
        runner = WorkloadRunner(adapter, oracle)

        result = runner.run([request])

        self.assertTrue(result.successful)
        self.assertEqual(oracle.expected_value(b"apple"), b"red")
        self.assertEqual(result.confirmed_writes, 1)
        self.assertEqual(result.requests_sent, 1)

    def test_successful_get_is_verified(self):
        oracle = Oracle()
        oracle.apply_put(b"apple", b"red")

        request = GetRequest(
            request_id="get-1",
            key=b"apple",
        )

        adapter = FakeAdapter([
            ValueResponse(
                request_id="get-1",
                value=b"red",
            )
        ])

        runner = WorkloadRunner(adapter, oracle)
        result = runner.run([request])

        self.assertTrue(result.successful)
        self.assertEqual(result.reads_checked, 1)

    def test_missing_key_get_is_verified(self):
        request = GetRequest(
            request_id="get-2",
            key=b"missing",
        )

        adapter = FakeAdapter([
            NotFoundResponse(request_id="get-2")
        ])

        runner = WorkloadRunner(adapter, Oracle())
        result = runner.run([request])

        self.assertTrue(result.successful)
        self.assertEqual(result.reads_checked, 1)

    def test_wrong_get_value_is_detected(self):
        oracle = Oracle()
        oracle.apply_put(b"apple", b"red")

        request = GetRequest(
            request_id="get-3",
            key=b"apple",
        )

        adapter = FakeAdapter([
            ValueResponse(
                request_id="get-3",
                value=b"blue",
            )
        ])

        runner = WorkloadRunner(adapter, oracle)
        result = runner.run([request])

        self.assertFalse(result.successful)
        self.assertEqual(len(result.failures), 1)
        self.assertTrue(result.stopped_early)

    def test_response_id_mismatch_is_detected(self):
        request = PutRequest(
            request_id="put-2",
            key=b"apple",
            value=b"red",
        )

        adapter = FakeAdapter([
            Acknowledgement(request_id="wrong-id")
        ])

        oracle = Oracle()
        runner = WorkloadRunner(adapter, oracle)

        result = runner.run([request])

        self.assertFalse(result.successful)
        self.assertIsNone(oracle.expected_value(b"apple"))

    def test_ambiguous_outcome_stops_execution(self):
        first_request = PutRequest(
            request_id="put-3",
            key=b"apple",
            value=b"red",
        )

        second_request = GetRequest(
            request_id="get-4",
            key=b"apple",
        )

        adapter = FakeAdapter([
            AdapterAmbiguousError("Commit outcome uncertain"),
            NotFoundResponse(request_id="get-4"),
        ])

        runner = WorkloadRunner(adapter, Oracle())
        result = runner.run([first_request, second_request])

        self.assertFalse(result.successful)
        self.assertTrue(result.stopped_early)
        self.assertTrue(result.failures[0].ambiguous)
        self.assertEqual(len(adapter.requests_received), 1)

    def test_unexpected_write_response_is_detected(self):
        request = PutRequest(
            request_id="put-4",
            key=b"apple",
            value=b"red",
        )

        adapter = FakeAdapter([
            ErrorResponse(
                request_id="put-4",
                error="Invalid request",
            )
        ])

        oracle = Oracle()
        runner = WorkloadRunner(adapter, oracle)

        result = runner.run([request])

        self.assertFalse(result.successful)
        self.assertIsNone(oracle.expected_value(b"apple"))

    def test_unready_adapter_rejects_workload(self):
        request = GetRequest(
            request_id="get-5",
            key=b"apple",
        )

        adapter = FakeAdapter(ready=False)
        runner = WorkloadRunner(adapter, Oracle())

        result = runner.run([request])

        self.assertFalse(result.successful)
        self.assertTrue(result.stopped_early)
        self.assertEqual(len(adapter.requests_received), 0)

    def test_report_contains_execution_summary(self):
        request = PutRequest(
            request_id="put-5",
            key=b"apple",
            value=b"red",
        )

        adapter = FakeAdapter([
            Acknowledgement(request_id="put-5")
        ])

        runner = WorkloadRunner(adapter, Oracle())
        result = runner.run([request])

        report = result.report()

        self.assertIn("WORKLOAD REPORT", report)
        self.assertIn("Requests sent:     1", report)
        self.assertIn("Confirmed writes:  1", report)
        self.assertIn("Successful:        True", report)


if __name__ == "__main__":
    unittest.main()

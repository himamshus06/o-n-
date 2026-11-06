
import unittest

from fault_scheduler import (
    FaultPlan,
    FaultScheduler,
    FaultScheduleError,
)
from runner import RunFailure, RunResult


class FakeController:
    """Simulates database start, crash, and readiness."""

    def __init__(self):
        self.ready = False
        self.start_count = 0
        self.crash_count = 0
        self.fail_start = False

    def start(self):
        if self.fail_start:
            raise RuntimeError("Simulated startup failure")

        self.ready = True
        self.start_count += 1

    def crash(self):
        if not self.ready:
            raise RuntimeError("Database is not running")

        self.ready = False
        self.crash_count += 1

    def is_ready(self):
        return self.ready


class FakeRunner:
    """Simulates workload execution."""

    def __init__(self):
        self.calls = []
        self.fail_on_call = None

    def run(self, requests):
        requests = list(requests)
        self.calls.append(requests)

        call_number = len(self.calls)

        if call_number == self.fail_on_call:
            return RunResult(
                requests_sent=1,
                confirmed_writes=0,
                reads_checked=0,
                dumps_checked=0,
                failures=[
                    RunFailure(
                        request_id="failed-request",
                        message="Simulated workload failure",
                    )
                ],
                stopped_early=True,
            )

        return RunResult(
            requests_sent=len(requests),
            confirmed_writes=0,
            reads_checked=0,
            dumps_checked=0,
            failures=[],
            stopped_early=False,
        )


class TestFaultPlan(unittest.TestCase):

    def test_empty_plan_is_valid(self):
        plan = FaultPlan()

        self.assertEqual(plan.crash_after_requests, ())

    def test_valid_plan_is_preserved(self):
        plan = FaultPlan((2, 5, 10))

        self.assertEqual(
            plan.crash_after_requests,
            (2, 5, 10),
        )

    def test_zero_position_is_rejected(self):
        with self.assertRaises(FaultScheduleError):
            FaultPlan((0,))

    def test_negative_position_is_rejected(self):
        with self.assertRaises(FaultScheduleError):
            FaultPlan((-1,))

    def test_duplicate_positions_are_rejected(self):
        with self.assertRaises(FaultScheduleError):
            FaultPlan((2, 2))

    def test_unsorted_positions_are_rejected(self):
        with self.assertRaises(FaultScheduleError):
            FaultPlan((5, 2))


class TestFaultScheduler(unittest.TestCase):

    def setUp(self):
        self.controller = FakeController()
        self.runner = FakeRunner()

        self.scheduler = FaultScheduler(
            self.controller,
            self.runner,
        )

        # Each request can be a simple placeholder for these tests.
        self.requests = [
            {"id": str(i), "op": "GET"}
            for i in range(1, 6)
        ]

    def test_empty_schedule_runs_entire_workload(self):
        result = self.scheduler.run(
            self.requests,
            FaultPlan(),
        )

        self.assertTrue(result.successful)
        self.assertEqual(result.workload.requests_sent, 5)
        self.assertEqual(len(result.fault_events), 0)

    def test_one_scheduled_crash(self):
        result = self.scheduler.run(
            self.requests,
            FaultPlan((2,)),
        )

        self.assertTrue(result.successful)
        self.assertEqual(len(result.fault_events), 1)
        self.assertEqual(self.controller.crash_count, 1)
        self.assertEqual(self.controller.start_count, 2)

    def test_multiple_scheduled_crashes(self):
        result = self.scheduler.run(
            self.requests,
            FaultPlan((1, 3)),
        )

        self.assertTrue(result.successful)
        self.assertEqual(len(result.fault_events), 2)
        self.assertEqual(self.controller.crash_count, 2)
        self.assertEqual(self.controller.start_count, 3)

    def test_crash_event_records_successful_recovery(self):
        result = self.scheduler.run(
            self.requests,
            FaultPlan((2,)),
        )

        event = result.fault_events[0]

        self.assertTrue(event.crash_succeeded)
        self.assertTrue(event.restart_succeeded)
        self.assertIsNone(event.error)

    def test_crash_after_final_request(self):
        result = self.scheduler.run(
            self.requests,
            FaultPlan((5,)),
        )

        self.assertTrue(result.successful)
        self.assertEqual(result.workload.requests_sent, 5)
        self.assertEqual(len(result.fault_events), 1)

    def test_position_beyond_workload_is_rejected(self):
        with self.assertRaises(FaultScheduleError):
            self.scheduler.run(
                self.requests,
                FaultPlan((6,)),
            )

    def test_workload_failure_stops_scheduling(self):
        self.runner.fail_on_call = 1

        result = self.scheduler.run(
            self.requests,
            FaultPlan((2,)),
        )

        self.assertFalse(result.successful)
        self.assertEqual(len(result.fault_events), 0)

    def test_restart_failure_is_recorded(self):
        # The initial start succeeds. The restart after the crash fails.
        self.controller.fail_start = False

        original_start = self.controller.start
        start_calls = 0

        def failing_second_start():
            nonlocal start_calls
            start_calls += 1

            if start_calls == 2:
                raise RuntimeError("Simulated restart failure")

            original_start()

        self.controller.start = failing_second_start

        result = self.scheduler.run(
            self.requests,
            FaultPlan((2,)),
        )

        self.assertFalse(result.successful)
        self.assertEqual(len(result.fault_events), 1)
        self.assertTrue(result.fault_events[0].crash_succeeded)
        self.assertFalse(result.fault_events[0].restart_succeeded)
        self.assertIsNotNone(result.fault_events[0].error)

    def test_report_contains_fault_information(self):
        result = self.scheduler.run(
            self.requests,
            FaultPlan((2,)),
        )

        report = result.report()

        self.assertIn("FAULT SCHEDULE REPORT", report)
        self.assertIn("Scheduled crashes: 1", report)
        self.assertIn("Overall successful: True", report)


if __name__ == "__main__":
    unittest.main()

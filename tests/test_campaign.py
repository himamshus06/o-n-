import unittest
from pathlib import Path
from unittest.mock import patch

import campaign
from campaign import (
    CampaignReport,
    ENOSPC_BATCH_BUDGET,
    ENOSPC_BATCH_VALUE_SIZE,
    finalize_not_tested,
    build_enospc_batch_request,
    build_large_batch_request,
    render_markdown,
)
from protocol import Acknowledgement, PutRequest


class CampaignConstructionTests(unittest.TestCase):
    def test_markdown_represents_every_report_field(self):
        report = CampaignReport(
            started_utc="started",
            seed=7,
            iterations=3,
            modes=["default"],
            passed=["passed-case"],
            failed=[{"mode": "failed-case", "error": "failure"}],
            not_tested=["not-tested-case"],
            simulated_only=["simulated-case"],
            environment_blocked=["blocked-case"],
            crashes=1,
            restarts=2,
            acknowledged_writes=3,
            failed_operations=4,
            recovery_failures=5,
            oracle_mismatches=6,
            operations_attempted=7,
            unknown_outcomes=8,
            integrity_check_failures=9,
            batch_atomicity_failures=10,
            crash_events=[{"kind": "test-crash", "delay_ms": 1}],
            finished_utc="finished",
            duration_seconds=1.25,
        )

        markdown = render_markdown(report)

        for value in (
            "Started UTC: `started`",
            "Finished UTC: `finished`",
            "- default",
            "- passed-case",
            "- failed-case: failure",
            "- not-tested-case",
            "- simulated-case",
            "- blocked-case",
            "Operations attempted: 7",
            "Acknowledged writes: 3",
            "Failed operations: 4",
            "UNKNOWN outcomes: 8",
            "Crashes: 1",
            "Restarts: 2",
            "Recovery failures: 5",
            "Oracle mismatches: 6",
            "Integrity-check failures: 9",
            "Batch-atomicity failures: 10",
            '"kind": "test-crash"',
        ):
            self.assertIn(value, markdown)

    def test_large_batch_contains_exactly_10000_operations(self):
        batch = build_large_batch_request()
        self.assertEqual(len(batch.operations), 10_000)

    def test_enospc_batch_setup_is_partway_and_distinct(self):
        batch = build_enospc_batch_request()
        self.assertEqual(len(batch.operations), 3)
        self.assertEqual(
            {operation.key for operation in batch.operations},
            {
                b"enospc-batch-key-0",
                b"enospc-batch-key-1",
                b"enospc-batch-key-2",
            },
        )
        self.assertEqual(
            {len(operation.value) for operation in batch.operations},
            {ENOSPC_BATCH_VALUE_SIZE},
        )
        self.assertGreater(ENOSPC_BATCH_BUDGET, 0)

    def test_completed_recovery_and_enospc_modes_are_not_untested(self):
        report = CampaignReport(
            started_utc="",
            seed=1,
            iterations=500,
            modes=["recovery-crash", "injected-enospc"],
            passed=[
                "recovery-started-recovery-crash",
                "recovery-transaction-applied-recovery-crash",
                "injected-enospc",
                "injected-enospc-batch-atomicity",
            ],
            failed=[],
            not_tested=[
                "recovery-crash mode",
                "disk-full/ENOSPC mode",
            ],
            simulated_only=[],
            environment_blocked=[],
        )

        finalize_not_tested(report)

        self.assertNotIn("recovery-crash mode", report.not_tested)
        self.assertNotIn("disk-full/ENOSPC mode", report.not_tested)
        self.assertIn("physical disk exhaustion", report.not_tested)
        self.assertIn("linearizability checking", report.not_tested)

    def test_missing_executable_blocker_is_preserved(self):
        report = CampaignReport(
            started_utc="",
            seed=1,
            iterations=500,
            modes=["recovery-crash"],
            passed=[],
            failed=[],
            not_tested=[
                "recovery-crash: fault-injection executable is unavailable",
                "injected-enospc: executable is unavailable",
            ],
            simulated_only=[],
            environment_blocked=[],
        )

        finalize_not_tested(report)

        self.assertIn(
            "recovery-crash: fault-injection executable is unavailable",
            report.not_tested,
        )
        self.assertIn("injected-enospc: executable is unavailable", report.not_tested)

    def test_unselected_modes_are_not_reported_as_results(self):
        report = CampaignReport(
            started_utc="",
            seed=1,
            iterations=500,
            modes=["default"],
            passed=["default-real-binary-workload"],
            failed=[],
            not_tested=[],
            simulated_only=[],
            environment_blocked=[],
        )

        finalize_not_tested(report)

        self.assertNotIn("recovery-crash mode", report.not_tested)
        self.assertNotIn("disk-full/ENOSPC mode", report.not_tested)
        self.assertNotIn("recovery-crash", report.passed)
        self.assertFalse(
            any(
                failure.get("mode") == "injected-enospc"
                for failure in report.failed
            )
        )

    def test_random_crash_counts_each_submitted_request_once(self):
        requests = [
            PutRequest(f"request-{index}", f"key-{index}".encode(), b"value")
            for index in range(3)
        ]

        class FakeAdapter:
            def execute(self, request):
                return Acknowledgement(request.request_id)

        class FakeController:
            def __init__(self, adapter):
                self.adapter = adapter

            def start(self):
                pass

            def stop(self):
                pass

            def is_ready(self):
                return True

        class NoOpTimer:
            def __init__(self, target, **kwargs):
                self.target = target

            def start(self):
                pass

            def join(self, timeout=None):
                pass

        report = CampaignReport(
            started_utc="",
            seed=7,
            iterations=len(requests),
            modes=["random-crash"],
            passed=[],
            failed=[],
            not_tested=[],
            simulated_only=[],
            environment_blocked=[],
        )

        with patch.object(campaign, "DatabaseController", FakeController), \
                patch.object(campaign, "_adapter", return_value=FakeAdapter()), \
                patch.object(campaign, "WorkloadGenerator") as generator, \
                patch.object(campaign.threading, "Thread", NoOpTimer), \
                patch.object(campaign, "_check"), \
                patch.object(campaign, "_verify_permitted_states"):
            generator.return_value.generate.return_value = requests
            campaign._run_random_crash(Path("kvdb"), 7, len(requests), report)

        self.assertEqual(report.operations_attempted, len(requests))


if __name__ == "__main__":
    unittest.main()

import unittest
from pathlib import Path
from unittest.mock import patch

import campaign
from campaign import (
    CampaignReport,
    ENOSPC_BATCH_BUDGET,
    ENOSPC_BATCH_VALUE_SIZE,
    build_enospc_batch_request,
    build_large_batch_request,
)
from protocol import Acknowledgement, PutRequest


class CampaignConstructionTests(unittest.TestCase):
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

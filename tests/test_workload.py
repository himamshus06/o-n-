
import unittest

from protocol import (
    BatchRequest,
    DeleteRequest,
    GetRequest,
    PutRequest,
)

from workload import WorkloadConfig, WorkloadGenerator


class TestWorkloadGenerator(unittest.TestCase):

    def test_generates_requested_number_of_operations(self):
        config = WorkloadConfig(
            seed=42,
            num_operations=100,
        )

        requests = WorkloadGenerator(config).generate()

        self.assertEqual(len(requests), 100)

    def test_same_seed_produces_same_workload(self):
        config = WorkloadConfig(
            seed=42,
            num_operations=100,
        )

        first = WorkloadGenerator(config).generate()
        second = WorkloadGenerator(config).generate()

        first_data = [request.to_dict() for request in first]
        second_data = [request.to_dict() for request in second]

        self.assertEqual(first_data, second_data)

    def test_different_seeds_produce_different_workloads(self):
        config_a = WorkloadConfig(
            seed=42,
            num_operations=100,
        )

        config_b = WorkloadConfig(
            seed=100,
            num_operations=100,
        )

        first = WorkloadGenerator(config_a).generate()
        second = WorkloadGenerator(config_b).generate()

        first_data = [request.to_dict() for request in first]
        second_data = [request.to_dict() for request in second]

        self.assertNotEqual(first_data, second_data)

    def test_request_ids_are_unique(self):
        config = WorkloadConfig(
            seed=42,
            num_operations=100,
        )

        requests = WorkloadGenerator(config).generate()

        all_ids = []

        for request in requests:
            all_ids.append(request.request_id)

            if isinstance(request, BatchRequest):
                for operation in request.operations:
                    all_ids.append(operation.request_id)

        self.assertEqual(len(all_ids), len(set(all_ids)))

    def test_all_single_operation_types_can_be_generated(self):
        config = WorkloadConfig(
            seed=42,
            num_operations=1000,
            batch_probability=0.0,
        )

        requests = WorkloadGenerator(config).generate()

        operation_types = {
            type(request)
            for request in requests
        }

        self.assertIn(PutRequest, operation_types)
        self.assertIn(GetRequest, operation_types)
        self.assertIn(DeleteRequest, operation_types)

    def test_batches_can_be_generated(self):
        config = WorkloadConfig(
            seed=42,
            num_operations=100,
            batch_probability=1.0,
        )

        requests = WorkloadGenerator(config).generate()

        self.assertTrue(
            all(isinstance(request, BatchRequest) for request in requests)
        )

    def test_batch_contains_only_put_and_delete(self):
        config = WorkloadConfig(
            seed=42,
            num_operations=100,
            batch_probability=1.0,
        )

        requests = WorkloadGenerator(config).generate()

        for request in requests:
            self.assertIsInstance(request, BatchRequest)

            for operation in request.operations:
                self.assertIsInstance(
                    operation,
                    (PutRequest, DeleteRequest),
                )

    def test_batch_size_stays_within_configured_limit(self):
        config = WorkloadConfig(
            seed=42,
            num_operations=100,
            batch_probability=1.0,
            max_batch_size=5,
        )

        requests = WorkloadGenerator(config).generate()

        for request in requests:
            self.assertGreaterEqual(len(request.operations), 1)
            self.assertLessEqual(len(request.operations), 5)

    def test_invalid_operation_count_is_rejected(self):
        with self.assertRaises(ValueError):
            WorkloadConfig(num_operations=0)

    def test_invalid_keyspace_size_is_rejected(self):
        with self.assertRaises(ValueError):
            WorkloadConfig(keyspace_size=0)

    def test_invalid_value_size_is_rejected(self):
        with self.assertRaises(ValueError):
            WorkloadConfig(max_value_size=0)

    def test_invalid_batch_probability_is_rejected(self):
        with self.assertRaises(ValueError):
            WorkloadConfig(batch_probability=1.5)

    def test_invalid_batch_size_is_rejected(self):
        with self.assertRaises(ValueError):
            WorkloadConfig(max_batch_size=0)


if __name__ == "__main__":
    unittest.main()


from __future__ import annotations

import random
from dataclasses import dataclass

from protocol import (
    BatchRequest,
    DeleteRequest,
    GetRequest,
    PutRequest,
)


@dataclass(frozen=True, slots=True)
class WorkloadConfig:
    """Settings controlling how a workload is generated."""

    seed: int = 42
    # The problem statement's default torture run is 500 top-level requests.
    num_operations: int = 500
    keyspace_size: int = 20
    max_value_size: int = 100
    batch_probability: float = 0.15
    max_batch_size: int = 5

    def __post_init__(self) -> None:
        if self.num_operations < 1:
            raise ValueError("num_operations must be at least 1")

        if self.keyspace_size < 1:
            raise ValueError("keyspace_size must be at least 1")

        if self.max_value_size < 1:
            raise ValueError("max_value_size must be at least 1")

        if not 0.0 <= self.batch_probability <= 1.0:
            raise ValueError("batch_probability must be between 0 and 1")

        if self.max_batch_size < 1:
            raise ValueError("max_batch_size must be at least 1")


class WorkloadGenerator:
    """Generates deterministic database request sequences."""

    def __init__(self, config: WorkloadConfig) -> None:
        self.config = config
        self.rng = random.Random(config.seed)

        # Used to ensure every request gets a unique ID.
        self._request_counter = 0

    def _next_id(self) -> str:
        self._request_counter += 1
        return f"workload-{self.config.seed}-{self._request_counter}"

    def _random_key(self) -> bytes:
        key_number = self.rng.randrange(self.config.keyspace_size)
        return f"key-{key_number:04d}".encode("utf-8")

    def _random_value(self) -> bytes:
        length = self.rng.randint(1, self.config.max_value_size)

        # Generate arbitrary bytes, not just readable text.
        return bytes(
            self.rng.randrange(256)
            for _ in range(length)
        )

    def _generate_single_operation(self):
        operation = self.rng.choices(
            population=["PUT", "GET", "DELETE"],
            weights=[0.50, 0.30, 0.20],
            k=1,
        )[0]

        request_id = self._next_id()
        key = self._random_key()

        if operation == "PUT":
            return PutRequest(
                request_id=request_id,
                key=key,
                value=self._random_value(),
            )

        if operation == "GET":
            return GetRequest(
                request_id=request_id,
                key=key,
            )

        return DeleteRequest(
            request_id=request_id,
            key=key,
        )

    def _generate_batch(self) -> BatchRequest:
        batch_id = self._next_id()

        operation_count = self.rng.randint(
            1,
            self.config.max_batch_size,
        )

        operations = []

        for _ in range(operation_count):
            key = self._random_key()

            if self.rng.random() < 0.65:
                operation = PutRequest(
                    request_id=self._next_id(),
                    key=key,
                    value=self._random_value(),
                )
            else:
                operation = DeleteRequest(
                    request_id=self._next_id(),
                    key=key,
                )

            operations.append(operation)

        return BatchRequest(
            request_id=batch_id,
            operations=operations,
        )

    def generate(self):
        """Generate the configured number of top-level requests."""

        requests = []

        for _ in range(self.config.num_operations):
            if self.rng.random() < self.config.batch_probability:
                request = self._generate_batch()
            else:
                request = self._generate_single_operation()

            requests.append(request)

        return requests

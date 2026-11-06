
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from protocol import (
    Acknowledgement,
    BatchRequest,
    DeleteRequest,
    DumpResponse,
    GetRequest,
    NotFoundResponse,
    PutRequest,
    ValueResponse,
)


@dataclass
class Oracle:
    """Tracks the expected state of the key-value database."""

    _data: dict[bytes, bytes] = field(default_factory=dict)

    def apply_put(self, key: bytes, value: bytes) -> None:
        self._data[key] = value

    def apply_delete(self, key: bytes) -> None:
        self._data.pop(key, None)

    def apply_batch(self, request: BatchRequest) -> None:
        # Work on a copy so the update is atomic from the Oracle's perspective.
        updated = self._data.copy()

        for operation in request.operations:
            if isinstance(operation, PutRequest):
                updated[operation.key] = operation.value
            elif isinstance(operation, DeleteRequest):
                updated.pop(operation.key, None)

        self._data = updated

    def expected_value(self, key: bytes) -> bytes | None:
        return self._data.get(key)

    def verify_get(
        self,
        request: GetRequest,
        response: ValueResponse | NotFoundResponse,
    ) -> None:
        expected = self.expected_value(request.key)

        if expected is None:
            if not isinstance(response, NotFoundResponse):
                raise AssertionError(
                    f"GET {request.key!r}: expected NOT_FOUND, got {response!r}"
                )
        elif not isinstance(response, ValueResponse) or response.value != expected:
            raise AssertionError(
                f"GET {request.key!r}: expected {expected!r}, got {response!r}"
            )

    def verify_dump(self, response: DumpResponse) -> None:
        actual = dict(response.entries)

        if actual != self._data:
            missing = self._data.keys() - actual.keys()
            unexpected = actual.keys() - self._data.keys()
            wrong_values = {
                key: (self._data[key], actual[key])
                for key in self._data.keys() & actual.keys()
                if self._data[key] != actual[key]
            }

            raise AssertionError(
                "DUMP does not match Oracle state. "
                f"Missing keys: {missing!r}; "
                f"unexpected keys: {unexpected!r}; "
                f"wrong values: {wrong_values!r}"
            )

    def apply_confirmed_write(self, request, response: Acknowledgement) -> None:
        """Update expected state only after a confirmed durable ACK."""

        if not isinstance(response, Acknowledgement):
            raise TypeError("writes can only be applied after an ACK")

        if isinstance(request, PutRequest):
            self.apply_put(request.key, request.value)
        elif isinstance(request, DeleteRequest):
            self.apply_delete(request.key)
        elif isinstance(request, BatchRequest):
            self.apply_batch(request)
        else:
            raise TypeError(f"not a write request: {type(request).__name__}")

    def snapshot(self) -> dict[bytes, bytes]:
        """Return a copy so callers cannot mutate Oracle state."""
        return self._data.copy()

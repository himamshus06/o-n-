
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from adapter import AdapterAmbiguousError
from oracle import Oracle
from protocol import (
    Acknowledgement,
    DeleteRequest,
    DumpRequest,
    DumpResponse,
    ErrorResponse,
    GetRequest,
    NotFoundResponse,
    PutRequest,
    BatchRequest,
    ValueResponse,
)
from workload import WorkloadGenerator


@dataclass
class RunFailure:
    """Details about one failed workload request."""

    request_id: str | int | None
    message: str
    ambiguous: bool = False


@dataclass
class RunResult:
    """Summary of one workload execution."""

    requests_sent: int = 0
    confirmed_writes: int = 0
    reads_checked: int = 0
    dumps_checked: int = 0
    failures: list[RunFailure] = field(default_factory=list)
    stopped_early: bool = False

    @property
    def successful(self) -> bool:
        return len(self.failures) == 0

    def report(self) -> str:
        lines = [
            "WORKLOAD REPORT",
            "----------------",
            f"Requests sent:     {self.requests_sent}",
            f"Confirmed writes:  {self.confirmed_writes}",
            f"Reads checked:     {self.reads_checked}",
            f"Dumps checked:     {self.dumps_checked}",
            f"Failures:          {len(self.failures)}",
            f"Stopped early:     {self.stopped_early}",
            f"Successful:        {self.successful}",
        ]

        for failure in self.failures:
            lines.append(
                f"  Request {failure.request_id}: {failure.message}"
            )
            if failure.ambiguous:
                lines.append("    Outcome is ambiguous.")

        return "\n".join(lines)


class WorkloadRunner:
    """Executes requests and checks them against an independent Oracle."""

    def __init__(self, adapter, oracle: Oracle) -> None:
        self.adapter = adapter
        self.oracle = oracle

    def _check_response_id(self, request, response) -> None:
        if response.request_id != request.request_id:
            raise AssertionError(
                f"Response ID mismatch: expected {request.request_id!r}, "
                f"received {response.request_id!r}"
            )

    def _execute_one(self, request, result: RunResult) -> None:
        response = self.adapter.execute(request)
        self._check_response_id(request, response)

        if isinstance(request, (PutRequest, DeleteRequest, BatchRequest)):
            if isinstance(response, ErrorResponse):
                raise AssertionError(
                    f"Database rejected generated write: {response.error}"
                )

            if not isinstance(response, Acknowledgement):
                raise AssertionError(
                    f"Expected ACK for write, got {type(response).__name__}"
                )

            self.oracle.apply_confirmed_write(request, response)
            result.confirmed_writes += 1
            return

        if isinstance(request, GetRequest):
            if not isinstance(response, (ValueResponse, NotFoundResponse)):
                raise AssertionError(
                    f"Expected VALUE or NOT_FOUND for GET, "
                    f"got {type(response).__name__}"
                )

            self.oracle.verify_get(request, response)
            result.reads_checked += 1
            return

        if isinstance(request, DumpRequest):
            if not isinstance(response, DumpResponse):
                raise AssertionError(
                    f"Expected DUMP response, got {type(response).__name__}"
                )

            self.oracle.verify_dump(response)
            result.dumps_checked += 1
            return

        raise TypeError(
            f"Unsupported request type: {type(request).__name__}"
        )

    def run(self, requests: Iterable) -> RunResult:
        """Execute requests sequentially, stopping on the first failure."""

        result = RunResult()

        if not self.adapter.readiness():
            result.failures.append(
                RunFailure(
                    request_id=None,
                    message="Database adapter is not ready.",
                )
            )
            result.stopped_early = True
            return result

        for request in requests:
            result.requests_sent += 1

            try:
                self._execute_one(request, result)

            except AdapterAmbiguousError as exc:
                result.failures.append(
                    RunFailure(
                        request_id=getattr(request, "request_id", None),
                        message=str(exc),
                        ambiguous=True,
                    )
                )
                result.stopped_early = True
                break

            except Exception as exc:
                result.failures.append(
                    RunFailure(
                        request_id=getattr(request, "request_id", None),
                        message=f"{type(exc).__name__}: {exc}",
                    )
                )
                result.stopped_early = True
                break

        return result

    def run_generated(self, generator: WorkloadGenerator) -> RunResult:
        """Generate a workload and execute it."""

        requests = generator.generate()
        return self.run(requests)

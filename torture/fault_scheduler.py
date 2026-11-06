
"""Deterministic fault scheduling for database torture tests.

Faults are injected between completed workload requests. This first
version deliberately avoids interrupting an in-flight request.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from controller import DatabaseController
from runner import RunFailure, RunResult


class FaultScheduleError(ValueError):
    """Raised when a fault schedule is invalid."""


@dataclass(frozen=True, slots=True)
class FaultPlan:
    """Defines the request positions after which crashes should occur."""

    crash_after_requests: tuple[int, ...] = ()

    def __post_init__(self) -> None:
        positions = self.crash_after_requests

        if any(position <= 0 for position in positions):
            raise FaultScheduleError(
                "Crash positions must be positive integers."
            )

        if tuple(sorted(set(positions))) != positions:
            raise FaultScheduleError(
                "Crash positions must be unique and in ascending order."
            )


@dataclass
class FaultEvent:
    """Records one injected crash and its recovery."""

    after_request: int
    crash_succeeded: bool = False
    restart_succeeded: bool = False
    error: str | None = None


@dataclass
class FaultScheduleResult:
    """Combined workload and fault-injection report."""

    workload: RunResult = field(default_factory=RunResult)
    fault_events: list[FaultEvent] = field(default_factory=list)

    @property
    def successful(self) -> bool:
        return (
            self.workload.successful
            and all(
                event.crash_succeeded and event.restart_succeeded
                for event in self.fault_events
            )
        )

    def report(self) -> str:
        lines = [
            "FAULT SCHEDULE REPORT",
            "---------------------",
            f"Requests sent: {self.workload.requests_sent}",
            f"Confirmed writes: {self.workload.confirmed_writes}",
            f"Reads checked: {self.workload.reads_checked}",
            f"Dumps checked: {self.workload.dumps_checked}",
            f"Scheduled crashes: {len(self.fault_events)}",
            f"Workload successful: {self.workload.successful}",
            f"Overall successful: {self.successful}",
        ]

        for event in self.fault_events:
            lines.append(
                f"  After request {event.after_request}: "
                f"crash={event.crash_succeeded}, "
                f"restart={event.restart_succeeded}"
            )
            if event.error:
                lines.append(f"    Error: {event.error}")

        for failure in self.workload.failures:
            lines.append(
                f"  Request {failure.request_id}: {failure.message}"
            )

        return "\n".join(lines)


class FaultScheduler:
    """Runs workloads and injects deterministic crashes between requests."""

    def __init__(self, controller: DatabaseController, runner) -> None:
        self.controller = controller
        self.runner = runner

    @staticmethod
    def _merge_results(target: RunResult, source: RunResult) -> None:
        target.requests_sent += source.requests_sent
        target.confirmed_writes += source.confirmed_writes
        target.reads_checked += source.reads_checked
        target.dumps_checked += source.dumps_checked
        target.failures.extend(source.failures)
        target.stopped_early = (
            target.stopped_early or source.stopped_early
        )

    def _ensure_ready(self) -> None:
        if not self.controller.is_ready():
            self.controller.start()

    def _inject_crash(
        self,
        request_count: int,
        result: FaultScheduleResult,
    ) -> bool:
        event = FaultEvent(after_request=request_count)
        result.fault_events.append(event)

        try:
            self.controller.crash()
            event.crash_succeeded = True

            self.controller.start()
            event.restart_succeeded = True

            return True

        except Exception as exc:
            event.error = f"{type(exc).__name__}: {exc}"
            result.workload.failures.append(
                RunFailure(
                    request_id=None,
                    message=f"Fault recovery failed: {event.error}",
                )
            )
            result.workload.stopped_early = True
            return False

    def run(
        self,
        requests: Iterable,
        plan: FaultPlan,
    ) -> FaultScheduleResult:
        """Execute requests and inject crashes at planned positions."""

        result = FaultScheduleResult()
        request_list = list(requests)

        if plan.crash_after_requests:
            if plan.crash_after_requests[-1] > len(request_list):
                raise FaultScheduleError(
                    "A crash position exceeds the workload length."
                )

        self._ensure_ready()

        next_fault_index = 0
        completed = 0
        chunk_start = 0

        for position in plan.crash_after_requests:
            chunk = request_list[chunk_start:position]

            chunk_result = self.runner.run(chunk)
            self._merge_results(result.workload, chunk_result)

            completed += chunk_result.requests_sent

            if not chunk_result.successful:
                return result

            if completed != position:
                result.workload.failures.append(
                    RunFailure(
                        request_id=None,
                        message="Workload stopped before the scheduled fault.",
                    )
                )
                result.workload.stopped_early = True
                return result

            if not self._inject_crash(completed, result):
                return result

            chunk_start = position
            next_fault_index += 1

        remaining = request_list[chunk_start:]
        if remaining:
            final_result = self.runner.run(remaining)
            self._merge_results(result.workload, final_result)

        return result

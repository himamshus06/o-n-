"""Bounded history-based linearizability checking for normal executions."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from protocol import (
    Acknowledgement,
    BatchRequest,
    DeleteRequest,
    ErrorResponse,
    GetRequest,
    NotFoundResponse,
    PutRequest,
    ValueResponse,
)


class CheckResult(str, Enum):
    LINEARIZABLE = "LINEARIZABLE"
    NOT_LINEARIZABLE = "NOT_LINEARIZABLE"
    INCONCLUSIVE = "INCONCLUSIVE"


@dataclass(frozen=True, slots=True)
class HistoryOperation:
    operation_id: str | int
    client_id: str
    operation: str
    key: bytes | None
    value: bytes | None
    batch: tuple[tuple[str, bytes, bytes | None], ...]
    invoked_ns: int
    completed_ns: int
    status: str
    returned_value: bytes | None = None
    error: str | None = None
    transaction_id: int | None = None


@dataclass
class CheckReport:
    result: CheckResult
    explored_states: int
    history_size: int
    reason: str = ""
    linearization: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "result": self.result.value,
            "explored_states": self.explored_states,
            "history_size": self.history_size,
            "reason": self.reason,
            "linearization": self.linearization,
        }


def _validate_history(history: list[HistoryOperation]) -> str | None:
    ids = [item.operation_id for item in history]
    if len(ids) != len(set(ids)):
        return "duplicate operation IDs"
    for item in history:
        if not isinstance(item.invoked_ns, int) or not isinstance(item.completed_ns, int):
            return f"missing timestamps for {item.operation_id!r}"
        if item.invoked_ns > item.completed_ns:
            return f"inverted timestamps for {item.operation_id!r}"
        if item.operation in {"PUT", "DELETE", "GET", "BATCH"}:
            continue
        return f"unsupported operation {item.operation!r}"
    return None


def _predecessors(history: list[HistoryOperation]) -> dict[object, set[object]]:
    result = {item.operation_id: set() for item in history}
    for left in history:
        for right in history:
            if left.operation_id != right.operation_id and left.completed_ns <= right.invoked_ns:
                result[right.operation_id].add(left.operation_id)
    return result


def _apply(item: HistoryOperation, state: dict[bytes, bytes]) -> tuple[bool, dict[bytes, bytes]]:
    next_state = dict(state)
    if item.status == "ERROR":
        return True, next_state
    if item.operation == "GET":
        present = item.key in state
        if present:
            return item.status == "VALUE" and item.returned_value == state[item.key], next_state
        return item.status == "NOT_FOUND", next_state
    if item.operation == "PUT":
        if item.status != "ACK" or item.key is None or item.value is None:
            return False, next_state
        next_state[item.key] = item.value
        return True, next_state
    if item.operation == "DELETE":
        if item.status != "ACK" or item.key is None:
            return False, next_state
        next_state.pop(item.key, None)
        return True, next_state
    if item.operation == "BATCH":
        if item.status != "ACK":
            return False, next_state
        for operation, key, value in item.batch:
            if operation == "PUT":
                if value is None:
                    return False, next_state
                next_state[key] = value
            elif operation == "DELETE":
                next_state.pop(key, None)
            else:
                return False, next_state
        return True, next_state
    return False, next_state


def check_history(history: list[HistoryOperation], *, max_states: int = 100_000) -> CheckReport:
    """Check a finite history; return INCONCLUSIVE when the budget is exhausted."""
    if max_states < 1:
        return CheckReport(CheckResult.INCONCLUSIVE, 0, len(history), "invalid search budget")
    error = _validate_history(history)
    if error:
        return CheckReport(CheckResult.NOT_LINEARIZABLE, 0, len(history), error)
    if not history:
        return CheckReport(CheckResult.LINEARIZABLE, 1, 0, "empty history", [])

    by_id = {item.operation_id: item for item in history}
    predecessors = _predecessors(history)
    explored = 0
    memo: dict[tuple[frozenset[object], tuple[tuple[bytes, bytes], ...]], bool] = {}
    found: list[str] = []
    exhausted = False

    def search(remaining: frozenset[object], state: dict[bytes, bytes], path: list[str]) -> bool:
        nonlocal explored, exhausted, found
        if not remaining:
            found = list(path)
            return True
        key = (remaining, tuple(sorted(state.items())))
        if key in memo:
            return memo[key]
        explored += 1
        if explored > max_states:
            exhausted = True
            return False
        candidates = [
            operation_id
            for operation_id in remaining
            if not (predecessors[operation_id] & remaining)
        ]
        for operation_id in sorted(candidates, key=str):
            valid, next_state = _apply(by_id[operation_id], state)
            if valid and search(remaining - {operation_id}, next_state, path + [str(operation_id)]):
                memo[key] = True
                return True
        memo[key] = False
        return False

    success = search(frozenset(by_id), {}, [])
    if success:
        return CheckReport(CheckResult.LINEARIZABLE, explored, len(history), "valid sequential explanation", found)
    if exhausted:
        return CheckReport(CheckResult.INCONCLUSIVE, explored, len(history), "search budget exhausted")
    return CheckReport(CheckResult.NOT_LINEARIZABLE, explored, len(history), "no legal sequential explanation")


def history_operation(request, response, *, client_id: str, invoked_ns: int, completed_ns: int) -> HistoryOperation:
    """Convert a typed protocol exchange into an independent history record."""
    if isinstance(response, Acknowledgement):
        status = "ACK"
    elif isinstance(response, ValueResponse):
        status = "VALUE"
    elif isinstance(response, NotFoundResponse):
        status = "NOT_FOUND"
    elif isinstance(response, ErrorResponse):
        status = "ERROR"
    else:
        raise TypeError(f"unsupported history response: {type(response).__name__}")
    returned_value = getattr(response, "value", None)
    error = getattr(response, "error", None)
    if isinstance(request, PutRequest):
        operation, key, value, batch = "PUT", request.key, request.value, ()
    elif isinstance(request, DeleteRequest):
        operation, key, value, batch = "DELETE", request.key, None, ()
    elif isinstance(request, GetRequest):
        operation, key, value, batch = "GET", request.key, None, ()
    elif isinstance(request, BatchRequest):
        operation, key, value = "BATCH", None, None
        batch = tuple(
            ("PUT", item.key, item.value) if isinstance(item, PutRequest) else ("DELETE", item.key, None)
            for item in request.operations
        )
    else:
        raise TypeError(f"unsupported history request: {type(request).__name__}")
    return HistoryOperation(
        operation_id=request.request_id,
        client_id=client_id,
        operation=operation,
        key=key,
        value=value,
        batch=batch,
        invoked_ns=invoked_ns,
        completed_ns=completed_ns,
        status=status,
        returned_value=returned_value,
        error=error,
        transaction_id=None,
    )

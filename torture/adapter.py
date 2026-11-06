"""The integration boundary between the torture framework and a database.

This module deliberately contains no subprocess or socket implementation.
The concrete Go adapter will be added after its command and JSON protocol are
confirmed.  A future adapter may use stdin/stdout, sockets, or another
transport without changing workload or checking code.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from protocol import DatabaseResponse, Request, Response


class AdapterError(RuntimeError):
    """Base class for harness/integration failures."""


class AdapterNotReadyError(AdapterError):
    """Raised when a request is attempted before the adapter is ready."""


class AdapterFailedError(AdapterError):
    """Raised when the current process returned an uncertain outcome."""


class AdapterCommunicationError(AdapterError):
    """Raised when transport communication fails before a response is known."""


class AdapterAmbiguousError(AdapterError):
    """Raised when communication stops after a request may have been applied."""


class AdapterRequestTimeoutError(AdapterAmbiguousError):
    """Raised when no response arrives before the request deadline."""


@dataclass(frozen=True, slots=True)
class AdapterConfig:
    """Provisional settings shared by concrete adapters.

    ``endpoint`` is intentionally opaque: it may later be an executable path,
    socket address, or another connection description.  ``acknowledgements``
    documents how the adapter interprets an ACK until the Go protocol defines
    an explicit durable-ack field.
    """

    endpoint: str
    acknowledgements: str = "explicit-durable-only"
    request_timeout_seconds: float = 5.0

    def __post_init__(self) -> None:
        if not self.endpoint:
            raise ValueError("endpoint must not be empty")
        if self.request_timeout_seconds <= 0:
            raise ValueError("request_timeout_seconds must be positive")


class DatabaseAdapter(ABC):
    """Abstract database connection used by the torture runner.

    Lifecycle methods are intentionally abstract.  They describe what the
    runner needs, but do not choose how the database process is started or
    killed.  A request that has no confirmed response must be represented as
    ambiguous by the concrete adapter, never silently treated as committed.
    """

    def __init__(self, config: AdapterConfig) -> None:
        self.config = config
        self._ready = False

    @abstractmethod
    def start(self) -> None:
        """Start or connect to the database and wait until it is ready."""

    @abstractmethod
    def stop(self) -> None:
        """Stop the database cleanly."""

    @abstractmethod
    def crash(self) -> None:
        """Abruptly stop the database, leaving in-flight requests ambiguous."""

    @abstractmethod
    def execute(self, request: Request) -> Response:
        """Send one validated request and return its typed response."""

    @abstractmethod
    def readiness(self) -> bool:
        """Return whether the adapter can currently accept requests."""

    def require_ready(self) -> None:
        if getattr(self, "_failed", False):
            raise AdapterFailedError("database adapter is failed; terminate and restart it")
        if not self.readiness():
            raise AdapterNotReadyError("database adapter is not ready")

    @property
    def ready(self) -> bool:
        return self._ready


def response_is_durable_ack(response: DatabaseResponse) -> bool:
    """Small shared predicate for future runners and reports."""
    return getattr(response, "is_durable", False) is True

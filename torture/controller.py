
from __future__ import annotations

from enum import Enum, auto

from adapter import DatabaseAdapter


class ControllerError(RuntimeError):
    """Raised when an invalid database lifecycle operation is attempted."""


class DatabaseState(Enum):
    STOPPED = auto()
    STARTING = auto()
    READY = auto()
    CRASHED = auto()
    FAILED = auto()


class DatabaseController:
    """
    Generic lifecycle controller.

    It does not know how a database is implemented.
    All process-specific work is delegated to its adapter.
    """

    def __init__(self, adapter: DatabaseAdapter):
        self.adapter = adapter
        self.state = DatabaseState.STOPPED

    def start(self) -> None:
        """Start the database and verify that it becomes ready."""

        if self.state not in (
            DatabaseState.STOPPED,
            DatabaseState.CRASHED,
        ):
            raise ControllerError(
                f"Cannot start database while state is {self.state.name}"
            )

        self.state = DatabaseState.STARTING

        try:
            self.adapter.start()

            if not self.adapter.readiness():
                raise ControllerError(
                    "Database process started but is not ready"
                )

            self.state = DatabaseState.READY

        except Exception:
            self.state = DatabaseState.FAILED
            raise

    def crash(self) -> None:
        """Abruptly terminate the database process."""

        self._require_ready()

        try:
            self.adapter.crash()
            self.state = DatabaseState.CRASHED

        except Exception:
            self.state = DatabaseState.FAILED
            raise

    def restart(self) -> None:
        """
        Restart the database.

        If it is running, crash it first.
        If it is already crashed or stopped, start it directly.
        """

        if self.state == DatabaseState.READY:
            self.crash()

        if self.state not in (
            DatabaseState.CRASHED,
            DatabaseState.STOPPED,
        ):
            raise ControllerError(
                f"Cannot restart database while state is {self.state.name}"
            )

        self.start()

    def stop(self) -> None:
        """Gracefully stop a running database."""

        if self.state == DatabaseState.STOPPED:
            return

        if self.state == DatabaseState.CRASHED:
            self.state = DatabaseState.STOPPED
            return

        self._require_ready()

        try:
            self.adapter.stop()
            self.state = DatabaseState.STOPPED

        except Exception:
            self.state = DatabaseState.FAILED
            raise

    def _require_ready(self) -> None:
        if self.state != DatabaseState.READY:
            raise ControllerError(
                f"Database must be READY; current state is {self.state.name}"
            )

        if not self.adapter.readiness():
            self.state = DatabaseState.FAILED
            raise ControllerError(
                "Controller expected a ready database, but adapter reports otherwise"
            )

    def is_ready(self) -> bool:
        """Check both the controller state and adapter readiness."""

        return (
            self.state == DatabaseState.READY
            and self.adapter.readiness()
        )

    def __enter__(self) -> DatabaseController:
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        if self.state == DatabaseState.READY:
            self.stop()

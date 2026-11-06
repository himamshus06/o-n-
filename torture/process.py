
"""Windows-compatible JSON-lines subprocess adapter.

This is a transport implementation, not a definition of the Go database
protocol. It uses the request/response conversion functions in protocol.py
and accepts a command list so the database command can be configured
without changing the torture runner.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
import json
import os
from pathlib import Path
import subprocess
import threading
import time
from typing import Any, Mapping

from adapter import (
    AdapterAmbiguousError,
    AdapterError,
    AdapterCommunicationError,
    AdapterConfig,
    AdapterNotReadyError,
    AdapterRequestTimeoutError,
    DatabaseAdapter,
)

from protocol import (
    ErrorResponse,
    Request,
    Response,
    ProtocolValidationError,
    request_to_dict,
    response_from_dict,
)


ResponseDecoder = Callable[[Mapping[str, Any]], Response]


@dataclass(frozen=True, slots=True)
class ProcessConfig:
    """Settings for a database child process."""

    command: tuple[str, ...]
    database_dir: Path | str
    response_timeout_seconds: float = 5.0
    shutdown_timeout_seconds: float = 2.0
    startup_timeout_seconds: float = 5.0
    stderr_buffer_lines: int = 200
    encoding: str = "utf-8"

    def __post_init__(self) -> None:
        if not self.command or any(
            not isinstance(part, str) or not part
            for part in self.command
        ):
            raise ValueError(
                "command must be a non-empty sequence of non-empty strings"
            )

        if (
            self.response_timeout_seconds <= 0
            or self.shutdown_timeout_seconds <= 0
            or self.startup_timeout_seconds <= 0
        ):
            raise ValueError("timeouts must be positive")

        if self.stderr_buffer_lines <= 0:
            raise ValueError("stderr_buffer_lines must be positive")


class SubprocessDatabaseAdapter(DatabaseAdapter):
    """A request/response adapter backed by one child process."""

    def __init__(
        self,
        config: ProcessConfig,
        response_decoder: ResponseDecoder = response_from_dict,
    ) -> None:

        super().__init__(
            AdapterConfig(
                endpoint=config.command[0],
                request_timeout_seconds=config.response_timeout_seconds,
            )
        )

        self.process_config = config
        self._response_decoder = response_decoder

        self._process: subprocess.Popen[str] | None = None

        self._stdout_thread: threading.Thread | None = None
        self._stderr_thread: threading.Thread | None = None

        self._write_lock = threading.Lock()
        self._state_lock = threading.Condition(threading.RLock())

        self._pending: set[object] = set()
        self._responses: dict[object, Response] = {}

        self._request_errors: dict[
            object, AdapterCommunicationError
        ] = {}

        self._terminal_error: AdapterError | None = None

        self._closing = False
        self._failed = False

        self._ready_event = threading.Event()

        self._startup_error: AdapterCommunicationError | None = None

        self._stderr_lines: deque[str] = deque(
            maxlen=config.stderr_buffer_lines
        )

    # ---------------------------------------------------------
    # START DATABASE
    # ---------------------------------------------------------

    def start(self) -> None:

        if self.is_running():
            raise AdapterCommunicationError(
                "database process is already running"
            )

        database_dir = Path(self.process_config.database_dir)
        database_dir.mkdir(parents=True, exist_ok=True)

        with self._state_lock:
            self._pending.clear()
            self._responses.clear()
            self._request_errors.clear()

            self._terminal_error = None
            self._closing = False
            self._failed = False

            self._ready_event.clear()
            self._startup_error = None

            self._stderr_lines.clear()

        try:
            process = subprocess.Popen(
                list(self.process_config.command),
                cwd=os.fspath(database_dir),
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding=self.process_config.encoding,
                errors="replace",
                bufsize=1,
                shell=False,
            )

        except OSError as exc:
            raise AdapterCommunicationError(
                f"could not start database process: {exc}"
            ) from exc

        self._process = process

        self._stdout_thread = threading.Thread(
            target=self._read_stdout,
            name="database-stdout",
            daemon=True,
        )

        self._stderr_thread = threading.Thread(
            target=self._read_stderr,
            name="database-stderr",
            daemon=True,
        )

        self._stdout_thread.start()
        self._stderr_thread.start()

        if not self._ready_event.wait(
            timeout=self.process_config.startup_timeout_seconds
        ):
            self._abort_startup(
                "database did not print READY before startup timeout"
            )

        if self._startup_error is not None:
            error = self._startup_error
            self._abort_startup(str(error))

        if not self.is_running():
            self._abort_startup(
                "database exited after READY"
            )

        self._ready = True

    # ---------------------------------------------------------
    # SEND REQUEST
    # ---------------------------------------------------------

    def send_request(self, request: Request) -> Response:
        """Send one request and wait for its matching response."""

        self.require_ready()

        request_id = request.request_id

        payload = (
            json.dumps(
                request_to_dict(request),
                ensure_ascii=False,
                separators=(",", ":"),
            )
            + "\n"
        )

        process = self._process

        if process is None or process.stdin is None:
            raise AdapterNotReadyError(
                "database process has no writable stdin"
            )

        with self._state_lock:

            if request_id in self._pending:
                raise AdapterCommunicationError(
                    f"request id is already pending: {request_id!r}"
                )

            self._pending.add(request_id)

        try:
            with self._write_lock:
                process.stdin.write(payload)
                process.stdin.flush()

        except (BrokenPipeError, OSError, ValueError) as exc:

            with self._state_lock:
                self._pending.discard(request_id)

            raise AdapterAmbiguousError(
                f"request {request_id!r} may have been sent before the pipe broke"
            ) from exc

        deadline = (
            time.monotonic()
            + self.process_config.response_timeout_seconds
        )

        with self._state_lock:

            while (
                request_id not in self._responses
                and request_id not in self._request_errors
            ):

                if self._terminal_error is not None:
                    self._pending.discard(request_id)
                    raise self._terminal_error

                remaining = deadline - time.monotonic()

                if remaining <= 0:
                    self._pending.discard(request_id)

                    raise AdapterRequestTimeoutError(
                        f"no response for request {request_id!r} "
                        f"within {self.process_config.response_timeout_seconds}s"
                    )

                self._state_lock.wait(timeout=remaining)

            error = self._request_errors.pop(request_id, None)

            if error is not None:
                self._pending.discard(request_id)
                raise error

            response = self._responses.pop(request_id)

            self._pending.discard(request_id)

            return response

    # ---------------------------------------------------------
    # RUNNER COMPATIBILITY
    # ---------------------------------------------------------

    def execute(self, request: Request) -> Response:
        """Implement the common adapter interface used by the runner."""

        return self.send_request(request)

    # ---------------------------------------------------------
    # SHUTDOWN
    # ---------------------------------------------------------

    def graceful_shutdown(self) -> None:

        process = self._process

        if process is None:
            self._ready = False
            return

        self._ready = False

        with self._state_lock:
            self._closing = True

        try:
            if process.stdin is not None:
                process.stdin.close()

        except (OSError, ValueError):
            pass

        try:
            process.wait(
                timeout=self.process_config.shutdown_timeout_seconds
            )

        except subprocess.TimeoutExpired:

            process.terminate()

            try:
                process.wait(
                    timeout=self.process_config.shutdown_timeout_seconds
                )

            except subprocess.TimeoutExpired as exc:
                raise AdapterCommunicationError(
                    "database did not shut down gracefully"
                ) from exc

        finally:
            self._finish_process()

    def stop(self) -> None:
        """Stop the database gracefully."""

        self.graceful_shutdown()

    # ---------------------------------------------------------
    # CRASH DATABASE
    # ---------------------------------------------------------

    def crash(self) -> None:
        """Abruptly terminate the process without graceful shutdown."""

        process = self._process

        if process is None:
            self._ready = False
            return

        self._ready = False

        with self._state_lock:
            self._closing = False

        try:
            process.kill()

            process.wait(
                timeout=self.process_config.shutdown_timeout_seconds
            )

        except subprocess.TimeoutExpired as exc:
            raise AdapterCommunicationError(
                "database process did not terminate after kill"
            ) from exc

        finally:
            self._finish_process()

    # ---------------------------------------------------------
    # RESTART DATABASE
    # ---------------------------------------------------------

    def restart(self) -> None:

        if self.is_running():
            self.graceful_shutdown()

        else:
            self._finish_process()

        self.start()

    # ---------------------------------------------------------
    # STATUS
    # ---------------------------------------------------------

    def is_running(self) -> bool:

        return (
            self._process is not None
            and self._process.poll() is None
        )

    def readiness(self) -> bool:

        if self._failed:
            return False

        if not self._ready or not self.is_running():
            self._ready = False
            return False

        return True

    @property
    def stderr_lines(self) -> tuple[str, ...]:

        with self._state_lock:
            return tuple(self._stderr_lines)

    # ---------------------------------------------------------
    # READ STANDARD OUTPUT
    # ---------------------------------------------------------

    def _read_stdout(self) -> None:

        process = self._process

        if process is None or process.stdout is None:
            return

        try:
            for line in process.stdout:
                self._handle_stdout_line(line)

        except (OSError, ValueError) as exc:

            self._set_terminal_error(
                AdapterCommunicationError(
                    f"stdout reader failed: {exc}"
                )
            )

        finally:

            try:
                process.stdout.close()

            except OSError:
                pass

            if not self._closing:
                self._set_terminal_error(
                    AdapterAmbiguousError(
                        "database process exited unexpectedly "
                        f"with code {process.returncode}"
                    )
                )

    # ---------------------------------------------------------
    # PROCESS RESPONSE
    # ---------------------------------------------------------

    def _handle_stdout_line(self, line: str) -> None:

        if not line.strip():
            self._set_terminal_error(
                AdapterCommunicationError(
                    "database emitted an empty response line"
                )
            )
            return

        try:
            raw_response = json.loads(line)

            if not isinstance(raw_response, Mapping):
                raise ProtocolValidationError(
                    "response JSON must be an object"
                )

            response = self._response_decoder(raw_response)

        except (
            json.JSONDecodeError,
            ProtocolValidationError,
            TypeError,
            ValueError,
        ) as exc:

            self._set_terminal_error(
                AdapterCommunicationError(
                    f"malformed database response: {exc}"
                )
            )
            return

        request_id = response.request_id

        with self._state_lock:

            if request_id not in self._pending:

                self._set_terminal_error_locked(
                    AdapterCommunicationError(
                        f"response received for unknown request id: {request_id!r}"
                    )
                )

                return

            if (
                isinstance(response, ErrorResponse)
                and response.outcome == "UNKNOWN"
            ):
                self._failed = True
                self._ready = False

            self._responses[request_id] = response

            self._state_lock.notify_all()

    # ---------------------------------------------------------
    # READ STANDARD ERROR / READINESS
    # ---------------------------------------------------------

    def _read_stderr(self) -> None:

        process = self._process

        if process is None or process.stderr is None:
            return

        try:
            for line in process.stderr:

                clean_line = line.rstrip("\r\n")

                if clean_line == "READY":
                    self._ready_event.set()

                else:
                    with self._state_lock:
                        self._stderr_lines.append(clean_line)

        except (OSError, ValueError):
            return

        finally:

            try:
                process.stderr.close()

            except OSError:
                pass

            if not self._ready_event.is_set() and not self._closing:

                self._startup_error = AdapterCommunicationError(
                    "database exited before READY "
                    f"with code {process.returncode}"
                )

                self._ready_event.set()

    # ---------------------------------------------------------
    # TERMINAL ERROR HANDLING
    # ---------------------------------------------------------

    def _set_terminal_error(self, error: AdapterError) -> None:

        with self._state_lock:
            self._set_terminal_error_locked(error)

    def _set_terminal_error_locked(self, error: AdapterError) -> None:

        if self._terminal_error is None:
            self._terminal_error = error

        self._state_lock.notify_all()

    # ---------------------------------------------------------
    # CLEAN UP PROCESS
    # ---------------------------------------------------------

    def _finish_process(self) -> None:

        process = self._process

        with self._state_lock:

            if self._pending:
                self._set_terminal_error_locked(
                    AdapterAmbiguousError(
                        "database process ended with requests still pending"
                    )
                )

            self._ready = False
            self._process = None

            self._state_lock.notify_all()

        for thread in (
            self._stdout_thread,
            self._stderr_thread,
        ):

            if (
                thread is not None
                and thread is not threading.current_thread()
            ):
                thread.join(timeout=1.0)

        if process is not None:

            for stream in (
                process.stdin,
                process.stdout,
                process.stderr,
            ):

                if stream is not None:

                    try:
                        stream.close()

                    except (OSError, ValueError):
                        pass

        self._stdout_thread = None
        self._stderr_thread = None

    # ---------------------------------------------------------
    # HANDLE STARTUP FAILURE
    # ---------------------------------------------------------

    def _abort_startup(self, message: str) -> None:

        process = self._process

        self._ready = False

        with self._state_lock:
            self._closing = True

        if process is not None and process.poll() is None:

            process.terminate()

            try:
                process.wait(
                    timeout=self.process_config.shutdown_timeout_seconds
                )

            except subprocess.TimeoutExpired:

                process.kill()

                process.wait(
                    timeout=self.process_config.shutdown_timeout_seconds
                )

        self._finish_process()

        raise AdapterCommunicationError(message)

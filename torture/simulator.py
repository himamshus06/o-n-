"""Independent persistent test double for the frozen Go database protocol.

The simulator is useful for testing the Python harness. It is not evidence
that the Go database has the same crash-safety properties.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import os
from pathlib import Path
import sys
import time
from typing import Any, Mapping

from protocol import (
    Acknowledgement,
    BatchRequest,
    DeleteRequest,
    DumpRequest,
    DumpResponse,
    ErrorResponse,
    GetRequest,
    MAX_KEY_SIZE,
    MAX_VALUE_SIZE,
    NotFoundResponse,
    ProtocolValidationError,
    PutRequest,
    Request,
    Response,
    ValueResponse,
    decode_base64,
    encode_bytes,
    request_from_dict,
    response_to_dict,
)


class SimulatorStorageError(RuntimeError):
    """Raised when simulator state cannot be loaded or persisted."""


@dataclass(frozen=True, slots=True)
class FailureControls:
    """Test-only failure injection, separate from normal operation behavior."""

    delay_ms: int = 0
    crash_after_persist: bool = False
    unknown_after_persist: bool = False
    exit_after_requests: int | None = None

    def __post_init__(self) -> None:
        if self.delay_ms < 0:
            raise ValueError("delay_ms must not be negative")
        if self.exit_after_requests is not None and self.exit_after_requests <= 0:
            raise ValueError("exit_after_requests must be positive")


class PersistentSimulator:
    """In-memory bytes map backed by atomically replaced JSON snapshots."""

    VERSION = 2

    def __init__(self, database_dir: Path | str, state_file: str = "simulator-state.json") -> None:
        self.database_dir = Path(database_dir)
        self.database_dir.mkdir(parents=True, exist_ok=True)
        self.state_path = self.database_dir / state_file
        self._data: dict[bytes, bytes] = self._load()

    def handle(self, request: Request, failures: FailureControls = FailureControls()) -> Response:
        if failures.delay_ms:
            time.sleep(failures.delay_ms / 1000)
        if isinstance(request, GetRequest):
            return ValueResponse(request.request_id, self._data[request.key]) if request.key in self._data else NotFoundResponse(request.request_id)
        if isinstance(request, DumpRequest):
            return DumpResponse(request.request_id, tuple(sorted(self._data.items())))
        if isinstance(request, PutRequest):
            updated = dict(self._data)
            updated[request.key] = request.value
            self._persist(updated)
            self._data = updated
            return Acknowledgement(request.request_id)
        if isinstance(request, DeleteRequest):
            updated = dict(self._data)
            updated.pop(request.key, None)
            self._persist(updated)
            self._data = updated
            return Acknowledgement(request.request_id)
        if isinstance(request, BatchRequest):
            updated = dict(self._data)
            for item in request.operations:
                if isinstance(item, PutRequest):
                    updated[item.key] = item.value
                elif isinstance(item, DeleteRequest):
                    updated.pop(item.key, None)
                else:
                    raise ProtocolValidationError("unsupported batch item")
            self._persist(updated)
            self._data = updated
            return Acknowledgement(request.request_id)
        raise ProtocolValidationError(f"unsupported request type: {type(request).__name__}")

    def _load(self) -> dict[bytes, bytes]:
        if not self.state_path.exists():
            return {}
        try:
            with self.state_path.open("r", encoding="utf-8") as state_file:
                snapshot = json.load(state_file)
            if not isinstance(snapshot, Mapping) or snapshot.get("version") != self.VERSION:
                raise SimulatorStorageError("unsupported simulator state format")
            entries = snapshot.get("entries")
            if not isinstance(entries, list):
                raise SimulatorStorageError("simulator state entries must be an array")
            data: dict[bytes, bytes] = {}
            for entry in entries:
                if not isinstance(entry, Mapping):
                    raise SimulatorStorageError("each state entry must be an object")
                key = decode_base64(entry["key"], field="state key", max_size=MAX_KEY_SIZE)
                value = decode_base64(entry["value"], field="state value", max_size=MAX_VALUE_SIZE)
                data[key] = value
            return data
        except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            raise SimulatorStorageError(f"could not load simulator state: {exc}") from exc

    def _persist(self, data: dict[bytes, bytes]) -> None:
        temporary_path = self.state_path.with_name(self.state_path.name + ".tmp")
        snapshot = {
            "version": self.VERSION,
            "entries": [
                {"key": encode_bytes(key, field="key"), "value": encode_bytes(value, field="value")}
                for key, value in sorted(data.items())
            ],
        }
        try:
            with temporary_path.open("w", encoding="utf-8", newline="\n") as state_file:
                json.dump(snapshot, state_file, ensure_ascii=False, sort_keys=True)
                state_file.write("\n")
                state_file.flush()
                os.fsync(state_file.fileno())
            os.replace(temporary_path, self.state_path)
        except (OSError, TypeError, ValueError) as exc:
            try:
                temporary_path.unlink(missing_ok=True)
            except OSError:
                pass
            raise SimulatorStorageError(f"could not persist simulator state: {exc}") from exc


def _error_request_id(raw_request: Any, default: str = "malformed") -> int | str:
    if isinstance(raw_request, Mapping):
        request_id = raw_request.get("id")
        if isinstance(request_id, str) and request_id:
            return request_id
        if isinstance(request_id, int) and not isinstance(request_id, bool) and request_id >= 0:
            return request_id
    return default


def run_json_lines(database_dir: Path | str, state_file: str, ready_file: str | None, failures: FailureControls) -> int:
    try:
        database = PersistentSimulator(database_dir, state_file)
    except SimulatorStorageError as exc:
        print(f"simulator startup failed: {exc}", file=sys.stderr, flush=True)
        return 2

    print("READY", file=sys.stderr, flush=True)
    if ready_file is not None:
        Path(ready_file).parent.mkdir(parents=True, exist_ok=True)
        Path(ready_file).write_text("ready\n", encoding="utf-8")

    request_count = 0
    for line in sys.stdin:
        request_count += 1
        if failures.exit_after_requests is not None and request_count > failures.exit_after_requests:
            os._exit(78)
        raw_request: Any = None
        try:
            raw_request = json.loads(line)
            request = request_from_dict(raw_request)
            response = database.handle(request, failures)
            if failures.unknown_after_persist and isinstance(response, Acknowledgement):
                response = ErrorResponse(request.request_id, "Commit outcome uncertain; database requires recovery", "UNKNOWN")
            if failures.crash_after_persist and isinstance(response, Acknowledgement):
                os._exit(77)
        except (json.JSONDecodeError, ProtocolValidationError) as exc:
            response = ErrorResponse(_error_request_id(raw_request), "Invalid request")
            print(json.dumps(response_to_dict(response), separators=(",", ":")), flush=True)
            continue
        except SimulatorStorageError as exc:
            response = ErrorResponse(_error_request_id(raw_request, "unknown"), str(exc))
        except Exception as exc:
            response = ErrorResponse(_error_request_id(raw_request, "unknown"), str(exc))
        print(json.dumps(response_to_dict(response), separators=(",", ":")), flush=True)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Persistent JSON Lines database simulator")
    parser.add_argument("--database-dir", required=True)
    parser.add_argument("--state-file", default="simulator-state.json")
    parser.add_argument("--ready-file", default=None)
    parser.add_argument("--delay-ms", type=int, default=0)
    parser.add_argument("--crash-after-persist", action="store_true")
    parser.add_argument("--unknown-after-persist", action="store_true")
    parser.add_argument("--exit-after-requests", type=int, default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        failures = FailureControls(args.delay_ms, args.crash_after_persist, args.unknown_after_persist, args.exit_after_requests)
    except ValueError as exc:
        print(f"invalid failure control: {exc}", file=sys.stderr)
        return 2
    return run_json_lines(args.database_dir, args.state_file, args.ready_file, failures)


if __name__ == "__main__":
    raise SystemExit(main())

"""Judge-facing real-binary torture campaign entry point.

This module is intentionally separate from the simulator. Every implemented
mode launches the configured Go executable as a child process and maintains an
independent Python oracle. Modes that require stronger facilities (recovery
crash hooks or filesystem exhaustion) are reported as not-tested rather than
being represented by a simulator result.
"""

from __future__ import annotations

import argparse
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import itertools
import json
import os
from pathlib import Path
import random
import subprocess
import threading
import tempfile
import time
from dataclasses import dataclass, asdict, field

from controller import DatabaseController
from adapter import AdapterAmbiguousError
from oracle import Oracle
from process import ProcessConfig, SubprocessDatabaseAdapter
from protocol import (
    Acknowledgement,
    BatchRequest,
    CompactRequest,
    CloseRequest,
    DeleteRequest,
    DumpRequest,
    ErrorResponse,
    GetRequest,
    PutRequest,
    ValueResponse,
)
from runner import WorkloadRunner
from workload import WorkloadConfig, WorkloadGenerator
from linearizability import CheckResult, check_history, history_operation

ENOSPC_BATCH_VALUE_SIZE = 256 * 1024
ENOSPC_BATCH_BUDGET = 400_000
PERMANENTLY_UNTESTED = (
    "physical disk exhaustion",
)
LEGACY_MODE_NOT_TESTED_LABELS = {
    "recovery-crash mode",
    "disk-full/ENOSPC mode",
}
EXPECTED_MODE_CASES = {
    "recovery-crash": {
        "recovery-started-recovery-crash",
        "recovery-transaction-applied-recovery-crash",
    },
    "injected-enospc": {
        "injected-enospc",
        "injected-enospc-batch-atomicity",
    },
}


@dataclass
class CampaignReport:
    started_utc: str
    seed: int
    iterations: int
    modes: list[str]
    passed: list[str]
    failed: list[dict[str, str]]
    not_tested: list[str]
    simulated_only: list[str]
    environment_blocked: list[str]
    crashes: int = 0
    restarts: int = 0
    acknowledged_writes: int = 0
    failed_operations: int = 0
    recovery_failures: int = 0
    oracle_mismatches: int = 0
    operations_attempted: int = 0
    unknown_outcomes: int = 0
    integrity_check_failures: int = 0
    batch_atomicity_failures: int = 0
    harness_active_kills: int = 0
    idle_kills: int = 0
    storage_phase_kills: int = 0
    operation_lifecycles: list[dict[str, object]] = field(default_factory=list)
    linearizability_results: list[dict[str, object]] = field(default_factory=list)
    crash_events: list[dict[str, object]] = field(default_factory=list)
    finished_utc: str = ""
    duration_seconds: float = 0.0


def finalize_not_tested(report: CampaignReport) -> None:
    """Keep only real capability gaps and specific execution blockers."""
    entries = list(PERMANENTLY_UNTESTED)
    entries.extend(
        item
        for item in report.not_tested
        if item not in LEGACY_MODE_NOT_TESTED_LABELS
        and item not in PERMANENTLY_UNTESTED
    )
    passed = set(report.passed)
    failed_modes = {
        str(failure.get("mode", "")) for failure in report.failed
    }
    for mode in report.modes:
        expected = EXPECTED_MODE_CASES.get(mode)
        if not expected:
            continue
        missing = sorted(expected - passed)
        if not missing or any(
            mode in failed_mode or mode in item
            for failed_mode in failed_modes
            for item in entries
        ):
            continue
        if any(item.startswith(f"{mode}:") for item in entries):
            continue
        entries.append(f"{mode}: cases not executed: {', '.join(missing)}")
    report.not_tested = list(dict.fromkeys(entries))


def render_markdown(report: CampaignReport) -> str:
    """Render the finalized report without recalculating any campaign data."""
    markdown = [
        "# Kill It and Prove It campaign",
        "",
        f"Started UTC: `{report.started_utc}`",
        f"Finished UTC: `{report.finished_utc}`",
        f"Seed: `{report.seed}`",
        f"Iterations: `{report.iterations}`",
        f"Duration seconds: `{report.duration_seconds:.3f}`",
        "",
        "## Modes",
        *[f"- {item}" for item in report.modes],
        "",
        "## Passed",
        *[f"- {item}" for item in report.passed],
        "",
        "## Failed",
        *[f"- {item['mode']}: {item['error']}" for item in report.failed],
        "",
        "## Not tested",
        *[f"- {item}" for item in report.not_tested],
        "",
        "## Simulated only",
        *[f"- {item}" for item in report.simulated_only],
        "",
        "## Environment blocked",
        *[f"- {item}" for item in report.environment_blocked],
        "",
        "## Metrics",
        f"- Operations attempted: {report.operations_attempted}",
        f"- Acknowledged writes: {report.acknowledged_writes}",
        f"- Failed operations: {report.failed_operations}",
        f"- UNKNOWN outcomes: {report.unknown_outcomes}",
        f"- Crashes: {report.crashes}",
        f"- Restarts: {report.restarts}",
        f"- Recovery failures: {report.recovery_failures}",
        f"- Oracle mismatches: {report.oracle_mismatches}",
        f"- Integrity-check failures: {report.integrity_check_failures}",
        f"- Batch-atomicity failures: {report.batch_atomicity_failures}",
        f"- Harness-active kills (not storage-confirmed): {report.harness_active_kills}",
        f"- Idle kills: {report.idle_kills}",
        f"- Storage-phase kills: {report.storage_phase_kills}",
        f"- Linearizability results: {json.dumps(report.linearizability_results, sort_keys=True)}",
        "",
        "## Crash events",
        *[f"- {json.dumps(item, sort_keys=True)}" for item in report.crash_events],
    ]
    return "\n".join(markdown) + "\n"


def _executable(value: str | None) -> Path:
    candidate = value or os.environ.get("KVDB_EXECUTABLE")
    if candidate:
        return Path(candidate).resolve()
    return Path(__file__).resolve().parents[1] / "kvdb.exe"


def _check(executable: Path, database: Path) -> None:
    completed = subprocess.run(
        [str(executable), "check", str(database)],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    if completed.returncode != 0:
        raise AssertionError(
            f"check failed with {completed.returncode}: "
            f"{completed.stdout.strip()} {completed.stderr.strip()}"
        )


def _adapter(
    executable: Path,
    directory: Path,
    env: dict[str, str] | None = None,
) -> SubprocessDatabaseAdapter:
    database = directory / "database.kv"
    return SubprocessDatabaseAdapter(
        ProcessConfig(
            command=(str(executable), "serve", str(database)),
            database_dir=directory,
            response_timeout_seconds=15.0,
            env=env,
        )
    )


def _run_default(executable: Path, seed: int, iterations: int, report: CampaignReport) -> None:
    with tempfile.TemporaryDirectory(prefix="kvdb-campaign-") as raw:
        directory = Path(raw)
        adapter = _adapter(executable, directory)
        controller = DatabaseController(adapter)
        oracle = Oracle()
        try:
            controller.start()
            workload = WorkloadGenerator(
                WorkloadConfig(
                    seed=seed,
                    num_operations=iterations,
                    batch_probability=0.25,
                    max_batch_size=10,
                )
            )
            result = WorkloadRunner(adapter, oracle).run_generated(workload)
            report.acknowledged_writes += result.confirmed_writes
            report.operations_attempted += result.requests_sent
            report.unknown_outcomes += sum(
                1 for failure in result.failures if failure.ambiguous
            )
            report.failed_operations += len(result.failures)
            if not result.successful:
                raise AssertionError(result.report())
            response = adapter.execute(DumpRequest("final-dump"))
            oracle.verify_dump(response)
            _check(executable, directory / "database.kv")
            report.passed.append("default-real-binary-workload")
        except Exception as exc:
            report.oracle_mismatches += 1 if "DUMP" in str(exc) or "Oracle" in str(exc) else 0
            report.failed.append({"mode": "default-real-binary-workload", "error": str(exc)})
        finally:
            if controller.is_ready():
                controller.stop()


def _run_large_value(executable: Path, report: CampaignReport) -> None:
    with tempfile.TemporaryDirectory(prefix="kvdb-large-value-") as raw:
        directory = Path(raw)
        adapter = _adapter(executable, directory)
        controller = DatabaseController(adapter)
        try:
            controller.start()
            value = bytes((index % 251 for index in range(1 << 20)))
            request = PutRequest("large-value", b"large", value)
            response = adapter.execute(request)
            report.operations_attempted += 1
            if not isinstance(response, Acknowledgement):
                raise AssertionError(f"large value was not acknowledged: {response!r}")
            report.acknowledged_writes += 1
            got = adapter.execute(GetRequest("large-get", b"large"))
            report.operations_attempted += 1
            if not isinstance(got, ValueResponse) or got.value != value:
                raise AssertionError("1 MiB value did not round-trip")

            controller.stop()

            adapter = _adapter(executable, directory)
            controller = DatabaseController(adapter)
            controller.start()
            restarted = adapter.execute(GetRequest("large-get-after-restart", b"large"))
            report.operations_attempted += 1
            if not isinstance(restarted, ValueResponse) or restarted.value != value:
                raise AssertionError("1 MiB value did not survive database restart")
            _check(executable, directory / "database.kv")
            report.passed.append("large-value")
        except Exception as exc:
            report.failed.append({"mode": "large-value", "error": str(exc)})
        finally:
            if controller.is_ready():
                controller.stop()


def _run_large_batch(executable: Path, report: CampaignReport) -> None:
    with tempfile.TemporaryDirectory(prefix="kvdb-large-batch-") as raw:
        directory = Path(raw)
        adapter = _adapter(executable, directory)
        controller = DatabaseController(adapter)
        try:
            controller.start()
            batch = build_large_batch_request()
            operations = batch.operations
            response = adapter.execute(batch)
            report.operations_attempted += 1
            if not isinstance(response, Acknowledgement):
                raise AssertionError(f"large batch was not acknowledged: {response!r}")
            report.acknowledged_writes += 1
            oracle = Oracle()
            oracle.apply_confirmed_write(batch, response)
            dump = adapter.execute(DumpRequest("large-batch-dump"))
            report.operations_attempted += 1
            oracle.verify_dump(dump)
            _check(executable, directory / "database.kv")
            report.passed.append("large-batch")
        except Exception as exc:
            report.failed.append({"mode": "large-batch", "error": str(exc)})
        finally:
            if controller.is_ready():
                controller.stop()


def build_large_batch_request() -> BatchRequest:
    """Build the maximum permitted 10,000-operation batch."""
    operations = tuple(
        PutRequest(f"batch-item-{index}", f"batch-{index}".encode(), b"v")
        for index in range(10_000)
    )
    return BatchRequest("large-batch", operations)


def build_enospc_batch_request() -> BatchRequest:
    """Build a batch whose positive ENOSPC budget fails after its first op."""
    value = bytes(index % 251 for index in range(ENOSPC_BATCH_VALUE_SIZE))
    operations = tuple(
        PutRequest(
            f"enospc-batch-{index}",
            f"enospc-batch-key-{index}".encode(),
            value,
        )
        for index in range(3)
    )
    return BatchRequest("enospc-batch", operations)


def _run_concurrent_writers(executable: Path, report: CampaignReport) -> None:
    with tempfile.TemporaryDirectory(prefix="kvdb-concurrent-") as raw:
        directory = Path(raw)
        adapter = _adapter(executable, directory)
        controller = DatabaseController(adapter)
        try:
            controller.start()

            def write(writer: int) -> int:
                acknowledged = 0
                for index in range(100):
                    response = adapter.execute(
                        PutRequest(
                            f"writer-{writer}-{index}",
                            f"writer-{writer}-{index}".encode(),
                            str(index).encode(),
                        )
                    )
                    report.operations_attempted += 1
                    if not isinstance(response, Acknowledgement):
                        raise AssertionError(f"writer response was {response!r}")
                    acknowledged += 1
                return acknowledged

            with ThreadPoolExecutor(max_workers=2) as pool:
                futures = [pool.submit(write, writer) for writer in range(2)]
                report.acknowledged_writes += sum(
                    future.result() for future in as_completed(futures)
                )
            dump = adapter.execute(DumpRequest("concurrent-dump"))
            if len(dump.entries) != 200:
                raise AssertionError("concurrent acknowledged writes were lost")
            _check(executable, directory / "database.kv")
            report.passed.append("concurrent-writers")
        except Exception as exc:
            report.failed.append({"mode": "concurrent-writers", "error": str(exc)})
        finally:
            if controller.is_ready():
                controller.stop()


def _apply_to_copy(oracle: Oracle, request) -> Oracle:
    candidate = Oracle(oracle.snapshot())
    if isinstance(request, PutRequest):
        candidate.apply_put(request.key, request.value)
    elif isinstance(request, DeleteRequest):
        candidate.apply_delete(request.key)
    elif isinstance(request, BatchRequest):
        candidate.apply_batch(request)
    return candidate


def _permitted_concurrent_states(base: Oracle, requests: list) -> list[Oracle]:
    """Enumerate acknowledged plus independently optional unacknowledged effects."""
    states = []
    for mask in range(1 << len(requests)):
        candidate = Oracle(base.snapshot())
        for index, request in enumerate(requests):
            if mask & (1 << index):
                candidate = _apply_to_copy(candidate, request)
        states.append(candidate)
    return states


def _run_concurrent_storage_crashes(
    executable: Path,
    probe_executable: Path,
    report: CampaignReport,
) -> None:
    """Run two concurrent transport writers while gating one storage commit."""
    cases = (
        ("put-append", "STORAGE_APPEND_BEGIN", PutRequest("cw-put", b"cw-put", b"value")),
        ("put-sync", "STORAGE_SYNC_BEGIN", PutRequest("cw-sync", b"cw-sync", b"value")),
        ("delete-append", "STORAGE_APPEND_BEGIN", DeleteRequest("cw-delete", b"cw-delete")),
        (
            "batch-append",
            "STORAGE_APPEND_BEGIN",
            BatchRequest("cw-batch-a", (PutRequest("cw-a", b"cw-a", b"a"), PutRequest("cw-b", b"cw-b", b"b"))),
        ),
        (
            "batch-sync",
            "STORAGE_SYNC_BEGIN",
            BatchRequest("cw-batch-s", (PutRequest("cw-s", b"cw-s", b"s"), PutRequest("cw-t", b"cw-t", b"t"))),
        ),
    )
    for label, wanted_event, selected in cases:
        with tempfile.TemporaryDirectory(prefix=f"kvdb-concurrent-probe-{label}-") as raw:
            directory = Path(raw)
            event_file = directory / "crashprobe.jsonl"
            gate_file = directory / "release-gate"
            adapter = _adapter(
                probe_executable,
                directory,
                {
                    "KVDB_CRASHPROBE_FILE": str(event_file),
                    "KVDB_CRASHPROBE_GATE": str(gate_file),
                    "KVDB_CRASHPROBE_GATE_EVENT": wanted_event,
                },
            )
            controller = DatabaseController(adapter)
            oracle = Oracle()
            active: dict[str, dict[str, object]] = {}
            active_lock = threading.Lock()
            results: dict[str, object] = {}
            other = PutRequest(f"{label}-other", f"{label}-other".encode(), b"other")
            requests = [("writer-1", selected), ("writer-2", other)]
            try:
                controller.start()

                def writer(writer_id: str, request) -> None:
                    lifecycle = {
                        "operation_id": request.request_id,
                        "writer_id": writer_id,
                        "operation_type": type(request).__name__,
                        "invoked_ns": time.monotonic_ns(),
                        "completed_ns": None,
                        "acknowledged": False,
                    }
                    with active_lock:
                        active[request.request_id] = lifecycle
                    try:
                        results[request.request_id] = adapter.execute(request)
                    except Exception as exc:
                        results[request.request_id] = exc
                    finally:
                        lifecycle["completed_ns"] = time.monotonic_ns()
                        lifecycle["acknowledged"] = isinstance(results.get(request.request_id), Acknowledgement)
                        with active_lock:
                            active.pop(request.request_id, None)
                        report.operation_lifecycles.append(dict(lifecycle))

                threads = [threading.Thread(target=writer, args=item, daemon=True) for item in requests]
                for thread in threads:
                    thread.start()
                deadline = time.monotonic() + 10.0
                observed = None
                while time.monotonic() < deadline:
                    if event_file.exists():
                        for line in event_file.read_text(encoding="utf-8").splitlines():
                            try:
                                event = json.loads(line)
                            except json.JSONDecodeError:
                                continue
                            if event.get("operation_id") == selected.request_id and event.get("event") == wanted_event:
                                observed = event
                                break
                    if observed is not None:
                        break
                    time.sleep(0.001)
                if observed is None:
                    raise AssertionError(f"{label} storage event {wanted_event} was not observed")
                with active_lock:
                    active_at_kill = [dict(item) for item in active.values()]
                controller.crash()
                report.crashes += 1
                for thread in threads:
                    thread.join(timeout=2.0)
                controller.start()
                report.restarts += 1
                _check(executable, directory / "database.kv")
                unacknowledged = []
                for _, request in requests:
                    response = results.get(request.request_id)
                    if isinstance(response, Acknowledgement):
                        oracle.apply_confirmed_write(request, response)
                        report.acknowledged_writes += 1
                    else:
                        unacknowledged.append(request)
                permitted = _permitted_concurrent_states(oracle, unacknowledged)
                _verify_permitted_states(adapter, permitted)
                report.operations_attempted += len(requests)
                report.storage_phase_kills += 1
                report.crash_events.append({
                    "kind": "concurrent-storage-phase",
                    "case": label,
                    "event": wanted_event,
                    "killed_operation_id": selected.request_id,
                    "probe_event": observed,
                    "active_operations_at_kill": active_at_kill,
                    "writer_count": 2,
                    "acknowledged_operations": [
                        request.request_id for _, request in requests
                        if isinstance(results.get(request.request_id), Acknowledgement)
                    ],
                })
                report.passed.append(f"concurrent-{label}")
            except Exception as exc:
                report.failed.append({"mode": f"concurrent-{label}", "error": str(exc)})
            finally:
                if controller.is_ready():
                    controller.stop()


def _run_gated_large_case(
    executable: Path,
    probe_executable: Path,
    report: CampaignReport,
    label: str,
    wanted_event: str,
    request,
) -> None:
    with tempfile.TemporaryDirectory(prefix=f"kvdb-{label}-") as raw:
        directory = Path(raw)
        database = directory / "database.kv"
        baseline = Oracle()
        seed_adapter = _adapter(executable, directory)
        seed_controller = DatabaseController(seed_adapter)
        seed_controller.start()
        try:
            for index in range(4):
                seed_request = PutRequest(
                    f"{label}-baseline-{index}",
                    f"{label}-baseline-{index}".encode(),
                    f"baseline-{index}".encode(),
                )
                response = seed_adapter.execute(seed_request)
                if not isinstance(response, Acknowledgement):
                    raise AssertionError(f"baseline write was not acknowledged: {response!r}")
                baseline.apply_confirmed_write(seed_request, response)
                report.acknowledged_writes += 1
                report.operations_attempted += 1
        finally:
            if seed_controller.is_ready():
                seed_controller.stop()

        event_file = directory / "crashprobe.jsonl"
        gate_file = directory / "release-gate"
        adapter = _adapter(
            probe_executable,
            directory,
            {
                "KVDB_CRASHPROBE_FILE": str(event_file),
                "KVDB_CRASHPROBE_GATE": str(gate_file),
                "KVDB_CRASHPROBE_GATE_EVENT": wanted_event,
            },
        )
        controller = DatabaseController(adapter)
        result: dict[str, object] = {}
        try:
            controller.start()

            def send() -> None:
                try:
                    result["response"] = adapter.execute(request)
                except Exception as exc:
                    result["error"] = exc

            sender = threading.Thread(target=send, daemon=True)
            sender.start()
            deadline = time.monotonic() + 30.0
            observed = None
            while time.monotonic() < deadline:
                if event_file.exists():
                    for line in event_file.read_text(encoding="utf-8").splitlines():
                        try:
                            event = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        if event.get("operation_id") == request.request_id and event.get("event") == wanted_event:
                            observed = event
                            break
                if observed is not None:
                    break
                time.sleep(0.001)
            if observed is None:
                raise AssertionError(f"{label}: storage event {wanted_event} was not observed")
            kill_timestamp = time.time_ns()
            controller.crash()
            report.crashes += 1
            sender.join(timeout=3.0)
            controller.start()
            report.restarts += 1
            _check(executable, database)
            response = result.get("response")
            acknowledged = isinstance(response, Acknowledgement)
            if acknowledged:
                baseline.apply_confirmed_write(request, response)
                report.acknowledged_writes += 1
            permitted = [baseline]
            if not acknowledged:
                permitted.append(_apply_to_copy(baseline, request))
            _verify_permitted_states(adapter, permitted)
            report.storage_phase_kills += 1
            report.crash_events.append(
                {
                    "kind": label,
                    "seed": report.seed,
                    "operation_id": request.request_id,
                    "operation_type": type(request).__name__,
                    "operation_count": len(request.operations) if isinstance(request, BatchRequest) else 1,
                    "baseline_acknowledged_operations": 4,
                    "probe_event": observed,
                    "kill_timestamp_ns": kill_timestamp,
                    "acknowledged": acknowledged,
                    "recovery_verified": True,
                    "oracle_verified": True,
                }
            )
            report.passed.append(label)
        except Exception as exc:
            report.failed.append({"mode": label, "error": str(exc)})
        finally:
            if controller.is_ready():
                controller.stop()


def _run_large_value_crashes(executable: Path, probe_executable: Path, report: CampaignReport) -> None:
    value = bytes(index % 251 for index in range(1 << 20))
    for phase in ("STORAGE_APPEND_BEGIN", "STORAGE_SYNC_BEGIN"):
        request = PutRequest(f"large-value-{phase}", b"large-value", value)
        _run_gated_large_case(
            executable,
            probe_executable,
            report,
            f"large-value-crash-{phase.removeprefix('STORAGE_').removesuffix('_BEGIN').lower()}",
            phase,
            request,
        )


def _run_large_batch_crashes(executable: Path, probe_executable: Path, report: CampaignReport) -> None:
    operations = []
    for index in range(10_000):
        key = f"large-batch-key-{index:05d}".encode()
        if index % 3 == 0:
            operations.append(DeleteRequest(f"large-batch-delete-{index}", key))
        else:
            operations.append(PutRequest(f"large-batch-put-{index}", key, f"value-{index}".encode()))
    request = BatchRequest("large-batch-crash", tuple(operations))
    for phase in ("STORAGE_APPEND_BEGIN", "STORAGE_SYNC_BEGIN"):
        _run_gated_large_case(
            executable,
            probe_executable,
            report,
            f"large-batch-crash-{phase.removeprefix('STORAGE_').removesuffix('_BEGIN').lower()}",
            phase,
            request,
        )


def _run_close_crashes(executable: Path, probe_executable: Path, report: CampaignReport) -> None:
    for phase in (
        "STORAGE_CLOSE_ENTER",
        "STORAGE_CLOSE_SYNC_BEGIN",
        "STORAGE_CLOSE_FILE_CLOSE_BEGIN",
        "STORAGE_CLOSE_EXIT",
    ):
        label = f"close-crash-{phase.removeprefix('STORAGE_CLOSE_').lower()}"
        with tempfile.TemporaryDirectory(prefix=f"kvdb-{label}-") as raw:
            directory = Path(raw)
            database = directory / "database.kv"
            oracle = Oracle()
            seed = _adapter(executable, directory)
            seed_controller = DatabaseController(seed)
            seed_controller.start()
            try:
                for index in range(4):
                    request = PutRequest(f"{label}-baseline-{index}", f"{label}-{index}".encode(), f"value-{index}".encode())
                    response = seed.execute(request)
                    if not isinstance(response, Acknowledgement):
                        raise AssertionError("close baseline write was not acknowledged")
                    oracle.apply_confirmed_write(request, response)
                    report.acknowledged_writes += 1
                    report.operations_attempted += 1
            finally:
                if seed_controller.is_ready():
                    seed_controller.stop()

            event_file = directory / "crashprobe.jsonl"
            gate_file = directory / "release-gate"
            adapter = _adapter(
                probe_executable,
                directory,
                {
                    "KVDB_CRASHPROBE_FILE": str(event_file),
                    "KVDB_CRASHPROBE_GATE": str(gate_file),
                    "KVDB_CRASHPROBE_GATE_EVENT": phase,
                    "KVDB_CRASHPROBE_ALLOW_CLOSE": "1",
                },
            )
            controller = DatabaseController(adapter)
            result: dict[str, object] = {}
            try:
                controller.start()

                def close_request() -> None:
                    try:
                        result["response"] = adapter.execute(CloseRequest(f"{label}-request"))
                    except Exception as exc:
                        result["error"] = exc

                sender = threading.Thread(target=close_request, daemon=True)
                sender.start()
                deadline = time.monotonic() + 10.0
                observed = None
                while time.monotonic() < deadline:
                    if event_file.exists():
                        for line in event_file.read_text(encoding="utf-8").splitlines():
                            try:
                                event = json.loads(line)
                            except json.JSONDecodeError:
                                continue
                            if event.get("event") == phase:
                                observed = event
                                break
                    if observed is not None:
                        break
                    time.sleep(0.001)
                if observed is None:
                    raise AssertionError(f"{label}: close event was not observed")
                controller.crash()
                report.crashes += 1
                sender.join(timeout=2.0)
                controller.start()
                report.restarts += 1
                _check(executable, database)
                _verify_permitted_states(adapter, [oracle])
                report.storage_phase_kills += 1
                report.crash_events.append({
                    "kind": "close-crash",
                    "phase": phase,
                    "probe_event": observed,
                    "baseline_acknowledged_operations": 4,
                    "recovery_verified": True,
                    "oracle_verified": True,
                })
                report.passed.append(label)
            except Exception as exc:
                report.failed.append({"mode": label, "error": str(exc)})
            finally:
                if controller.is_ready():
                    controller.stop()


def _history_record(item) -> dict[str, object]:
    return {
        "operation_id": item.operation_id,
        "client_id": item.client_id,
        "operation": item.operation,
        "key": base64.b64encode(item.key).decode() if item.key is not None else None,
        "value": base64.b64encode(item.value).decode() if item.value is not None else None,
        "batch": [
            {"operation": operation, "key": base64.b64encode(key).decode(), "value": base64.b64encode(value).decode() if value is not None else None}
            for operation, key, value in item.batch
        ],
        "invoked_ns": item.invoked_ns,
        "completed_ns": item.completed_ns,
        "status": item.status,
        "returned_value": base64.b64encode(item.returned_value).decode() if item.returned_value is not None else None,
        "error": item.error,
        "transaction_id": item.transaction_id,
    }


def _run_linearizability(executable: Path, seed: int, report: CampaignReport) -> None:
    with tempfile.TemporaryDirectory(prefix="kvdb-linearizability-") as raw:
        directory = Path(raw)
        adapter = _adapter(executable, directory)
        controller = DatabaseController(adapter)
        controller.start()
        try:
            requests = [
                ("writer-1", PutRequest(f"lin-{seed}-put-1", b"shared", b"one")),
                ("reader-1", GetRequest(f"lin-{seed}-get-1", b"shared")),
                ("writer-2", DeleteRequest(f"lin-{seed}-delete-1", b"shared")),
                ("writer-1", BatchRequest(f"lin-{seed}-batch", (PutRequest("lin-b-a", b"batch-a", b"A"), DeleteRequest("lin-b-d", b"missing")))),
                ("reader-1", GetRequest(f"lin-{seed}-get-2", b"batch-a")),
                ("writer-2", PutRequest(f"lin-{seed}-put-2", b"shared", b"two")),
                ("reader-1", GetRequest(f"lin-{seed}-get-3", b"shared")),
            ]
            history = []
            history_lock = threading.Lock()

            def execute(client_id: str, request) -> None:
                invoked = time.monotonic_ns()
                try:
                    response = adapter.execute(request)
                    completed = time.monotonic_ns()
                    record = history_operation(
                        request,
                        response,
                        client_id=client_id,
                        invoked_ns=invoked,
                        completed_ns=completed,
                    )
                    with history_lock:
                        history.append(record)
                except Exception as exc:
                    completed = time.monotonic_ns()
                    with history_lock:
                        history.append(history_operation(
                            request,
                            ErrorResponse(request.request_id, str(exc)),
                            client_id=client_id,
                            invoked_ns=invoked,
                            completed_ns=completed,
                        ))

            with ThreadPoolExecutor(max_workers=3) as pool:
                futures = [pool.submit(execute, client_id, request) for client_id, request in requests]
                for future in futures:
                    future.result()
            result = check_history(history, max_states=100_000)
            report.linearizability_results.append({
                "result": result.result.value,
                "history_size": result.history_size,
                "explored_states": result.explored_states,
                "search_budget": 100_000,
                "linearization": result.linearization,
                "history": [_history_record(item) for item in history],
            })
            if result.result is CheckResult.NOT_LINEARIZABLE:
                report.failed.append({"mode": "linearizability", "error": result.reason})
            elif result.result is CheckResult.INCONCLUSIVE:
                report.not_tested.append(f"linearizability: {result.reason}")
            else:
                report.passed.append("linearizability")
        finally:
            if controller.is_ready():
                controller.stop()


def _verify_permitted_states(adapter, oracles: list[Oracle]) -> dict[bytes, bytes]:
    response = adapter.execute(DumpRequest(f"recovery-dump-{time.monotonic_ns()}"))
    if not hasattr(response, "entries"):
        raise AssertionError(f"recovery dump was not a DUMP response: {response!r}")
    actual = dict(response.entries)
    if not any(actual == candidate.snapshot() for candidate in oracles):
        raise AssertionError(
            "recovered state was not one of the permitted acknowledged/pre-post states"
        )
    return actual
    return actual


def _run_random_crash(executable: Path, seed: int, iterations: int, report: CampaignReport) -> None:
    """Run process-level crashes at seeded millisecond delays.

    This is process-kill evidence only. Active-operation evidence describes the
    parent harness interval around a request; it is not a claim that the Go
    storage critical section was executing at the exact kill instruction.
    It does not claim power-loss safety.
    """
    with tempfile.TemporaryDirectory(prefix="kvdb-random-crash-") as raw:
        directory = Path(raw)
        adapter = _adapter(executable, directory)
        controller = DatabaseController(adapter)
        oracle = Oracle()
        rng = random.Random(seed ^ 0xC0DEC0DE)
        active: dict[str, dict[str, object]] = {}
        active_lock = threading.Lock()
        try:
            controller.start()
            requests = WorkloadGenerator(
                WorkloadConfig(
                    seed=seed,
                    num_operations=iterations,
                    batch_probability=0.30,
                    max_batch_size=8,
                )
            ).generate()
            for iteration, request in enumerate(requests, start=1):
                delay_ms = rng.randint(1, 20)
                fired = threading.Event()
                kill_evidence: dict[str, object] = {}
                lifecycle = {
                    "operation_id": request.request_id,
                    "operation_type": type(request).__name__,
                    "writer_id": "random-crash-writer-0",
                    "invoked_ns": time.monotonic_ns(),
                    "completed_ns": None,
                    "acknowledged": False,
                }
                with active_lock:
                    active[str(request.request_id)] = lifecycle

                def crash_later() -> None:
                    time.sleep(delay_ms / 1000.0)
                    if controller.is_ready():
                        kill_evidence["kill_ns"] = time.monotonic_ns()
                        with active_lock:
                            kill_evidence["active_operation_ids"] = list(active)
                        try:
                            controller.crash()
                            fired.set()
                        except Exception:
                            pass

                killer = threading.Thread(target=crash_later, daemon=True)
                killer.start()
                acknowledged = False
                try:
                    report.operations_attempted += 1
                    response = adapter.execute(request)
                    if isinstance(response, Acknowledgement):
                        oracle.apply_confirmed_write(request, response)
                        report.acknowledged_writes += 1
                        acknowledged = True
                except AdapterAmbiguousError:
                    pass
                finally:
                    lifecycle["completed_ns"] = time.monotonic_ns()
                    lifecycle["acknowledged"] = acknowledged
                    with active_lock:
                        active.pop(str(request.request_id), None)
                    killer.join(timeout=2.0)

                if fired.is_set() or not controller.is_ready():
                    report.crashes += 1
                    controller.start()
                    report.restarts += 1
                    _check(executable, directory / "database.kv")
                    permitted = [oracle]
                    if not acknowledged and isinstance(
                        request, (PutRequest, DeleteRequest, BatchRequest)
                    ):
                        permitted.append(_apply_to_copy(oracle, request))
                    report.crash_events.append(
                        {
                            "kind": "random-process",
                            "delay_ms": delay_ms,
                            "request_id": request.request_id,
                            "kill_ns": kill_evidence.get("kill_ns"),
                            "active_operation_ids": kill_evidence.get("active_operation_ids", []),
                            "harness_active": bool(kill_evidence.get("active_operation_ids")),
                        }
                    )
                    report.operation_lifecycles.append(dict(lifecycle))
                    if kill_evidence.get("active_operation_ids"):
                        report.harness_active_kills += 1
                    else:
                        report.idle_kills += 1
                    recovered = _verify_permitted_states(adapter, permitted)
                    # An unacknowledged write may have committed before the
                    # process was killed.  Once the recovered snapshot has
                    # been validated against the permitted states, use it as
                    # the baseline for subsequent requests.
                    oracle._data = recovered
                    print(
                        f"iteration {iteration}/{iterations}: crash/restart "
                        f"request={request.request_id} acknowledged={acknowledged}",
                        flush=True,
                    )
                else:
                    report.operation_lifecycles.append(dict(lifecycle))
                    print(
                        f"iteration {iteration}/{iterations}: completed "
                        f"request={request.request_id} acknowledged={acknowledged}",
                        flush=True,
                    )
            _check(executable, directory / "database.kv")
            _verify_permitted_states(adapter, [oracle])
            report.passed.append("random-process-crashes")
        except Exception as exc:
            report.recovery_failures += 1 if "recovery" in str(exc).lower() else 0
            report.failed.append({"mode": "random-process-crashes", "error": str(exc)})
        finally:
            if controller.is_ready():
                controller.stop()


def _run_storage_phase_crashes(
    executable: Path,
    probe_executable: Path,
    report: CampaignReport,
) -> None:
    """Kill a crash-probe child after an observed storage phase event."""
    cases = (
        ("append", "STORAGE_APPEND_BEGIN", PutRequest("probe-put", b"probe", b"value")),
        ("sync", "STORAGE_SYNC_BEGIN", PutRequest("probe-sync", b"probe", b"value")),
        (
            "batch",
            "STORAGE_APPEND_BEGIN",
            BatchRequest(
                "probe-batch",
                (
                    PutRequest("probe-batch-a", b"batch-a", b"one"),
                    PutRequest("probe-batch-b", b"batch-b", b"two"),
                ),
            ),
        ),
    )
    for label, wanted_event, request in cases:
        with tempfile.TemporaryDirectory(prefix=f"kvdb-probe-{label}-") as raw:
            directory = Path(raw)
            event_file = directory / "crashprobe.jsonl"
            gate_file = directory / "release-gate"
            adapter = _adapter(
                probe_executable,
                directory,
                {
                    "KVDB_CRASHPROBE_FILE": str(event_file),
                    "KVDB_CRASHPROBE_GATE": str(gate_file),
                    "KVDB_CRASHPROBE_GATE_EVENT": wanted_event,
                },
            )
            controller = DatabaseController(adapter)
            oracle = Oracle()
            result: dict[str, object] = {}
            try:
                controller.start()

                def send() -> None:
                    try:
                        result["response"] = adapter.execute(request)
                    except Exception as exc:  # process termination is expected
                        result["error"] = exc

                sender = threading.Thread(target=send, daemon=True)
                sender.start()
                deadline = time.monotonic() + 10.0
                observed = None
                while time.monotonic() < deadline:
                    if event_file.exists():
                        for line in event_file.read_text(encoding="utf-8").splitlines():
                            try:
                                event = json.loads(line)
                            except json.JSONDecodeError:
                                continue
                            if (
                                event.get("operation_id") == request.request_id
                                and event.get("event") == wanted_event
                            ):
                                observed = event
                                break
                    if observed is not None:
                        break
                    time.sleep(0.001)
                if observed is None:
                    raise AssertionError(f"{label} probe event {wanted_event} was not observed")
                controller.crash()
                report.crashes += 1
                sender.join(timeout=2.0)
                controller.start()
                report.restarts += 1
                _check(executable, directory / "database.kv")
                response = result.get("response")
                acknowledged = isinstance(response, Acknowledgement)
                if acknowledged:
                    oracle.apply_confirmed_write(request, response)
                permitted = [oracle]
                if not acknowledged:
                    permitted.append(_apply_to_copy(oracle, request))
                _verify_permitted_states(adapter, permitted)
                report.storage_phase_kills += 1
                report.crash_events.append(
                    {
                        "kind": "storage-phase",
                        "case": label,
                        "event": wanted_event,
                        "operation_id": request.request_id,
                        "probe_event": observed,
                        "acknowledged": acknowledged,
                    }
                )
                report.passed.append(f"storage-{label}-crash")
            except Exception as exc:
                report.failed.append({"mode": f"storage-{label}-crash", "error": str(exc)})
            finally:
                if controller.is_ready():
                    controller.stop()


def _run_recovery_crashes(
    executable: Path,
    fault_executable: Path,
    report: CampaignReport,
) -> None:
    for point in ("recovery-started", "recovery-transaction-applied"):
        with tempfile.TemporaryDirectory(prefix="kvdb-recovery-crash-") as raw:
            directory = Path(raw)
            database = directory / "database.kv"
            seed_adapter = _adapter(executable, directory)
            seed_controller = DatabaseController(seed_adapter)
            oracle = Oracle()
            try:
                seed_controller.start()
                for index in range(5):
                    request = PutRequest(
                        f"recovery-seed-{index}",
                        f"recovery-{index}".encode(),
                        f"value-{index}".encode(),
                    )
                    response = seed_adapter.execute(request)
                    if not isinstance(response, Acknowledgement):
                        raise AssertionError("recovery seed write was not acknowledged")
                    oracle.apply_confirmed_write(request, response)
                seed_controller.stop()

                crashing = _adapter(
                    fault_executable,
                    directory,
                    {"KVDB_CRASH_POINT": point},
                )
                try:
                    crashing.start()
                except Exception as exc:
                    if "code 91" not in str(exc):
                        raise AssertionError(
                            f"{point} did not reach the intended hook: {exc}"
                        ) from exc
                else:
                    raise AssertionError(f"{point} did not crash during recovery")

                normal = _adapter(executable, directory)
                controller = DatabaseController(normal)
                controller.start()
                _check(executable, database)
                response = normal.execute(DumpRequest(f"{point}-dump"))
                oracle.verify_dump(response)
                report.restarts += 1
                report.crash_events.append({"kind": "recovery", "point": point})
                report.passed.append(f"{point}-recovery-crash")
                controller.stop()
            except Exception as exc:
                report.recovery_failures += 1
                report.failed.append({"mode": point, "error": str(exc)})
            finally:
                if seed_controller.is_ready():
                    seed_controller.stop()


def _run_compaction_crashes(
    executable: Path,
    fault_executable: Path,
    report: CampaignReport,
) -> None:
    points = (
        "compaction-before-temp",
        "compaction-after-temp-sync",
        "compaction-after-replacement",
        "compaction-before-backup-delete",
    )
    for point in points:
        with tempfile.TemporaryDirectory(prefix="kvdb-compaction-crash-") as raw:
            directory = Path(raw)
            seed = _adapter(executable, directory)
            controller = DatabaseController(seed)
            oracle = Oracle()
            try:
                controller.start()
                for key, value in ((b"keep", b"yes"), (b"gone", b"old")):
                    request = PutRequest(f"seed-{key!r}", key, value)
                    response = seed.execute(request)
                    if not isinstance(response, Acknowledgement):
                        raise AssertionError("compaction seed write was not acknowledged")
                    oracle.apply_confirmed_write(request, response)
                delete = DeleteRequest("delete-gone", b"gone")
                response = seed.execute(delete)
                if not isinstance(response, Acknowledgement):
                    raise AssertionError("compaction delete was not acknowledged")
                oracle.apply_confirmed_write(delete, response)
                controller.stop()

                crashing = _adapter(fault_executable, directory, {"KVDB_CRASH_POINT": point})
                crashing_controller = DatabaseController(crashing)
                crashing_controller.start()
                injected_exit = False
                try:
                    crashing.execute(CompactRequest(f"compact-{point}"))
                except Exception as exc:
                    if "code 91" not in str(exc):
                        raise AssertionError(
                            f"{point} did not reach the intended hook: {exc}"
                        ) from exc
                    injected_exit = True
                if not injected_exit:
                    raise AssertionError(f"{point} did not interrupt compaction")

                normal = _adapter(executable, directory)
                normal_controller = DatabaseController(normal)
                normal_controller.start()
                _check(executable, directory / "database.kv")
                oracle.verify_dump(normal.execute(DumpRequest(f"dump-{point}")))
                followup = PutRequest("after-compaction", b"after", b"ok")
                if not isinstance(normal.execute(followup), Acknowledgement):
                    raise AssertionError("database was not writable after compaction recovery")
                report.restarts += 1
                report.crash_events.append({"kind": "compaction", "point": point})
                report.passed.append(f"{point}-compaction-crash")
                normal_controller.stop()
            except Exception as exc:
                report.recovery_failures += 1
                report.failed.append({"mode": point, "error": str(exc)})
            finally:
                if controller.is_ready():
                    controller.stop()


def _run_injected_enospc(executable: Path, report: CampaignReport) -> None:
    injected = os.environ.get("KVDB_ENOSPC_EXECUTABLE")
    if not injected:
        report.not_tested.append(
            "deterministic injected ENOSPC (set KVDB_ENOSPC_EXECUTABLE to a -tags kvdb_enospc binary)"
        )
        return
    injected_executable = Path(injected).resolve()
    if not injected_executable.exists():
        report.environment_blocked.append(f"ENOSPC executable not found: {injected_executable}")
        return
    with tempfile.TemporaryDirectory(prefix="kvdb-enospc-") as raw:
        directory = Path(raw)
        seed = _adapter(executable, directory)
        seed_controller = DatabaseController(seed)
        try:
            seed_controller.start()
            stable = PutRequest("stable", b"stable", b"durable")
            if not isinstance(seed.execute(stable), Acknowledgement):
                raise AssertionError("stable preload was not acknowledged")
            seed_controller.stop()

            failing = _adapter(
                injected_executable,
                directory,
                {"KVDB_ENOSPC_AFTER_BYTES": "0"},
            )
            failing_controller = DatabaseController(failing)
            failing_controller.start()
            response = failing.execute(PutRequest("full", b"full", b"rejected"))
            if not isinstance(response, ErrorResponse):
                raise AssertionError(f"injected ENOSPC unexpectedly returned {response!r}")
            if response.outcome == "UNKNOWN":
                raise AssertionError("injected ENOSPC produced UNKNOWN instead of a recoverable write error")
            failing_controller.stop()

            normal = _adapter(executable, directory)
            normal_controller = DatabaseController(normal)
            normal_controller.start()
            _check(executable, directory / "database.kv")
            dump = normal.execute(DumpRequest("enospc-dump"))
            if dict(dump.entries) != {b"stable": b"durable"}:
                raise AssertionError("acknowledged data did not survive injected ENOSPC")
            report.passed.append("injected-enospc")
            normal_controller.stop()
        except Exception as exc:
            report.failed.append({"mode": "injected-enospc", "error": str(exc)})
        finally:
            if seed_controller.is_ready():
                seed_controller.stop()


def _run_injected_enospc_batch(
    executable: Path,
    injected_executable: Path,
    report: CampaignReport,
) -> None:
    """Verify all-or-nothing recovery when ENOSPC interrupts a batch."""
    with tempfile.TemporaryDirectory(prefix="kvdb-enospc-batch-") as raw:
        directory = Path(raw)
        batch = build_enospc_batch_request()
        oracle = Oracle()
        seed = _adapter(executable, directory)
        seed_controller = DatabaseController(seed)
        calibration_controller = None
        failing_controller = None
        normal_controller = None
        try:
            seed_controller.start()
            stable = PutRequest("stable", b"stable", b"durable")
            stable_response = seed.execute(stable)
            report.operations_attempted += 1
            if not isinstance(stable_response, Acknowledgement):
                raise AssertionError("stable preload was not acknowledged")
            oracle.apply_confirmed_write(stable, stable_response)
            seed_controller.stop()

            # A one-operation calibration proves the budget is sufficient for
            # the first batch record, while the full batch must exceed it.
            calibration_dir = directory / "calibration"
            calibration = _adapter(
                injected_executable,
                calibration_dir,
                {"KVDB_ENOSPC_AFTER_BYTES": str(ENOSPC_BATCH_BUDGET)},
            )
            calibration_controller = DatabaseController(calibration)
            calibration_controller.start()
            calibration_response = calibration.execute(batch.operations[0])
            report.operations_attempted += 1
            if not isinstance(calibration_response, Acknowledgement):
                raise AssertionError(
                    "ENOSPC budget failed before the first batch operation"
                )
            calibration_controller.stop()

            failing = _adapter(
                injected_executable,
                directory,
                {"KVDB_ENOSPC_AFTER_BYTES": str(ENOSPC_BATCH_BUDGET)},
            )
            failing_controller = DatabaseController(failing)
            failing_controller.start()
            batch_response = failing.execute(batch)
            report.operations_attempted += 1
            if not isinstance(batch_response, ErrorResponse):
                raise AssertionError(
                    f"partway batch ENOSPC unexpectedly returned {batch_response!r}"
                )
            if batch_response.outcome == "UNKNOWN":
                raise AssertionError(
                    "partway batch ENOSPC returned UNKNOWN instead of ERROR"
                )
            failing_controller.stop()

            normal = _adapter(executable, directory)
            normal_controller = DatabaseController(normal)
            normal_controller.start()
            _check(executable, directory / "database.kv")
            recovered = normal.execute(DumpRequest("enospc-batch-dump"))
            report.operations_attempted += 1
            oracle.verify_dump(recovered)
            if any(
                entry[0].startswith(b"enospc-batch-key-")
                for entry in recovered.entries
            ):
                raise AssertionError("partially applied ENOSPC batch became visible")

            followup = PutRequest("after-enospc-batch", b"after", b"usable")
            followup_response = normal.execute(followup)
            report.operations_attempted += 1
            if not isinstance(followup_response, Acknowledgement):
                raise AssertionError("database was not writable after ENOSPC recovery")
            report.acknowledged_writes += 2
            oracle.apply_confirmed_write(followup, followup_response)
            oracle.verify_dump(normal.execute(DumpRequest("enospc-batch-final")))
            _check(executable, directory / "database.kv")
            report.passed.append("injected-enospc-batch-atomicity")
            normal_controller.stop()
        except Exception as exc:
            report.failed.append(
                {"mode": "injected-enospc-batch-atomicity", "error": str(exc)}
            )
        finally:
            for child_controller in (
                calibration_controller,
                failing_controller,
                normal_controller,
            ):
                if child_controller is not None and child_controller.is_ready():
                    child_controller.stop()
            if seed_controller.is_ready():
                seed_controller.stop()


def main() -> int:
    parser = argparse.ArgumentParser(description="Run real Go database torture modes")
    parser.add_argument("--executable", default=None)
    parser.add_argument("--fault-executable", default=None)
    parser.add_argument("--crashprobe-executable", default=None)
    parser.add_argument(
        "--seed",
        type=int,
        required=True,
        help="mandatory deterministic seed",
    )
    parser.add_argument("--iterations", type=int, default=500)
    parser.add_argument(
        "--mode",
        action="append",
        choices=(
            "default",
            "random-crash",
            "recovery-crash",
            "compaction-crash",
            "compaction",
            "large-value",
            "large-values",
            "large-batch",
            "concurrent-writers",
            "injected-enospc",
            "storage-crash",
            "concurrent-storage-crash",
            "large-value-crash",
            "large-batch-crash",
            "close-crash",
            "linearizability",
        ),
        default=[],
    )
    parser.add_argument("--json-report", type=Path, default=Path("artifacts/campaign.json"))
    args = parser.parse_args()
    aliases = {
        "large-values": "large-value",
        "compaction": "compaction-crash",
    }
    requested_modes = args.mode or ["random-crash", "concurrent-writers"]
    modes = [aliases.get(mode, mode) for mode in requested_modes]
    report = CampaignReport(
        started_utc=datetime.now(timezone.utc).isoformat(),
        seed=args.seed,
        iterations=args.iterations,
        modes=modes,
        passed=[],
        failed=[],
        not_tested=list(PERMANENTLY_UNTESTED),
        simulated_only=[],
        environment_blocked=[],
        crash_events=[],
    )
    executable = _executable(args.executable)
    fault_value = args.fault_executable or os.environ.get("KVDB_FAULT_EXECUTABLE")
    fault_executable = Path(fault_value).resolve() if fault_value else None
    probe_value = args.crashprobe_executable or os.environ.get("KVDB_CRASHPROBE_EXECUTABLE")
    probe_executable = Path(probe_value).resolve() if probe_value else None
    try:
        if not executable.exists():
            raise FileNotFoundError(executable)
        for mode in modes:
            if mode == "default":
                _run_default(executable, args.seed, args.iterations, report)
            elif mode == "large-value":
                _run_large_value(executable, report)
            elif mode == "large-batch":
                _run_large_batch(executable, report)
            elif mode == "concurrent-writers":
                _run_concurrent_writers(executable, report)
            elif mode == "random-crash":
                _run_random_crash(executable, args.seed, args.iterations, report)
            elif mode == "recovery-crash":
                if fault_executable is None or not fault_executable.exists():
                    report.not_tested.append("recovery-crash: fault-injection executable is unavailable")
                else:
                    _run_recovery_crashes(executable, fault_executable, report)
            elif mode == "compaction-crash":
                if fault_executable is None or not fault_executable.exists():
                    report.not_tested.append("compaction-crash: fault-injection executable is unavailable")
                else:
                    _run_compaction_crashes(executable, fault_executable, report)
            elif mode == "injected-enospc":
                _run_injected_enospc(executable, report)
                injected = os.environ.get("KVDB_ENOSPC_EXECUTABLE")
                if injected and Path(injected).exists():
                    _run_injected_enospc_batch(
                        executable,
                        Path(injected).resolve(),
                        report,
                    )
            elif mode == "storage-crash":
                if probe_executable is None or not probe_executable.exists():
                    report.not_tested.append("storage-crash: crash-probe executable is unavailable")
                else:
                    _run_storage_phase_crashes(executable, probe_executable, report)
            elif mode == "concurrent-storage-crash":
                if probe_executable is None or not probe_executable.exists():
                    report.not_tested.append("concurrent-storage-crash: crash-probe executable is unavailable")
                else:
                    _run_concurrent_storage_crashes(executable, probe_executable, report)
            elif mode == "large-value-crash":
                if probe_executable is None or not probe_executable.exists():
                    report.not_tested.append("large-value-crash: crash-probe executable is unavailable")
                else:
                    _run_large_value_crashes(executable, probe_executable, report)
            elif mode == "large-batch-crash":
                if probe_executable is None or not probe_executable.exists():
                    report.not_tested.append("large-batch-crash: crash-probe executable is unavailable")
                else:
                    _run_large_batch_crashes(executable, probe_executable, report)
            elif mode == "close-crash":
                if probe_executable is None or not probe_executable.exists():
                    report.not_tested.append("close-crash: crash-probe executable is unavailable")
                else:
                    _run_close_crashes(executable, probe_executable, report)
            elif mode == "linearizability":
                _run_linearizability(executable, args.seed, report)
    except (OSError, PermissionError) as exc:
        report.environment_blocked.append(str(exc))
    args.json_report.parent.mkdir(parents=True, exist_ok=True)
    report.finished_utc = datetime.now(timezone.utc).isoformat()
    report.duration_seconds = (
        datetime.fromisoformat(report.finished_utc).timestamp()
        - datetime.fromisoformat(report.started_utc).timestamp()
    )
    finalize_not_tested(report)
    args.json_report.write_text(json.dumps(asdict(report), indent=2), encoding="utf-8")
    args.json_report.with_suffix(".md").write_text(
        render_markdown(report), encoding="utf-8"
    )
    print(json.dumps(asdict(report), indent=2))
    return 1 if report.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())

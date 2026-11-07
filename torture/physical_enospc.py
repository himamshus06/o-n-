"""Run physical ENOSPC checks inside an already-mounted disposable filesystem.

The shell wrapper creates and owns the bounded ext4 image. This helper refuses
to run unless the database path is on a mounted filesystem and records the
actual responses from the normal executable.
"""

from __future__ import annotations

import argparse
import base64
import errno
import json
import os
from pathlib import Path
import subprocess
import sys
import time


_LAST_REPORT: dict | None = None


def free_bytes(path: Path) -> int:
    stat = os.statvfs(path)
    return stat.f_bavail * stat.f_frsize


def capacity_bytes(path: Path) -> int:
    stat = os.statvfs(path)
    return stat.f_blocks * stat.f_frsize


class Server:
    def __init__(self, executable: Path, database: Path):
        self.process = subprocess.Popen(
            [os.fspath(executable), "serve", os.fspath(database)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        assert self.process.stdin and self.process.stdout and self.process.stderr
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            line = self.process.stderr.readline()
            if line == "":
                break
            if line.strip() == "READY":
                return
        self.stop()
        raise RuntimeError("database did not emit READY")

    def request(self, request: dict) -> dict:
        assert self.process.stdin and self.process.stdout
        self.process.stdin.write(json.dumps(request) + "\n")
        self.process.stdin.flush()
        line = self.process.stdout.readline()
        if not line:
            raise RuntimeError(f"database exited with code {self.process.poll()}")
        return json.loads(line)

    def stop(self) -> None:
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)


def b64(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def put(server: Server, ident: str, key: bytes, value: bytes) -> dict:
    return server.request({"id": ident, "op": "PUT", "key": b64(key), "value": b64(value)})


def dump(server: Server, ident: str) -> dict[bytes, bytes]:
    response = server.request({"id": ident, "op": "DUMP"})
    if response.get("status") != "DUMP":
        raise RuntimeError(f"DUMP failed: {response}")
    return {base64.b64decode(k): base64.b64decode(v) for k, v in response["entries"]}


def fill_until(path: Path, minimum_free: int) -> tuple[list[Path], int]:
    files: list[Path] = []
    while free_bytes(path) > minimum_free:
        target = path / f"filler-{len(files):04d}.bin"
        try:
            with target.open("wb") as stream:
                while free_bytes(path) > minimum_free:
                    stream.write(b"X" * (1024 * 1024))
                    stream.flush()
            files.append(target)
        except OSError as exc:
            if exc.errno != errno.ENOSPC:
                raise
            if target.exists():
                files.append(target)
            break
    return files, free_bytes(path)


def remove_fillers(files: list[Path]) -> None:
    for filler in files:
        filler.unlink(missing_ok=True)


def record_assertion(report: dict, name: str, passed: bool, detail: str = "") -> None:
    report.setdefault("assertions", []).append({
        "name": name,
        "passed": passed,
        "detail": detail,
    })
    if not passed:
        report.setdefault("failures", []).append({
            "assertion": name,
            "detail": detail,
        })


def require_assertion(report: dict, name: str, passed: bool, detail: str) -> None:
    record_assertion(report, name, passed, detail)
    if not passed:
        raise AssertionError(f"{name}: {detail}")


def write_report(report: dict, report_path: Path) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")


def require_physical_enospc(response: dict, report: dict, name: str) -> None:
    error = str(response.get("error", "")).lower()
    passed = response.get("status") == "ERROR" and ("no space" in error or "enospc" in error)
    require_assertion(report, name, passed, f"expected physical ENOSPC, got {response}")


def run(executable: Path, database: Path, report_path: Path) -> int:
    global _LAST_REPORT
    mount = database.parent
    if not os.path.ismount(mount):
        raise RuntimeError(f"refusing to run: {mount} is not a mount point")
    baseline = {
        b"alpha": b"one",
        b"beta": b"two",
        b"large": b"L" * (512 * 1024),
    }
    report: dict = {
        "label": "PHYSICAL_ENOSPC",
        "result": "RUNNING",
        "filesystem_capacity_bytes": capacity_bytes(mount),
        "filesystem_free_before": free_bytes(mount),
        "baseline_ack_count": 0,
        "put": {},
        "batch": {},
        "recovery": {},
        "post_recovery_write": {},
        "rollback_failure": {"status": "NOT_TESTED", "reason": "not safely forced"},
        "assertions": [],
        "failures": [],
    }
    _LAST_REPORT = report
    server = Server(executable, database)
    try:
        for key, value in baseline.items():
            response = put(server, f"baseline-{key.decode()}", key, value)
            if response.get("status") != "ACK":
                raise RuntimeError(f"baseline write failed: {response}")
            report["baseline_ack_count"] += 1
        require_assertion(report, "baseline_dump_preserved_before_fill", dump(server, "baseline-dump") == baseline, "baseline DUMP mismatch")
    finally:
        server.stop()

    server = Server(executable, database)
    try:
        before = free_bytes(mount)
        fillers, available = fill_until(mount, 1500 * 1024)
        report["put"].update({"free_before_fill": before, "free_before_failure": available})
        response = put(server, "physical-put", b"physical-put", b"P" * (1024 * 1024))
        require_physical_enospc(response, report, "put_received_physical_enospc")
        report["put"].update({"response": response, "error": response.get("error")})
    finally:
        server.stop()
        remove_fillers(fillers)

    server = Server(executable, database)
    try:
        recovered = dump(server, "after-put-dump")
        baseline_preserved = recovered == baseline
        failed_key_absent = b"physical-put" not in recovered
        report["put"].update({"baseline_preserved": baseline_preserved, "failed_key_present": not failed_key_absent})
        require_assertion(report, "put_baseline_preserved_after_restart", baseline_preserved, "baseline entries changed after failed PUT")
        require_assertion(report, "failed_put_key_absent_after_restart", failed_key_absent, "failed PUT key became visible")
    finally:
        server.stop()

    server = Server(executable, database)
    try:
        before = free_bytes(mount)
        fillers, available = fill_until(mount, 1500 * 1024)
        operations = [
            {"op": "PUT", "key": b64(b"batch-a"), "value": b64(b"A" * (512 * 1024))},
            {"op": "PUT", "key": b64(b"batch-b"), "value": b64(b"B" * (512 * 1024))},
        ]
        response = server.request({"id": "physical-batch", "op": "BATCH", "operations": operations})
        require_physical_enospc(response, report, "batch_received_physical_enospc")
        report["batch"].update({"free_before_fill": before, "free_before_failure": available, "response": response, "error": response.get("error")})
    finally:
        server.stop()
        remove_fillers(fillers)

    server = Server(executable, database)
    try:
        recovered = dump(server, "after-batch-dump")
        baseline_preserved = recovered == baseline
        partial_visible = any(key in recovered for key in (b"batch-a", b"batch-b"))
        report["batch"].update({"baseline_preserved": baseline_preserved, "partial_batch_visible": partial_visible})
        require_assertion(report, "batch_baseline_preserved_after_restart", baseline_preserved, "baseline entries changed after failed BATCH")
        require_assertion(report, "failed_batch_keys_absent_after_restart", not partial_visible, "a failed BATCH key became visible")
        check_ok = subprocess.run([os.fspath(executable), "check", os.fspath(database)], capture_output=True, text=True).returncode == 0
        report["recovery"]["check"] = check_ok
        require_assertion(report, "check_succeeds_after_enospc", check_ok, "CLI CHECK failed after ENOSPC")
        followup = put(server, "post-recovery", b"post-recovery", b"usable")
        followup_ack = followup.get("status") == "ACK"
        report["post_recovery_write"] = {"response": followup, "acknowledged": followup_ack}
        require_assertion(report, "post_recovery_put_acknowledged", followup_ack, f"post-recovery PUT was not ACKed: {followup}")
    finally:
        server.stop()
    report["result"] = "PASS"
    write_report(report, report_path)
    print(json.dumps(report, indent=2))
    return 0


def run_fail_closed(executable: Path, database: Path, report_path: Path) -> int:
    global _LAST_REPORT
    _LAST_REPORT = None
    try:
        return run(executable, database, report_path)
    except Exception as exc:
        report = _LAST_REPORT or {
            "label": "PHYSICAL_ENOSPC",
            "assertions": [],
            "failures": [],
            "rollback_failure": {"status": "NOT_TESTED", "reason": "not safely forced"},
        }
        report["result"] = "FAIL"
        report.setdefault("failures", []).append({"assertion": "run", "detail": str(exc)})
        write_report(report, report_path)
        print(json.dumps(report, indent=2))
        print(f"physical ENOSPC test failed: {exc}", file=sys.stderr)
        return 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--executable", type=Path, required=True)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    return run_fail_closed(args.executable.resolve(), args.database.resolve(), args.report.resolve())


if __name__ == "__main__":
    raise SystemExit(main())

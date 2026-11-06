# Kill It and Prove It — advanced evidence report

Run date: 2026-11-06. All disposable databases were created below the OS
temporary directory and removed after each case.

## Passed

- Real compiled fault-injection binary seeded workloads: 4/4 passed, 1,000
  operations each, seeds `7`, `42`, `424242`, and `9001`.
- Write-boundary process termination: 6/6 passed — before operation write,
  after operation write, before commit marker, after commit marker, before
  sync, and after sync.
- Batch crash atomicity: 4/4 passed — after operation write, before commit
  marker, after commit marker, and before sync. Recovery produced either the
  complete batch or the pre-batch state; no partial batch was observed.
- Compaction interruption: 4/4 passed — before temporary file, after temp
  sync, after replacement, and before backup deletion. Live data survived,
  deleted data did not resurrect, and recovery remained writable.
- Existing concurrent reader/writer/compaction Go test: passed.
- Tagged `KVDB_FAIL_SYNC_ONCE=1` executable test: returned `ERROR` with
  `outcome=UNKNOWN`; reopening recovered an empty dump. This is an actual
  injected `File.Sync` failure, not an ENOSPC test.

## Simulated only

- `src/kvdb/storage_test.go` memory-file short writes, rollback failures, and
  sync failures.
- `torture/simulator.py` failure controls.

## Environment blocked

- `go test -race` could not run: cgo is enabled only with a C compiler, and
  `gcc` is not installed. No race-freedom claim is made.
- WSL was unavailable with `E_ACCESSDENIED`, so Linux/WSL validation was not
  possible.

## Not tested / not claimed

- Power-loss safety, disk-cache loss, filesystem crash consistency, and actual
  hardware ENOSPC.
- Linearizability: the concurrency test checks errors, corruption, and final
  validity, but does not record invocation/completion intervals or search for
  a legal sequential history.
- Concurrent JSONL clients: the server protocol is exercised sequentially;
  concurrency evidence is for the Go DB API test, not a multi-client protocol
  claim.
- Acknowledgement-boundary kills after `Sync` and before response delivery are
  represented conservatively as ambiguous, but were not separately scheduled.

## Reproduction

```powershell
.\torture\advanced-campaign.ps1 -Operations 1000
```

The machine-readable output is `artifacts/advanced-campaign.json`. The
tagged hooks are disabled in normal builds and are implemented in
`src/kvdb/fault_hook_inject.go` only for `-tags kvdb_faultinject`.

## Strongest defensible conclusion

The tested transaction, batch, checkpoint replacement, recovery, and
sync-failure paths behaved correctly under deterministic same-host process
termination and injected failures. This is evidence for those specific
boundaries and workloads. It is not evidence of power-loss durability,
hardware-level ENOSPC behavior, race freedom, or linearizability.

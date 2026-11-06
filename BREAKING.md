# Crash points, recovery, and known weaknesses

## Write ordering

```text
validate -> append operations -> append commit -> File.Sync()
-> update map -> success/ACK
```

The map is not updated before synchronization. A batch is not visible in an intermediate state. If rollback or rollback synchronization fails, storage is poisoned and writes are rejected with `ErrRecoveryRequired`. The JSONL layer reports an UNKNOWN outcome and stops serving normal requests.

## Recovery behavior

- Valid `KVCPv003` checkpoints load from offset zero.
- Only newer committed JSON transactions replay.
- Uncommitted groups are ignored.
- Complete malformed records and checksum failures are rejected.
- Physically incomplete final frames may be repaired.
- Non-checkpoint files use legacy full-log recovery.

## Tested crash/failure points

Go tests cover short writes, operation-record failure, commit-marker failure, rollback failure, rollback-sync failure, transaction sync failure, incomplete-tail repair, checkpoint corruption/truncation, malformed post-checkpoint records, and compaction replacement boundaries through subprocess termination hooks. They also cover backup/temp-file recovery states.

Actual Go executable tests cover process kill/restart, READY recovery, and survival of acknowledged data. They do not inject a real OS-level `File.Sync()` failure into the running serve process.

## Theoretical or incomplete coverage

These scenarios are reasoned about but are not equivalent to arbitrary power-loss testing:

- Power loss while Windows directory metadata is being persisted.
- Every internal byte-level step of `ReplaceFileW`.
- Real disk-full behavior on the target filesystem.
- A production serve-process sync failure caused by an OS or hardware fault.

## Known weaknesses

1. One-million-key checkpoint recovery measured approximately three seconds, above the two-second target.
2. The active index is a copied `map[string][]byte`, not a physical record-location index.
3. Windows directory-entry durability cannot be forced through the current portable implementation.
4. The Python torture workflow does not issue compaction through JSONL.
5. Race-detector validation is unavailable in the current environment because CGO/GCC is unavailable.
6. The CLI does not persist `maxBytes` between independent commands.

The JSONL protocol now includes the additive maintenance operation `COMPACT`. Existing CRUD, BATCH, GET, DELETE, and DUMP clients remain compatible.

## Compaction

Compaction synchronizes and independently validates `.compact-tmp`, then uses Windows `ReplaceFileW` or the portable fallback while retaining `.compact-old`. A missing authoritative path is restored from the backup during open. The backup is not an independent authority after successful recovery.

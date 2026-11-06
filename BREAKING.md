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

Actual Go executable tests cover process kill/restart, READY recovery, survival
of acknowledged data, and injected sync failure through a dedicated test build.
The real OS-level ENOSPC experiment below is separate from sync-failure
injection.

## Theoretical or incomplete coverage

These scenarios are reasoned about but are not equivalent to arbitrary power-loss testing:

- Power loss while Windows directory metadata is being persisted.
- Every internal byte-level step of `ReplaceFileW`.
- Every possible disk-full and rollback-failure combination.
- A production serve-process sync failure caused by an OS or hardware fault.

## Known weaknesses

1. One-million-key recovery remains sensitive to runtime and host load; the latest five-run mean was 1,898 ms, with individual runs from 1,854 to 1,955 ms.
2. The active index is a copied `map[string][]byte`, not a physical record-location index.
3. Windows directory-entry durability cannot be forced through the current portable implementation.
4. Rollback failure itself was not independently injected under actual ENOSPC.
5. Race detection covers only the execution paths exercised by the test suite.
6. The CLI does not persist `maxBytes` between independent commands.

The JSONL protocol now includes the additive maintenance operation `COMPACT`. Existing CRUD, BATCH, GET, DELETE, and DUMP clients remain compatible.

## Verified real ENOSPC experiment

Ubuntu WSL 2 used an isolated 128 MB ext4 loopback filesystem containing
`/mnt/kvdb-enospc/test.kv`. After acknowledging `acknowledged=original` and a
1 MB `oversized` value, the filesystem was filled until approximately 684 KB
remained. A batch containing `batch-a`, `batch-b`, and `batch-c` failed with the
genuine Linux error:

`write /mnt/kvdb-enospc/test.kv: no space left on device`

After reopening, the two acknowledged values were intact, all three batch keys
were absent, integrity check returned `OK`, and a subsequent write succeeded
after freeing space. This validates the tested write-error rollback path, not
all possible disk-full or rollback-failure scenarios. It is distinct from the
configured `maxBytes` test.

## Verified race detection

`CGO_ENABLED=1 go test -race ./...` passed under Ubuntu WSL 2 for all tested
packages, with no reported data races. The result covers only paths exercised
by the test suite.

The command completed with the following package results:

| Package | Result |
|---|---|
| `killitandproveit/src/cmd/kvdb` | Passed — 16.273s |
| `killitandproveit/src/kvdb` | Passed — 11.013s |
| `killitandproveit/tests` | Passed — 1.089s |
| `src/cmd/recoverybench` | No test files |
| `src/cmd/recoveryprofile` | No test files |

No data races were reported in the tested execution paths.

## Recovery optimization evidence

Profiling found duplicate million-entry checkpoint validation and index
reconstruction on the normal open path. The implementation now performs one
definitive validation when no replacement artifacts exist, while retaining
independent validation for backup/temp candidates. The latest five runs were
1,854 ms, 1,955 ms, 1,876 ms, 1,874 ms, and 1,931 ms; mean 1,898 ms. All runs
recovered 1,000,000 keys with the same checksum. Allocation profiling fell
from approximately 1.49 GB to 937 MB.

## Compaction

Compaction synchronizes and independently validates `.compact-tmp`, then uses Windows `ReplaceFileW` or the portable fallback while retaining `.compact-old`. A missing authoritative path is restored from the backup during open. The backup is not an independent authority after successful recovery.

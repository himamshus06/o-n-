# Architecture

## Runtime structure

```text
CLI / JSONL serve -> DB API -> DB RWMutex -> committed map[string][]byte
                                  |
                                  v
                         storage mutex and file engine
                                  |
                    JSON transactions / KVCPv003 checkpoint
```

`DB.mu` prevents readers from observing an index halfway through a batch and blocks DB operations during compaction. `storage.mu` serializes file work. `Get` returns a copy. `Batch` writes the complete transaction before changing the map.

## Normal transaction ordering

```text
validate -> JSON operation records -> commit marker -> File.Sync()
-> in-memory index update -> success/ACK
```

If append or synchronization fails, storage attempts to truncate and synchronize back to the transaction start. Uncertain outcomes poison storage; future writes return `ErrRecoveryRequired`.

### Write failure and rollback

`storage.commit` calculates the complete transaction size, records the original
file size, appends operation records and the commit record, calls `File.Sync`,
and advances the transaction ID only after synchronization succeeds. The DB
map is changed only after the storage commit returns success.

On a write failure, storage truncates to the original size, seeks back to the
append position, and synchronizes the rollback. If rollback succeeds, the
original write error is returned and the index is unchanged. If rollback or
rollback synchronization fails, storage is poisoned and recovery is required.
A synchronization failure has an uncertain outcome; later writes are rejected
until reopen/recovery. JSONL reports `ERROR` with `outcome: UNKNOWN` and stops
serving after that state is entered.

## KVCPv003 checkpoint

Compaction creates a new file whose first frame is a binary checkpoint. All integer fields are little-endian:

```text
0       8     magic: KVCPv003
8       2     version
10      2     header size
12      8     generation
20      8     highest included transaction ID
28      8     entry count
36      8     payload length
44      4     payload CRC32C
48      4     header CRC32C
52      N     sorted binary key/value payload
52+N    4     complete-frame CRC32C
56+N    4     completion marker: 0xC0DEC0DE
```

Each payload entry is `uint32 key length`, `uint32 value length`, key bytes, and value bytes. The implementation validates the magic, version, header size, checksums, bounds, sorted unique keys, entry limits, and trailing marker before allocating entry buffers. Keys are limited to 1,024 bytes, values to 1 MB, entries to 10,000,000, and the checkpoint frame to 512 MB.

The payload CRC covers payload bytes only. The complete-frame CRC covers the header and payload, excluding its own field and the completion marker. The implementation's header CRC covers the header bytes preceding the header checksum field, including the payload CRC.

## Recovery

1. Open the authoritative path, restoring `.compact-old` only when the base path is missing.
2. Read the first eight bytes.
3. If they equal `KVCPv003`, validate and load the checkpoint.
4. Begin JSON scanning exactly after the checkpoint frame.
5. Replay only committed transaction IDs greater than the checkpoint boundary.
6. Ignore incomplete transaction groups.
7. Reject complete malformed records and transaction-ID regressions.
8. Repair only a physically incomplete final frame where tail-repair rules permit it.

If the first eight bytes are not `KVCPv003`, recovery uses the legacy JSON log path from byte zero. A file declaring `KVCPv003` but failing validation is not silently treated as legacy.

## Compaction and replacement

While holding the DB write lock, compaction captures the committed snapshot and current highest transaction ID. It writes and synchronizes `.compact-tmp`, closes it, reopens it, and independently validates the checkpoint. Only then does replacement proceed. Windows uses `ReplaceFileW` with `.compact-old`; other platforms use the portable rename fallback. The new file is reopened before backup cleanup. Directory synchronization is attempted where supported; Windows directory fsync is not provided here, so directory-entry persistence remains filesystem/OS dependent.

## Verified hardening experiments

### Real OS-level ENOSPC

Under Ubuntu in WSL 2, the Linux executable was run with the database at
`/mnt/kvdb-enospc/test.kv` on an isolated 128 MB ext4 loopback filesystem. The
test acknowledged `acknowledged=original`, stored a 1 MB `oversized` value,
filled the filesystem until approximately 684 KB remained, and submitted a
three-operation batch for `batch-a`, `batch-b`, and `batch-c`. Linux returned
`write /mnt/kvdb-enospc/test.kv: no space left on device`.

After reopening, `acknowledged` and `oversized` retained their values; all
three batch keys were `NOT_FOUND`; integrity check returned `OK`; and a
subsequent write succeeded after the filler file was removed. This verifies the
tested write-error rollback path, not every disk or rollback failure mode.

### Race detection

Under Ubuntu WSL 2, `CGO_ENABLED=1 go test -race ./...` completed successfully:

| Package | Result |
|---|---|
| `killitandproveit/src/cmd/kvdb` | passed, 16.273s |
| `killitandproveit/src/kvdb` | passed, 11.013s |
| `killitandproveit/tests` | passed, 1.089s |
| `src/cmd/recoverybench` | no test files |
| `src/cmd/recoveryprofile` | no test files |

No races were reported in the exercised paths; this is not proof that every
possible execution path is race-free.

### Recovery optimization

Profiling found that normal checkpoint recovery reconstructed the million-key
index twice: once in `validStorageCandidate` and again during recovery. When
there are no replacement artifacts, normal open now performs one definitive
validation pass. Backup and temporary candidates, or a missing authoritative
path, still receive independent validation.

On the same 1,000,000-key checkpointed database, five runs measured 1,854 ms,
1,955 ms, 1,876 ms, 1,874 ms, and 1,931 ms; mean 1,898 ms. Every run recovered
1,000,000 keys with the same logical checksum. The mean recovery time was 1.898 seconds, meeting the under-two-second average target. Individual runs ranged from 1.854 to 1.955 seconds. Allocation profiling decreased
from approximately 1.49 GB to 937 MB (about 37.1%).

## Limits and concurrency

`maxBytes` is checked before a complete transaction append and before checkpoint installation. `SpaceStats` live/obsolete values are estimates based on encoding. Readers use `DB.mu.RLock`; writers and compaction use `DB.mu.Lock`.

## Scenario-to-mechanism matrix

| Scenario | Mechanism | Result |
|---|---|---|
| A1 operation append | write loop and rollback | No index update |
| A2 commit append | rollback to transaction start | No committed visibility |
| A3 sync failure | storage poisoning | UNKNOWN/recovery-required path |
| A4 incomplete final frame | tail repair | Physically incomplete tail only |
| B1 checkpoint truncation | bounds/marker validation | Open fails; no legacy reinterpretation |
| B2 checkpoint corruption | CRC32C validation | Checkpoint rejected |
| B3 checkpoint replay | transaction boundary | Newer committed transactions replay |
| B4 malformed post-checkpoint record | record validation | Complete corruption rejected |
| C1 temp failure | temp cleanup | Old file retained |
| C2 replacement failure | backup restoration | Old file recoverable |
| C3 replacement crash | ReplaceFileW/backup recovery | Valid old/new file selected |
| C4 cleanup failure | authoritative base retained | Backup can be cleaned later |
| D1 max size | preflight capacity check | Append/compaction rejected |
| D2 key/value limit | validation | Request rejected |
| D3 oversized checkpoint | checkpoint bound | Compaction rejected |
| D4 empty/live snapshot | checkpoint generation | Valid checkpoint produced |
| E1 legacy file | magic detection | Full legacy replay |
| E2 uncommitted transaction | pending map | Group ignored |
| E3 overwrite | ordered replay | Last committed value wins |
| E4 delete | delete replay | Key absent |
| F1 invalid JSONL request | request validation | JSON ERROR |
| F2 missing key | lookup | NOT_FOUND |
| F3 uncertain outcome | ErrRecoveryRequired | UNKNOWN and serving stops |
| F4 concurrent batch/read | DB RWMutex | No partial batch visibility |

The additive JSONL maintenance request `{"id":"c1","op":"COMPACT"}` calls `DB.Compact()` through the same locking and replacement path. It exposes no storage internals and does not change the checkpoint format.

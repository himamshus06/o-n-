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

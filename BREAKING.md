# Crash points, recovery, and known limits

## Crash-point table

The table describes implementation behavior. “Not verified” means the exact
process-kill boundary was not exercised; it is not a claim that the mechanism
is safe under every power-loss condition.

| Point | Source function | Possible disk state | Recovery action / evidence |
|---|---|---|---|
| PUT/DELETE operation write fails | `src/kvdb/storage.go: storage.commit`, `rollbackAfterFailure` | Partial append or no append | Truncate, seek, and sync back to the original size; rollback tests pass. Exact process kill: not verified. |
| Commit marker write fails | `storage.commit`, `rollbackAfterFailure` | Operation records without a commit marker | Roll back; recovery does not apply the pending transaction. `TestCommitHandlesShortWritesAndRollsBackFailures`. |
| Commit marker exists before `File.Sync` returns | `storage.commit`, `scanFrom` | A complete committed transaction may be present, or the tail may be incomplete | Recovery uses the physical valid commit marker, not knowledge of whether sync returned: it replays a complete committed group or ignores an uncommitted group. Exact kill: not verified. |
| Sync failure after append | `storage.commit`, `rollbackAfterFailure`; `DB.Batch`; `writeServeError` | Append may have reached the file; commit outcome is uncertain | Rollback is attempted, storage is poisoned, JSONL returns `ERROR`/`UNKNOWN`, and serving stops. Executable injected-sync tests cover this path. |
| Incomplete final record during open | `recover`, `scanFrom`, `incompleteTail` | Physically truncated final frame | Repair is allowed only for an incomplete physical tail; complete malformed records are rejected. `TestRecoveryRepairsOnlyIncompleteTails`. |
| Complete malformed record during open | `scanFrom`, `validateRecord` | Complete but invalid JSON/checksum/structure | Open fails; the record is not silently discarded. `TestRecoveryRejectsChecksumAndStructuralCorruption`, `TestRecordValidationRejectsMalformedFrames`. |
| Batch lacks its commit marker | `storage.commit`, `scanFrom` | Some or all operation records, no commit marker | Pending operations remain invisible after recovery. Exact halfway process kill: not verified. |
| Batch commit succeeds before caller returns | `DB.Batch`, `storage.commit`, `recover` | Synced committed batch, but caller may receive no response if killed | Reopen replays the committed batch. Exact post-ACK kill: not verified. `TestBatchLastOperationWins` verifies ordering, not this kill boundary. |
| Temporary checkpoint write/sync fails | `storage.compactLocked` | A handled write, sync, close, or validation error can leave an incomplete `.compact-tmp` | Error paths attempt to close/remove `.compact-tmp`; a process crash can leave a partial temporary candidate, which `openStorage` validates or discards. `TestCompactionCrashBoundariesRecover`. |
| Replacement is interrupted | `replace_windows.go: replaceAuthoritative`; `replace_other.go: replaceAuthoritative`; `openStorage` | Windows uses one `ReplaceFileW` operation; the portable path can leave the old path, `.compact-old`, or `.compact-tmp` at an interruption boundary | On open, existing path, backup, and temporary candidates are validated. If the base is missing/invalid, a valid backup is preferred; if the base is missing and backup is invalid, a valid temporary candidate can be renamed into place. A valid base is otherwise recovered, then temporary/backup cleanup is attempted. |
| Crash after replacement before backup cleanup | `compactLocked`, `openStorage` | New authoritative checkpoint plus `.compact-old` | New file is reopened and validated on recovery; backup is removed only after successful open/recovery. `TestCompactionCrashBoundariesRecover`. |
| Crash during close | `storage.close`, `DB.Close`, `recover` | File sync/close may be in progress | Close syncs while storage is healthy; reopen performs normal recovery. Exact close-time process kill: not verified. |

## Write-ordering argument

`DB.Put` and `DB.Delete` delegate to `DB.Batch`. The storage layer calculates
the complete transaction size, records the original file size, writes each
operation record, writes the commit marker, and calls `File.Sync`. Only after a
successful sync does it advance the transaction ID. Only after `storage.commit`
returns success does `DB.Batch` update the in-memory map; only then can JSONL
emit `ACK`.

Thus an acknowledged operation has passed the file sync and map update before
success is reported. A transaction can be committed on disk but unacknowledged
if the process dies after sync and before the response; recovery replays a
committed transaction if
the complete commit marker is present. An incomplete or uncommitted transaction
has no matching commit marker and is withheld from the recovered index. A sync
failure is different: its outcome is uncertain, rollback is attempted, storage
is poisoned, and the JSONL server reports `UNKNOWN` rather than a false ACK.

The DB write lock makes a batch’s map update atomic to readers. Compaction
holds that lock, snapshots the committed map and transaction boundary, writes
`.compact-tmp`, syncs and closes it, reopens and independently validates the
checkpoint, then performs the platform replacement while retaining
`.compact-old`. The replacement file is reopened before backup cleanup. On
Windows, directory-entry synchronization is a no-op in this implementation;
rename metadata durability is therefore weaker than on platforms where the
directory can be synced.

## Known weaknesses

- Five one-million-key recovery runs averaged 1,898 ms (1,854–1,955 ms). The mean
  met the two-second target, but runtime and host load still affect individual
  runs; allocation profiling remained approximately 937 MB.
- The live index is `map[string][]byte`, not a physical record-location index,
  so memory use scales with key/value data.
- Windows directory metadata cannot be durably synchronized through the
  current portable implementation.
- Rollback failure was tested with storage doubles, but was not independently
  injected under genuine ENOSPC.
- `maxBytes` is supplied to each open and is not persisted across independent
  CLI invocations.
- Race detection covers only the test paths exercised by the WSL2 race run;
  it does not prove all possible schedules or filesystem failures.

## What failed / lessons learned

- The initial JSONL scanner limit was 4 MiB. Large requests required a larger
  scanner bound; the current server uses a 16 MiB maximum.
- Profiling showed normal checkpoint recovery validated the authoritative file
  twice, rebuilding the million-entry index twice. The normal path now performs
  one definitive validation when no replacement artifacts exist, while backup
  and temporary candidates remain independently validated.
- Earlier checkpoint recovery measurements were about 27.5 seconds, then about
  3.06 seconds after checkpointing. The validation optimization produced the
  latest five-run mean of 1.898 seconds without changing the checkpoint format
  or validation rules.
- Windows replacement and directory synchronization cannot be treated as
  identical to POSIX rename plus directory fsync; the limitation remains
  documented rather than hidden.

## Next 24-hour improvements

1. Add a controlled executable-level crash matrix for the still-unverified
   write, batch, recovery, and close boundaries, with acknowledged-operation
   tracking and conservative UNKNOWN handling.
2. Add a supported Linux/WSL ENOSPC harness that separately exercises rollback
   failure and compaction-space exhaustion without touching the host volume.
3. Add a bounded index/checkpoint memory study and evaluate a record-location
   index only if profiling shows it reduces the remaining recovery and memory
   risk without weakening validation.

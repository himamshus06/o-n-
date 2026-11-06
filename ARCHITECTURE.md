## Architecture

The public `DB` owns the committed in-memory map and a read/write mutex. Writers serialize the complete transaction while readers take a read lock. `storage` owns the file descriptor, append framing, transaction IDs, synchronization, and recovery scan.

```text
CLI / serve
    |
    v
DB API -- RWMutex -- committed value map
    |
    v
storage -- append operation records -- append commit marker -- Sync()
    |
    v
database file
```

Open scans the file from the beginning. Uncommitted transaction groups are ignored; only groups followed by a commit marker are replayed. A batch is invisible until its commit marker is durable and all index updates are applied under the same write lock.

The storage boundary is intentionally isolated so the binary record implementation and future compaction implementation can be replaced without changing the public API.

## Compaction and space management

`DB.Compact` takes `DB.mu` exclusively, so no read, write, or batch can observe a file switch or change the live-value snapshot during compaction. Storage serializes file work with its own mutex. Compaction writes each live value as a committed transaction using the existing JSON record format, validates the temporary log, syncs it, then moves the current file to `.compact-old` and installs `.compact-tmp` as the database. The backup remains until the new file and containing directory have been synchronized. On open, if the database path is absent but the backup exists, the backup is restored. A stale temp file is discarded; once a valid base path exists it is authoritative.

This two-rename protocol supports platforms where rename cannot replace an existing file. On POSIX, directory synchronization is attempted after replacement and cleanup. Windows does not provide directory `fsync` through this implementation; the database file itself is synced before replacement. Power-loss guarantees therefore depend on the filesystem and OS. The prior file is not removed before the replacement path exists.

Append checks configured max bytes per complete framed record. Any append, capacity, or sync failure truncates back to the transaction's starting offset and syncs the rollback before returning; the in-memory map is only changed after `storage.commit` succeeds. `ErrMaxBytes` distinguishes configured-capacity rejection from OS errors such as ENOSPC. `SpaceStats` reports current and maximum file sizes, an estimate of live bytes under the current encoding, estimated obsolete bytes, available configured capacity, and compaction counters. Live/obsolete values are estimates because they model rewritten records and do not include filesystem allocation overhead.

## Compaction crash points

- Before temp creation: the old file remains authoritative.
- During copy or before temp sync: the old file remains authoritative; an incomplete temp is ignored and removed on next open.
- After temp sync and validation, before moving the old file: the old file remains authoritative.
- After old-to-backup rename but before temp-to-base rename: base is absent, so open restores `.compact-old`.
- After temp-to-base rename: the new complete file is authoritative; `.compact-old` is retained until directory sync and cleanup.
- During/after cleanup: the base remains authoritative; a leftover backup is safe to discard on the next successful open.

## Crash and transaction points

- Operation records may be present without a commit marker after a process crash. Recovery ignores those records.
- The commit marker is synchronized before `Put`, `Delete`, or `Batch` returns successfully.
- The in-memory index is updated only after synchronization succeeds.
- A failed transaction is never applied to the index.
- `Get` returns a copy of the stored value, so callers cannot mutate database state.
- `Close` synchronizes before closing the file descriptor.

The current implementation uses one process-wide writer lock per database instance. This favors clear atomic visibility over parallel write throughput.

## Compaction, capacity, and concurrency

- Compaction blocks all DB reads and writes while it snapshots live values and replaces the log. This can pause service for the full rewrite duration.
- The replacement protocol uses `.compact-tmp` and `.compact-old`. A crash with no base file restores the backup at next open. A stale temp is not considered authoritative.
- File sync is required before installation. Directory sync is attempted where supported; Windows directory synchronization is not available through this implementation, so rename metadata persistence depends on the filesystem and OS.
- Compaction can fail with `ErrMaxBytes` if the live set itself exceeds the configured file limit, or with OS errors such as ENOSPC while temporary space is unavailable. In either case the old log is retained.
- Appends are rolled back to their original offset after record-write or sync errors. If truncation or rollback sync itself fails, the returned error includes that failure; recovery then depends on the underlying filesystem.
- `SpaceStats` live and obsolete byte counts are estimates based on compacted record encoding, not allocated disk blocks.
- Deterministic crash hooks and an atomic directory-handle replacement implementation are future work. Current tests cover compaction value preservation and capacity rejection, not process termination at each filesystem boundary.

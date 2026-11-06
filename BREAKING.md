## Crash and transaction points

- Operation records may be present without a commit marker after a process crash. Recovery ignores those records.
- The commit marker is synchronized before `Put`, `Delete`, or `Batch` returns successfully.
- The in-memory index is updated only after synchronization succeeds.
- A failed transaction is never applied to the index.
- `Get` returns a copy of the stored value, so callers cannot mutate database state.
- `Close` synchronizes before closing the file descriptor.

The current implementation uses one process-wide writer lock per database instance. This favors clear atomic visibility over parallel write throughput.

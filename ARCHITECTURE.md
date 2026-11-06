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

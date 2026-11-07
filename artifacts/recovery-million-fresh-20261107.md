# Fresh One-Million-Key Recovery Benchmark

Date: 2026-11-07

## Environment

- OS: WSL2 Linux, kernel `6.18.54.1-microsoft-standard-WSL2`
- CPU: AMD Ryzen 7 7435HS, 16 logical CPUs
- RAM: 7.7 GiB visible to WSL; 2.0 GiB swap
- Go: `go1.26.8 linux/amd64`
- Filesystem: `tmpfs` mounted at `/tmp`
- Database: fresh disposable file, not a historical benchmark database

## Reproduction

From the repository root in WSL:

```bash
go build -o /tmp/recoverybench-fresh ./src/cmd/recoverybench
go build -o /tmp/kvdb-fresh ./src/cmd/kvdb
rm -rf /tmp/kvdb-million-fresh-20261107
mkdir /tmp/kvdb-million-fresh-20261107
/tmp/recoverybench-fresh \
  -path /tmp/kvdb-million-fresh-20261107/million.kv \
  -keys 1000000 -repeats 0 -generate -compact
/tmp/kvdb-fresh check /tmp/kvdb-million-fresh-20261107/million.kv
/tmp/recoverybench-fresh \
  -path /tmp/kvdb-million-fresh-20261107/million.kv \
  -keys 1000000 -repeats 5
```

Generation is excluded from the timings. The benchmark launches a fresh
`kvdb.Open` for every measured run, calls `Keys`, and computes the checksum.
Cache state was not controlled, so these are ordinary repeated measurements,
not cold-cache results.

## Dataset and correctness

- Keys: exactly `1,000,000`
- Deterministic keys: `key-%08d`
- Deterministic values: `value-%08d`
- Total value bytes: `14,000,000`
- Checkpoint-based file magic: `KVCPv003`
- Compacted database size: `34,000,060` bytes
- Checkpoint size: `34,000,060` bytes (the compacted authoritative file contains the checkpoint frame)
- Check before measurement: `OK`
- SHA-256 checksum on every run:
  `78f35e2f62e790a2c5a3dea427fa07ed5920c8f1a1ef14c0b61ad38716c81795`

## Recovery measurements

| Run | Keys | Recovery ms | Heap delta bytes | Checksum |
|---:|---:|---:|---:|---|
| 1 | 1,000,000 | 2,579 | 647,325,696 | matching |
| 2 | 1,000,000 | 2,222 | 637,244,440 | matching |
| 3 | 1,000,000 | 2,294 | 638,580,408 | matching |
| 4 | 1,000,000 | 2,504 | 639,080,568 | matching |
| 5 | 1,000,000 | 2,294 | 640,203,768 | matching |

- Mean: `2,378.6 ms`
- Median: `2,294 ms`
- Minimum: `2,222 ms`
- Maximum: `2,579 ms`
- Population standard deviation: approximately `138 ms`
- All five runs recovered exactly 1,000,000 keys with the same checksum.

## Assessment

This fresh run is correct but did not meet the under-two-second target: the
mean was approximately `2.379 s` and the best run was `2.222 s`. It is slower
than the historical approximately `1.898 s` mean. No production code was
changed and no optimization is claimed from this benchmark. The reported heap
deltas are large allocation indicators, not peak resident-memory measurements.

## Profiling pass

The existing `src/cmd/recoveryprofile` helper was run against a newly
regenerated copy of the same deterministic dataset. It reported
`recovery_ms=2611`. The CPU profile sampled 3.41 seconds over a 2.61-second
profile interval. The largest visible costs were hash/map reconstruction and
allocation/runtime work: `aeshashbody` (20.53%), map control matching
(14.37%), allocation size-class handling (9.38%), map insertion-related work
(`runtime.mapassign_faststr`, 55.13% cumulative), and
`storage.loadCheckpoint` (41.64% cumulative). This indicates index
reconstruction and allocation remain the main optimization area; no code was
changed based on this profile.

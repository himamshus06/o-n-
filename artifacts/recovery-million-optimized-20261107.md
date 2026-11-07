# One-Million-Key Recovery Optimization Comparison

Date: 2026-11-07. The historical report and fresh baseline report were not
modified.

## Change

The retained optimization preallocates the recovered checkpoint index from a
bounded entry-count hint and preallocates the DB's committed `map[string][]byte`
from the recovered count. The hint is not trusted for correctness: the normal
`loadCheckpoint` path still validates header, payload, frame checksum, marker,
entry bounds, ordering, and transaction metadata. Values and keys are still
copied into owned memory. Legacy recovery and transaction replay are unchanged.

Files changed for the optimization:

- `src/kvdb/storage.go`
- `src/kvdb/db.go`

## Dataset and environment

The candidate used a newly generated database with the same deterministic
dataset as the baseline: exactly 1,000,000 keys, 14,000,000 value bytes,
34,000,060-byte KVCPv003 checkpoint file, and checksum
`78f35e2f62e790a2c5a3dea427fa07ed5920c8f1a1ef14c0b61ad38716c81795`.
CHECK returned `OK` before timing. Environment: WSL2 Linux on an AMD Ryzen 7
7435HS with 16 logical CPUs, 7.7 GiB visible RAM, Go 1.26.8, `tmpfs`.
Cache state was not controlled.

## Five fresh-process runs

| Version | Runs (ms) | Mean | Median | Min | Max | Population SD | Mean heap delta |
|---|---|---:|---:|---:|---:|---:|---:|
| Historical | unavailable individually | 1,898 | unavailable | unavailable | unavailable | unavailable | unavailable |
| Fresh baseline | 2579, 2222, 2294, 2504, 2294 | 2378.6 | 2294 | 2222 | 2579 | 137.6 | unavailable as aggregate |
| Optimized | 1484, 1162, 1149, 1121, 1207 | 1224.6 | 1162 | 1121 | 1484 | 132.6 | 480,042,709 bytes |

The optimized mean improved by 1,154.0 ms, or approximately 48.5%, versus the
fresh baseline. Every run recovered exactly 1,000,000 keys and produced the
same checksum. The optimized mean and every individual run were under two
seconds on this environment; this is not a cross-hardware guarantee.

## Profile comparison

The baseline profile identified `loadCheckpoint` at 41.64% cumulative CPU and
`runtime.mapassign_faststr` at 55.13% cumulative. The optimized profile
reported 1,480 ms recovery; `runtime.mapassign_faststr` was 30.81% cumulative,
and the dominant remaining costs were Go map control-group matching, memmove,
allocation, and garbage-collector scanning. This supports map growth/allocation
as the main avoidable overhead addressed by the change.

## Regression verification

- `go test ./...`: passed
- Python regression plus torture tests: `63 passed, 4 subtests passed`
- `git diff --check`: passed
- Checksum and CHECK: passed for the candidate benchmark
- Corruption/legacy/compaction tests remain covered by the existing Go suite
- No disk format, CRC, transaction, durability, or legacy-recovery behavior was changed

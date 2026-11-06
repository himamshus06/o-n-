# Quick real-binary campaign

Executable: `D:\Srinivas\kvdb-baseline.exe`
Seed: 424242; operations: 250

- crud-and-oracle: 1/1 passed
- deterministic-random: 1/1 passed
- batch-compaction-restart: 1/1 passed
- protocol-malformed-input: 1/1 passed

Skipped/unverified:
- precise in-flight write/commit crash boundaries: no instrumented build supplied
- race detector and linearizability: not applicable to this sequential quick campaign
- power-loss and ENOSPC: not safely reproducible on the host volume

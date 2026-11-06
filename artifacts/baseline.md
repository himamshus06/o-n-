# Baseline evidence — 2026-11-06

## Results

- Go tests: package tests were attempted with a workspace-local `GOCACHE`.
  Windows Application Control blocked the command-package subprocess tests and
  the `src/kvdb` test binary; the `tests` package passed.
- Python tests: not run because the Windows Python launcher reports no
  installed Python interpreter. The repository harness is present under
  `tests/` and `torture/tests/`.
- Real-binary quick campaign against `kvdb-baseline.exe`: CRUD/oracle,
  250-operation randomized workload (seed `424242`), batch+compaction+restart,
  and malformed-input continuity all passed.
- Precise in-flight crash boundaries, race/linearizability, power-loss, and
  ENOSPC were intentionally skipped/unverified.

See `artifacts/quick-campaign.json` and `artifacts/quick-campaign.md` for the
machine-readable and human-readable campaign output.

An older executable rejected an empty value despite the documented contract;
the newer baseline executable passed it. This is recorded as a stale-binary
discrepancy, not as a current-source defect.

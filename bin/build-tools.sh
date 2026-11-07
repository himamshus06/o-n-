#!/usr/bin/env bash
set -euo pipefail

repo_root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
out_dir="${1:-$repo_root/.go-tmp/bin}"
mkdir -p "$out_dir"
cd "$repo_root"

go build -o "$out_dir/kvdb" ./src/cmd/kvdb
go build -tags kvdb_faultinject -o "$out_dir/kvdb-fault" ./src/cmd/kvdb
go build -tags kvdb_enospc -o "$out_dir/kvdb-enospc" ./src/cmd/kvdb
go build -tags kvdb_crashprobe -o "$out_dir/kvdb-crashprobe" ./src/cmd/kvdb
go build -tags kvdb_syncfail -o "$out_dir/kvdb-syncfail" ./src/cmd/kvdb

printf 'Built executables in %s:\n' "$out_dir"
printf '  kvdb          normal build\n'
printf '  kvdb-fault    recovery/compaction crash hooks\n'
printf '  kvdb-enospc   injected ENOSPC\n'
printf '  kvdb-crashprobe storage-phase probes\n'
printf '  kvdb-syncfail injected sync failure\n'

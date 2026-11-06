# Kill It and Prove It

**Kill It and Prove It** is an embedded, crash-resilient key-value
database written in Go. It stores data in a single authoritative
database file and exposes a command-line interface and a JSON Lines
server protocol.

## Build and test

### Requirements

-   Go toolchain compatible with the version declared in `go.mod`.
-   Python 3 for the Python regression suite.
-   `pytest` to run that suite.

Run the Go tests and build from the repository root:

``` sh
go test ./...
go build ./src/cmd/kvdb
```

On Windows, build an executable with:

``` powershell
go build -o kvdb.exe ./src/cmd/kvdb
```

Run the Python regression suite from the repository root:

``` powershell
$env:PYTHONPATH = "torture"
python -m pytest torture/tests -v
```

### Bounded real-binary campaign

Run a bounded campaign against an already-built executable:

``` powershell
.\torture\quick-campaign.ps1 -Executable .\kvdb-baseline.exe -Seed 424242 -Operations 250
```

It uses fresh temporary databases and writes `artifacts/quick-campaign.json`
and `artifacts/quick-campaign.md`. It checks CRUD, arbitrary bytes, an
independent map oracle, deterministic randomized workloads, batches,
compaction, restart/recovery, and malformed-input protocol continuity.
Precise in-flight crash boundaries, race freedom, linearizability, power-loss,
and ENOSPC remain unverified unless an instrumented executable and isolated
filesystem are supplied.

For deterministic process-kill boundaries, batch atomicity, compaction
interruption, four 1,000-operation seeds, and the existing Go concurrency
stress test, run:

``` powershell
.\torture\advanced-campaign.ps1 -Operations 1000
```

This builds a disposable `kvdb_faultinject` executable and writes
`artifacts/advanced-campaign.json` and `artifacts/advanced-campaign.md`.
The fault hooks are disabled in normal builds. See
`artifacts/final-report.md` for the claim boundaries and blocked environments.

The unified Python campaign entry point uses the problem-statement default of
500 iterations and supports a user seed:

``` powershell
$env:PYTHONPATH = "torture"
python -m campaign --seed 424242 --iterations 500 --mode default
python -m campaign --seed 424242 --mode large-value --mode large-batch --mode concurrent-writers
```

Crash, recovery-interruption, compaction-interruption, and deterministic
injected-ENOSPC modes are separate real-binary modes. They remain distinct from
simulator-only faults and from physical disk exhaustion.

### WSL Ubuntu validation

From WSL Ubuntu, with the repository available at `/mnt/d/Srinivas`:

```bash
sudo apt update
sudo apt install -y golang python3 python3-venv build-essential
cd /mnt/d/Srinivas
go version
python3 --version
go build -o kvdb ./src/cmd/kvdb
go build -tags kvdb_faultinject -o kvdb-fault ./src/cmd/kvdb
go build -tags kvdb_enospc -o kvdb-enospc ./src/cmd/kvdb
python3 -m venv .venv-wsl
. .venv-wsl/bin/activate
python -m pip install --upgrade pip pytest
PYTHONPATH=torture python -m unittest discover tests -v
PYTHONPATH=torture python -m unittest discover torture/tests -v
PYTHONPATH=torture python -m campaign \
  --executable ./kvdb \
  --fault-executable ./kvdb-fault \
  --seed 424242 --iterations 500 --mode default
PYTHONPATH=torture python -m campaign \
  --executable ./kvdb \
  --fault-executable ./kvdb-fault \
  --seed 424242 \
  --mode random-crash --mode recovery-crash --mode compaction-crash \
  --mode large-value --mode large-batch --mode concurrent-writers
KVDB_ENOSPC_EXECUTABLE="$PWD/kvdb-enospc" \
  PYTHONPATH=torture python -m campaign \
  --executable ./kvdb --mode injected-enospc
```

The campaign writes JSON and Markdown reports beside the requested JSON path.
The ENOSPC mode above is deterministic injected ENOSPC, not a claim about
physical disk exhaustion. Use `go test -race ./...` separately when the WSL
toolchain has a working C compiler.

This Python regression suite is distinct from the randomized
crash-torture runner described by the project requirements. Passing the
regression suite does not establish that the full randomized torture
campaign has completed.

The Go module files (`go.mod` and, if present, `go.sum`) define the
project's external dependency requirements. For an offline build, all
required modules must already be available locally. The commands above
do not by themselves guarantee a clean offline build.

## Command-line interface

The executable provides these commands:

``` text
kvdb open <path> [--max-bytes N]
kvdb put <path> <key> <value>
kvdb get <path> <key>
kvdb del <path> <key>
kvdb batch <path> <file>
kvdb dump <path>
kvdb check <path>
kvdb serve <path>
```

-   `open` opens the database, performs recovery, and reports the key
    count and recovery time. The optional `--max-bytes` setting applies
    to that invocation; it is not persisted for later independent
    invocations.

-   `put` writes a command-line text key and value.

-   `get` reads a command-line text key.

-   `del` deletes a command-line text key.

-   `batch` reads operations from a text file and applies them
    atomically. Each non-empty operation line uses one of these forms:

    ``` text
    PUT <key> <value>
    DELETE <key>
    ```

-   `dump` prints live entries in sorted key order.

-   `check` validates the database storage file.

-   `serve` starts the JSON Lines protocol described below.

The simple command-line `put`, `get`, and `del` interface is
text-oriented. Use the JSONL protocol to represent arbitrary key and
value bytes.

## JSON Lines server protocol

Start the long-running server with:

``` sh
kvdb serve <path>
```

The process completes database recovery before writing `READY` to
standard error. Each input line on standard input is one JSON request.
Responses are written as one JSON object per line to standard output.
Standard output is reserved for protocol JSON; diagnostics are written
to standard error.

Request IDs may be JSON strings or non-negative integers. The response
echoes the ID using its original JSON type. Keys and values use standard
padded RFC 4648 Base64.

### Requests

``` json
{"id":"1","op":"PUT","key":"YXBwbGU=","value":"cmVk"}
{"id":2,"op":"GET","key":"YXBwbGU="}
{"id":"3","op":"DELETE","key":"YXBwbGU="}
{"id":"4","op":"BATCH","operations":[{"op":"PUT","key":"YQ==","value":"MQ=="},{"op":"DELETE","key":"Yg=="}]}
{"id":"5","op":"DUMP"}
{"id":"6","op":"COMPACT"}
```

`PUT` requires both `key` and `value`. An empty value is valid and is
represented by an empty Base64 string (`""`). A missing or `null` value
is not the same as an empty value and is invalid for `PUT`.

`BATCH` accepts `PUT` and `DELETE` operations only, with a maximum of
10,000 operations. A batch is applied all-or-nothing. `DUMP` returns
committed live entries sorted by raw key bytes. `COMPACT` invokes
database compaction.

### Responses

Typical responses include:

``` json
{"id":"1","status":"ACK"}
{"id":2,"status":"VALUE","value":"cmVk"}
{"id":"3","status":"NOT_FOUND"}
{"id":"5","status":"DUMP","entries":[]}
{"id":"6","status":"ACK"}
```

A `GET` of a present empty value returns `VALUE` with `"value":""`; a
missing key returns `NOT_FOUND`.

An `ACK` is emitted only after the requested transaction or maintenance
operation has completed successfully. For a committed write, the commit
data is synchronized before the acknowledgement is sent. `COMPACT`
returns `ACK` after successful replacement.

If a write or synchronization failure leaves the commit outcome
uncertain, the server reports an `UNKNOWN` outcome and stops accepting
normal work until the database is reopened. For example:

``` json
{"id":"7","status":"ERROR","outcome":"UNKNOWN","error":"Commit outcome uncertain; database requires recovery"}
```

Do not interpret `UNKNOWN` as either a confirmed commit or a confirmed
rollback. Reopen the database and inspect its recovered state.

## On-disk transaction format

Normal transactions use length-prefixed JSON records. Each frame
consists of:

1.  A four-byte unsigned body length, encoded in little-endian byte
    order.
2.  A JSON-encoded `diskRecord` body of that length.

The record structure contains these fields:

  Field        Go type           Purpose
  ------------ ----------------- ---------------------------------------------------
  `TxID`       `uint64`          Identifies the transaction
  `Type`       `OperationType`   Identifies the operation type
  `Key`        `[]byte`          Operation key; JSON encodes byte slices as Base64
  `Value`      `[]byte`          PUT value; JSON encodes byte slices as Base64
  `Commit`     `bool`            Identifies a commit record
  `Checksum`   `*uint32`         Optional CRC32C checksum field

Operation records carry PUT or DELETE operations. A commit record uses
the same transaction ID and has `Commit: true`. The checksum uses CRC32C
with the Castagnoli polynomial and is calculated over the JSON record
with the checksum field omitted. During recovery, a checksum is verified
when present.

A transaction is applied only when its records and commit marker
validate. The implementation synchronizes the transaction before
reporting a successful write. The in-memory index is updated only after
that synchronization succeeds.

The exact serialization and validation rules are implemented in
`src/kvdb/storage.go`. Keep this description synchronized with that
implementation if the format changes.

## Checkpoint format: `KVCPv003`

Compaction writes a checkpoint as the first frame of the replacement
database file. Its magic is `KVCPv003`. The checkpoint contains the live
committed key-value map and the highest transaction ID included in the
snapshot. Recovery loads this snapshot and replays only newer JSON
transactions.

The checkpoint header is **52 bytes**:

    Offset      Size Field
  -------- --------- ---------------------------------
         0   8 bytes Magic: `KVCPv003`
         8   2 bytes Version
        10   2 bytes Header size
        12   8 bytes Generation
        20   8 bytes Highest included transaction ID
        28   8 bytes Entry count
        36   8 bytes Payload length
        44   4 bytes Payload CRC32C
        48   4 bytes Header CRC32C

The payload contains entries sorted by key. Each entry is encoded as:

-   Four-byte key length.
-   Four-byte value length.
-   Key bytes.
-   Value bytes.

The eight-byte trailer contains a four-byte complete-frame CRC32C
followed by the four-byte completion marker `0xC0DEC0DE`.

Checksum coverage:

-   Header CRC32C: header bytes 0--47.
-   Payload CRC32C: payload bytes only.
-   Complete-frame CRC32C: all 52 header bytes followed by the payload.
-   Completion marker: stored separately in the trailer.

Recovery validates the checkpoint before using it. See `ARCHITECTURE.md`
and the checkpoint encoder/decoder in `src/kvdb/storage.go` for the
implementation details.

## Recovery

On open, the database validates the available storage state before
making the database available.

For a checkpoint-based file, recovery validates the checkpoint, restores
its live entries, and replays committed transactions whose transaction
IDs are newer than the checkpoint's included transaction ID. A file
without the `KVCPv003` magic is handled through legacy full-log
recovery.

The transaction log is processed using transaction boundaries and commit
markers:

-   A complete, valid committed transaction is applied.
-   An incomplete transaction group without a valid commit marker is not
    applied.
-   A complete malformed record is treated as corruption and rejected.
-   A physically incomplete final frame may be truncated and
    synchronized during normal recovery. Candidate validation does not
    perform this repair.

Recovery also considers compaction artifacts. A valid existing
authoritative database path takes priority. If the authoritative path is
missing or invalid, a valid `.compact-old` backup is preferred. If no
valid backup is available and the authoritative path is missing, a valid
`.compact-tmp` candidate may be promoted. An invalid temporary candidate
is not accepted as a replacement. The candidate-validation and selection
logic is implemented in `openStorage()` and `validStorageCandidate()` in
`src/kvdb/storage.go`.

## Compaction and platform notes

Compaction captures a committed snapshot while holding the database
write lock, writes a temporary checkpoint, synchronizes and closes the
temporary file, and independently validates it before replacement.

On Windows, replacement uses the Windows `ReplaceFileW` API and retains
the previous authoritative file as `.compact-old`. On other platforms,
the fallback uses two rename operations: it first renames the
authoritative file to `.compact-old`, then renames `.compact-tmp` to the
authoritative path. If the second rename fails, it attempts to restore
the backup. Recovery handles the intermediate states.

After replacement, the implementation calls `syncDir()` for the
containing directory. **Directory-sync errors are intentionally
ignored**: they are not returned to the caller, and compaction can
report success even if the directory synchronization call fails. On
Windows, `syncDir()` is a no-op because this implementation does not
provide directory-entry synchronization through a directory handle.
Therefore, persistence of rename and deletion metadata depends on the
operating system and filesystem; the implementation does not claim
stronger directory-entry durability than it can provide.

## Capacity and failure behavior

The database supports a configurable maximum file size through
`--max-bytes` where exposed by the CLI. The setting is not persisted
across independent CLI invocations.

The implementation checks the configured limit before appending a
transaction. The operating system can still report a physical disk-full
error even when the configured limit permits the write. If an append or
synchronization failure makes the transaction outcome uncertain, the
database enters a recovery-required state rather than continuing to
accept writes on an uncertain storage state. Reopen the database to run
recovery before continuing.

A failed batch must not be treated as partially successful. After a
failure, use recovery and `check` to establish the recovered state.

## Further technical details

-   `ARCHITECTURE.md` describes the component layout, write ordering,
    recovery and compaction flows, and the problem-statement scenario
    matrix.
-   `BREAKING.md` records crash points, ordering arguments, known
    weaknesses, failed approaches, and prioritized improvements.
-   `PROMPTS.md` records the available AI-prompt history and its
    provenance limitations.

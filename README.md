# Kill It and Prove It

**Kill It and Prove It** is an embedded, crash-resilient key-value
database written in Go. It stores data in a single authoritative
database file and exposes a command-line interface and a JSON Lines
server protocol.

## Simple step-by-step demo

This is the easiest way to try the project from a fresh WSL Ubuntu
installation. The commands below use a temporary sample database, so they do
not touch an important database on your computer.

### Step 1: Open WSL and install the tools

Open **Ubuntu** from the Windows Start menu. Then type:

```bash
sudo apt update
sudo apt install -y git golang python3 python3-venv build-essential
```

Check that the tools are ready:

```bash
go version
python3 --version
git --version
```

### Step 2: Download the project

Clone the project and enter its folder:

```bash
cd /mnt/d
git clone https://github.com/himamshus06/o-n-.git killitandproveit
cd killitandproveit
```

If you already downloaded the project, use its real folder instead. You must
be in the folder that contains `go.mod`. Check with:

```bash
ls go.mod src/cmd/kvdb
```

If this command says a file is missing, stop and change to the correct project
folder. Do not run `go mod init` for this project.

### Step 3: Build the database programs

Build the normal program and the two special test programs:

```bash
go build -o kvdb ./src/cmd/kvdb
go build -tags kvdb_faultinject -o kvdb-fault ./src/cmd/kvdb
go build -tags kvdb_enospc -o kvdb-enospc ./src/cmd/kvdb
```

You should now have these files:

```bash
ls -l kvdb kvdb-fault kvdb-enospc
```

`kvdb` is the normal database. `kvdb-fault` is used to stop the database at
planned recovery or compaction points. `kvdb-enospc` simulates a disk-full
error in a controlled test. It is not a real full hard drive test.

### Step 4: Make a Python environment

```bash
python3 -m venv .venv-wsl
. .venv-wsl/bin/activate
python3 -m pip install --upgrade pip pytest
```

The `.venv-wsl` folder is only a local Python environment for this project.

### Step 5: Run the tests

Run the Go tests:

```bash
go test -count=1 ./...
```

Run the Python framework tests:

```bash
PYTHONPATH=torture python3 -m unittest discover tests -v
PYTHONPATH=torture python3 -m unittest discover torture/tests -v
```

The command must finish without a `FAIL` line. If it fails, save the error
text; do not call the test a pass.

### Step 6: Try a small sample database

Create a temporary folder and a small batch file:

```bash
DEMO_DIR="$(mktemp -d /tmp/kvdb-demo-XXXXXX)"
DB="$DEMO_DIR/demo.kv"
BATCH_FILE="$DEMO_DIR/batch.txt"

printf 'PUT beta two\nPUT alpha one\nDELETE missing\n' > "$BATCH_FILE"
```

Run normal database commands:

```bash
./kvdb open "$DB"
./kvdb put "$DB" greeting hello
./kvdb get "$DB" greeting
./kvdb del "$DB" greeting
./kvdb get "$DB" greeting
./kvdb batch "$DB" "$BATCH_FILE"
./kvdb dump "$DB"
./kvdb check "$DB"
```

You should see:

- `hello` after the first `get`;
- `NOTFOUND` after deleting `greeting`;
- `alpha one` and `beta two` in the dump;
- `OK` from `check`.

Now reopen the same file:

```bash
./kvdb open "$DB"
./kvdb dump "$DB"
```

The `alpha` and `beta` data should still be there. This shows that committed
data survives a separate process run.

Keep this temporary folder until after the JSON Lines example below. It is
safe because it was created by `mktemp`.

### Step 7: Try the JSON Lines server

The database can also run as a server. The server reads **one JSON object per
line** and writes **one JSON response per line**. Keys and values must be
Base64 text.

For example:

| What you want | JSON Lines request |
| --- | --- |
| Save `hello` as `world` | `{"id":1,"op":"PUT","key":"aGVsbG8=","value":"d29ybGQ="}` |
| Read `hello` | `{"id":2,"op":"GET","key":"aGVsbG8="}` |
| Delete `hello` | `{"id":3,"op":"DELETE","key":"aGVsbG8="}` |
| Show all data | `{"id":4,"op":"DUMP"}` |
| Compact the database | `{"id":5,"op":"COMPACT"}` |

`aGVsbG8=` is Base64 for `hello`. `d29ybGQ=` is Base64 for `world`.

Use the same temporary database from the previous step:

```bash
cat > "$DEMO_DIR/requests.jsonl" <<'JSONL'
{"id":1,"op":"PUT","key":"aGVsbG8=","value":"d29ybGQ="}
{"id":2,"op":"GET","key":"aGVsbG8="}
{"id":3,"op":"DELETE","key":"aGVsbG8="}
{"id":4,"op":"DUMP"}
JSONL

./kvdb serve "$DB" \
  < "$DEMO_DIR/requests.jsonl" \
  > "$DEMO_DIR/responses.jsonl" \
  2> "$DEMO_DIR/server.log"

cat "$DEMO_DIR/responses.jsonl"
cat "$DEMO_DIR/server.log"
```

The response file should contain an `ACK` for the `PUT`, a `VALUE` response
for the `GET`, an `ACK` for the `DELETE`, and a `DUMP` response. The `READY`
message and other diagnostics are written to `server.log`, not to the JSON
response file.

A JSON Lines batch is one request containing several operations. For example:

```bash
cat > "$DEMO_DIR/batch-request.jsonl" <<'JSONL'
{"id":10,"op":"BATCH","operations":[{"op":"PUT","key":"YWxwaGE=","value":"b25l"},{"op":"PUT","key":"YmV0YQ==","value":"dHdv"},{"op":"DELETE","key":"bm90LWZvdW5k"}]}
{"id":11,"op":"DUMP"}
JSONL

./kvdb serve "$DB" \
  < "$DEMO_DIR/batch-request.jsonl" \
  > "$DEMO_DIR/batch-responses.jsonl" \
  2> "$DEMO_DIR/batch-server.log"

cat "$DEMO_DIR/batch-responses.jsonl"
```

The batch should return one `ACK`. The `DUMP` should show `alpha` with value
`one` and `beta` with value `two`. A batch is all-or-nothing: after an error,
do not assume that any part of it was saved until you reopen the database and
inspect it with `DUMP` and `check`.

When finished with the complete sample, remove its temporary folder:

```bash
rm -rf "$DEMO_DIR"
```

This removes only the folder created by `mktemp`; it does not remove the
project or the compiled programs.

### Step 8: Run the real-binary campaigns

Every command below uses the compiled Go database, not a simulator. Each
command writes a JSON report and a Markdown report. The seed makes the random
workload repeatable.

Run the normal 500-operation workload:

```bash
PYTHONPATH=torture python3 -m campaign \
  --executable ./kvdb \
  --seed 424242 \
  --iterations 500 \
  --mode default \
  --json-report artifacts/demo-default.json
```

Run concurrent writers, a 1 MiB value, and a 10,000-operation batch:

```bash
PYTHONPATH=torture python3 -m campaign \
  --executable ./kvdb \
  --seed 424242 \
  --iterations 500 \
  --mode concurrent-writers \
  --mode large-value \
  --mode large-batch \
  --json-report artifacts/demo-boundaries.json
```

Run process crashes and recovery:

```bash
PYTHONPATH=torture python3 -m campaign \
  --executable ./kvdb \
  --seed 424242 \
  --iterations 500 \
  --mode random-crash \
  --json-report artifacts/demo-random-crash.json

PYTHONPATH=torture python3 -m campaign \
  --executable ./kvdb \
  --fault-executable ./kvdb-fault \
  --seed 424242 \
  --iterations 500 \
  --mode recovery-crash \
  --mode compaction-crash \
  --json-report artifacts/demo-recovery-compaction.json
```

Run deterministic injected ENOSPC tests:

```bash
KVDB_ENOSPC_EXECUTABLE="$PWD/kvdb-enospc" \
PYTHONPATH=torture python3 -m campaign \
  --executable ./kvdb \
  --seed 424242 \
  --iterations 500 \
  --mode injected-enospc \
  --json-report artifacts/demo-enospc.json
```

### Step 9: Read and verify the reports

For every campaign, replace the filename below with the report you want to
inspect:

```bash
cat artifacts/demo-default.json
cat artifacts/demo-default.md
```

A successful report has an empty `failed` list and zero values for recovery
failures and oracle mismatches. It also shows the passed test cases. The
Markdown report contains the same finalized report information as the JSON
report.

The report should keep these limitations under `not_tested`:

- `physical disk exhaustion`
- `linearizability checking`

The injected ENOSPC campaign is a controlled simulation. It must not be
described as proof of physical disk exhaustion. The random-crash campaign also
does not prove that every crash interrupted an in-flight request.

### Step 10: Repeat a seeded run

Run the same default campaign twice with different report names:

```bash
PYTHONPATH=torture python3 -m campaign \
  --executable ./kvdb --seed 424242 --iterations 500 --mode default \
  --json-report artifacts/repro-1.json

PYTHONPATH=torture python3 -m campaign \
  --executable ./kvdb --seed 424242 --iterations 500 --mode default \
  --json-report artifacts/repro-2.json
```

The meaningful fields should match. Start time, finish time, and duration may
be different. The two Markdown files are `artifacts/repro-1.md` and
`artifacts/repro-2.md`.

## Build and test

### Requirements

-   Go toolchain compatible with the version declared in `go.mod`.
-   Python 3 for the Python regression suite.
-   `pytest` to run the pytest suite. Install the development dependency
    with `python -m pip install -r requirements-dev.txt` when pytest is not
    already available. The Go module has no third-party dependencies.

Run the Go tests and build from the repository root:

``` sh
go test ./...
go build ./src/cmd/kvdb
```

On Windows, a clean-clone verification sequence is:

``` powershell
go build -o kvdb.exe ./src/cmd/kvdb
go test ./...
python -m pip install -r requirements-dev.txt
$env:PYTHONPATH = "torture"
python -m pytest torture/tests -v
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
.\torture\quick-campaign.ps1 -Executable .\kvdb.exe -Seed 424242 -Operations 250
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

For judge-style arguments, use the checked-in launcher from the repository
root's `bin` directory:

```bash
./bin/torture --iterations 500 --seed 12345
./bin/torture --iterations 100 --seed 999 --mode large-values
./bin/torture --iterations 100 --seed 7 --mode compaction
./bin/torture --iterations 100 --seed 42 --mode recovery-crash
```

The launcher requires `--seed`, forwards the executable and fault-executable
environment settings, and maps the judge aliases `large-values` and
`compaction` to the implemented modes. The repository already contains a
directory named `torture`, so a root-level file named `./torture` cannot coexist
with it on normal filesystems; `./bin/torture` is the equivalent executable
entry point. The no-mode launcher run executes the seeded random-crash and
concurrent-writer modes.

To reproduce a clean seeded run from a fresh WSL checkout, run the following
from the repository root. The build commands create all executables required by
the normal and fault-injection modes; the reports are written under `artifacts/`.

```bash
python3 -m venv .venv-wsl
. .venv-wsl/bin/activate
python -m pip install -r requirements-dev.txt
go build -o kvdb ./src/cmd/kvdb
go build -tags kvdb_faultinject -o kvdb-fault ./src/cmd/kvdb
go build -tags kvdb_enospc -o kvdb-enospc ./src/cmd/kvdb
go test ./...
PYTHONPATH=torture KVDB_EXECUTABLE=./kvdb python -m pytest torture/tests -q
./bin/torture --iterations 200 --seed 12345 --json-report artifacts/judge-200.json
KVDB_FAULT_EXECUTABLE="$PWD/kvdb-fault" ./bin/torture \
  --iterations 100 --seed 42 --mode recovery-crash \
  --json-report artifacts/judge-recovery.json
KVDB_FAULT_EXECUTABLE="$PWD/kvdb-fault" ./bin/torture \
  --iterations 100 --seed 7 --mode compaction \
  --json-report artifacts/judge-compaction.json
```

The campaign exits `0` only when no campaign failure is recorded. Each run
prints its final JSON report and writes a Markdown report beside the requested
JSON path. A seeded run is reproducible for the same executable, platform, and
seed; a different seed is intentionally a different workload. The random-crash
mode uses real child-process termination and restart. The recovery and
compaction modes use the separately built fault-injection executable.

Crash, recovery-interruption, compaction-interruption, and deterministic
injected-ENOSPC modes are separate real-binary modes. They remain distinct from
simulator-only faults and from physical disk exhaustion.

### WSL Ubuntu validation

From WSL Ubuntu, with the repository available under `/mnt/d/<repository-directory>`:

```bash
sudo apt update
sudo apt install -y golang python3 python3-venv build-essential
cd /mnt/d/<repository-directory>
go version
python3 --version
go build -o kvdb ./src/cmd/kvdb
go build -tags kvdb_faultinject -o kvdb-fault ./src/cmd/kvdb
go build -tags kvdb_enospc -o kvdb-enospc ./src/cmd/kvdb
python3 -m venv .venv-wsl
. .venv-wsl/bin/activate
python -m pip install --upgrade pip pytest
go test ./...
KVDB_EXECUTABLE=./kvdb PYTHONPATH=torture python -m pytest torture/tests -v
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

### Quick demo with a fresh database

The following self-contained examples create a disposable database, write
sample values, exercise CRUD and batch operations, validate the file, and
reopen it to verify persistence. Do not replace the temporary path with an
important existing database path.

On WSL/Linux:

```bash
DEMO_DIR="$(mktemp -d /tmp/kvdb-demo-XXXXXX)"
DB="$DEMO_DIR/demo.kv"
BATCH_FILE="$DEMO_DIR/batch.txt"

printf 'PUT beta two\nPUT alpha one\nDELETE missing\n' > "$BATCH_FILE"

go build -o kvdb ./src/cmd/kvdb
./kvdb open "$DB"
./kvdb put "$DB" greeting hello
./kvdb get "$DB" greeting
./kvdb del "$DB" greeting
./kvdb get "$DB" greeting
./kvdb batch "$DB" "$BATCH_FILE"
./kvdb dump "$DB"
./kvdb check "$DB"
./kvdb open "$DB"

rm -rf "$DEMO_DIR"
```

Expected highlights are `OK` for successful writes and deletes,
`NOTFOUND` after deleting `greeting`, a sorted dump containing `alpha one` and
`beta two`, and `OK` from `check`. The final `open` demonstrates that the
committed data can be recovered after a separate process invocation.

On Windows PowerShell:

```powershell
$DemoDir = Join-Path $env:TEMP ("kvdb-demo-" + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $DemoDir | Out-Null
$DB = Join-Path $DemoDir "demo.kv"
$BatchFile = Join-Path $DemoDir "batch.txt"
@"
PUT beta two
PUT alpha one
DELETE missing
"@ | Set-Content $BatchFile

go build -o kvdb.exe ./src/cmd/kvdb
.\kvdb.exe open $DB
.\kvdb.exe put $DB greeting hello
.\kvdb.exe get $DB greeting
.\kvdb.exe del $DB greeting
.\kvdb.exe get $DB greeting
.\kvdb.exe batch $DB $BatchFile
.\kvdb.exe dump $DB
.\kvdb.exe check $DB
.\kvdb.exe open $DB

Remove-Item -LiteralPath $DemoDir -Recurse -Force
```

The cleanup command removes only the temporary demo directory created by the
example.

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

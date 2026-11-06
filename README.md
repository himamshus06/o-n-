# Kill It and Prove It

Kill It and Prove It is an embedded crash-resilient key-value database written in Go.

## Build and test

```text
go test ./...
go build -o kvdb.exe ./src/cmd/kvdb
```

The Python torture suite is run with its source directory on `PYTHONPATH`:

```powershell
$env:PYTHONPATH = "D:\path\to\torture"
python -m unittest discover -s torture/tests -v
```

## CLI

```text
kvdb open <path> [--max-bytes N]
kvdb put <path> <key> <value>
kvdb get <path> <key>
kvdb del <path> <key>
kvdb batch <path> <file>
kvdb dump <path>
kvdb check <path>
kvdb serve <path>
```

`open` performs recovery and reports the key count and recovery time. `put`, `get`, and `del` operate on command-line text. `batch` reads `PUT <key> <value>` and `DELETE <key>` lines and applies the file atomically. `dump` prints sorted live entries; `check` validates the storage file. The current simple CLI does not persist `--max-bytes` for later independent invocations.

## JSONL serve protocol

`kvdb serve <path>` is a long-running JSON Lines process. Recovery completes before `READY\n` is written to stderr. Each stdin line is one JSON request and each valid request produces one JSON response on stdout. stdout contains protocol JSON only; diagnostics go to stderr.

IDs may be JSON strings or non-negative integers and are echoed with their original JSON type. Keys and values use standard padded RFC 4648 Base64.

```json
{"id":"1","op":"PUT","key":"YXBwbGU=","value":"cmVk"}
{"id":2,"op":"GET","key":"YXBwbGU="}
{"id":"3","op":"DELETE","key":"YXBwbGU="}
{"id":"4","op":"BATCH","operations":[{"op":"PUT","key":"YQ==","value":"MQ=="},{"op":"DELETE","key":"Yg=="}]}
{"id":"5","op":"DUMP"}
```

Responses include `ACK`, `VALUE`, `NOT_FOUND`, `DUMP`, and `ERROR`:

```json
{"id":"1","status":"ACK"}
{"id":2,"status":"VALUE","value":"cmVk"}
{"id":"3","status":"NOT_FOUND"}
{"id":"5","status":"DUMP","entries":[]}
{"id":"6","status":"ERROR","error":"Invalid request"}
```

`BATCH` accepts PUT and DELETE only, up to 10,000 operations, and is all-or-nothing. `DUMP` returns committed live entries sorted by raw key bytes. An `ACK` is emitted only after the transaction has been synchronized successfully. An uncertain commit returns an UNKNOWN error and the serve process stops accepting normal work until the database is reopened:

```json
{"id":"7","status":"ERROR","outcome":"UNKNOWN","error":"Commit outcome uncertain; database requires recovery"}
```

## Storage, checkpoints, and recovery

Normal transactions use length-prefixed JSON operation records followed by a commit marker. The commit marker is synchronized before success, and the in-memory map changes only afterward. Uncommitted groups are ignored; complete malformed records are rejected; a physically incomplete final frame may be repaired under the recovery rules.

Compaction creates a new authoritative file whose first frame is a validated `KVCPv003` binary checkpoint containing the live committed map and highest included transaction ID. Recovery loads the checkpoint and replays only newer JSON transactions. Files without the `KVCPv003` magic remain readable through legacy full-log recovery.

Compaction writes and synchronizes `.compact-tmp`, independently validates it, then replaces the authoritative file using Windows `ReplaceFileW` or the portable fallback. `.compact-old` is used when the authoritative path is missing. Windows directory-entry synchronization is not available through this implementation, so rename metadata persistence remains filesystem/OS dependent.

# Kill It and Prove It

This repository contains a crash-resilient embedded key-value database written in Go.

## Build

Install Go 1.22 or newer, then run:

```bash
go test ./...
go build -o kvdb ./src/cmd/kvdb
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

Each write is represented by a transaction containing operation records followed by a commit record. The file is synchronized before the operation returns, and the in-memory map is changed only after synchronization succeeds.

The initial serve protocol uses base64 for keys and values:

```text
<id> PUT <base64-key> <base64-value>
<id> GET <base64-key>
<id> DELETE <base64-key>
```

Responses are `ACK`, `VALUE`, `NOTFOUND`, or `ERROR`. Diagnostics are written to stderr.

//go:build kvdb_enospc && !kvdb_syncfail

package kvdb

import (
	"errors"
	"os"
	"strconv"
)

// enospcFile is used only by the tagged campaign binary. It consumes a
// process-local byte budget and returns an ENOSPC-shaped error once exhausted.
// This is deterministic injected ENOSPC, not physical disk exhaustion.
type enospcFile struct {
	*os.File
	remaining int64
}

func openStorageFile(path string, flags int, perm os.FileMode) (storageFile, error) {
	f, err := os.OpenFile(path, flags, perm)
	if err != nil {
		return nil, err
	}
	budget := int64(-1)
	if raw := os.Getenv("KVDB_ENOSPC_AFTER_BYTES"); raw != "" {
		budget, err = strconv.ParseInt(raw, 10, 64)
		if err != nil || budget < 0 {
			_ = f.Close()
			return nil, errors.New("invalid KVDB_ENOSPC_AFTER_BYTES")
		}
	}
	return &enospcFile{File: f, remaining: budget}, nil
}

func (f *enospcFile) Write(p []byte) (int, error) {
	if f.remaining < 0 {
		return f.File.Write(p)
	}
	if f.remaining == 0 {
		return 0, errors.New("injected ENOSPC")
	}
	if int64(len(p)) > f.remaining {
		n, err := f.File.Write(p[:int(f.remaining)])
		f.remaining -= int64(n)
		if err != nil {
			return n, err
		}
		return n, errors.New("injected ENOSPC")
	}
	n, err := f.File.Write(p)
	f.remaining -= int64(n)
	return n, err
}

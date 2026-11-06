//go:build kvdb_syncfail

package kvdb

import (
	"errors"
	"os"
	"sync/atomic"
)

// syncFailureFile is compiled only into the dedicated executable test build.
// KVDB_FAIL_SYNC_ONCE=1 makes the first Sync call fail, after all preceding
// writes have gone through the real operating-system file.
type syncFailureFile struct {
	*os.File
}

var failNextSync atomic.Bool

func init() {
	failNextSync.Store(os.Getenv("KVDB_FAIL_SYNC_ONCE") == "1")
}

func openStorageFile(path string, flags int, perm os.FileMode) (storageFile, error) {
	f, err := os.OpenFile(path, flags, perm)
	if err != nil {
		return nil, err
	}
	return &syncFailureFile{File: f}, nil
}

func (f *syncFailureFile) Sync() error {
	if failNextSync.CompareAndSwap(true, false) {
		return errors.New("injected File.Sync failure")
	}
	return f.File.Sync()
}

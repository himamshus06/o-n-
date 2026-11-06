//go:build !kvdb_syncfail

package kvdb

import "os"

func openStorageFile(path string, flags int, perm os.FileMode) (storageFile, error) {
	return os.OpenFile(path, flags, perm)
}

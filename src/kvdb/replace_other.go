//go:build !windows

package kvdb

import "os"

// replaceAuthoritative is the portable fallback. The old authoritative file
// is retained as backup until the new file has been installed.
func replaceAuthoritative(path, tmp, backup string) error {
	if err := os.Rename(path, backup); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Rename(backup, path)
		return err
	}
	return nil
}

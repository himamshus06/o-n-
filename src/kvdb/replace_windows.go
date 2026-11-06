//go:build windows

package kvdb

import (
	"fmt"
	"syscall"
	"unsafe"
)

var replaceFileW = syscall.NewLazyDLL("kernel32.dll").NewProc("ReplaceFileW")

// replaceAuthoritative uses the Windows ReplaceFile primitive. It replaces
// path with tmp and writes the previous authoritative file to backup as one
// OS-level replacement operation, avoiding the old-file-missing window caused
// by two independent renames.
func replaceAuthoritative(path, tmp, backup string) error {
	p, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return err
	}
	t, err := syscall.UTF16PtrFromString(tmp)
	if err != nil {
		return err
	}
	b, err := syscall.UTF16PtrFromString(backup)
	if err != nil {
		return err
	}
	r, _, callErr := replaceFileW.Call(uintptr(unsafe.Pointer(p)), uintptr(unsafe.Pointer(t)), uintptr(unsafe.Pointer(b)), 0, 0, 0)
	if r == 0 {
		if callErr != nil && callErr != syscall.Errno(0) {
			return callErr
		}
		return fmt.Errorf("ReplaceFileW failed")
	}
	return nil
}

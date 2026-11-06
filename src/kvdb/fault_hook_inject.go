//go:build kvdb_faultinject

package kvdb

import (
	"os"
)

// The tagged executable exits only at explicitly named storage boundaries.
// This models an abrupt process termination, not power loss or an fsync
// guarantee. The hook is disabled in normal builds.
func faultInjectionPoint(name string) {
	if os.Getenv("KVDB_CRASH_POINT") == name {
		os.Exit(91)
	}
}

//go:build !kvdb_crashprobe

package kvdb

// Crash-probe hooks are deliberately inert in normal builds.
func CrashProbeSetOperation(string, string) {}
func CrashProbeClearOperation()             {}
func CrashProbeCloseRequestEnabled() bool   { return false }
func crashProbeEvent(string, uint64)        {}

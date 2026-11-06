//go:build !kvdb_faultinject

package kvdb

// faultInjectionPoint is deliberately inert in production builds.
func faultInjectionPoint(string) {}

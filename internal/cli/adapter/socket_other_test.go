//go:build !linux

package adapter

import "testing"

func isolateSystemNativeSocket(t *testing.T) {
	t.Helper()
}

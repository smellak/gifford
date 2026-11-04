package detect

import (
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The node classifies every pane on every pass, so an emulator left open
// keeps its reply reader and its screen for the life of the process.
func TestClassifyAndVerifyReleaseTheirEmulators(t *testing.T) {
	const calls = 200
	before := runtime.NumGoroutine()
	for range calls {
		_ = Classify(HarnessShell, DefaultTable(),
			[]byte("user@h:~$ "), 80, 24, 100, shellTree())
		_, _ = VerifyNotice(HarnessShell, DefaultTable(),
			[]byte("user@h:~$ hello"), 80, 24, "hello")
	}
	require.Eventually(t, func() bool {
		return runtime.NumGoroutine() < before+calls/10
	}, 5*time.Second, 10*time.Millisecond,
		"each call must close its emulator: %d goroutines before, %d after",
		before, runtime.NumGoroutine())
}

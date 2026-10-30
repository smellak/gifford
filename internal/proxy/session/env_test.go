//go:build unix

package session

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEnvReachesTheHarness gives the session an Env: the harness sees the
// variable it was given and not one only this process has, and the value
// reaches neither the events nor the recording. The harness prints the
// value's length, never the value. It is serial: it sets a variable of
// the test process.
func TestEnvReachesTheHarness(t *testing.T) {
	const (
		given     = "GIFFORD_SESSION_TEST_GIVEN"
		inherited = "GIFFORD_SESSION_TEST_INHERITED"
		value     = "env-canary-4h8w2m"
	)
	t.Setenv(inherited, "from the process")
	dataDir := t.TempDir()
	master, slave := testPTY(t)
	sink := &chanSink{}
	script := `printf '<given %s|inherited %s>\n' "${#` + given + `}" "${` + inherited + `-unset}"; read -r line; exit 0`
	opts := testOptions(dataDir, "harness-env", slave, slave, sink, []string{"sh", "-c", script})
	opts.Env = []string{given + "=" + value}
	s, err := NewSession(opts)
	require.NoError(t, err)
	done := runAsync(t, s)
	readUntil(t, master, "<given "+strconv.Itoa(len(value))+"|inherited unset>")
	writeKeys(t, master, "q\n")
	waitDone(t, done)

	events, err := json.Marshal(sink.all())
	require.NoError(t, err)
	require.NotContains(t, string(events), value)
	recording, err := os.ReadFile(filepath.Join(dataDir, recordingsDir, "harness-env.raw"))
	require.NoError(t, err)
	require.NotEmpty(t, recording)
	require.NotContains(t, string(recording), value)
}

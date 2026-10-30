//go:build linux

package orchestrator

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

func writeProcFixture(
	t *testing.T,
	root string,
	pid, parent, start int64,
	children string,
) {
	t.Helper()
	directory := filepath.Join(root, fmt.Sprint(pid), "task", fmt.Sprint(pid))
	require.NoError(t, os.MkdirAll(directory, 0o700))
	fields := make([]string, 2, 21)
	fields[0] = "S"
	fields[1] = fmt.Sprint(parent)
	fields = append(fields, make([]string, 17)...)
	for index := 2; index < len(fields); index++ {
		fields[index] = "0"
	}
	fields = append(fields, fmt.Sprint(start))
	stat := fmt.Sprintf("%d (process with spaces) %s\n", pid, strings.Join(fields, " "))
	require.NoError(t, os.WriteFile(filepath.Join(root, fmt.Sprint(pid), "stat"), []byte(stat), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(directory, "children"), []byte(children), 0o600))
}

func TestProcessInspectorFindsAllDescendantsWithStartTimes(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeProcFixture(t, root, 100, 1, 10, "101 102")
	writeProcFixture(t, root, 101, 100, 11, "103")
	writeProcFixture(t, root, 102, 100, 12, "")
	writeProcFixture(t, root, 103, 101, 13, "")
	writeProcFixture(t, root, 104, 100, 14, "")
	threadDir := filepath.Join(root, "100", "task", "105")
	require.NoError(t, os.MkdirAll(threadDir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(threadDir, "children"), []byte("104"), 0o600))
	inspector := NewProcessInspector(root)
	got, err := inspector.Descendants(protocol.ProcessIdentity{PID: 100, ProcessStartTime: 10})
	require.NoError(t, err)
	require.Equal(t, []ProcessToken{
		{PID: 101, StartTime: 11},
		{PID: 102, StartTime: 12},
		{PID: 103, StartTime: 13},
		{PID: 104, StartTime: 14},
	}, got)
}

func TestProcessInspectorFailsClosedOnUnreadableOrReusedProcess(t *testing.T) {
	t.Parallel()

	t.Run("missing child", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		writeProcFixture(t, root, 200, 1, 20, "201")
		_, err := NewProcessInspector(root).Descendants(
			protocol.ProcessIdentity{PID: 200, ProcessStartTime: 20})
		require.ErrorContains(t, err, "read stat of pid 201")
	})

	t.Run("pid reused", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		writeProcFixture(t, root, 300, 1, 30, "")
		_, err := NewProcessInspector(root).Descendants(
			protocol.ProcessIdentity{PID: 300, ProcessStartTime: 31})
		require.ErrorContains(t, err, "identity changed")
	})
}

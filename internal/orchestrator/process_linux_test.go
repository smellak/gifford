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
	bootDir := filepath.Join(root, "sys", "kernel", "random")
	require.NoError(t, os.MkdirAll(bootDir, 0o700))
	bootPath := filepath.Join(bootDir, "boot_id")
	if _, err := os.Stat(bootPath); os.IsNotExist(err) {
		require.NoError(t, os.WriteFile(bootPath,
			[]byte(executorTestBootID+"\n"), 0o600))
	}
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
	require.NoError(t, os.Symlink(
		fmt.Sprintf("/opt/gifford-test/process-%d", pid),
		filepath.Join(root, fmt.Sprint(pid), "exe"),
	))
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
	got, err := inspector.Inspect(protocol.ProcessIdentity{PID: 100, ProcessStartTime: 10})
	require.NoError(t, err)
	require.Equal(t, executorTestBootID, got.BootID)
	require.Equal(t, ProcessToken{PID: 100, StartTime: 10}, got.Root)
	require.Equal(t, []ProcessNode{
		{
			Token:      ProcessToken{PID: 101, StartTime: 11},
			Parent:     ProcessToken{PID: 100, StartTime: 10},
			Executable: "/opt/gifford-test/process-101",
		},
		{
			Token:      ProcessToken{PID: 102, StartTime: 12},
			Parent:     ProcessToken{PID: 100, StartTime: 10},
			Executable: "/opt/gifford-test/process-102",
		},
		{
			Token:      ProcessToken{PID: 103, StartTime: 13},
			Parent:     ProcessToken{PID: 101, StartTime: 11},
			Executable: "/opt/gifford-test/process-103",
		},
		{
			Token:      ProcessToken{PID: 104, StartTime: 14},
			Parent:     ProcessToken{PID: 100, StartTime: 10},
			Executable: "/opt/gifford-test/process-104",
		},
	}, got.Descendants)
}

func TestProcessInspectorFailsClosedOnUnreadableOrReusedProcess(t *testing.T) {
	t.Parallel()

	t.Run("missing child", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		writeProcFixture(t, root, 200, 1, 20, "201")
		_, err := NewProcessInspector(root).Inspect(
			protocol.ProcessIdentity{PID: 200, ProcessStartTime: 20})
		require.ErrorContains(t, err, "read stat of pid 201")
	})

	t.Run("pid reused", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		writeProcFixture(t, root, 300, 1, 30, "")
		_, err := NewProcessInspector(root).Inspect(
			protocol.ProcessIdentity{PID: 300, ProcessStartTime: 31})
		require.ErrorContains(t, err, "identity changed")
	})

	t.Run("child reparented", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		writeProcFixture(t, root, 400, 1, 40, "401")
		writeProcFixture(t, root, 401, 999, 41, "")
		_, err := NewProcessInspector(root).Inspect(
			protocol.ProcessIdentity{PID: 400, ProcessStartTime: 40})
		require.ErrorContains(t, err, "parent changed")
	})

	t.Run("unreadable executable", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		writeProcFixture(t, root, 500, 1, 50, "501")
		writeProcFixture(t, root, 501, 500, 51, "")
		require.NoError(t, os.Remove(filepath.Join(root, "501", "exe")))
		_, err := NewProcessInspector(root).Inspect(
			protocol.ProcessIdentity{PID: 500, ProcessStartTime: 50})
		require.ErrorContains(t, err, "read executable of pid 501")
	})
}

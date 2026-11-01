//go:build linux

package orchestrator

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

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

func TestProcessInspectorRejectsIncompleteDescendantTraversal(t *testing.T) {
	t.Parallel()

	t.Run("before stat", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		writeProcFixture(t, root, 200, 1, 20, "201")
		tree, err := NewProcessInspector(root).Inspect(
			protocol.ProcessIdentity{PID: 200, ProcessStartTime: 20})
		require.Error(t, err)
		require.True(t, isProcessObservationIncomplete(err))
		require.ErrorIs(t, err, os.ErrNotExist)
		require.Empty(t, tree.Descendants)
	})

	t.Run("process disappears while reading children", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		writeProcFixture(t, root, 210, 1, 20, "211")
		writeProcFixture(t, root, 211, 210, 21, "")
		require.NoError(t, os.RemoveAll(filepath.Join(root, "211", "task")))
		tree, err := NewProcessInspector(root).Inspect(
			protocol.ProcessIdentity{PID: 210, ProcessStartTime: 20})
		require.Error(t, err)
		require.True(t, isProcessObservationIncomplete(err))
		require.ErrorIs(t, err, os.ErrNotExist)
		require.Empty(t, tree.Descendants)
	})

	t.Run("thread disappears while reading children", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		writeProcFixture(t, root, 101, 1, 10, "102")
		writeProcFixture(t, root, 102, 101, 11, "103")
		writeProcFixture(t, root, 103, 102, 12, "104")
		writeProcFixture(t, root, 104, 103, 13, "")
		require.NoError(t, os.MkdirAll(filepath.Join(root, "103", "task", "105"), 0o700))

		tree, err := NewProcessInspector(root).Inspect(
			protocol.ProcessIdentity{PID: 101, ProcessStartTime: 10})
		require.Error(t, err)
		require.True(t, isProcessObservationIncomplete(err))
		require.ErrorIs(t, err, os.ErrNotExist)
		require.Empty(t, tree.Descendants)
	})
}

func TestProcessInspectorKeepsUnreadableExecutableAsUntrustedNode(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeProcFixture(t, root, 500, 1, 50, "501")
	writeProcFixture(t, root, 501, 500, 51, "")
	require.NoError(t, os.Remove(filepath.Join(root, "501", "exe")))
	tree, err := NewProcessInspector(root).Inspect(
		protocol.ProcessIdentity{PID: 500, ProcessStartTime: 50})
	require.NoError(t, err)
	require.Equal(t, []ProcessNode{{
		Token:  ProcessToken{PID: 501, StartTime: 51},
		Parent: ProcessToken{PID: 500, StartTime: 50},
	}}, tree.Descendants)
}

func TestStableExecutableRequiresTwoMatchingReads(t *testing.T) {
	t.Parallel()

	const executable = "/opt/gifford-test/process"
	require.Equal(t, executable, stableExecutable(executable, nil, executable, nil))
	require.Empty(t, stableExecutable("", os.ErrPermission, executable, nil))
	require.Empty(t, stableExecutable(executable, nil, "", os.ErrPermission))
	require.Empty(t, stableExecutable(executable, nil, executable+"-new", nil))
}

func TestProcessInspectorKeepsRootAndNonTransientErrorsStrict(t *testing.T) {
	t.Parallel()

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

	t.Run("root disappeared", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		writeProcFixture(t, root, 600, 1, 60, "")
		require.NoError(t, os.Remove(filepath.Join(root, "600", "stat")))
		_, err := NewProcessInspector(root).Inspect(
			protocol.ProcessIdentity{PID: 600, ProcessStartTime: 60})
		require.ErrorContains(t, err, "read stat of pid 600")
		require.False(t, isProcessObservationIncomplete(err))
	})

	t.Run("non-transient child stat error", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		writeProcFixture(t, root, 700, 1, 70, "701")
		writeProcFixture(t, root, 701, 700, 71, "")
		statPath := filepath.Join(root, "701", "stat")
		require.NoError(t, os.Remove(statPath))
		require.NoError(t, os.Mkdir(statPath, 0o700))
		_, err := NewProcessInspector(root).Inspect(
			protocol.ProcessIdentity{PID: 700, ProcessStartTime: 70})
		require.ErrorContains(t, err, "read stat of pid 701")
		require.False(t, isProcessObservationIncomplete(err))
	})
}

func TestZombieDescendantDefersWithoutBlockingBinding(t *testing.T) {
	fixture := newExecutorFixture(t)
	directory := t.TempDir()
	harnessPIDPath := filepath.Join(directory, "harness.pid")
	readyPath := filepath.Join(directory, "ready")
	const harnessScript = `
printf '%s\n' "$$" > "$1"
kill -STOP "$$"
sh -c '(exit 0) & exec sleep 30' &
printf 'ready\n' > "$2"
wait
`
	command := exec.CommandContext(
		t.Context(),
		"sh", "-c", `sh -c "$1" sh "$2" "$3" & wait`,
		"sh", harnessScript, harnessPIDPath, readyPath,
	)
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	require.NoError(t, command.Start())
	t.Cleanup(func() {
		_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		_ = command.Wait()
	})

	var harnessPID int
	require.Eventually(t, func() bool {
		raw, err := os.ReadFile(harnessPIDPath)
		if err != nil {
			return false
		}
		harnessPID, err = strconv.Atoi(strings.TrimSpace(string(raw)))
		return err == nil && harnessPID > 0
	}, 5*time.Second, 10*time.Millisecond, "stopped harness did not publish its pid")

	inspector := NewProcessInspector("")
	start, _, err := inspector.(*procInspector).readStat(int64(command.Process.Pid))
	require.NoError(t, err)
	record := processBindingRecord()
	record.process = protocol.ProcessIdentity{
		PID: int64(command.Process.Pid), ProcessStartTime: start,
	}
	fixture.executor.processes = inspector
	pass, _, err := callProcessGuard(t, fixture.executor, record)
	require.NoError(t, err)
	require.True(t, pass)
	state, reason := storedProcessBinding(t, fixture.store.DB())
	require.Equal(t, processBindingStateBound, state)
	require.Empty(t, reason)

	require.NoError(t, syscall.Kill(harnessPID, syscall.SIGCONT))
	require.Eventually(t, func() bool {
		_, err := os.Stat(readyPath)
		return err == nil
	}, 5*time.Second, 10*time.Millisecond, "resumed harness did not start its child")
	require.Eventually(t, func() bool {
		tree, err := inspector.Inspect(record.process)
		if err != nil {
			return false
		}
		for _, node := range tree.Descendants {
			if node.Executable == "" {
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond, "zombie descendant was not observed")

	pass, detail, err := callProcessGuard(t, fixture.executor, record)
	require.NoError(t, err)
	require.False(t, pass)
	require.Contains(t, detail, untrustedProcessDetail)
	state, reason = storedProcessBinding(t, fixture.store.DB())
	require.Equal(t, processBindingStateBound, state)
	require.Empty(t, reason)
}

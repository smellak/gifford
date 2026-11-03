//go:build linux

package tmuxlock

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

const journalCrashEnv = "GIFFORD_TMUXLOCK_JOURNAL_CRASH"

// newStateFixture provisions one pane's .state beside the lock fixture's
// lock file, with the record contract's size, mode and ACL.
func newStateFixture(t *testing.T) (panelLockFixture, LiveBinding, string) {
	t.Helper()
	fixture := newPanelLockFixture(t)
	live := LiveBinding{
		PanelBinding: fixture.binding, Host: "node-a",
		BootID: journalBootID, PanePID: 424242, PaneStart: 987654,
	}
	binding, err := liveRecordBinding(live)
	require.NoError(t, err)
	raw, err := encodeStateRecord(neutralRecordPolicy(t), initialState(binding, nodeOwner, journalClock()))
	require.NoError(t, err)
	statePath := filepath.Join(fixture.hashDir, "12.state")
	require.NoError(t, os.WriteFile(statePath, raw, 0o600))
	setFileACL(t, statePath, fixture.binding.OwnerUID)
	return fixture, live, statePath
}

func TestOpenStateFileJournalsProvisionedRecord(t *testing.T) {
	t.Parallel()

	fixture, live, statePath := newStateFixture(t)
	file, err := openStateAt(fixture.root, fixture.binding)
	require.NoError(t, err)
	journal, err := BeginAttempt(file, live, journalOperation, journalWriter, journalPolicy(), journalClock)
	require.NoError(t, err)
	require.NoError(t, journal.Step(1))
	require.NoError(t, journal.Finish(false))
	require.NoError(t, journal.Close())

	raw, err := os.ReadFile(statePath)
	require.NoError(t, err)
	record, err := decodeStateRecord(neutralRecordPolicy(t), raw)
	require.NoError(t, err)
	require.Equal(t, phaseUncertain, record.Phase)
	require.Equal(t, uint64(3), record.Gen)
	info, err := os.Stat(statePath)
	require.NoError(t, err)
	require.Equal(t, int64(recordSize), info.Size(), "updates rewrite in place, never resize")
}

func TestOpenStateFileRefusesUnsafeRecord(t *testing.T) {
	t.Parallel()

	cases := map[string]func(t *testing.T, statePath string){
		"short record": func(t *testing.T, statePath string) {
			require.NoError(t, os.Truncate(statePath, recordSize-1))
		},
		"long record": func(t *testing.T, statePath string) {
			require.NoError(t, os.Truncate(statePath, recordSize+1))
		},
		"extra link": func(t *testing.T, statePath string) {
			require.NoError(t, os.Link(statePath, statePath+".twin"))
		},
		"symlink": func(t *testing.T, statePath string) {
			target := statePath + ".real"
			require.NoError(t, os.Rename(statePath, target))
			require.NoError(t, os.Symlink(target, statePath))
		},
		"group writable without ACL": func(t *testing.T, statePath string) {
			require.NoError(t, unix.Removexattr(statePath, accessACLName))
			require.NoError(t, os.Chmod(statePath, 0o660))
		},
		"missing": func(t *testing.T, statePath string) {
			require.NoError(t, os.Remove(statePath))
		},
	}
	for name, breakIt := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fixture, _, statePath := newStateFixture(t)
			breakIt(t, statePath)
			_, err := openStateAt(fixture.root, fixture.binding)
			require.Error(t, err)
		})
	}
}

func TestParseProcessStartCountsFromLastParenthesis(t *testing.T) {
	t.Parallel()

	stat := []byte("4242 (we ird) (name)) S 1 4242 4242 0 -1 4194560 1 0 0 0 0 0 0 0 20 0 1 0 987654 0 0")
	start, err := parseProcessStart(stat)
	require.NoError(t, err)
	require.Equal(t, uint64(987654), start)
	for _, raw := range []string{"4242 no parenthesis", "4242 (short) S 1 2", "4242 (x) S" + " 0" + " x"} {
		_, err := parseProcessStart([]byte(raw))
		require.Error(t, err, raw)
	}
	writer, err := CurrentWriter()
	require.NoError(t, err)
	require.Equal(t, int64(os.Getpid()), writer.PID)
	require.NotZero(t, writer.Start)
}

// Crash boundaries the journal must survive. Each one kills the writer with
// SIGKILL at that point; effect.log records the simulated effect, so the
// parent can tell whether anything was typed.
var journalCrashBoundaries = []struct {
	name       string
	wantPhase  string
	wantStep   int
	wantEffect bool
}{
	{name: "before-begin", wantPhase: phaseIdle, wantStep: 0},
	{name: "between-begin-and-effect", wantPhase: phaseRunning, wantStep: 1},
	{name: "between-effect-and-child", wantPhase: phaseRunning, wantStep: 1, wantEffect: true},
	{name: "between-effect-and-terminal", wantPhase: phaseRunning, wantStep: 4, wantEffect: true},
}

func TestJournalCrashAtEachBoundary(t *testing.T) {
	t.Parallel()

	for _, boundary := range journalCrashBoundaries {
		t.Run(boundary.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			statePath := filepath.Join(dir, "12.state")
			raw, err := encodeStateRecord(neutralRecordPolicy(t), journalRecord(t))
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(statePath, raw, 0o600))

			command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestJournalCrashHelper$", "-test.count=1")
			command.Env = append(os.Environ(), journalCrashEnv+"="+boundary.name+":"+dir)
			output, err := command.CombinedOutput()
			var exit *exec.ExitError
			require.ErrorAs(t, err, &exit, "helper must die, output: %s", output)
			status, ok := exit.Sys().(syscall.WaitStatus)
			require.True(t, ok)
			require.True(t, status.Signaled() && status.Signal() == syscall.SIGKILL,
				"helper must die by SIGKILL, output: %s", output)

			raw, err = os.ReadFile(statePath)
			require.NoError(t, err)
			record, err := decodeStateRecord(neutralRecordPolicy(t), raw)
			require.NoError(t, err, "every boundary leaves a whole record")
			require.Equal(t, boundary.wantPhase, record.Phase)
			require.Equal(t, boundary.wantStep, record.Step)
			effect, err := os.ReadFile(filepath.Join(dir, "effect.log"))
			if !errors.Is(err, os.ErrNotExist) {
				require.NoError(t, err)
			}
			require.Equal(t, boundary.wantEffect, len(effect) > 0)

			// A restarted writer meets the record left behind: an attempt
			// that began counts as uncertain and blocks every new effect.
			file := &stateDescriptor{fd: openRaw(t, statePath)}
			_, err = BeginAttempt(file, journalLive, journalOperation, journalWriter, journalPolicy(), journalClock)
			_ = file.Close()
			if boundary.wantPhase == phaseIdle {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, ErrAttemptUncertain)
			}
		})
	}
}

// TestJournalCrashHelper runs one attempt in a child process and kills
// itself at the boundary named in the environment. It never spawns.
func TestJournalCrashHelper(t *testing.T) {
	spec := os.Getenv(journalCrashEnv)
	if spec == "" {
		t.Skip("crash helper only runs as a child of TestJournalCrashAtEachBoundary")
	}
	boundary, dir, ok := strings.Cut(spec, ":")
	require.True(t, ok)
	die := func(at string) {
		if at == boundary {
			_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
			select {}
		}
	}
	effect := func() {
		log, err := os.OpenFile(filepath.Join(dir, "effect.log"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
		require.NoError(t, err)
		_, err = log.WriteString("typed\n")
		require.NoError(t, err)
		require.NoError(t, log.Sync())
		require.NoError(t, log.Close())
	}
	file := &stateDescriptor{fd: openRaw(t, filepath.Join(dir, "12.state"))}
	die("before-begin")
	journal, err := BeginAttempt(file, journalLive, journalOperation, journalWriter, journalPolicy(), journalClock)
	require.NoError(t, err)
	require.NoError(t, journal.Step(1))
	die("between-begin-and-effect")
	effect()
	die("between-effect-and-child")
	require.NoError(t, journal.Child(0, 0))
	require.NoError(t, journal.Step(2))
	require.NoError(t, journal.Step(3))
	require.NoError(t, journal.Child(0, 0))
	require.NoError(t, journal.Step(4))
	die("between-effect-and-terminal")
	require.NoError(t, journal.Finish(true))
	t.Fatalf("boundary %q never reached", boundary)
}

func openRaw(t *testing.T, path string) int {
	t.Helper()
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	require.NoError(t, err)
	return fd
}

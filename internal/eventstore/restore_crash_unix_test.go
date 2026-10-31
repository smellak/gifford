//go:build unix

package eventstore

import (
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	restoreHelperEnv         = "GIFFORD_RESTORE_CRASH_HELPER"
	restoreHelperStepEnv     = "GIFFORD_RESTORE_CRASH_STEP"
	restoreHelperSourceEnv   = "GIFFORD_RESTORE_CRASH_SOURCE"
	restoreHelperDatabaseEnv = "GIFFORD_RESTORE_CRASH_DATABASE"
)

func TestRestoreSurvivesSIGKILLAtEverySwapStep(t *testing.T) {
	steps := []restoreStep{
		restoreStepMarkerSynced,
		restoreStepSidecarsSynced,
		restoreStepCanonicalReplaced,
		restoreStepReplacementSynced,
		restoreStepMarkerRemoved,
		restoreStepMarkerRemoveSynced,
	}
	for _, step := range steps {
		t.Run(string(step), func(t *testing.T) {
			dir := t.TempDir()
			databasePath := filepath.Join(dir, "gifford.db")
			current := createRestoreInventory(t, databasePath, "before-kill")
			require.NoError(t, current.Close())
			sourcePath := filepath.Join(dir, "source.db")
			source := createRestoreInventory(t, sourcePath, "after-kill")
			require.NoError(t, source.Close())
			sourceDigest := rawFileDigest(t, sourcePath)

			cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestRestoreSIGKILLHelper$")
			cmd.Env = append(os.Environ(),
				restoreHelperEnv+"=1",
				restoreHelperStepEnv+"="+string(step),
				restoreHelperSourceEnv+"="+sourcePath,
				restoreHelperDatabaseEnv+"="+databasePath,
			)
			output, err := cmd.CombinedOutput()
			var exitErr *exec.ExitError
			require.ErrorAs(t, err, &exitErr, string(output))
			waitStatus, ok := exitErr.Sys().(syscall.WaitStatus)
			require.True(t, ok, string(output))
			require.True(t, waitStatus.Signaled(), string(output))
			require.Equal(t, syscall.SIGKILL, waitStatus.Signal(), string(output))
			require.Equal(t, sourceDigest, rawFileDigest(t, sourcePath), "restore source changed")

			// This is the same recovery gate the hub calls before eventstore.Open.
			require.NoError(t, RecoverInterruptedRestore(t.Context(), databasePath))
			require.NoFileExists(t, restoreMarkerPath(databasePath))
			info, statErr := os.Stat(databasePath)
			require.NoError(t, statErr)
			require.Positive(t, info.Size(), "recovery exposed an empty database")
			restored, openErr := Open(t.Context(), databasePath)
			require.NoError(t, openErr)
			require.Equal(t, "after-kill", readRestoreInventory(t, restored.DB()))
			require.NoError(t, restored.Close())

			recoveryPaths, globErr := filepath.Glob(databasePath + ".recovery-*")
			require.NoError(t, globErr)
			require.Len(t, recoveryPaths, 1)
			recovery, openErr := Open(t.Context(), recoveryPaths[0])
			require.NoError(t, openErr)
			require.Equal(t, "before-kill", readRestoreInventory(t, recovery.DB()))
			require.NoError(t, recovery.Close())
			require.FileExists(t, recoveryPaths[0])
		})
	}
}

func TestRestoreSIGKILLHelper(t *testing.T) {
	if os.Getenv(restoreHelperEnv) != "1" {
		return
	}
	wantStep := restoreStep(os.Getenv(restoreHelperStepEnv))
	hooks := restoreHooks{afterStep: func(step restoreStep) {
		if step != wantStep {
			return
		}
		if err := syscall.Kill(os.Getpid(), syscall.SIGKILL); err != nil {
			t.Fatalf("kill restore helper: %v", err)
		}
		select {}
	}}
	_, err := restore(
		t.Context(),
		os.Getenv(restoreHelperSourceEnv),
		os.Getenv(restoreHelperDatabaseEnv),
		hooks,
	)
	if err != nil {
		t.Fatalf("restore helper: %v", err)
	}
	t.Fatalf("restore helper did not reach step %q", wantStep)
}

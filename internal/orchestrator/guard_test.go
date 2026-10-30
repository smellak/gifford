package orchestrator

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDeliveryOperationGuardRejectsPlantedCaller(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "scripts"), 0o700))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "internal", "bad"), 0o700))
	script, err := os.ReadFile(filepath.Join("..", "..", "scripts", "check_guards.sh"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, "scripts", "check_guards.sh"), script, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(root, "internal", "bad", "bad.go"), []byte(
		"package bad\n\nconst forbidden = protocol.AgentOpCommitDelivery\n"), 0o600))

	for _, args := range [][]string{{"init", "-q"}, {"add", "."}} {
		command := exec.CommandContext(t.Context(), "git", args...)
		command.Dir = root
		output, err := command.CombinedOutput()
		require.NoError(t, err, string(output))
	}
	command := exec.CommandContext(t.Context(), "bash", "scripts/check_guards.sh")
	command.Dir = root
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "internal/bad/bad.go")
	require.Contains(t, string(output), "agent delivery operation outside internal/orchestrator/executor.go")
}

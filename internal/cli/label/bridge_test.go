package label

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenCodeBridgeContentIsFixedAndClosed(t *testing.T) {
	t.Parallel()

	sum := sha256.Sum256(openCodeBridge)
	require.Equal(t, "5769eee7bd1ae8b1272c269d9b80e055052fd8392280718b09c26d6da3249dd0", hex.EncodeToString(sum[:]))
	text := string(openCodeBridge)
	wantEvents := []string{
		"session.created", "permission.asked", "permission.replied", "question.asked",
		"question.replied", "question.rejected", "session.error",
	}
	for _, event := range wantEvents {
		require.Contains(t, text, `"`+event+`"`)
	}
	rest, found := strings.CutPrefix(text, "const forwardedEvents = new Set([\n")
	require.True(t, found)
	block, _, found := strings.Cut(rest, "])\n")
	require.True(t, found)
	var gotEvents []string
	for _, line := range strings.Split(block, "\n") {
		if event := strings.Trim(strings.TrimSpace(line), `",`); event != "" {
			gotEvents = append(gotEvents, event)
		}
	}
	require.Equal(t, wantEvents, gotEvents)
	for _, forbidden := range []string{"title", "directory", "patterns", "questions", "answers"} {
		require.NotContains(t, text, forbidden)
	}
	require.Contains(t, text, `Bun.spawn(["gifford", "label", "hook", "opencode"]`)
	require.NotContains(t, text, "shell")
	require.Contains(t, text, "}, 250)")
}

func TestInstallOpenCodeBridgeInNewOrEmptyDir(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "new", true: "empty"}[existing], func(t *testing.T) {
			base := t.TempDir()
			t.Setenv("XDG_CONFIG_HOME", filepath.Join(base, "usual"))
			target := filepath.Join(base, "isolated")
			if existing {
				require.NoError(t, os.Mkdir(target, 0o755))
			}
			require.NoError(t, installOpenCodeBridge(target))
			assertMode(t, target, 0o700)
			assertMode(t, filepath.Join(target, openCodeBridgeDir), 0o700)
			plugin := filepath.Join(target, openCodeBridgeDir, openCodeBridgeFile)
			assertMode(t, plugin, 0o600)
			got, err := os.ReadFile(plugin)
			require.NoError(t, err)
			require.Equal(t, openCodeBridge, got)
		})
	}
}

func TestInstallOpenCodeBridgeRefusesExistingConfig(t *testing.T) {
	base := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(base, "usual"))
	target := filepath.Join(base, "isolated")
	require.NoError(t, os.Mkdir(target, 0o700))
	marker := filepath.Join(target, "keep")
	require.NoError(t, os.WriteFile(marker, []byte("unchanged"), 0o600))

	err := installOpenCodeBridge(target)
	require.ErrorIs(t, err, errBridgeNotEmpty)
	got, readErr := os.ReadFile(marker)
	require.NoError(t, readErr)
	require.Equal(t, "unchanged", string(got))
	_, statErr := os.Stat(filepath.Join(target, openCodeBridgeDir))
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestInstallOpenCodeBridgePreservesConcurrentFile(t *testing.T) {
	base := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(base, "usual"))
	target := filepath.Join(base, "isolated")
	foreign := []byte("created by another process")

	err := installOpenCodeBridgeWithOpenFile(target, func(path string, flags int, perm os.FileMode) (*os.File, error) {
		require.NoError(t, os.WriteFile(path, foreign, 0o600))
		return os.OpenFile(path, flags, perm)
	})

	require.ErrorIs(t, err, errBridgeWrite)
	plugin := filepath.Join(target, openCodeBridgeDir, openCodeBridgeFile)
	got, readErr := os.ReadFile(plugin)
	require.NoError(t, readErr)
	require.Equal(t, foreign, got)
}

func TestInstallOpenCodeBridgeRefusesUsualConfigAndLinks(t *testing.T) {
	base := t.TempDir()
	xdg := filepath.Join(base, "config")
	t.Setenv("XDG_CONFIG_HOME", xdg)
	usual := filepath.Join(xdg, "opencode")
	require.NoError(t, os.MkdirAll(usual, 0o700))
	require.ErrorIs(t, installOpenCodeBridge(usual), errBridgeDefaultDir)
	insideUsual := filepath.Join(usual, "isolated")
	require.ErrorIs(t, installOpenCodeBridge(insideUsual), errBridgeDefaultDir)
	require.NoDirExists(t, insideUsual)

	realTarget := filepath.Join(base, "real")
	require.NoError(t, os.Mkdir(realTarget, 0o700))
	linkedTarget := filepath.Join(base, "linked")
	require.NoError(t, os.Symlink(realTarget, linkedTarget))
	require.ErrorIs(t, installOpenCodeBridge(linkedTarget), errBridgeUnsafe)
	_, err := os.Stat(filepath.Join(realTarget, openCodeBridgeDir))
	require.ErrorIs(t, err, os.ErrNotExist)

	realParent := filepath.Join(base, "parent")
	require.NoError(t, os.Mkdir(realParent, 0o700))
	linkedParent := filepath.Join(base, "parent-link")
	require.NoError(t, os.Symlink(realParent, linkedParent))
	require.ErrorIs(t, installOpenCodeBridge(filepath.Join(linkedParent, "isolated")), errBridgeUnsafe)
	_, err = os.Stat(filepath.Join(realParent, "isolated"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestOpenCodeBridgeCommandIsSilentAndDoesNotLeakPaths(t *testing.T) {
	base := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(base, "usual"))
	target := filepath.Join(base, "CANARY-private-path")

	cmd := newOpenCodeBridgeCmd()
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{target})
	require.NoError(t, cmd.Execute())
	require.Empty(t, stdout.String())
	require.Empty(t, stderr.String())

	cmd = newOpenCodeBridgeCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{target})
	err := cmd.Execute()
	require.Error(t, err)
	require.True(t, errors.Is(err, errBridgeNotEmpty) || errors.Is(err, errBridgeUnsafe))
	require.NotContains(t, err.Error(), target)
	require.NotContains(t, err.Error(), "CANARY")
}

func TestOpenCodeBridgeHelpDocumentsIsolation(t *testing.T) {
	t.Parallel()

	cmd := newOpenCodeBridgeCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"--help"})
	require.NoError(t, cmd.Execute())
	for _, want := range []string{
		"env OPENCODE_CONFIG_DIR=<config-dir> gifford run ... -- opencode",
		"empty isolated config dir", "usual configuration", "version probing",
	} {
		require.Contains(t, out.String(), want)
	}
	require.NotContains(t, out.String(), "-- env OPENCODE_CONFIG_DIR")
}

func assertMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, want, info.Mode().Perm())
}

func TestBridgeErrorsContainNoInputData(t *testing.T) {
	t.Parallel()

	for _, err := range []error{errBridgeDefaultDir, errBridgeNotEmpty, errBridgeUnsafe, errBridgeWrite} {
		require.NotContains(t, strings.ToLower(err.Error()), "canary")
	}
}

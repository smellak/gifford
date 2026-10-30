package adapters

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

func TestProbeHarnessCodexAppServer(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("test helper is a shell script")
	}
	binary := fakeCodex(t, `
case "$1" in
  --version) printf '%s\n' 'codex-cli 7.8.9' ;;
  app-server)
    while IFS= read -r line; do
      case "$line" in
        *'"method":"initialize"'*)
          printf '%s\n' '{"id":1,"result":{"userAgent":"gifford_adapterprobe/7.8.9 (test)","platformFamily":"unix","platformOs":"linux"}}'
          ;;
        *'"method":"initialized"'*) exit 0 ;;
      esac
    done
    ;;
esac
`)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	got := ProbeHarness(ctx, ProbeOptions{Binary: binary, Harness: protocol.HarnessCodex})
	require.True(t, got.Verifiable)
	require.False(t, got.Attachable)
	require.Equal(t, "7.8.9", got.Version)
	require.False(t, got.Capabilities.StructuredEvents)
	require.False(t, got.Capabilities.InternalMessageDelivery)
	require.Contains(t, got.Verdict, "structured_events documentado, no medido")
	require.Contains(t, got.Verdict, "sin adjuncion documentada")
}

func TestProbeHarnessReportsNoVerifiableChannel(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("test helper is a shell script")
	}
	binary := fakeCodex(t, `
case "$1" in
  --version) printf '%s\n' 'codex-cli 1.2.3' ;;
  app-server) exit 1 ;;
esac
`)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	got := ProbeHarness(ctx, ProbeOptions{Binary: binary, Harness: protocol.HarnessCodex})
	require.False(t, got.Verifiable)
	require.Equal(t, "1.2.3", got.Version)
	require.Equal(t, "sin canal verificable", got.Verdict)
}

func TestProbeHarnessDoesNotGuessUnsupportedChannel(t *testing.T) {
	t.Parallel()

	got := ProbeHarness(t.Context(), ProbeOptions{Binary: "unused", Harness: protocol.HarnessClaudeCode})
	require.False(t, got.Verifiable)
	require.Equal(t, "sin canal verificable", got.Verdict)
}

func TestParseHarnessVersion(t *testing.T) {
	t.Parallel()

	require.Equal(t, "0.154.0", parseHarnessVersion("codex-cli 0.154.0"))
	require.Empty(t, parseHarnessVersion("codex-cli nightly"))
}

func fakeCodex(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "codex")
	content := "#!/bin/sh\nset -eu\n" + body
	syscall.ForkLock.RLock()
	err := os.WriteFile(path, []byte(content), 0o700)
	syscall.ForkLock.RUnlock()
	require.NoError(t, err)
	return path
}

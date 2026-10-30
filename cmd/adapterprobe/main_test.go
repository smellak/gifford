package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestProbeCodexAppServer(t *testing.T) {
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
	got := probe(ctx, binary)
	require.True(t, got.Verifiable)
	require.False(t, got.Attachable)
	require.Equal(t, "7.8.9", got.Version)
	require.True(t, got.Capabilities.StructuredEvents)
	require.False(t, got.Capabilities.InternalMessageDelivery)
	require.Contains(t, got.Verdict, "sin adjuncion documentada")
}

func TestProbeReportsNoVerifiableChannel(t *testing.T) {
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
	got := probe(ctx, binary)
	require.False(t, got.Verifiable)
	require.Equal(t, "1.2.3", got.Version)
	require.Equal(t, "sin canal verificable", got.Verdict)
}

func TestRunMain(t *testing.T) {
	t.Parallel()

	var stdout, stderr bytes.Buffer
	require.Equal(t, 2, runMain(nil, &stdout, &stderr))
	require.Contains(t, stderr.String(), "usage:")
}

func TestParseVersion(t *testing.T) {
	t.Parallel()

	require.Equal(t, "0.154.0", parseVersion("codex-cli 0.154.0"))
	require.Empty(t, parseVersion("codex-cli nightly"))
}

func fakeCodex(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "codex")
	content := "#!/bin/sh\nset -eu\n" + body
	require.NoError(t, os.WriteFile(path, []byte(content), 0o700))
	return path
}

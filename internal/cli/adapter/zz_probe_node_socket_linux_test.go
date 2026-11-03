//go:build linux

package adapter

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/deps"
	clihub "github.com/smellak/gifford/internal/cli/hub"
	"github.com/smellak/gifford/internal/cli/status"
	"github.com/smellak/gifford/internal/nativeevents"
)

type probeBuffer struct {
	mu  chan struct{}
	buf bytes.Buffer
}

func newProbeBuffer() *probeBuffer { return &probeBuffer{mu: make(chan struct{}, 1)} }

func (b *probeBuffer) Write(p []byte) (int, error) {
	b.mu <- struct{}{}
	defer func() { <-b.mu }()
	return b.buf.Write(p)
}

func (b *probeBuffer) String() string {
	b.mu <- struct{}{}
	defer func() { <-b.mu }()
	return b.buf.String()
}

// Committed canary for H2 (miner-3 hostile review, mapper order): a real hub
// with and without node_socket configured outside /run/gifford, the real
// Claude Code hook command run by the same user, and gifford status. The
// system entry is an injected scratch directory: the real /run/gifford is
// never touched. In both cases the hook must resolve the node entry and
// never reach the scratch system entry. Removing the node_socket step from
// socketPath resolves the system entry instead when node_socket is set, so
// this test kills that mutant.
func TestProbeHookMissesTheConfiguredNativeEntry(t *testing.T) {
	for _, configured := range []bool{false, true} {
		t.Run(fmt.Sprint("node_socket=", configured), func(t *testing.T) {
			base, err := os.MkdirTemp("", "pn") //nolint:usetesting // socket paths must stay short
			require.NoError(t, err)
			t.Cleanup(func() { _ = os.RemoveAll(base) })
			t.Setenv("XDG_CONFIG_HOME", filepath.Join(base, "c"))
			t.Setenv("XDG_DATA_HOME", filepath.Join(base, "d"))
			t.Setenv("XDG_RUNTIME_DIR", filepath.Join(base, "r"))
			t.Setenv(SocketEnvironment, "")
			body := "schema_version: 1\nnode_id: node-a\n"
			shared := filepath.Join(base, "s")
			nodeEntry := filepath.Join(base, "r", "gifford", nativeevents.NodeSocketName)
			if configured {
				require.NoError(t, os.Mkdir(shared, 0o755))
				body += fmt.Sprintf("node_socket:\n  path: %s\n  bridge_uid: 65534\n", filepath.Join(shared, "node.sock"))
				nodeEntry = filepath.Join(shared, nativeevents.NodeSocketName)
			}
			require.NoError(t, os.MkdirAll(filepath.Join(base, "c", "gifford"), 0o700))
			require.NoError(t, os.WriteFile(filepath.Join(base, "c", "gifford", "config.yaml"), []byte(body), 0o600))

			sys := filepath.Join(base, "sys")
			require.NoError(t, os.Mkdir(sys, 0o700))
			listener, err := (&net.ListenConfig{}).Listen(context.Background(), "unix",
				filepath.Join(sys, nativeevents.NodeSocketName))
			require.NoError(t, err)
			defer func() { _ = listener.Close() }()
			accepted := make(chan struct{}, 8)
			go func() {
				for {
					conn, err := listener.Accept()
					if err != nil {
						return
					}
					accepted <- struct{}{}
					_ = conn.Close()
				}
			}()
			previous := systemNativeSocketDir
			systemNativeSocketDir = sys
			t.Cleanup(func() { systemNativeSocketDir = previous })

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hubCmd := clihub.NewCmd(&deps.Deps{})
			hubErr := newProbeBuffer()
			hubCmd.SilenceErrors, hubCmd.SilenceUsage = true, true
			hubCmd.SetOut(hubErr)
			hubCmd.SetErr(hubErr)
			hubCmd.SetArgs([]string{})
			done := make(chan error, 1)
			go func() { done <- hubCmd.ExecuteContext(ctx) }()
			require.Eventually(t, func() bool {
				info, err := os.Lstat(nodeEntry)
				return err == nil && info.Mode().Type() == os.ModeSocket &&
					strings.Contains(hubErr.String(), "ready:")
			}, 20*time.Second, 20*time.Millisecond, "node entry never served: %s", hubErr.String())

			resolved := socketPath()
			hook := NewCmd(&deps.Deps{})
			hook.SetIn(strings.NewReader(`{"hook_event_name":"SessionStart","session_id":"s-probe","source":"startup"}`))
			hook.SetOut(newProbeBuffer())
			hook.SetErr(newProbeBuffer())
			hook.SetArgs([]string{"hook", "claude-code"})
			require.NoError(t, hook.ExecuteContext(context.Background()))
			reached := 0
			for wait := true; wait; {
				select {
				case <-accepted:
					reached++
				case <-time.After(300 * time.Millisecond):
					wait = false
				}
			}

			st := status.NewCmd(&deps.Deps{})
			stOut := newProbeBuffer()
			st.SilenceErrors, st.SilenceUsage = true, true
			st.SetOut(stOut)
			st.SetErr(stOut)
			st.SetArgs([]string{})
			_ = st.ExecuteContext(context.Background())
			var nativeLine string
			for _, line := range strings.Split(stOut.String(), "\n") {
				if strings.HasPrefix(line, "native") {
					nativeLine = strings.ReplaceAll(line, "\t", " ")
				}
			}
			t.Logf("node_socket configured=%v: node serves %s; the hook resolves %s; "+
				"scratch system entry reached %d time(s); status: %q",
				configured, strings.TrimPrefix(nodeEntry, base), strings.TrimPrefix(resolved, base), reached, nativeLine)
			require.Equal(t, nodeEntry, resolved, "hook must resolve the node entry")
			require.Zero(t, reached, "scratch system entry must stay silent")
			cancel()
			require.NoError(t, <-done)
		})
	}
}

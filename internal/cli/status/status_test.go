package status

import (
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/nativeevents"
	"github.com/smellak/gifford/internal/protocol"
)

func TestStatusOfANodeThatIsDown(t *testing.T) {
	base := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(base, "config"))
	t.Setenv("XDG_DATA_HOME", filepath.Join(base, "data"))
	t.Setenv("XDG_RUNTIME_DIR", filepath.Join(base, "run"))
	cmd := NewCmd(&deps.Deps{})
	cmd.SilenceUsage, cmd.SilenceErrors = true, true
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(io.Discard)
	var ec *deps.ExitError
	require.ErrorAs(t, cmd.Execute(), &ec)
	require.Equal(t, exitcode.Error, ec.Code)
	require.Equal(t, "node     down\nsockets  hub.sock no answer, daemon.sock no answer\nnative   entry no answer\n", out.String())
}

func TestStatusShowsTheAgentsAndTheChannel(t *testing.T) {
	t.Parallel()
	agent := func(id, harness string, state protocol.SessionState, phase protocol.AgentPhase, queued int64) protocol.AgentListItem {
		return protocol.AgentListItem{
			AgentID: protocol.AgentID(id), Harness: protocol.Harness(harness), SessionState: state,
			Phase: protocol.AgentPhaseStatus{Value: phase}, Queue: protocol.AgentQueueCounts{Queued: queued},
		}
	}
	r := report{
		hubSocket: true, controlSocket: true, pid: 4242,
		channel: protocol.ChannelState{Mode: protocol.ChannelModeActive, EffectiveMode: protocol.ChannelModeActive},
		agents: []protocol.AgentListItem{
			agent("agent-1", "claude-code", "CONNECTED", protocol.AgentPhaseIdleAtPrompt, 2),
			agent("agent-22", "codex", "DISCONNECTED", protocol.AgentPhaseUnknown, 0),
		},
	}
	var out bytes.Buffer
	require.NoError(t, r.write(&out))
	require.Equal(t, "node     up (PID 4242)\n"+
		"sockets  hub.sock ok, daemon.sock ok\n"+
		"native   entry no answer\n"+
		"channel  ACTIVE\n"+
		"agents   2\n"+
		"         agent-1   claude-code  CONNECTED     IDLE_AT_PROMPT  queued 2\n"+
		"         agent-22  codex        DISCONNECTED  UNKNOWN         queued 0\n", out.String())

	r = report{
		hubSocket: true, controlSocket: true, more: true,
		channel: protocol.ChannelState{Mode: protocol.ChannelModePaused, EffectiveMode: protocol.ChannelModeOff},
	}
	out.Reset()
	require.NoError(t, r.write(&out))
	require.Contains(t, out.String(), "node     up (not started by gifford up)\n")
	require.Contains(t, out.String(), "channel  PAUSED, effective OFF\n")
	require.Contains(t, out.String(), "agents   0 shown; see gifford panel for the rest\n")
}

func TestStatusShowsNativeReceptionsPerInstance(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 11, 3, 0, 40, 0, 0, time.UTC)
	r := report{
		hubSocket: true, controlSocket: true,
		native: nativeReport{serving: true, receptions: []nativeReception{
			{agent: "agent-1", harness: "codex", harnessVersion: "1.2.3", acceptedAt: at},
		}},
		nativeSeen: true,
	}
	var out bytes.Buffer
	require.NoError(t, r.write(&out))
	require.Contains(t, out.String(), "native   entry ok, 1 instance(s)\n")
	require.Contains(t, out.String(), "agent-1  codex  1.2.3  2026-11-03T00:40:00Z\n")
}

func TestQueryNativeReadsTheEntryAnswer(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	serveNativeStatus(t, dir)
	report, ok := queryNative(context.Background(), dir)
	require.True(t, ok)
	require.True(t, report.serving)
	require.Len(t, report.receptions, 1)
	require.Equal(t, "agent-1", report.receptions[0].agent)
	require.Equal(t, "codex", report.receptions[0].harness)
	require.Equal(t, "1.2.3", report.receptions[0].harnessVersion)
}

func TestStatusUsesTheConfiguredNativeSocketDirectory(t *testing.T) {
	root, err := os.MkdirTemp("", "gifford-status-native-") //nolint:usetesting // Unix socket paths must stay below sun_path.
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(root)) })

	for _, test := range []struct {
		name       string
		configured bool
	}{
		{name: "beside node socket", configured: true},
		{name: "private runtime fallback", configured: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			base := filepath.Join(root, test.name)
			configHome := filepath.Join(base, "config")
			runtimeHome := filepath.Join(base, "run")
			dataHome := filepath.Join(base, "data")
			configDir := filepath.Join(configHome, "gifford")
			runtimeDir := filepath.Join(runtimeHome, "gifford")
			sharedDir := filepath.Join(base, "shared")
			require.NoError(t, os.MkdirAll(configDir, 0o700))
			require.NoError(t, os.MkdirAll(runtimeDir, 0o700))
			require.NoError(t, os.MkdirAll(sharedDir, 0o755))

			configBody := "schema_version: 1\n"
			wantDir := runtimeDir
			if test.configured {
				configBody += "node_socket:\n  path: " + filepath.Join(sharedDir, "node.sock") + "\n  bridge_uid: 1001\n"
				wantDir = sharedDir
			}
			require.NoError(t, os.WriteFile(filepath.Join(configDir, "config.yaml"), []byte(configBody), 0o600))
			serveNativeStatus(t, wantDir)

			t.Setenv("XDG_CONFIG_HOME", configHome)
			t.Setenv("XDG_DATA_HOME", dataHome)
			t.Setenv("XDG_RUNTIME_DIR", runtimeHome)
			cmd := NewCmd(&deps.Deps{})
			cmd.SilenceUsage, cmd.SilenceErrors = true, true
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(io.Discard)
			var ec *deps.ExitError
			require.ErrorAs(t, cmd.Execute(), &ec, "the private hub and control sockets are intentionally absent")
			require.Contains(t, out.String(), "native   entry ok, 1 instance(s)\n")
		})
	}
}

func serveNativeStatus(t *testing.T, dir string) {
	t.Helper()
	path := filepath.Join(dir, nativeevents.NodeSocketName)
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "unix", path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, listener.Close()) })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = conn.Close() }()
				message, err := nativeevents.ReadMessage(conn)
				if err != nil {
					return
				}
				if _, ok := message.(nativeevents.StatusRequest); !ok {
					return
				}
				_ = nativeevents.WriteMessage(conn, nativeevents.StatusResult{
					Operation: nativeevents.OperationStatusResult, Serving: true,
					Receptions: []nativeevents.InstanceReception{{
						Agent: "agent-1", Harness: "codex",
						HarnessVersion:   "1.2.3",
						AcceptedAtUnixMs: 1793509200123,
					}},
				})
			}()
		}
	}()
}

func TestQueryNativeReportsNoAnswerWithoutAnEntry(t *testing.T) {
	t.Parallel()
	_, ok := queryNative(context.Background(), t.TempDir())
	require.False(t, ok)
}

func TestStatusIsDownWhenOneSocketDoesNotAnswer(t *testing.T) {
	t.Parallel()
	for _, r := range []report{{hubSocket: true}, {controlSocket: true}, {}} {
		require.False(t, r.up())
	}
	require.True(t, report{hubSocket: true, controlSocket: true}.up())
}

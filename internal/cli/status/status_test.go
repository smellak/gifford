package status

import (
	"bytes"
	"context"
	"errors"
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
	cmd := newCmd(filepath.Join(t.TempDir(), "absent-panel-lock-root"))
	cmd.SilenceUsage, cmd.SilenceErrors = true, true
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(io.Discard)
	var ec *deps.ExitError
	require.ErrorAs(t, cmd.Execute(), &ec)
	require.Equal(t, exitcode.Error, ec.Code)
	require.Equal(t, "node     down\nsockets  hub.sock no answer, daemon.sock no answer\nnative   entry no answer\n", out.String())
}

func TestStatusShowsMissingPanelLockConfiguration(t *testing.T) {
	base := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(base, "config"))
	t.Setenv("XDG_DATA_HOME", filepath.Join(base, "data"))
	t.Setenv("XDG_RUNTIME_DIR", filepath.Join(base, "run"))

	cmd := newCmd(t.TempDir())
	cmd.SilenceUsage, cmd.SilenceErrors = true, true
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(io.Discard)
	var ec *deps.ExitError
	require.ErrorAs(t, cmd.Execute(), &ec, "the node sockets are intentionally absent")
	require.Equal(t, exitcode.Error, ec.Code)
	require.Contains(t, out.String(),
		"panels   WARN: pane-lock root is present but tmux_locks is not configured; all tmux panel writes will be rejected\n")
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

// Each queued agent names its first hold: a pending approval with its
// note, the detector route's wait, a held pane lock, or the latest
// decision's failed guard. A read that fails only leaves its cause out.
const (
	holdApproval protocol.AgentID = "agent-approval"
	holdDecision protocol.AgentID = "agent-decision"
	holdFailing  protocol.AgentID = "agent-failing"
)

func TestStatusNamesWhatHoldsEachAgent(t *testing.T) {
	t.Parallel()
	agents := []protocol.AgentListItem{
		{AgentID: holdApproval, Queue: protocol.AgentQueueCounts{Queued: 1}},
		{
			AgentID: "agent-waiting", Queue: protocol.AgentQueueCounts{Queued: 1},
			Waiting: "p_idle_threshold: no orchestration model is available; deterministic-pty-v1 is not eligible: harness is busy",
		},
		{
			AgentID: "agent-pane", Queue: protocol.AgentQueueCounts{Queued: 1},
			PaneLock: &protocol.AgentPaneLockStatus{Readable: true, Phase: protocol.PaneLockPhaseUncertain},
		},
		{AgentID: holdDecision, Queue: protocol.AgentQueueCounts{Queued: 2}},
		{AgentID: "agent-idle", PaneLock: &protocol.AgentPaneLockStatus{Readable: true, Phase: protocol.PaneLockPhaseIdle}},
		{AgentID: holdFailing, Queue: protocol.AgentQueueCounts{Queued: 1}},
	}
	var asked []protocol.AgentID
	call := func(_ context.Context, method protocol.ControlMethod, params, result any) error {
		switch method {
		case protocol.ControlMethodApprovalList:
			require.Equal(t, protocol.ApprovalStatePending, params.(protocol.ListApprovalsParams).State)
			*result.(*protocol.ApprovalList) = protocol.ApprovalList{Approvals: []protocol.ApprovalRequest{
				{ApprovalID: "apr_01JB0000000000000000000055", AgentID: holdApproval, RequestNote: "older"},
				{
					ApprovalID: "apr_01JB0000000000000000000056", AgentID: holdApproval,
					RequestNote: "automatic delivery deferred: p_idle_threshold",
				},
				{ApprovalID: "apr_01JB0000000000000000000057"},
			}}
		case protocol.ControlMethodDeliveryDecisionList:
			request := params.(protocol.DeliveryDecisionListParams)
			asked = append(asked, request.AgentID)
			require.Equal(t, int64(1), *request.Limit)
			if request.AgentID == holdFailing {
				return errors.New("synthetic read failure")
			}
			*result.(*protocol.DeliveryDecisionListResult) = protocol.DeliveryDecisionListResult{
				Decisions: []protocol.DeliveryDecisionListItem{{Hold: "human_draft_closed: human draft is not CLOSED"}},
			}
		default:
			t.Fatalf("unexpected control method %s", method)
		}
		return nil
	}
	pending, read, holds := readHolds(t.Context(), call, agents)
	require.True(t, read)
	require.Equal(t, 3, pending)
	require.Equal(t, map[protocol.AgentID]string{
		holdApproval:    "approval apr_01JB0000000000000000000056: automatic delivery deferred: p_idle_threshold",
		"agent-waiting": "waiting: " + agents[1].Waiting,
		"agent-pane":    "pane-lock INCIERTO",
		holdDecision:    "human_draft_closed: human draft is not CLOSED",
	}, holds)
	require.Equal(t, []protocol.AgentID{holdDecision, holdFailing}, asked)

	_, read, holds = readHolds(t.Context(), func(context.Context, protocol.ControlMethod, any, any) error {
		return errors.New("synthetic read failure")
	}, agents[:1])
	require.False(t, read)
	require.Empty(t, holds)

	r := report{
		hubSocket: true, controlSocket: true, approvals: 3, approvalsRead: true,
		channel: protocol.ChannelState{Mode: protocol.ChannelModeActive, EffectiveMode: protocol.ChannelModeActive},
		agents:  agents[3:5], holds: map[protocol.AgentID]string{holdDecision: "human_draft_closed"},
	}
	var out bytes.Buffer
	require.NoError(t, r.write(&out))
	require.Contains(t, out.String(), "approvals  3 pending\n")
	require.Regexp(t, string(holdDecision)+` +queued 2  held: human_draft_closed\n`, out.String())
	require.Regexp(t, `agent-idle +queued 0\n`, out.String())
}

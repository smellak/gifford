package status

import (
	"bytes"
	"io"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
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
	require.Equal(t, "node     down\nsockets  hub.sock no answer, daemon.sock no answer\n", out.String())
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

func TestStatusIsDownWhenOneSocketDoesNotAnswer(t *testing.T) {
	t.Parallel()
	for _, r := range []report{{hubSocket: true}, {controlSocket: true}, {}} {
		require.False(t, r.up())
	}
	require.True(t, report{hubSocket: true, controlSocket: true}.up())
}

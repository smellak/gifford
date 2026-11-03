package doctor

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/protocol"
)

const (
	flagLive       = "--live"
	flagRuntimeDir = "--runtime-dir"
)

type testMetadataReader struct {
	methods []protocol.ControlMethod
	fail    protocol.ControlMethod
	last    *protocol.ModelLastCall
}

func (r *testMetadataReader) Call(_ context.Context, method protocol.ControlMethod, _ any, result any) error {
	r.methods = append(r.methods, method)
	if method == r.fail {
		return errors.New("unavailable")
	}
	switch value := result.(type) {
	case *protocol.ChannelState:
		value.Mode = protocol.ChannelModeOff
		value.EffectiveMode = protocol.ChannelModeOff
	case *protocol.AgentListResult:
		value.Agents = make([]protocol.AgentListItem, 1)
	case *protocol.ModelStatusResult:
		value.LastCall = r.last
	default:
		return errors.New("unexpected read")
	}
	return nil
}

func TestLiveMetadataIsReadOnlyAndDoesNotClaimModelAvailability(t *testing.T) {
	t.Parallel()
	reader := &testMetadataReader{}
	checks := checkMetadata(context.Background(), reader)
	require.Equal(t, []protocol.ControlMethod{
		protocol.ControlMethodChannelGetGlobal,
		protocol.ControlMethodAgentList,
		protocol.ControlMethodModelStatus,
	}, reader.methods)
	var report bytes.Buffer
	require.NoError(t, writeLiveReport(&report, checks))
	require.Contains(t, report.String(), "channel OFF")
	require.Contains(t, report.String(), "1 registered agents; this is not a delivery test")
	require.Contains(t, report.String(), "current Glimmer/fallback availability is not verified")
}

func TestLiveModelHistoryIsNotCurrentHealth(t *testing.T) {
	t.Parallel()
	reader := &testMetadataReader{last: &protocol.ModelLastCall{
		Status: protocol.GlimmerCallStatusOK, LatencyMS: 12,
		ObservedAt: time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC),
	}}
	var report bytes.Buffer
	require.NoError(t, writeLiveReport(&report, checkMetadata(context.Background(), reader)))
	require.Contains(t, report.String(), "last call ok at 2026-01-01T00:00:00Z (12 ms)")
	require.Contains(t, report.String(), "not a live availability probe")
}

func TestLiveMetadataFailureFailsCommand(t *testing.T) {
	t.Parallel()
	for _, method := range []protocol.ControlMethod{
		protocol.ControlMethodChannelGetGlobal, protocol.ControlMethodAgentList, protocol.ControlMethodModelStatus,
	} {
		t.Run(string(method), func(t *testing.T) {
			t.Parallel()
			reader := &testMetadataReader{fail: method}
			var report bytes.Buffer
			var ec *deps.ExitError
			require.ErrorAs(t, writeLiveReport(&report, checkMetadata(context.Background(), reader)), &ec)
			require.Equal(t, exitcode.Error, ec.Code)
			require.Contains(t, report.String(), stateFail)
		})
	}
}

func TestLiveDoctorDoesNotCreateRuntime(t *testing.T) {
	t.Parallel()
	runtimeDir := filepath.Join(t.TempDir(), "absent")
	cmd := NewCmd(&deps.Deps{})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{flagLive, flagRuntimeDir, runtimeDir})
	var ec *deps.ExitError
	require.ErrorAs(t, cmd.Execute(), &ec)
	require.Equal(t, exitcode.Error, ec.Code)
	_, err := os.Stat(runtimeDir)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.Contains(t, out.String(), "no accessible listener")
	require.Contains(t, out.String(), "per-agent access not probed")
}

func TestLiveFlagsRejectInvalidCombinationWithoutCreatingDirectories(t *testing.T) {
	t.Parallel()
	for _, args := range [][]string{
		{flagRuntimeDir, "/tmp/doctor-unused"},
		{"--service", "gifford.service"},
		{flagLive, flagRuntimeDir, "relative"},
		{flagLive, "--agent", "agent-one"},
	} {
		cmd := NewCmd(&deps.Deps{})
		cmd.SetOut(io.Discard)
		cmd.SetErr(io.Discard)
		cmd.SetArgs(args)
		var ec *deps.ExitError
		require.ErrorAs(t, cmd.Execute(), &ec)
		require.Equal(t, exitcode.Usage, ec.Code)
	}
}

func TestLiveUnimplementedPeerCannotLookHealthy(t *testing.T) {
	t.Parallel()
	var out bytes.Buffer
	err := writeLiveReport(&out, []liveCheck{
		{"FEDERATION", stateFail, "requested peer check is not implemented"},
	})
	require.Error(t, err)
	require.Contains(t, out.String(), stateFail)
}

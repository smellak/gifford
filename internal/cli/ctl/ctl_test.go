package ctl

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/controlapi"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
)

func TestCtlNotImplemented(t *testing.T) {
	t.Parallel()

	for _, args := range []string{
		"hold-message m1",
		"deliver-next agent-1",
		"inspect obj-1",
		"retry-artifact art-1",
		"unlock agent-1",
		"revoke-node node-b",
		"replay obj-1",
		"model status",
		"model activate adapter-1 v1",
		"model rollback",
		"loop create --thread t1 --participants a1,a2 --mode auto",
		"loop status loop-1",
		"loop list",
		"loop list --active",
		"loop set-mode loop-1 auto",
		"loop next loop-1",
		"loop budget loop-1 --add-turns 2",
		"loop stop loop-1 --reason done",
		"loop complete loop-1",
		"approvals",
		"approvals --pending",
		"approve ap-1",
		"reject ap-1 --reason no",
		"approve-message m1",
		"reject-message m1 --reason no",
		"release-hold m1",
	} {
		t.Run(args, func(t *testing.T) {
			t.Parallel()

			cmd := NewCmd(&deps.Deps{})
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)
			cmd.SetArgs(strings.Fields(args))

			var ec *deps.ExitError
			require.ErrorAs(t, cmd.Execute(), &ec)
			require.Equal(t, exitcode.NotImplemented, ec.Code)
		})
	}
}

func TestCtlPauseResumeRestoresPriorState(t *testing.T) {
	base := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", base)
	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	engine, err := channel.New(channel.Options{Clock: clock.System{}})
	require.NoError(t, err)
	controller, err := channel.NewController(channel.ControllerOptions{DB: store.DB(), Engine: engine, Clock: clock.System{}, NodeID: "node-a", Entropy: bytes.NewReader(make([]byte, 1024))})
	require.NoError(t, err)
	require.NoError(t, controller.Reload(t.Context()))
	actor := channel.Actor{Type: protocol.ActorTypeHuman, ID: "operator"}
	initial, err := controller.SetAgentMode(t.Context(), protocol.SetAgentModeParams{AgentID: "agent-1", Mode: protocol.AgentModeReceiveOnly, ExpectedVersion: 0}, actor)
	require.NoError(t, err)
	require.True(t, initial.AutoDelivery)

	runtimeDir := filepath.Join(base, "gifford")
	server, err := controlapi.New(controlapi.Options{RuntimeDir: runtimeDir, DB: store.DB(), Controller: controller})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	waitForControlSocket(t, server.Path())

	for _, args := range []string{"pause agent-1", "resume agent-1"} {
		cmd := NewCmd(&deps.Deps{})
		cmd.SetOut(io.Discard)
		cmd.SetErr(io.Discard)
		cmd.SetArgs(strings.Fields(args))
		require.NoError(t, cmd.ExecuteContext(t.Context()))
	}
	final, err := controller.Agent(t.Context(), "agent-1")
	require.NoError(t, err)
	require.False(t, final.TX)
	require.True(t, final.RX)
	require.True(t, final.AutoDelivery)
	require.False(t, final.AutoContinue)
	cancel()
	require.NoError(t, <-done)
}

func waitForControlSocket(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		require.True(t, time.Now().Before(deadline), "control socket was not created")
		time.Sleep(5 * time.Millisecond)
	}
}

func TestCtlBareGroupsShowHelp(t *testing.T) {
	t.Parallel()

	for _, args := range []string{
		"",
		"model",
		"channel",
		"link",
		"loop",
	} {
		t.Run("group/"+args, func(t *testing.T) {
			t.Parallel()

			cmd := NewCmd(&deps.Deps{})
			cmd.SetOut(io.Discard)
			cmd.SetArgs(strings.Fields(args))
			require.NoError(t, cmd.Execute())
		})
	}
}

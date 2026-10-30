package ctl

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

const ctlTestNodeID protocol.NodeID = "node-a"

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
		"approve ap-1",
		"approve ap-1 --once",
		"approve ap-1 --deliver-now --steps 1",
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
	controller, err := channel.NewController(channel.ControllerOptions{DB: store.DB(), Engine: engine, Clock: clock.System{}, NodeID: ctlTestNodeID, Entropy: bytes.NewReader(make([]byte, 1024))})
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

type controllerApprovalService struct {
	controller *channel.Controller
	db         *sql.DB
}

func (s controllerApprovalService) ResolveApproval(
	ctx context.Context,
	params protocol.ResolveApprovalParams,
	actor channel.Actor,
) (protocol.ApprovalRequest, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return protocol.ApprovalRequest{}, err
	}
	defer func() { _ = tx.Rollback() }()
	request, err := s.controller.ResolveOrchestratorApproval(ctx, tx, params, actor)
	if err != nil {
		return protocol.ApprovalRequest{}, err
	}
	if err := tx.Commit(); err != nil {
		return protocol.ApprovalRequest{}, err
	}
	return request, nil
}

func TestCtlApprovalCommandsUseTypedControlMethods(t *testing.T) {
	base := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", base)
	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	engine, err := channel.New(channel.Options{Clock: clock.System{}})
	require.NoError(t, err)
	controller, err := channel.NewController(channel.ControllerOptions{
		DB: store.DB(), Engine: engine, Clock: clock.System{}, NodeID: ctlTestNodeID,
		Entropy: bytes.NewReader(make([]byte, 1024)),
	})
	require.NoError(t, err)
	require.NoError(t, controller.Reload(t.Context()))
	tx, err := store.DB().BeginTx(t.Context(), nil)
	require.NoError(t, err)
	first, _, err := controller.EnsureOrchestratorApproval(t.Context(), tx,
		"01ARZ3NDEKTSV4RRFFQ69G5FAV", "agent-one", "first pending request")
	require.NoError(t, err)
	second, _, err := controller.EnsureOrchestratorApproval(t.Context(), tx,
		"01ARZ3NDEKTSV4RRFFQ69G5FAW", "agent-two", "second pending request")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	server, err := controlapi.New(controlapi.Options{
		RuntimeDir: filepath.Join(base, "gifford"), DB: store.DB(), Controller: controller,
		Approvals: controllerApprovalService{controller: controller, db: store.DB()},
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	waitForControlSocket(t, server.Path())

	var pending protocol.ApprovalList
	runCtlJSON(t, []string{"approvals", "--pending"}, &pending)
	require.Len(t, pending.Approvals, 2)

	var approved protocol.ApprovalRequest
	runCtlJSON(t, []string{"approve", string(first.ApprovalID), "--deliver-now"}, &approved)
	require.Equal(t, protocol.ApprovalStateApproved, approved.State)
	require.NotNil(t, approved.Resolution)
	require.Equal(t, protocol.ApprovalOption("deliver_now"), approved.Resolution.Option)

	var rejected protocol.ApprovalRequest
	runCtlJSON(t, []string{"reject", string(second.ApprovalID), "--reason", "operator declined"}, &rejected)
	require.Equal(t, protocol.ApprovalStateRejected, rejected.State)
	require.NotNil(t, rejected.Resolution)
	require.Equal(t, "operator declined", rejected.Resolution.Reason)

	runCtlJSON(t, []string{"approvals", "--pending"}, &pending)
	require.Empty(t, pending.Approvals)
	cancel()
	require.NoError(t, <-done)
}

func TestCtlRejectRequiresReason(t *testing.T) {
	cmd := NewCmd(&deps.Deps{})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"reject", "approval-1"})
	var exit *deps.ExitError
	require.ErrorAs(t, cmd.Execute(), &exit)
	require.Equal(t, exitcode.Usage, exit.Code)
}

func runCtlJSON(t *testing.T, args []string, into any) {
	t.Helper()
	cmd := NewCmd(&deps.Deps{})
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(io.Discard)
	cmd.SetArgs(args)
	require.NoError(t, cmd.ExecuteContext(t.Context()))
	require.NoError(t, json.Unmarshal(output.Bytes(), into))
}

func TestCtlModelStatusPrintsControlReadJSON(t *testing.T) {
	base := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", base)
	databasePath := filepath.Join(t.TempDir(), "gifford.db")
	store, err := eventstore.Open(t.Context(), databasePath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	readDB, err := sql.Open("sqlite", databasePath+"?_pragma=busy_timeout(5000)&_pragma=query_only(1)")
	require.NoError(t, err)
	require.NoError(t, readDB.PingContext(t.Context()))
	t.Cleanup(func() { require.NoError(t, readDB.Close()) })
	engine, err := channel.New(channel.Options{Clock: clock.System{}})
	require.NoError(t, err)
	controller, err := channel.NewController(channel.ControllerOptions{
		DB: store.DB(), Engine: engine, Clock: clock.System{}, NodeID: ctlTestNodeID,
		Entropy: bytes.NewReader(make([]byte, 1024)),
	})
	require.NoError(t, err)
	require.NoError(t, controller.Reload(t.Context()))
	server, err := controlapi.New(controlapi.Options{
		RuntimeDir: filepath.Join(base, "gifford"), DB: store.DB(),
		ReadDB: readDB, Controller: controller,
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	waitForControlSocket(t, server.Path())

	cmd := NewCmd(&deps.Deps{})
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"model", modelStatusUse})
	require.NoError(t, cmd.ExecuteContext(t.Context()))
	var status protocol.ModelStatusResult
	require.NoError(t, json.Unmarshal(output.Bytes(), &status))
	require.Equal(t, protocol.MetadataAvailabilityUnavailable,
		status.Deployment.BaseRevision.Availability)
	require.Nil(t, status.ActiveProfile)

	cancel()
	require.NoError(t, <-done)
}

func TestCtlCallHonorsCancellation(t *testing.T) {
	base := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", base)
	runtimeDir := filepath.Join(base, "gifford")
	require.NoError(t, os.Mkdir(runtimeDir, 0o700))
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "unix", filepath.Join(runtimeDir, "daemon.sock"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	received := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseServer := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseServer)
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_, _ = bufio.NewReader(conn).ReadBytes('\n')
		close(received)
		<-release
	}()

	ctx, cancel := context.WithCancel(t.Context())
	cmd := NewCmd(&deps.Deps{})
	cmd.SetContext(ctx)
	done := make(chan error, 1)
	go func() {
		var result protocol.ChannelState
		done <- call(cmd, protocol.ControlMethodChannelGetGlobal, protocol.EmptyParams{}, &result)
	}()
	select {
	case <-received:
	case <-time.After(2 * time.Second):
		t.Fatal("fake control server did not receive the request")
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("ctl call ignored context cancellation")
	}
	releaseServer()
	<-serverDone
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

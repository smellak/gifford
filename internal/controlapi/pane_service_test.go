package controlapi

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

const (
	paneServiceTestHost   = "node-1"
	paneServiceTestBootID = "12345678-1234-1234-1234-123456789abc"
	paneServiceTestSocket = "/run/tmux/default"
)

type paneServiceCall struct {
	method    protocol.ControlMethod
	operation protocol.PaneOperationParams
	attempt   protocol.PaneAttemptParams
	actor     PaneOperationActor
}

type recordingPaneService struct {
	calls   []paneServiceCall
	takeErr error
}

func (s *recordingPaneService) Provision(
	_ context.Context,
	params protocol.PaneOperationParams,
	actor PaneOperationActor,
) (protocol.PaneOperationResult, error) {
	s.calls = append(s.calls, paneServiceCall{
		method: protocol.ControlMethodPaneProvision, operation: params, actor: actor,
	})
	return protocol.PaneOperationResult{Changed: true}, nil
}

func (s *recordingPaneService) Take(
	_ context.Context,
	params protocol.PaneOperationParams,
	actor PaneOperationActor,
) (protocol.PaneOperationResult, error) {
	s.calls = append(s.calls, paneServiceCall{
		method: protocol.ControlMethodPaneTake, operation: params, actor: actor,
	})
	if s.takeErr != nil {
		return protocol.PaneOperationResult{}, s.takeErr
	}
	return protocol.PaneOperationResult{Changed: true}, nil
}

func (s *recordingPaneService) Return(
	_ context.Context,
	params protocol.PaneOperationParams,
	actor PaneOperationActor,
) (protocol.PaneOperationResult, error) {
	s.calls = append(s.calls, paneServiceCall{
		method: protocol.ControlMethodPaneReturn, operation: params, actor: actor,
	})
	return protocol.PaneOperationResult{Changed: true}, nil
}

func (s *recordingPaneService) Reconcile(
	_ context.Context,
	params protocol.PaneAttemptParams,
	actor PaneOperationActor,
) (protocol.PaneOperationResult, error) {
	s.calls = append(s.calls, paneServiceCall{
		method: protocol.ControlMethodPaneReconcile, attempt: params, actor: actor,
	})
	return protocol.PaneOperationResult{Changed: true}, nil
}

func (s *recordingPaneService) Repair(
	_ context.Context,
	params protocol.PaneOperationParams,
	actor PaneOperationActor,
) (protocol.PaneOperationResult, error) {
	s.calls = append(s.calls, paneServiceCall{
		method: protocol.ControlMethodPaneRepair, operation: params, actor: actor,
	})
	return protocol.PaneOperationResult{Changed: true}, nil
}

func TestPaneServicesAuthorizeUIDAndReceiveTrustedActor(t *testing.T) {
	t.Parallel()

	server, _ := testServer(t, filepath.Join(t.TempDir(), "runtime"))
	loads := 0
	authorizer, err := newPaneOperationAuthorizer("/etc/example/pane-admin-uids",
		func(string) ([]uint32, error) {
			loads++
			return []uint32{42}, nil
		})
	require.NoError(t, err)
	service := &recordingPaneService{}
	var telemetry bytes.Buffer
	server.paneAuth = authorizer
	server.paneCreate = service
	server.paneOwner = service
	server.paneRepair = service
	server.paneLog = NewPaneOperationTelemetry(slog.New(slog.NewTextHandler(&telemetry, nil)))

	target := protocol.PaneOperationTarget{
		OwnerUID: 1000, Socket: paneServiceTestSocket, PaneID: "%7", Host: paneServiceTestHost,
		BootID: paneServiceTestBootID, PanePID: 2345, PaneStart: 987654,
	}
	operation := protocol.PaneOperationParams{Target: target}
	attempt := protocol.PaneAttemptParams{
		Target: target, ExpectedAttempt: "0123456789abcdef0123456789abcdef",
	}
	requests := []struct {
		method protocol.ControlMethod
		params any
	}{
		{method: protocol.ControlMethodPaneProvision, params: operation},
		{method: protocol.ControlMethodPaneTake, params: operation},
		{method: protocol.ControlMethodPaneReturn, params: operation},
		{method: protocol.ControlMethodPaneReconcile, params: attempt},
		{method: protocol.ControlMethodPaneRepair, params: operation},
	}
	peer := Peer{PID: os.Getpid(), UID: 42, GID: 7, Verified: true}
	for _, request := range requests {
		callsBefore := len(service.calls)
		denied := peer
		denied.UID = 41
		response := server.Handle(t.Context(), denied, rpcRequest(t, request.method, request.params))
		require.NotNil(t, response.Error)
		require.Equal(t, int64(rpcDomainError), response.Error.Code)
		require.Equal(t, protocol.ErrorCodePolicyDenied, response.Error.Data.ErrorCode)
		require.Len(t, service.calls, callsBefore, "denied UID reached pane backend")

		response = server.Handle(t.Context(), peer, rpcRequest(t, request.method, request.params))
		require.Nil(t, response.Error)
		require.JSONEq(t, `{"changed":true}`, string(response.Result))
		require.Len(t, service.calls, callsBefore+1)
	}
	require.Equal(t, len(requests)*2, loads, "UID policy must be reloaded for every call")
	require.Len(t, service.calls, len(requests))
	for index, request := range requests {
		call := service.calls[index]
		require.Equal(t, request.method, call.method)
		require.Equal(t, peer.paneActor(), call.actor)
		if request.method == protocol.ControlMethodPaneReconcile {
			require.Equal(t, attempt, call.attempt)
		} else {
			require.Equal(t, operation, call.operation)
		}
	}
	require.Equal(t, len(requests), strings.Count(telemetry.String(), "event=pane_admin_operation"))
	require.Equal(t, len(requests), strings.Count(telemetry.String(), "result=denied"))
	require.NotContains(t, telemetry.String(), target.Socket)
}

func TestPaneServicesFailClosedWithoutAuthorizer(t *testing.T) {
	t.Parallel()

	server, _ := testServer(t, filepath.Join(t.TempDir(), "runtime"))
	service := &recordingPaneService{}
	server.paneOwner = service
	params := protocol.PaneOperationParams{Target: protocol.PaneOperationTarget{
		OwnerUID: 1000, Socket: paneServiceTestSocket, PaneID: "%7", Host: paneServiceTestHost,
		BootID: paneServiceTestBootID, PanePID: 2345, PaneStart: 987654,
	}}
	peer := Peer{PID: os.Getpid(), UID: uint32(os.Getuid()), GID: uint32(os.Getgid()), Verified: true}
	response := server.Handle(t.Context(), peer,
		rpcRequest(t, protocol.ControlMethodPaneTake, params))
	require.NotNil(t, response.Error)
	require.Equal(t, protocol.ErrorCodePolicyDenied, response.Error.Data.ErrorCode)
	require.Empty(t, service.calls)
}

func TestPaneTakeReportsUnpublishedPanel(t *testing.T) {
	t.Parallel()

	server, _ := testServer(t, filepath.Join(t.TempDir(), "runtime"))
	authorizer, err := newPaneOperationAuthorizer("/etc/example/pane-admin-uids",
		func(string) ([]uint32, error) { return []uint32{42}, nil })
	require.NoError(t, err)
	service := &recordingPaneService{takeErr: ErrPaneNotPublished}
	server.paneAuth = authorizer
	server.paneOwner = service
	params := protocol.PaneOperationParams{Target: protocol.PaneOperationTarget{
		OwnerUID: 1000, Socket: paneServiceTestSocket, PaneID: "%7", Host: paneServiceTestHost,
		BootID: paneServiceTestBootID, PanePID: 2345, PaneStart: 987654,
	}}
	peer := Peer{PID: os.Getpid(), UID: 42, GID: 7, Verified: true}

	response := server.Handle(t.Context(), peer,
		rpcRequest(t, protocol.ControlMethodPaneTake, params))
	require.NoError(t, response.Validate())
	require.NotNil(t, response.Error)
	require.Equal(t, int64(rpcDomainError), response.Error.Code)
	require.Equal(t, paneNotPublishedMessage, response.Error.Message)
	require.Equal(t, protocol.ErrorCodePaneNotPublished, response.Error.Data.ErrorCode)
	require.Len(t, service.calls, 1)
}

func TestPaneServicesLogDeniedUnverifiedPeerBeforePolicy(t *testing.T) {
	t.Parallel()

	server, _ := testServer(t, filepath.Join(t.TempDir(), "runtime"))
	loads := 0
	authorizer, err := newPaneOperationAuthorizer("/etc/example/pane-admin-uids",
		func(string) ([]uint32, error) {
			loads++
			return []uint32{42}, nil
		})
	require.NoError(t, err)
	service := &recordingPaneService{}
	var telemetry bytes.Buffer
	server.paneAuth = authorizer
	server.paneOwner = service
	server.paneLog = NewPaneOperationTelemetry(slog.New(slog.NewTextHandler(&telemetry, nil)))
	params := protocol.PaneOperationParams{Target: protocol.PaneOperationTarget{
		OwnerUID: 1000, Socket: paneServiceTestSocket, PaneID: "%7", Host: paneServiceTestHost,
		BootID: paneServiceTestBootID, PanePID: 2345, PaneStart: 987654,
	}}

	response := server.Handle(t.Context(), Peer{PID: 700, UID: 42},
		rpcRequest(t, protocol.ControlMethodPaneTake, params))
	require.NotNil(t, response.Error)
	require.Equal(t, protocol.ErrorCodePolicyDenied, response.Error.Data.ErrorCode)
	require.Zero(t, loads)
	require.Empty(t, service.calls)
	require.Contains(t, telemetry.String(), "result=denied")
	require.Contains(t, telemetry.String(), "actor.uid=42")
	require.Contains(t, telemetry.String(), "actor.pid=700")
}

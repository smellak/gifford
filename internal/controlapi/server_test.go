package controlapi

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/orchestrator"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

const (
	controlTestNodeA = "node-a"
	controlTestNodeB = "node-b"
	controlTestOrgB  = "org-b"

	controlTestApprovalDeliverNow = "deliver_now"
	controlTestApprovalReject     = "reject"
	controlTestLoopID             = "loop_01JB0000000000000000000003"
)

func testServer(t *testing.T, runtimeDir string) (*Server, *eventstore.Store) {
	t.Helper()
	store, err := eventstore.Open(t.Context(), filepath.Join(t.TempDir(), "gifford.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	engine, err := channel.New(channel.Options{Clock: clock.System{}})
	require.NoError(t, err)
	controller, err := channel.NewController(channel.ControllerOptions{DB: store.DB(), Engine: engine, Clock: clock.System{}, NodeID: controlTestNodeA, Entropy: bytes.NewReader(make([]byte, 1024))})
	require.NoError(t, err)
	require.NoError(t, controller.Reload(t.Context()))
	server, err := New(Options{RuntimeDir: runtimeDir, DB: store.DB(), Controller: controller})
	require.NoError(t, err)
	return server, store
}

func rpcRequest(t *testing.T, method protocol.ControlMethod, params any) []byte {
	t.Helper()
	raw, err := json.Marshal(params)
	require.NoError(t, err)
	request := protocol.RPCRequest{JSONRPC: protocol.JSONRPCVersion, ID: 1, Method: method, Params: raw}
	raw, err = json.Marshal(request)
	require.NoError(t, err)
	return raw
}

func TestServerCASConflictAndUnsupported(t *testing.T) {
	t.Parallel()
	server, _ := testServer(t, filepath.Join(t.TempDir(), "runtime"))
	peer := Peer{PID: os.Getpid(), UID: uint32(os.Getuid()), GID: uint32(os.Getgid()), Verified: true}
	set := protocol.SetGlobalParams{Mode: protocol.ChannelModePaused, ExpectedVersion: 0}
	first := server.Handle(t.Context(), peer, rpcRequest(t, protocol.ControlMethodChannelSetGlobal, set))
	require.Nil(t, first.Error)
	stale := server.Handle(t.Context(), peer, rpcRequest(t, protocol.ControlMethodChannelSetGlobal, set))
	require.Equal(t, int64(rpcDomainError), stale.Error.Code)
	require.Equal(t, protocol.ErrorCodeControlVersionConflict, stale.Error.Data.ErrorCode)
	require.Equal(t, int64(1), *stale.Error.Data.CurrentVersion)

	notFound := server.Handle(t.Context(), peer, rpcRequest(t, protocol.ControlMethodLoopGet, protocol.LoopParams{LoopID: controlTestLoopID}))
	require.Equal(t, int64(rpcDomainError), notFound.Error.Code)
	require.Equal(t, protocol.ErrorCodeNotFound, notFound.Error.Data.ErrorCode)

	unsupported := server.Handle(t.Context(), peer, rpcRequest(t, protocol.ControlMethodLoopAdvance, protocol.AdvanceLoopParams{
		LoopID: controlTestLoopID, Steps: 1, ExpectedVersion: 1,
	}))
	require.Equal(t, int64(rpcInternalError), unsupported.Error.Code)
	require.Nil(t, unsupported.Error.Data)
}

func TestServerUnverifiedPeerIsReadOnly(t *testing.T) {
	t.Parallel()
	server, _ := testServer(t, filepath.Join(t.TempDir(), "runtime"))
	peer := Peer{}
	read := server.Handle(t.Context(), peer, rpcRequest(t, protocol.ControlMethodChannelGetGlobal, protocol.EmptyParams{}))
	require.Nil(t, read.Error)
	mutation := server.Handle(t.Context(), peer, rpcRequest(t, protocol.ControlMethodChannelSetGlobal, protocol.SetGlobalParams{Mode: protocol.ChannelModePaused, ExpectedVersion: 0}))
	require.Equal(t, int64(rpcDomainError), mutation.Error.Code)
	require.Equal(t, protocol.ErrorCodePolicyDenied, mutation.Error.Data.ErrorCode)
}

type fakeNodeService struct {
	calls []protocol.ControlMethod
}

type fakeModelService struct {
	calls           int
	actor           channel.Actor
	pipelineCalls   int
	pipelineRequest protocol.RegisterModelPipelineRequest
	pipelineActor   channel.Actor
}

func (f *fakeModelService) Register(
	context.Context,
	protocol.RegisterModelMetadataRequest,
	channel.Actor,
) (protocol.ModelMetadataRegistrationResult, error) {
	return protocol.ModelMetadataRegistrationResult{}, nil
}

func (f *fakeModelService) RegisterPipeline(
	_ context.Context,
	request protocol.RegisterModelPipelineRequest,
	actor channel.Actor,
) (protocol.ModelMetadataRegistrationResult, error) {
	f.pipelineCalls++
	f.pipelineRequest = request
	f.pipelineActor = actor
	return protocol.ModelMetadataRegistrationResult{}, nil
}

func (f *fakeModelService) Activate(
	context.Context,
	protocol.ActivateAdapterRequest,
	channel.Actor,
) (protocol.ModelActivationResult, error) {
	return protocol.ModelActivationResult{}, nil
}

func (f *fakeModelService) Rollback(
	_ context.Context,
	_ protocol.RollbackToBaseRequest,
	actor channel.Actor,
) (protocol.ModelActivationResult, error) {
	f.calls++
	f.actor = actor
	return protocol.ModelActivationResult{}, nil
}

func TestModelLifecycleIsOperatorOnlyOnLocalControlAPI(t *testing.T) {
	t.Parallel()
	server, _ := testServer(t, filepath.Join(t.TempDir(), "runtime"))
	service := &fakeModelService{}
	server.model = service
	params := protocol.RollbackToBaseRequest{Contract: protocol.ModelContractAnalyzeAgentState}

	blocked := server.Handle(t.Context(), Peer{}, rpcRequest(t, protocol.ControlMethodModelRollback, params))
	require.Equal(t, int64(rpcDomainError), blocked.Error.Code)
	require.Equal(t, protocol.ErrorCodePolicyDenied, blocked.Error.Data.ErrorCode)
	require.Zero(t, service.calls)

	peer := Peer{PID: os.Getpid(), UID: uint32(os.Getuid()), GID: uint32(os.Getgid()), Verified: true}
	allowed := server.Handle(t.Context(), peer, rpcRequest(t, protocol.ControlMethodModelRollback, params))
	require.Nil(t, allowed.Error)
	require.Equal(t, 1, service.calls)
	require.Equal(t, peer.actor(), service.actor)
}

func TestModelPipelineUsesTypedLocalControlDispatch(t *testing.T) {
	t.Parallel()
	server, _ := testServer(t, filepath.Join(t.TempDir(), "runtime"))
	service := &fakeModelService{}
	server.model = service
	reference := protocol.ModelEvidenceReference{
		Path:   "/var/lib/gifford/evidence.json",
		SHA256: protocol.SHA256Hex(strings.Repeat("a", 64)),
	}
	params := protocol.RegisterModelPipelineRequest{
		EvaluationRunID: "evaluation-1", Manifest: reference,
		TrainingRunMetadata: reference, AdapterMetadata: reference,
		BasePredictions: reference, CandidatePredictions: reference,
		SafetyManifest: reference, SafetyBasePredictions: reference,
		SafetyCandidatePredictions: reference,
	}
	peer := Peer{PID: os.Getpid(), UID: uint32(os.Getuid()), GID: uint32(os.Getgid()), Verified: true}

	response := server.Handle(t.Context(), peer,
		rpcRequest(t, protocol.ControlMethodModelRegisterPipeline, params))
	require.Nil(t, response.Error)
	require.Equal(t, 1, service.pipelineCalls)
	require.Equal(t, params, service.pipelineRequest)
	require.Equal(t, peer.actor(), service.pipelineActor)
}

type fakeApprovalService struct {
	request protocol.ApprovalRequest
	params  protocol.ResolveApprovalParams
	actor   channel.Actor
	err     error
	calls   int
}

func (f *fakeApprovalService) ResolveApproval(
	_ context.Context,
	params protocol.ResolveApprovalParams,
	actor channel.Actor,
) (protocol.ApprovalRequest, error) {
	f.calls++
	f.params = params
	f.actor = actor
	if f.err != nil {
		return protocol.ApprovalRequest{}, f.err
	}
	f.request.State = protocol.ApprovalStateApproved
	if params.Option == controlTestApprovalReject {
		f.request.State = protocol.ApprovalStateRejected
	}
	f.request.Resolution = &protocol.ApprovalResolution{Option: params.Option, Reason: params.Reason}
	f.request.ResolvedBy = actor.ID
	f.request.ResolvedAt = f.request.CreatedAt.Add(time.Second)
	return f.request, nil
}

func TestServerDispatchesOrchestratorApprovalMethodsOnly(t *testing.T) {
	t.Parallel()

	server, _ := testServer(t, filepath.Join(t.TempDir(), "runtime"))
	tx, err := server.db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	request, _, err := server.controller.EnsureOrchestratorApproval(t.Context(), tx,
		"01ARZ3NDEKTSV4RRFFQ69G5FAV", "agent-approval", "automatic delivery deferred")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	listed := server.Handle(t.Context(), Peer{}, rpcRequest(t,
		protocol.ControlMethodApprovalList,
		protocol.ListApprovalsParams{State: protocol.ApprovalStatePending, AgentID: "agent-approval"}))
	require.Nil(t, listed.Error)
	var list protocol.ApprovalList
	require.NoError(t, json.Unmarshal(listed.Result, &list))
	require.Equal(t, []protocol.ApprovalRequest{request}, list.Approvals)
	got := server.Handle(t.Context(), Peer{}, rpcRequest(t,
		protocol.ControlMethodApprovalGet, protocol.ApprovalParams{ApprovalID: request.ApprovalID}))
	require.Nil(t, got.Error)
	var fetched protocol.ApprovalRequest
	require.NoError(t, json.Unmarshal(got.Result, &fetched))
	require.Equal(t, request, fetched)

	service := &fakeApprovalService{request: request}
	server.approvals = service
	peer := Peer{PID: os.Getpid(), UID: uint32(os.Getuid()), GID: uint32(os.Getgid()), Verified: true}
	params := protocol.ResolveApprovalParams{
		ApprovalID: request.ApprovalID, Option: controlTestApprovalDeliverNow, Reason: "operator requested delivery",
	}
	resolved := server.Handle(t.Context(), peer, rpcRequest(t, protocol.ControlMethodApprovalResolve, params))
	require.Nil(t, resolved.Error)
	require.Equal(t, params, service.params)
	require.Equal(t, peer.actor(), service.actor)
	require.Equal(t, 1, service.calls)

	messageApprove := server.Handle(t.Context(), peer, rpcRequest(t,
		protocol.ControlMethodMessageApprove,
		protocol.ApproveMessageParams{MessageID: "01ARZ3NDEKTSV4RRFFQ69G5FAV"}))
	require.NotNil(t, messageApprove.Error)
	require.Equal(t, int64(rpcInternalError), messageApprove.Error.Code)
}

func TestApprovalResolveRejectsUnsupportedSourceBeforeService(t *testing.T) {
	t.Parallel()

	service := &fakeApprovalService{}
	server := &Server{approvals: service}
	request := protocol.ApprovalRequest{
		ApprovalID:  "apr_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Source:      protocol.ApprovalSourceLink,
		ObjectType:  protocol.ApprovalObjectTypeMessage,
		ObjectID:    "01ARZ3NDEKTSV4RRFFQ69G5FAV",
		AgentID:     "agent-approval",
		Options:     []protocol.ApprovalOption{"approve_once", controlTestApprovalReject},
		RequestNote: "synthetic non-orchestrator approval",
		State:       protocol.ApprovalStatePending,
		CreatedAt:   time.Now().UTC(),
	}
	require.NoError(t, request.Validate())
	_, err := server.resolveReadApproval(t.Context(), request,
		protocol.ResolveApprovalParams{ApprovalID: request.ApprovalID, Option: controlTestApprovalReject},
		channel.Actor{Type: protocol.ActorTypeHuman, ID: "operator"})
	require.ErrorIs(t, err, errUnsupported)
	require.Zero(t, service.calls)
}

func TestApprovalResolvePreservesNotFoundBeforeService(t *testing.T) {
	t.Parallel()

	server, _ := testServer(t, filepath.Join(t.TempDir(), "runtime"))
	service := &fakeApprovalService{}
	server.approvals = service
	peer := Peer{PID: os.Getpid(), UID: uint32(os.Getuid()), GID: uint32(os.Getgid()), Verified: true}
	response := server.Handle(t.Context(), peer, rpcRequest(t,
		protocol.ControlMethodApprovalResolve,
		protocol.ResolveApprovalParams{
			ApprovalID: "apr_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			Option:     controlTestApprovalDeliverNow,
		}))
	require.Equal(t, int64(rpcDomainError), response.Error.Code)
	require.Equal(t, "Not found", response.Error.Message)
	require.Equal(t, protocol.ErrorCodeNotFound, response.Error.Data.ErrorCode)
	require.Zero(t, service.calls)
}

func TestApprovalResolveMapsWrappedDeliveryDeferredWithoutGuardDetail(t *testing.T) {
	t.Parallel()

	server, _ := testServer(t, filepath.Join(t.TempDir(), "runtime"))
	tx, err := server.db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	request, _, err := server.controller.EnsureOrchestratorApproval(t.Context(), tx,
		"01ARZ3NDEKTSV4RRFFQ69G5FAV", "agent-approval", "automatic delivery deferred")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	const guardDetail = "private guard detail must not cross the control socket"
	deferred := fmt.Errorf("resolve approval: %w",
		fmt.Errorf("%w: %s", orchestrator.ErrDeliveryDeferred, guardDetail))
	service := &fakeApprovalService{err: deferred}
	server.approvals = service
	peer := Peer{PID: os.Getpid(), UID: uint32(os.Getuid()), GID: uint32(os.Getgid()), Verified: true}
	response := server.Handle(t.Context(), peer, rpcRequest(t,
		protocol.ControlMethodApprovalResolve,
		protocol.ResolveApprovalParams{ApprovalID: request.ApprovalID, Option: controlTestApprovalDeliverNow}))
	require.Equal(t, int64(rpcDomainError), response.Error.Code)
	require.Equal(t, "Delivery deferred", response.Error.Message)
	require.Equal(t, protocol.ErrorCodeRecipientNotDeliverable, response.Error.Data.ErrorCode)
	require.Equal(t, deliveryDeferredDetail, response.Error.Data.Detail)
	require.Equal(t, 1, service.calls)

	raw, err := json.Marshal(response)
	require.NoError(t, err)
	require.NotContains(t, string(raw), guardDetail)
	stored, err := server.controller.Approval(t.Context(), request.ApprovalID)
	require.NoError(t, err)
	require.Equal(t, protocol.ApprovalStatePending, stored.State)
}

func (f *fakeNodeService) EnrollToken(
	_ context.Context,
	_ protocol.NodeEnrollTokenParams,
) (protocol.NodeEnrollTokenResult, error) {
	f.calls = append(f.calls, protocol.ControlMethodNodeEnrollToken)
	return protocol.NodeEnrollTokenResult{Token: "token", NodeID: controlTestNodeA}, nil
}

func (f *fakeNodeService) Join(
	_ context.Context,
	_ protocol.NodeJoinParams,
) (protocol.NodeJoinResult, error) {
	f.calls = append(f.calls, protocol.ControlMethodNodeJoin)
	return protocol.NodeJoinResult{Peer: protocol.NodePeer{
		NodeID: controlTestNodeB, OrganizationID: controlTestOrgB, Status: "TRUSTED",
		PolicyVersion: "policy-v1:000000000000",
	}}, nil
}

func (f *fakeNodeService) Peers(context.Context) (protocol.NodePeersResult, error) {
	f.calls = append(f.calls, protocol.ControlMethodNodePeers)
	return protocol.NodePeersResult{}, nil
}

func (f *fakeNodeService) Revoke(
	_ context.Context,
	_ protocol.NodeRevokeParams,
) (protocol.NodeRevokeResult, error) {
	f.calls = append(f.calls, protocol.ControlMethodNodeRevoke)
	return protocol.NodeRevokeResult{Peer: protocol.NodePeer{
		NodeID: controlTestNodeB, OrganizationID: controlTestOrgB, Status: "REVOKED",
		PolicyVersion: "policy-v1:000000000000",
	}}, nil
}

func TestServerDispatchesNodeOperationsInsideHub(t *testing.T) {
	t.Parallel()
	server, _ := testServer(t, filepath.Join(t.TempDir(), "runtime"))
	service := &fakeNodeService{}
	server.node = service
	verified := Peer{PID: os.Getpid(), UID: uint32(os.Getuid()), GID: uint32(os.Getgid()), Verified: true}
	fingerprint := protocol.SHA256Hex("abababababababababababababababababababababababababababababababab")

	requests := []struct {
		method protocol.ControlMethod
		params any
		peer   Peer
	}{
		{protocol.ControlMethodNodeEnrollToken, protocol.NodeEnrollTokenParams{}, verified},
		{protocol.ControlMethodNodeJoin, protocol.NodeJoinParams{URL: "https://node-a.example.invalid", Token: "token", Fingerprint: fingerprint}, verified},
		{protocol.ControlMethodNodePeers, protocol.EmptyParams{}, Peer{}},
		{protocol.ControlMethodNodeRevoke, protocol.NodeRevokeParams{NodeID: controlTestNodeB}, verified},
	}
	for _, request := range requests {
		response := server.Handle(t.Context(), request.peer, rpcRequest(t, request.method, request.params))
		require.Nil(t, response.Error, request.method)
	}
	require.Equal(t, []protocol.ControlMethod{
		protocol.ControlMethodNodeEnrollToken, protocol.ControlMethodNodeJoin,
		protocol.ControlMethodNodePeers, protocol.ControlMethodNodeRevoke,
	}, service.calls)

	blocked := server.Handle(t.Context(), Peer{}, rpcRequest(t,
		protocol.ControlMethodNodeEnrollToken, protocol.NodeEnrollTokenParams{}))
	require.Equal(t, int64(rpcDomainError), blocked.Error.Code)
	require.Equal(t, protocol.ErrorCodePolicyDenied, blocked.Error.Data.ErrorCode)
	require.Len(t, service.calls, 4)
}

func TestNodeStateErrorsAreStable(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		err     error
		message string
	}{
		{registry.ErrNodeUnenrolled, "Node is not enrolled"},
		{registry.ErrNodeAlreadyEnrolled, "Node is already enrolled"},
		{registry.ErrFederationUnavailable, "Federation is not configured"},
	} {
		response := mapError(7, fmt.Errorf("node operation: %w", test.err))
		require.Equal(t, int64(rpcDomainError), response.Error.Code)
		require.Equal(t, test.message, response.Error.Message)
		require.Equal(t, protocol.ErrorCodeNodeOff, response.Error.Data.ErrorCode)
	}
}

func TestServerRejectsRegisteredAgentMutationButAllowsRead(t *testing.T) {
	t.Parallel()
	server, store := testServer(t, filepath.Join(t.TempDir(), "runtime"))
	process, err := registry.CurrentProcess()
	require.NoError(t, err)
	processJSON, err := json.Marshal(process)
	require.NoError(t, err)
	now := eventstore.FormatTime(time.Now().UTC())
	_, err = store.DB().ExecContext(t.Context(), `INSERT INTO agents (agent_id,node_id,harness,created_at,updated_at) VALUES (?,?,?,?,?)`, "agent-1", controlTestNodeA, "codex", now, now)
	require.NoError(t, err)
	_, err = store.DB().ExecContext(t.Context(), `INSERT INTO sessions (session_id,agent_id,state,capture_mode,deliverable,manifest_json,process_json,started_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?)`, "session-1", "agent-1", "CONNECTED", "pty_proxy", 1, `{}`, string(processJSON), now, now)
	require.NoError(t, err)
	peer := Peer{PID: os.Getpid(), UID: uint32(os.Getuid()), GID: uint32(os.Getgid()), Verified: true}

	read := server.Handle(t.Context(), peer, rpcRequest(t, protocol.ControlMethodChannelGetGlobal, protocol.EmptyParams{}))
	require.Nil(t, read.Error)
	mutation := server.Handle(t.Context(), peer, rpcRequest(t, protocol.ControlMethodChannelSetGlobal, protocol.SetGlobalParams{Mode: protocol.ChannelModeOff, ExpectedVersion: 0}))
	require.Equal(t, int64(rpcDomainError), mutation.Error.Code)
	require.Equal(t, protocol.ErrorCodePolicyDenied, mutation.Error.Data.ErrorCode)
}

func TestServerSocketModesAndNewlineFraming(t *testing.T) {
	t.Parallel()
	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	server, _ := testServer(t, runtimeDir)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(server.Path()); err == nil {
			break
		}
		require.True(t, time.Now().Before(deadline), "control socket was not created")
		time.Sleep(5 * time.Millisecond)
	}
	dirInfo, err := os.Stat(runtimeDir)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o700), dirInfo.Mode().Perm())
	socketInfo, err := os.Stat(server.Path())
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), socketInfo.Mode().Perm())
	conn, err := (&net.Dialer{}).DialContext(t.Context(), "unix", server.Path())
	require.NoError(t, err)
	request := append(rpcRequest(t, protocol.ControlMethodChannelGetGlobal, protocol.EmptyParams{}), '\n')
	_, err = conn.Write(request)
	require.NoError(t, err)
	line, err := bufio.NewReader(conn).ReadBytes('\n')
	require.NoError(t, err)
	require.NotEmpty(t, line)
	require.NoError(t, conn.Close())
	cancel()
	require.NoError(t, <-done)
}

func TestServerRefusesExistingSocketPath(t *testing.T) {
	t.Parallel()
	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	require.NoError(t, os.MkdirAll(runtimeDir, 0o700))
	path := filepath.Join(runtimeDir, "daemon.sock")
	require.NoError(t, os.WriteFile(path, []byte("keep"), 0o600))
	server, _ := testServer(t, runtimeDir)

	err := server.Serve(t.Context())
	require.ErrorContains(t, err, "socket path already exists")
	raw, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	require.Equal(t, "keep", string(raw))
}

func TestServerCancellationClosesIdleConnection(t *testing.T) {
	t.Parallel()
	runtimeDir := filepath.Join(t.TempDir(), "runtime")
	server, _ := testServer(t, runtimeDir)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(server.Path()); err == nil {
			break
		}
		require.True(t, time.Now().Before(deadline), "control socket was not created")
		time.Sleep(5 * time.Millisecond)
	}
	conn, err := (&net.Dialer{}).DialContext(t.Context(), "unix", server.Path())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	request := append(rpcRequest(t, protocol.ControlMethodChannelGetGlobal, protocol.EmptyParams{}), '\n')
	_, err = conn.Write(request)
	require.NoError(t, err)
	_, err = bufio.NewReader(conn).ReadBytes('\n')
	require.NoError(t, err)

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("Serve did not close an accepted idle connection")
	}
}

func TestServerExplainsStoredMessageDecision(t *testing.T) {
	t.Parallel()
	server, store := testServer(t, filepath.Join(t.TempDir(), "runtime"))
	actor := channel.Actor{Type: protocol.ActorTypeHuman, ID: "operator"}
	_, err := server.controller.SetGlobal(t.Context(), protocol.SetGlobalParams{Mode: protocol.ChannelModeOff, ExpectedVersion: 0}, actor)
	require.NoError(t, err)
	gate, err := channel.New(channel.Options{Clock: clock.System{}})
	require.NoError(t, err)
	tx, err := store.DB().BeginTx(t.Context(), nil)
	require.NoError(t, err)
	messageID := protocol.ULID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	message := channel.Message{
		ID: messageID, From: "agent-1", FromNode: controlTestNodeA, ToNode: controlTestNodeB,
		To:       []channel.Recipient{{Agent: "agent-2", Deliverable: true, Policy: channel.PolicyAllowed}},
		ThreadID: "thread-1",
	}
	_, _, err = gate.Admit(t.Context(), tx, message, func(context.Context, protocol.AdmissionResult) error { return nil })
	require.ErrorIs(t, err, channel.ErrDenied)
	require.NoError(t, tx.Commit())

	peer := Peer{}
	response := server.Handle(t.Context(), peer, rpcRequest(t, protocol.ControlMethodChannelExplain, protocol.ExplainParams{MessageID: messageID}))
	require.Nil(t, response.Error)
	var explained protocol.Explanation
	require.NoError(t, json.Unmarshal(response.Result, &explained))
	require.Equal(t, protocol.DecisionObjectTypeMessage, explained.ObjectType)
	require.Equal(t, protocol.OpaqueID(messageID), explained.ObjectID)
}

func TestServerReportsLoopExplanationUnavailable(t *testing.T) {
	t.Parallel()
	server, _ := testServer(t, filepath.Join(t.TempDir(), "runtime"))
	response := server.Handle(t.Context(), Peer{}, rpcRequest(t, protocol.ControlMethodChannelExplain, protocol.ExplainParams{
		FromAgent: "agent-1", ToAgent: "agent-2", Plane: protocol.PlaneMessage,
		LoopID: controlTestLoopID,
	}))
	require.NotNil(t, response.Error)
	require.Equal(t, int64(rpcInternalError), response.Error.Code)
	require.Equal(t, "Loop explanation unavailable", response.Error.Message)
}

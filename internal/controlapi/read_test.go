package controlapi

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/channel"
	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/eventstore"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/registry"
)

var readTestTime = time.Date(2026, time.October, 30, 19, 0, 0, 0, time.UTC)

const (
	readTestAgent        protocol.AgentID           = "agent-read"
	readTestSender       protocol.AgentID           = "agent-sender"
	readTestNode         protocol.NodeID            = "node-read"
	readTestSession      protocol.OpaqueID          = "session-read"
	readTestMessage      protocol.ULID              = "01JB0000000000000000000011"
	readTestChannel      protocol.ChannelDecisionID = "chd_01JB0000000000000000000022"
	readTestOrchestrator protocol.ULID              = "01JB0000000000000000000033"
	readTestOperator                                = "operator"
)

type fixedReadClock struct{ now time.Time }

func (c fixedReadClock) Now() time.Time { return c.now }

type readFixture struct {
	server          *Server
	store           *eventstore.Store
	engine          *channel.Engine
	controller      *channel.Controller
	readDB          *sql.DB
	channelDecision protocol.ChannelDecisionID
}

func newReadFixture(t *testing.T, queryOnly bool) *readFixture {
	t.Helper()
	path := filepath.Join(t.TempDir(), "gifford.db")
	store, err := eventstore.Open(t.Context(), path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	engine, err := channel.New(channel.Options{Clock: fixedReadClock{now: readTestTime}})
	require.NoError(t, err)
	controller, err := channel.NewController(channel.ControllerOptions{
		DB: store.DB(), Engine: engine, Clock: fixedReadClock{now: readTestTime},
		NodeID: readTestNode, Entropy: bytes.NewReader(make([]byte, 4096)),
	})
	require.NoError(t, err)
	require.NoError(t, controller.Reload(t.Context()))
	dsn := path + "?_pragma=busy_timeout(5000)"
	if queryOnly {
		dsn += "&_pragma=query_only(1)"
	}
	readDB, err := sql.Open("sqlite", dsn)
	require.NoError(t, err)
	readDB.SetMaxOpenConns(4)
	require.NoError(t, readDB.PingContext(t.Context()))
	t.Cleanup(func() { require.NoError(t, readDB.Close()) })
	server, err := New(Options{
		RuntimeDir: filepath.Join(t.TempDir(), "runtime"), DB: store.DB(),
		ReadDB: readDB, Controller: controller,
	})
	require.NoError(t, err)
	return &readFixture{
		server: server, store: store, engine: engine, controller: controller, readDB: readDB,
		channelDecision: readTestChannel,
	}
}

func readPeer() Peer {
	return Peer{PID: os.Getpid(), UID: uint32(os.Getuid()), GID: uint32(os.Getgid()), Verified: true}
}

func readResult[T any](t *testing.T, server *Server, method protocol.ControlMethod, params any) T {
	t.Helper()
	response := server.Handle(t.Context(), readPeer(), rpcRequest(t, method, params))
	require.Nil(t, response.Error, "%s: %+v", method, response.Error)
	var result T
	require.NoError(t, json.Unmarshal(response.Result, &result))
	return result
}

func seedReadRows(t *testing.T, fixture *readFixture) {
	t.Helper()
	manifest := protocol.CapabilityManifest{
		SchemaVersion: protocol.SchemaVersion, AgentID: readTestAgent,
		Harness: protocol.HarnessCodex, HarnessVersion: "1.2.3",
		AdapterType:        protocol.AdapterTypeGenericPTY,
		InputGuardProvider: protocol.InputGuardProviderPTYProxy,
		CaptureMode:        protocol.CaptureModePTYProxy, Deliverable: true,
		Capabilities: protocol.Capabilities{PTYCapture: true},
	}
	require.NoError(t, manifest.Validate())
	manifestRaw, err := json.Marshal(manifest)
	require.NoError(t, err)
	processRaw, err := json.Marshal(protocol.ProcessIdentity{
		PID: 900001, ProcessStartTime: 2, UID: 3, GID: 4,
	})
	require.NoError(t, err)
	stamp := eventstore.FormatTime(readTestTime)
	expires := eventstore.FormatTime(readTestTime.Add(time.Hour))
	_, err = fixture.store.DB().ExecContext(t.Context(), `INSERT INTO agents
		(agent_id, node_id, harness, created_at, updated_at) VALUES (?, ?, ?, ?, ?)`,
		readTestAgent, readTestNode, protocol.HarnessCodex, stamp, stamp)
	require.NoError(t, err)
	_, err = fixture.store.DB().ExecContext(t.Context(), `INSERT INTO sessions
		(session_id, agent_id, state, capture_mode, deliverable, manifest_json,
		 process_json, started_at, updated_at)
		VALUES (?, ?, 'CONNECTED', 'pty_proxy', 1, ?, ?, ?, ?)`,
		readTestSession, readTestAgent, string(manifestRaw), string(processRaw), stamp, stamp)
	require.NoError(t, err)
	guardStore, err := agentlink.NewInputGuardStore(fixture.store.DB(), fixedReadClock{now: readTestTime})
	require.NoError(t, err)
	written, err := guardStore.WriteInputGuard(t.Context(), protocol.InputGuardReport{
		AgentID: readTestAgent, SessionID: readTestSession,
		DraftState: protocol.DraftStateClosed, ActivityState: protocol.ActivityStateIdle,
		DraftGeneration: 2, InputGuardVersion: 7,
		DetectionSource: protocol.InputGuardProviderPTYProxy, Confidence: 1,
	})
	require.NoError(t, err)
	require.True(t, written)
	_, err = fixture.controller.SetAgentMode(t.Context(), protocol.SetAgentModeParams{
		AgentID: readTestAgent, Mode: protocol.AgentModeManual,
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: readTestOperator})
	require.NoError(t, err)
	fixture.channelDecision = writeReadChannelDecision(t, fixture)

	// Synthetic delivery rows use the same complete column set as the Hub's
	// enqueue path; this test isolates the read projection and its ordering.
	_, err = fixture.store.DB().ExecContext(t.Context(), `INSERT INTO delivery_queue
		(message_id, agent_id, delivery_generation, from_agent, thread_ref,
		 state, priority, attempts, created_at, expires_at, updated_at,
		 hop_count, route_digest)
		VALUES (?, ?, 1, ?, ?, 'RETRY_WAIT', 1, 2, ?, ?, ?, 0, ?)`,
		readTestMessage, readTestAgent, readTestSender,
		"thr_01JB0000000000000000000000", stamp, expires, stamp,
		"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	require.NoError(t, err)
	_, err = fixture.store.DB().ExecContext(t.Context(), `INSERT INTO delivery_attempts
		(delivery_id, message_id, agent_id, delivery_generation, adapter_session_id,
		 orchestrator_decision_id, channel_decision_id, draft_generation,
		 input_guard_version, control_versions_json, state, terminal_reason,
		 created_at, updated_at)
		VALUES (?, ?, ?, 1, ?, ?, ?, 2, 7, '{}', 'UNCERTAIN', 'DISCONNECTED', ?, ?)`,
		"delivery-read", readTestMessage, readTestAgent, readTestSession,
		readTestOrchestrator, fixture.channelDecision, stamp, stamp)
	require.NoError(t, err)
	guards := `[{"guard":"human_draft_closed","outcome":"fail","detail":"canary-free-text"}]`
	_, err = fixture.store.DB().ExecContext(t.Context(), `INSERT INTO orchestrator_decisions
		(decision_id, agent_id, message_id, state_id, allowed_actions_json,
		 selected_action, hard_guards_json, policy_version, channel_decision_id,
		 created_at, execution_status)
		VALUES (?, ?, ?, ?, '["QUEUE","DEFER"]', 'QUEUE', ?, 'policy-v1', ?, ?, 'PENDING')`,
		readTestOrchestrator, readTestAgent, readTestMessage,
		"01JB0000000000000000000044", guards, fixture.channelDecision, stamp)
	require.NoError(t, err)

	// No production writer exists yet for this table. The seeded row exercises
	// only the future-compatible status mapping.
	_, err = fixture.store.DB().ExecContext(t.Context(), `INSERT INTO glimmer_calls
		(call_id, contract, inference_profile, base_model_repo, base_model_revision,
		 adapter_name, adapter_revision, dataset_version, runtime, latency_ms,
		 redaction_policy_version, input_sha256, output_sha256, status, created_at)
		VALUES ('call-read', 'ANALYZE_AGENT_STATE', 'analyst-lora',
		 'example.invalid/model', 'base-rev-1', 'analyst', 'adapter-rev-2',
		 'dataset-v3', 'test', 87, 'redaction-v1', ?, ?, 'ok', ?)`,
		strings.Repeat("a", 64), strings.Repeat("b", 64), stamp)
	require.NoError(t, err)
}

func writeReadChannelDecision(t *testing.T, fixture *readFixture) protocol.ChannelDecisionID {
	t.Helper()
	linkVersionKey := "link:" + string(readTestSender) + "→" + string(readTestAgent) + ":message"
	_, err := fixture.controller.SetLink(t.Context(), protocol.SetLinkParams{
		FromAgent: readTestSender, ToAgent: readTestAgent,
		Plane: protocol.PlaneMessage, Mode: protocol.LinkModeHold,
		ExpectedVersions: protocol.ControlVersions{linkVersionKey: 0},
	}, channel.Actor{Type: protocol.ActorTypeHuman, ID: readTestOperator})
	require.NoError(t, err)
	message := channel.Message{
		ID: readTestMessage, From: readTestSender, FromNode: readTestNode,
		To: []channel.Recipient{{
			Agent: readTestAgent, Deliverable: true, Policy: channel.PolicyAllowed,
		}},
		ToNode: readTestNode, ThreadID: "canary-readable-thread",
	}
	tx, err := fixture.store.DB().BeginTx(t.Context(), nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	decided, _, err := fixture.engine.Admit(t.Context(), tx, message,
		func(ctx context.Context, _ protocol.AdmissionResult) error {
			stamp := eventstore.FormatTime(readTestTime)
			_, insertErr := tx.ExecContext(ctx, `INSERT INTO messages
				(message_id, client_msg_id, origin_node, from_agent, destination_node,
				 thread_ref, thread_id, priority, state, created_at, expires_at,
				 updated_at, hop_count, route_digest)
				VALUES (?, 'client-control-read', ?, ?, ?, ?, ?, 'HIGH', 'ADMITTED',
				 ?, ?, ?, 0, ?)`,
				readTestMessage, readTestNode, readTestSender, readTestNode,
				"thr_01JB0000000000000000000000", message.ThreadID, stamp,
				eventstore.FormatTime(readTestTime.Add(time.Hour)), stamp,
				"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
			return insertErr
		})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	return decided.ID()
}

func insertSyntheticReadAgent(
	t *testing.T,
	fixture *readFixture,
	agent protocol.AgentID,
	started time.Time,
) {
	t.Helper()
	manifest := protocol.CapabilityManifest{
		SchemaVersion: protocol.SchemaVersion, AgentID: agent,
		Harness: protocol.HarnessCodex, HarnessVersion: "synthetic-test",
		AdapterType:        protocol.AdapterTypeGenericPTY,
		InputGuardProvider: protocol.InputGuardProviderPTYProxy,
		CaptureMode:        protocol.CaptureModePTYProxy, Deliverable: true,
		Capabilities: protocol.Capabilities{PTYCapture: true},
	}
	manifestRaw, err := json.Marshal(manifest)
	require.NoError(t, err)
	stamp := eventstore.FormatTime(started)
	_, err = fixture.store.DB().ExecContext(t.Context(), `INSERT INTO agents
		(agent_id, node_id, harness, created_at, updated_at) VALUES (?, ?, ?, ?, ?)`,
		agent, readTestNode, protocol.HarnessCodex, stamp, stamp)
	require.NoError(t, err)
	_, err = fixture.store.DB().ExecContext(t.Context(), `INSERT INTO sessions
		(session_id, agent_id, state, capture_mode, deliverable, manifest_json,
		 started_at, updated_at)
		VALUES (?, ?, 'CONNECTED', 'pty_proxy', 1, ?, ?, ?)`,
		"session-"+string(agent), agent, string(manifestRaw), stamp, stamp)
	require.NoError(t, err)
}

func TestPanelReadMethodsMapMetadataOnly(t *testing.T) {
	fixture := newReadFixture(t, true)
	seedReadRows(t, fixture)

	agents := readResult[protocol.AgentListResult](t, fixture.server,
		protocol.ControlMethodAgentList, protocol.AgentListParams{})
	require.Len(t, agents.Agents, 1)
	agent := agents.Agents[0]
	require.Equal(t, readTestAgent, agent.AgentID)
	require.Equal(t, "1.2.3", agent.HarnessVersion)
	controllerView, err := fixture.controller.Agent(t.Context(), readTestAgent)
	require.NoError(t, err)
	require.Equal(t, controllerView.TX, agent.Channel.TX)
	require.Equal(t, controllerView.RX, agent.Channel.RX)
	require.Equal(t, controllerView.AutoDelivery, agent.Channel.AutoDelivery)
	require.Equal(t, controllerView.AutoContinue, agent.Channel.AutoContinue)
	require.Equal(t, controllerView.Mode, agent.Channel.Mode)
	require.NotNil(t, agent.Keyboard)
	require.Equal(t, int64(1), agent.Queue.Queued)
	require.Equal(t, int64(1), agent.Queue.Uncertain)
	require.Equal(t, protocol.AgentPhaseUnknown, agent.Phase.Value)
	require.Equal(t, protocol.AgentPhaseSourceNone, agent.Phase.Source)

	messages := readResult[protocol.MessageListResult](t, fixture.server,
		protocol.ControlMethodMessageList, protocol.MessageListParams{})
	require.Len(t, messages.Messages, 1)
	require.Equal(t, protocol.PriorityHigh, messages.Messages[0].Priority)
	require.Equal(t, protocol.ReasonCode("DISCONNECTED"), messages.Messages[0].LastReason)

	decisions := readResult[protocol.DecisionListResult](t, fixture.server,
		protocol.ControlMethodDecisionList, protocol.DecisionListParams{})
	require.Len(t, decisions.Decisions, 2)
	for _, decision := range decisions.Decisions {
		if decision.Type == protocol.DecisionTypeOrchestrator {
			require.Equal(t, []protocol.ReasonCode{"human_draft_closed"}, decision.ReasonCodes)
		}
	}
	channelDecision := readResult[protocol.DecisionGetResult](t, fixture.server,
		protocol.ControlMethodDecisionGet,
		protocol.DecisionGetParams{DecisionID: protocol.DecisionID(fixture.channelDecision)})
	require.Equal(t, protocol.ExplanationScopeLink, channelDecision.ChannelEvidence.DecidingScope)
	linkVersionKey := "link:" + string(readTestSender) + "→" + string(readTestAgent) + ":message"
	require.Equal(t, int64(1), channelDecision.ChannelEvidence.ControlVersions[linkVersionKey])
	raw, err := json.Marshal(channelDecision)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "canary-readable-thread")

	orchestratorDecision := readResult[protocol.DecisionGetResult](t, fixture.server,
		protocol.ControlMethodDecisionGet, protocol.DecisionGetParams{DecisionID: protocol.DecisionID(readTestOrchestrator)})
	require.Equal(t, []protocol.ReasonCode{"human_draft_closed"}, orchestratorDecision.ReasonCodes)
	raw, err = json.Marshal(orchestratorDecision)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "canary-free-text")

	status := readResult[protocol.ModelStatusResult](t, fixture.server,
		protocol.ControlMethodModelStatus, protocol.EmptyParams{})
	require.Equal(t, protocol.InferenceProfileAnalystLoRA, status.ActiveProfile.Profile)
	require.Equal(t, protocol.ModelProfileSourceGlimmerCall, status.ActiveProfile.Source)
	require.Equal(t, readTestTime, status.ActiveProfile.ObservedAt)
	require.Equal(t, protocol.OpaqueID("call-read"), status.LastCall.CallID)
	require.Equal(t, protocol.GlimmerCallStatusOK, status.LastCall.Status)
	require.Equal(t, int64(87), status.LastCall.LatencyMS)
	require.Equal(t, readTestTime, status.LastCall.ObservedAt)
	require.Equal(t, protocol.MetadataAvailabilityKnown, status.Deployment.BaseRevision.Availability)
	require.Equal(t, protocol.ModelMetadataValue("base-rev-1"), status.Deployment.BaseRevision.Value)
	require.Equal(t, protocol.ModelMetadataValue("adapter-rev-2"), status.Deployment.AdapterRevision.Value)
	require.Equal(t, protocol.ModelMetadataValue("dataset-v3"), status.Deployment.DatasetVersion.Value)
	require.Equal(t, protocol.MetadataAvailabilityUnavailable, status.Deployment.ABVariant.Availability)
}

func TestAgentListPrefersLiveThenLatestSession(t *testing.T) {
	fixture := newReadFixture(t, true)
	seedReadRows(t, fixture)
	endedManifest := protocol.CapabilityManifest{
		SchemaVersion: protocol.SchemaVersion, AgentID: readTestAgent,
		Harness: protocol.HarnessCodex, HarnessVersion: "ended-newer",
		AdapterType:        protocol.AdapterTypeNone,
		InputGuardProvider: protocol.InputGuardProviderNone,
		CaptureMode:        protocol.CaptureModeBootstrapPipePane,
		Deliverable:        false,
		Capabilities:       protocol.Capabilities{PTYCapture: true},
	}
	require.NoError(t, endedManifest.Validate())
	manifestRaw, err := json.Marshal(endedManifest)
	require.NoError(t, err)
	started := eventstore.FormatTime(readTestTime.Add(time.Hour))
	ended := eventstore.FormatTime(readTestTime.Add(2 * time.Hour))
	_, err = fixture.store.DB().ExecContext(t.Context(), `INSERT INTO sessions
		(session_id, agent_id, state, capture_mode, deliverable, manifest_json,
		 started_at, ended_at, updated_at)
		VALUES ('session-ended-later', ?, 'DISCONNECTED', 'bootstrap_pipe_pane', 0,
		 ?, ?, ?, ?)`, readTestAgent, string(manifestRaw), started, ended, ended)
	require.NoError(t, err)

	live := readResult[protocol.AgentListResult](t, fixture.server,
		protocol.ControlMethodAgentList, protocol.AgentListParams{})
	require.Equal(t, "1.2.3", live.Agents[0].HarnessVersion)
	require.Equal(t, protocol.AgentRunModeRun, live.Agents[0].Mode)
	require.NotNil(t, live.Agents[0].Keyboard)

	_, err = fixture.store.DB().ExecContext(t.Context(), `UPDATE sessions
		SET state = 'DISCONNECTED', ended_at = ?, updated_at = ?
		WHERE session_id = ?`, ended, ended, readTestSession)
	require.NoError(t, err)
	latest := readResult[protocol.AgentListResult](t, fixture.server,
		protocol.ControlMethodAgentList, protocol.AgentListParams{})
	require.Equal(t, "ended-newer", latest.Agents[0].HarnessVersion)
	require.Equal(t, protocol.AgentRunModeObserved, latest.Agents[0].Mode)
	require.Equal(t, protocol.SessionStateDisconnected, latest.Agents[0].SessionState)
	require.False(t, latest.Agents[0].Deliverable)
	require.Nil(t, latest.Agents[0].Keyboard)
}

func TestOrchestratorReasonsFallBackToChannelThenSelectedAction(t *testing.T) {
	fixture := newReadFixture(t, true)
	seedReadRows(t, fixture)
	stamp := eventstore.FormatTime(readTestTime.Add(time.Minute))
	linkedID := protocol.DecisionID("01JB0000000000000000000055")
	selectedID := protocol.DecisionID("01JB0000000000000000000066")
	_, err := fixture.store.DB().ExecContext(t.Context(), `INSERT INTO orchestrator_decisions
		(decision_id, agent_id, message_id, state_id, allowed_actions_json,
		 selected_action, hard_guards_json, policy_version, channel_decision_id,
		 created_at, execution_status)
		VALUES (?, ?, ?, '01JB0000000000000000000088', '["DEFER"]', 'DEFER',
		 '[]', 'policy-v1', ?, ?, 'PENDING'),
		       (?, ?, ?, '01JB0000000000000000000099', '["DEFER"]', 'DEFER',
		 '[]', 'policy-v1', 'chd_01JB0000000000000000000077', ?, 'PENDING')`,
		linkedID, readTestAgent, readTestMessage, fixture.channelDecision, stamp,
		selectedID, readTestAgent, readTestMessage, stamp)
	require.NoError(t, err)

	linked := readResult[protocol.DecisionGetResult](t, fixture.server,
		protocol.ControlMethodDecisionGet, protocol.DecisionGetParams{DecisionID: linkedID})
	require.Equal(t, []protocol.ReasonCode{"LINK_HOLD"}, linked.ReasonCodes)
	selected := readResult[protocol.DecisionGetResult](t, fixture.server,
		protocol.ControlMethodDecisionGet, protocol.DecisionGetParams{DecisionID: selectedID})
	require.Equal(t, []protocol.ReasonCode{"DEFER"}, selected.ReasonCodes)
}

func TestPanelReadPaginationBindsMethodAndFilters(t *testing.T) {
	fixture := newReadFixture(t, true)
	seedReadRows(t, fixture)
	stamp := eventstore.FormatTime(readTestTime.Add(-time.Minute))
	expires := eventstore.FormatTime(readTestTime.Add(time.Hour))
	_, err := fixture.store.DB().ExecContext(t.Context(), `INSERT INTO delivery_queue
		(message_id, agent_id, delivery_generation, from_agent, thread_ref,
		 state, priority, created_at, expires_at, updated_at, hop_count, route_digest)
		VALUES ('01JB0000000000000000000001', ?, 1, ?,
		 'thr_01JB0000000000000000000000', 'QUEUED', 2, ?, ?, ?, 0, ?)`,
		readTestAgent, readTestSender, stamp, expires, stamp,
		"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	require.NoError(t, err)
	one := int64(1)
	first := readResult[protocol.MessageListResult](t, fixture.server,
		protocol.ControlMethodMessageList, protocol.MessageListParams{Limit: &one})
	require.Len(t, first.Messages, 1)
	require.NotEmpty(t, first.NextCursor)
	newerMessage := protocol.ULID("01JB0000000000000000000099")
	newerStamp := eventstore.FormatTime(readTestTime.Add(time.Minute))
	_, err = fixture.store.DB().ExecContext(t.Context(), `INSERT INTO delivery_queue
		(message_id, agent_id, delivery_generation, from_agent, thread_ref,
		 state, priority, created_at, expires_at, updated_at, hop_count, route_digest)
		VALUES (?, ?, 1, ?, 'thr_01JB0000000000000000000000',
		 'DELIVERING', 2, ?, ?, ?, 0, ?)`,
		newerMessage, readTestAgent, readTestSender, newerStamp, expires, newerStamp,
		"sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
	require.NoError(t, err)
	fresh := readResult[protocol.MessageListResult](t, fixture.server,
		protocol.ControlMethodMessageList, protocol.MessageListParams{Limit: &one})
	require.Equal(t, newerMessage, fresh.Messages[0].MessageID)
	second := readResult[protocol.MessageListResult](t, fixture.server,
		protocol.ControlMethodMessageList, protocol.MessageListParams{Limit: &one, Cursor: first.NextCursor})
	require.Len(t, second.Messages, 1)
	require.NotEqual(t, first.Messages[0].MessageID, second.Messages[0].MessageID)
	require.NotEqual(t, newerMessage, second.Messages[0].MessageID,
		"new rows belong to a fresh first page")

	wrongFilter := fixture.server.Handle(t.Context(), readPeer(), rpcRequest(t,
		protocol.ControlMethodMessageList,
		protocol.MessageListParams{Limit: &one, Cursor: first.NextCursor, State: protocol.DeliveryQueueStateQueued}))
	require.Equal(t, int64(rpcInvalidParams), wrongFilter.Error.Code)
	wrongMethod := fixture.server.Handle(t.Context(), readPeer(), rpcRequest(t,
		protocol.ControlMethodAgentList, protocol.AgentListParams{Limit: &one, Cursor: first.NextCursor}))
	require.Equal(t, int64(rpcInvalidParams), wrongMethod.Error.Code)

	longAgent := strings.Repeat("a", 64)
	cursor, err := encodeReadCursor("m", cursorFilter(protocol.ControlMethodMessageList, longAgent, ""), []string{
		eventstore.FormatTime(readTestTime), string(readTestMessage), longAgent, "9223372036854775807",
	})
	require.NoError(t, err)
	require.LessOrEqual(t, len(cursor), 256)
}

func TestAgentAndDecisionPagesApplyBoundsAndFiltersBeforeLimit(t *testing.T) {
	fixture := newReadFixture(t, true)
	seedReadRows(t, fixture)
	insertSyntheticReadAgent(t, fixture, "agent-z", readTestTime.Add(time.Minute))
	one := int64(1)

	agentFirst := readResult[protocol.AgentListResult](t, fixture.server,
		protocol.ControlMethodAgentList, protocol.AgentListParams{Limit: &one})
	require.Len(t, agentFirst.Agents, 1)
	require.Equal(t, readTestAgent, agentFirst.Agents[0].AgentID)
	require.NotEmpty(t, agentFirst.NextCursor)
	agentSecond := readResult[protocol.AgentListResult](t, fixture.server,
		protocol.ControlMethodAgentList,
		protocol.AgentListParams{Limit: &one, Cursor: agentFirst.NextCursor})
	require.Len(t, agentSecond.Agents, 1)
	require.Equal(t, protocol.AgentID("agent-z"), agentSecond.Agents[0].AgentID)
	require.Empty(t, agentSecond.NextCursor)

	decisionFirst := readResult[protocol.DecisionListResult](t, fixture.server,
		protocol.ControlMethodDecisionList, protocol.DecisionListParams{Limit: &one})
	require.Len(t, decisionFirst.Decisions, 1)
	require.NotEmpty(t, decisionFirst.NextCursor)
	decisionSecond := readResult[protocol.DecisionListResult](t, fixture.server,
		protocol.ControlMethodDecisionList,
		protocol.DecisionListParams{Limit: &one, Cursor: decisionFirst.NextCursor})
	require.Len(t, decisionSecond.Decisions, 1)
	require.NotEqual(t, decisionFirst.Decisions[0].DecisionID, decisionSecond.Decisions[0].DecisionID)

	channels := readResult[protocol.DecisionListResult](t, fixture.server,
		protocol.ControlMethodDecisionList,
		protocol.DecisionListParams{Type: protocol.DecisionTypeChannel})
	require.Len(t, channels.Decisions, 1)
	require.Equal(t, protocol.DecisionTypeChannel, channels.Decisions[0].Type)
	orchestratorRows := readResult[protocol.DecisionListResult](t, fixture.server,
		protocol.ControlMethodDecisionList,
		protocol.DecisionListParams{Type: protocol.DecisionTypeOrchestrator})
	require.Len(t, orchestratorRows.Decisions, 1)
	require.Equal(t, protocol.DecisionTypeOrchestrator, orchestratorRows.Decisions[0].Type)

	wrongCursorFilter := fixture.server.Handle(t.Context(), readPeer(), rpcRequest(t,
		protocol.ControlMethodDecisionList, protocol.DecisionListParams{
			Limit: &one, Cursor: decisionFirst.NextCursor, Type: protocol.DecisionTypeChannel,
		}))
	require.Equal(t, int64(rpcInvalidParams), wrongCursorFilter.Error.Code)
	otherAgent := readResult[protocol.DecisionListResult](t, fixture.server,
		protocol.ControlMethodDecisionList,
		protocol.DecisionListParams{AgentID: "agent-z"})
	require.Empty(t, otherAgent.Decisions)
	otherMessage := readResult[protocol.DecisionListResult](t, fixture.server,
		protocol.ControlMethodDecisionList,
		protocol.DecisionListParams{MessageID: "01JB0000000000000000000099"})
	require.Empty(t, otherMessage.Decisions)
}

func TestListReadLimitsAndMessageStateFilter(t *testing.T) {
	fixture := newReadFixture(t, true)
	seedReadRows(t, fixture)
	retrying := readResult[protocol.MessageListResult](t, fixture.server,
		protocol.ControlMethodMessageList,
		protocol.MessageListParams{State: protocol.DeliveryQueueStateRetryWait})
	require.Len(t, retrying.Messages, 1)
	queued := readResult[protocol.MessageListResult](t, fixture.server,
		protocol.ControlMethodMessageList,
		protocol.MessageListParams{State: protocol.DeliveryQueueStateQueued})
	require.Empty(t, queued.Messages)

	zero, over := int64(0), protocol.MaxListLimit+1
	for method, params := range map[protocol.ControlMethod][2]any{
		protocol.ControlMethodAgentList: {
			protocol.AgentListParams{Limit: &zero}, protocol.AgentListParams{Limit: &over},
		},
		protocol.ControlMethodMessageList: {
			protocol.MessageListParams{Limit: &zero}, protocol.MessageListParams{Limit: &over},
		},
		protocol.ControlMethodDecisionList: {
			protocol.DecisionListParams{Limit: &zero}, protocol.DecisionListParams{Limit: &over},
		},
	} {
		for _, invalid := range params {
			response := fixture.server.Handle(t.Context(), readPeer(), rpcRequest(t, method, invalid))
			require.Equal(t, int64(rpcInvalidParams), response.Error.Code, method)
		}
	}
}

func TestDecisionGetReturnsNotFoundForBothIDFamilies(t *testing.T) {
	fixture := newReadFixture(t, true)
	for _, id := range []protocol.DecisionID{
		"chd_01JB0000000000000000000099",
		"01JB0000000000000000000099",
	} {
		response := fixture.server.Handle(t.Context(), readPeer(), rpcRequest(t,
			protocol.ControlMethodDecisionGet, protocol.DecisionGetParams{DecisionID: id}))
		require.Equal(t, int64(rpcDomainError), response.Error.Code)
		require.Equal(t, protocol.ErrorCodeNotFound, response.Error.Data.ErrorCode)
	}
}

func TestPanelReadsFailClosedWithoutQueryOnly(t *testing.T) {
	unsafe := newReadFixture(t, false)
	for method, params := range map[protocol.ControlMethod]any{
		protocol.ControlMethodAgentList:    protocol.AgentListParams{},
		protocol.ControlMethodMessageList:  protocol.MessageListParams{},
		protocol.ControlMethodDecisionList: protocol.DecisionListParams{},
		protocol.ControlMethodDecisionGet:  protocol.DecisionGetParams{DecisionID: protocol.DecisionID(unsafe.channelDecision)},
		protocol.ControlMethodModelStatus:  protocol.EmptyParams{},
	} {
		response := unsafe.server.Handle(t.Context(), readPeer(), rpcRequest(t, method, params))
		require.Equal(t, int64(rpcInternalError), response.Error.Code, method)
	}

	readonly := newReadFixture(t, true)
	_, err := readonly.readDB.ExecContext(t.Context(), `UPDATE agents SET harness = 'codex'`)
	require.ErrorContains(t, err, "readonly database")
	readResult[protocol.AgentListResult](t, readonly.server,
		protocol.ControlMethodAgentList, protocol.AgentListParams{})
}

func TestReadPoolEnforcesQueryOnlyOnEveryConcurrentConnection(t *testing.T) {
	fixture := newReadFixture(t, true)
	txs := make([]*sql.Tx, 4)
	t.Cleanup(func() {
		for _, tx := range txs {
			if tx != nil {
				_ = tx.Rollback()
			}
		}
	})
	ready := make(chan struct {
		tx  *sql.Tx
		err error
	}, len(txs))
	for range txs {
		go func() {
			tx, err := fixture.server.beginRead(t.Context())
			ready <- struct {
				tx  *sql.Tx
				err error
			}{tx: tx, err: err}
		}()
	}
	for i := range txs {
		opened := <-ready
		require.NoError(t, opened.err)
		txs[i] = opened.tx
	}
	_, err := fixture.store.DB().ExecContext(t.Context(),
		"UPDATE agents SET updated_at = updated_at")
	require.NoError(t, err, "the writer pool remains usable while read transactions are open")
	for _, tx := range txs {
		_, err := tx.ExecContext(t.Context(), "UPDATE agents SET updated_at = updated_at")
		require.ErrorContains(t, err, "readonly database")
		require.NoError(t, tx.Rollback())
	}
	clear(txs)
	readResult[protocol.AgentListResult](t, fixture.server,
		protocol.ControlMethodAgentList, protocol.AgentListParams{})
}

func TestPanelReadsRemainUnavailableWithoutReadPool(t *testing.T) {
	fixture := newReadFixture(t, true)
	fixture.server.readDB = nil
	for method, params := range map[protocol.ControlMethod]any{
		protocol.ControlMethodAgentList:    protocol.AgentListParams{},
		protocol.ControlMethodMessageList:  protocol.MessageListParams{},
		protocol.ControlMethodDecisionList: protocol.DecisionListParams{},
		protocol.ControlMethodDecisionGet:  protocol.DecisionGetParams{DecisionID: protocol.DecisionID(fixture.channelDecision)},
		protocol.ControlMethodModelStatus:  protocol.EmptyParams{},
	} {
		response := fixture.server.Handle(t.Context(), readPeer(), rpcRequest(t, method, params))
		require.Equal(t, int64(rpcInternalError), response.Error.Code, method)
		require.Equal(t, "Internal error", response.Error.Message, method)
	}
}

func TestPanelReadsFailClosedWhenPeerIdentityLookupFails(t *testing.T) {
	fixture := newReadFixture(t, true)
	closedDB, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "closed.db"))
	require.NoError(t, err)
	require.NoError(t, closedDB.Close())
	server := *fixture.server
	server.db = closedDB
	for method, params := range map[protocol.ControlMethod]any{
		protocol.ControlMethodAgentList:    protocol.AgentListParams{},
		protocol.ControlMethodMessageList:  protocol.MessageListParams{},
		protocol.ControlMethodDecisionList: protocol.DecisionListParams{},
		protocol.ControlMethodDecisionGet:  protocol.DecisionGetParams{DecisionID: protocol.DecisionID(fixture.channelDecision)},
		protocol.ControlMethodModelStatus:  protocol.EmptyParams{},
	} {
		response := server.Handle(t.Context(), readPeer(), rpcRequest(t, method, params))
		require.Equal(t, int64(rpcInternalError), response.Error.Code, method)
		require.Equal(t, "Internal error", response.Error.Message, method)
	}
}

func TestPanelReadsRequireVerifiedHumanPeer(t *testing.T) {
	fixture := newReadFixture(t, true)
	methods := map[protocol.ControlMethod]any{
		protocol.ControlMethodAgentList:    protocol.AgentListParams{},
		protocol.ControlMethodMessageList:  protocol.MessageListParams{},
		protocol.ControlMethodDecisionList: protocol.DecisionListParams{},
		protocol.ControlMethodDecisionGet:  protocol.DecisionGetParams{DecisionID: protocol.DecisionID(fixture.channelDecision)},
		protocol.ControlMethodModelStatus:  protocol.EmptyParams{},
	}
	for method, params := range methods {
		response := fixture.server.Handle(t.Context(), Peer{}, rpcRequest(t, method, params))
		require.Equal(t, int64(rpcDomainError), response.Error.Code, method)
		require.Equal(t, protocol.ErrorCodePolicyDenied, response.Error.Data.ErrorCode, method)
		require.Equal(t, "Control read forbidden", response.Error.Message, method)
	}

	process, err := registry.CurrentProcess()
	require.NoError(t, err)
	processRaw, err := json.Marshal(process)
	require.NoError(t, err)
	manifest := protocol.CapabilityManifest{
		SchemaVersion: protocol.SchemaVersion, AgentID: "agent-current",
		Harness: protocol.HarnessCodex, HarnessVersion: "test",
		AdapterType:        protocol.AdapterTypeGenericPTY,
		InputGuardProvider: protocol.InputGuardProviderPTYProxy,
		CaptureMode:        protocol.CaptureModePTYProxy, Deliverable: true,
		Capabilities: protocol.Capabilities{PTYCapture: true},
	}
	manifestRaw, err := json.Marshal(manifest)
	require.NoError(t, err)
	stamp := eventstore.FormatTime(readTestTime)
	_, err = fixture.store.DB().ExecContext(t.Context(), `INSERT INTO agents
		(agent_id, node_id, harness, created_at, updated_at) VALUES
		('agent-current', ?, 'codex', ?, ?)`, readTestNode, stamp, stamp)
	require.NoError(t, err)
	_, err = fixture.store.DB().ExecContext(t.Context(), `INSERT INTO sessions
		(session_id, agent_id, state, capture_mode, deliverable, manifest_json,
		 process_json, started_at, updated_at)
		VALUES ('session-current', 'agent-current', 'CONNECTED', 'pty_proxy', 1, ?, ?, ?, ?)`,
		string(manifestRaw), string(processRaw), stamp, stamp)
	require.NoError(t, err)
	for method, params := range methods {
		response := fixture.server.Handle(t.Context(), readPeer(), rpcRequest(t, method, params))
		require.Equal(t, int64(rpcDomainError), response.Error.Code, method)
		require.Equal(t, protocol.ErrorCodePolicyDenied, response.Error.Data.ErrorCode, method)
	}
}

func TestPanelReadSQLPrivacyAllowlist(t *testing.T) {
	blacklist := []string{
		"message_envelopes", "thread_id", "client_msg_id", "requested_budget_json",
		"requested_autonomy", "payload_json", "process_json", "evidence_json",
		"confidence_json", "probabilities_json", "matched_controls_json",
		"effective_control_json", "actor_id", "input_sha256", "output_sha256",
		"public_key", "select *",
	}
	allowedPaths := map[string]bool{
		"$.harness_version": true, "$.blocker": true, "$.lines[#-1].scope": true,
		"$.guard": true, "$.outcome": true,
	}
	for _, query := range controlReadSQL {
		lower := strings.ToLower(strings.TrimSpace(query))
		require.True(t, strings.HasPrefix(lower, "select ") || strings.HasPrefix(lower, "with "), query)
		require.Contains(t, lower, "limit", query)
		for _, forbidden := range blacklist {
			require.NotContains(t, lower, forbidden, query)
		}
		for rest := query; ; {
			start := strings.Index(rest, "'$.")
			if start < 0 {
				break
			}
			rest = rest[start+1:]
			end := strings.IndexByte(rest, '\'')
			require.GreaterOrEqual(t, end, 0, query)
			require.True(t, allowedPaths[rest[:end]], "unexpected JSON path %q", rest[:end])
			rest = rest[end+1:]
		}
	}
}

func TestUnavailableModelStatusHasNoInventedValues(t *testing.T) {
	fixture := newReadFixture(t, true)
	status := readResult[protocol.ModelStatusResult](t, fixture.server,
		protocol.ControlMethodModelStatus, protocol.EmptyParams{})
	require.Nil(t, status.ActiveProfile)
	require.Nil(t, status.LastCall)
	require.Equal(t, protocol.MetadataAvailabilityUnavailable, status.Deployment.BaseRevision.Availability)
	require.Equal(t, protocol.MetadataAvailabilityUnavailable, status.Deployment.AdapterRevision.Availability)
	require.Equal(t, protocol.MetadataAvailabilityUnavailable, status.Deployment.DatasetVersion.Availability)
	require.Equal(t, protocol.MetadataAvailabilityUnavailable, status.Deployment.ABVariant.Availability)
}

func TestModelStatusDistinguishesUnknownFromUnavailableMetadata(t *testing.T) {
	fixture := newReadFixture(t, true)
	seedReadRows(t, fixture)
	_, err := fixture.store.DB().ExecContext(t.Context(), `UPDATE glimmer_calls
		SET adapter_revision = ?, dataset_version = NULL`, "invalid\nrevision")
	require.NoError(t, err)
	status := readResult[protocol.ModelStatusResult](t, fixture.server,
		protocol.ControlMethodModelStatus, protocol.EmptyParams{})
	require.Equal(t, protocol.MetadataAvailabilityUnknown,
		status.Deployment.AdapterRevision.Availability)
	require.Empty(t, status.Deployment.AdapterRevision.Value)
	require.Equal(t, protocol.MetadataAvailabilityUnavailable,
		status.Deployment.DatasetVersion.Availability)
}

func TestStoredReadFieldsFailRequiredAndOmitInvalidOptionalValues(t *testing.T) {
	t.Run("required", func(t *testing.T) {
		fixture := newReadFixture(t, true)
		seedReadRows(t, fixture)
		_, err := fixture.store.DB().ExecContext(t.Context(),
			"UPDATE sessions SET state = 'BROKEN' WHERE session_id = ?", readTestSession)
		require.NoError(t, err)
		response := fixture.server.Handle(t.Context(), readPeer(), rpcRequest(t,
			protocol.ControlMethodAgentList, protocol.AgentListParams{}))
		require.Equal(t, int64(rpcInternalError), response.Error.Code)
	})

	t.Run("optional", func(t *testing.T) {
		fixture := newReadFixture(t, true)
		seedReadRows(t, fixture)
		_, err := fixture.store.DB().ExecContext(t.Context(), `UPDATE delivery_attempts
			SET terminal_reason = 'free text that is not a reason code'`)
		require.NoError(t, err)
		_, err = fixture.store.DB().ExecContext(t.Context(), `UPDATE orchestrator_decisions
			SET decision_model = ?, glimmer_call_id = ?`, "invalid\nmodel", "invalid call id")
		require.NoError(t, err)

		messages := readResult[protocol.MessageListResult](t, fixture.server,
			protocol.ControlMethodMessageList, protocol.MessageListParams{})
		require.Empty(t, messages.Messages[0].LastReason)
		decision := readResult[protocol.DecisionGetResult](t, fixture.server,
			protocol.ControlMethodDecisionGet,
			protocol.DecisionGetParams{DecisionID: protocol.DecisionID(readTestOrchestrator)})
		require.Empty(t, decision.OrchestratorEvidence.DecisionModel)
		require.Empty(t, decision.OrchestratorEvidence.GlimmerCallID)
	})
}

func TestExistingReadsRemainAvailableToUnverifiedPeers(t *testing.T) {
	fixture := newReadFixture(t, true)
	response := fixture.server.Handle(t.Context(), Peer{}, rpcRequest(t,
		protocol.ControlMethodChannelGetGlobal, protocol.EmptyParams{}))
	require.Nil(t, response.Error)
}

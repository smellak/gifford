package protocol

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Sample identifiers shared by the rule tables.
const (
	testEventID = "01JB0000000000000000000000"
	testMsgID   = "01JB0000000000000000000011"
	testAgentID = "agent-1"
	testAgent2  = "agent-2"
	testAgent3  = "agent-3"
	testCapture = "cap-1"
	testSession = "sess-1"
	testNode    = "node-a"
	testThread  = "thread-1"
	testUsage   = "1/2"
	testLoop    = "loop_01JB0000000000000000000000"
	testAdapter = "adapter-1"
	testVersion = "1.2.3"
)

func validEnvelope() EventEnvelope {
	return EventEnvelope{
		EventID:          testEventID,
		SchemaVersion:    SchemaVersion,
		NodeID:           testNode,
		SessionID:        testSession,
		AgentID:          testAgentID,
		AdapterType:      AdapterTypeNone,
		SourceQuality:    SourceQualityBootstrapPipePane,
		EventType:        EventTypeCaptureChunk,
		Direction:        DirectionOut,
		Stream:           StreamPTY,
		Origin:           OriginHarness,
		WallTime:         time.Date(2026, 10, 26, 12, 41, 2, 314000000, time.FixedZone("CET", 3600)),
		MonotonicNS:      883425102931,
		Sequence:         18382,
		Payload:          Payload(`{"capture_session_id":"cap-1","byte_count":24,"ansi_present":true,"raw_blob_ref":"blob://capture/cap-1/0001"}`),
		Sensitivity:      SensitivityNormal,
		RedactionApplied: false,
	}
}

// TestEnvelopeRules covers every cross-field rule of EventEnvelope.
func TestEnvelopeRules(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		valid bool
		patch func(*EventEnvelope)
	}{
		{name: "agent capture chunk passes", valid: true, patch: func(*EventEnvelope) {}},
		{name: "agent scoped needs session", patch: func(e *EventEnvelope) { e.SessionID = "" }},
		{name: "agent scoped needs agent", patch: func(e *EventEnvelope) { e.AgentID = "" }},
		{name: "agent scoped needs adapter", patch: func(e *EventEnvelope) { e.AdapterType = "" }},
		{name: "gifford scoped control passes", valid: true, patch: func(e *EventEnvelope) {
			e.EventType = EventTypeControlPause
			e.SessionID, e.AgentID, e.AdapterType = "", "", ""
			e.Direction, e.Stream, e.Origin = "", "", OriginSystem
			e.SourceQuality = SourceQualityInternal
			e.Payload = Payload(`{}`)
		}},
		{name: "gifford scoped needs system origin", patch: func(e *EventEnvelope) {
			e.EventType = EventTypeControlPause
			e.SessionID, e.AgentID, e.AdapterType = "", "", ""
			e.Direction, e.Stream = "", ""
			e.Origin = OriginHarness
			e.SourceQuality = SourceQualityInternal
			e.Payload = Payload(`{}`)
		}},
		{name: "internal quality is never agent scoped", patch: func(e *EventEnvelope) {
			e.SourceQuality = SourceQualityInternal
		}},
		{name: "native stream needs harness origin", patch: func(e *EventEnvelope) {
			e.EventType = EventTypeApprovalRequested
			e.Stream = StreamNativeEvent
			e.Origin = OriginSystem
			e.SourceQuality = SourceQualityNativeHook
			e.Payload = Payload(`{}`)
		}},
		{name: "native stream needs native quality", patch: func(e *EventEnvelope) {
			e.EventType = EventTypeApprovalRequested
			e.Stream = StreamNativeEvent
			e.Origin = OriginHarness
			e.SourceQuality = SourceQualityPTYScreen
			e.Payload = Payload(`{}`)
		}},
		{name: "native approval passes", valid: true, patch: func(e *EventEnvelope) {
			e.EventType = EventTypeApprovalRequested
			e.Stream = StreamNativeEvent
			e.Origin = OriginHarness
			e.SourceQuality = SourceQualityNativeHook
			e.Payload = Payload(`{}`)
		}},
		{name: "capture needs pty stream", patch: func(e *EventEnvelope) { e.Stream = StreamStdout }},
		{name: "capture chunk needs out direction", patch: func(e *EventEnvelope) { e.Direction = DirectionIn }},
		{name: "message control needs message id", patch: func(e *EventEnvelope) {
			e.EventType = "message.control_held"
			e.SessionID, e.AgentID, e.AdapterType = "", "", ""
			e.Direction, e.Stream = "", ""
			e.Origin = OriginSystem
			e.SourceQuality = SourceQualityInternal
			e.Payload = Payload(`{}`)
		}},
		{name: "bad payload fails", patch: func(e *EventEnvelope) {
			e.Payload = Payload(`{"capture_session_id":"cap-1","byte_count":0,"ansi_present":true,"raw_blob_ref":"blob://capture/cap-1/0001"}`)
		}},
		{name: "wrong version wraps unsupported", patch: func(e *EventEnvelope) { e.SchemaVersion = 2 }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := validEnvelope()
			tc.patch(&e)
			err := e.Validate()
			if tc.valid {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			if tc.name == "wrong version wraps unsupported" {
				require.ErrorIs(t, err, ErrUnsupportedVersion)
			} else {
				require.ErrorIs(t, err, ErrInvalid)
			}
		})
	}
}

func TestCapturePTYScreenEnvelopeFixtures(t *testing.T) {
	t.Parallel()
	for _, name := range []string{
		"valid_pty_screen_quota.json",
		"valid_pty_screen_gap.json",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.NoError(t, readEnvelopeFixture(t, name).Validate())
		})
	}
	t.Run("capture chunk remains bootstrap-only", func(t *testing.T) {
		t.Parallel()
		err := readEnvelopeFixture(t, "invalid_pty_screen_chunk.json").Validate()
		require.ErrorIs(t, err, ErrInvalid)
		var validationErr *ValidationError
		require.ErrorAs(t, err, &validationErr)
		require.Equal(t, pathSourceQuality, validationErr.Path)
	})
}

func readEnvelopeFixture(t *testing.T, name string) EventEnvelope {
	t.Helper()
	raw, err := os.ReadFile("testdata/event_envelope/" + name)
	require.NoError(t, err)
	var event EventEnvelope
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	require.NoError(t, dec.Decode(&event))
	return event
}

// TestSnapshotRules covers the state snapshot cross-field rules.
func TestSnapshotRules(t *testing.T) {
	t.Parallel()

	base := func() StateSnapshot {
		return StateSnapshot{
			StateID: testEventID, StateVersion: 1, AgentID: testAgentID,
			ObservedAt:   testTimesOnly(),
			SessionState: SessionStateConnected, HumanDraftState: DraftStateClosed,
			HumanActivityState: ActivityStateIdle, AgentPhase: AgentPhaseIdleAtPrompt,
			AttentionState: AttentionStateNone, HealthState: HealthStateHealthy,
			DeliverySafety: DeliverySafetySafe, ChannelEffective: ChannelEffective{
				Global: ChannelModeActive, Node: ChannelModeActive,
				Tx: true, Rx: true, AutoDelivery: true, AutoContinue: true,
			},
			Confidence:     Confidence{HumanDraftState: 0.5, AgentPhase: 0.5, AttentionState: 0.5, DeliverySafety: 0.5},
			AnalystVersion: "v1", ActiveLoops: []LoopSummary{}, EvidenceEventIDs: []ULID{},
		}
	}
	valid := base()
	require.NoError(t, valid.Validate())

	observed := base()
	observed.SessionState = SessionStateObserved
	observed.HumanDraftState = DraftStateUnknown
	require.NoError(t, observed.Validate())
	observed.HumanDraftState = DraftStateOpen
	require.ErrorIs(t, observed.Validate(), ErrInvalid)

	dup := base()
	dup.ActiveLoops = []LoopSummary{
		{LoopID: testLoop, Mode: LoopModeAuto, Turns: testUsage, Messages: testUsage},
		{LoopID: testLoop, Mode: LoopModeStep, Turns: testUsage, Messages: testUsage},
	}
	require.ErrorIs(t, dup.Validate(), ErrInvalid)
}

// TestMessageRules covers the message and wire cross-field rules.
func TestMessageRules(t *testing.T) {
	t.Parallel()

	base := func() LogicalMessage {
		return LogicalMessage{
			MessageID: testMsgID, ClientMsgID: "cli-1", ThreadID: testThread,
			FromAgent: testAgentID, ToAgents: []AgentID{testAgent2},
			OriginNode: testNode, DestinationNode: "node-b",
			Subject: "hi", Body: "hello", Priority: PriorityNormal, DeliveryMode: DeliveryModeWhenSafe,
			Sensitivity: MessageSensitivityInternal, ThreadSequence: 1,
			RouteDigest: "sha256:0000000000000000000000000000000000000000000000000000000000000000",
		}
	}
	valid := base()
	valid.CreatedAt, valid.ExpiresAt = testTimes()
	require.NoError(t, valid.Validate())

	expired := base()
	expired.CreatedAt, expired.ExpiresAt = testTimes()
	expired.ExpiresAt = expired.CreatedAt
	require.ErrorIs(t, expired.Validate(), ErrInvalid)

	self := base()
	self.CreatedAt, self.ExpiresAt = testTimes()
	self.ToAgents = []AgentID{testAgentID}
	require.ErrorIs(t, self.Validate(), ErrInvalid)

	loopRoot := base()
	loopRoot.CreatedAt, loopRoot.ExpiresAt = testTimes()
	loopRoot.LoopID = testLoop
	require.ErrorIs(t, loopRoot.Validate(), ErrInvalid)

	partial := base()
	partial.CreatedAt, partial.ExpiresAt = testTimes()
	partial.ParentMessageID = "01JB0000000000000000000000"
	partial.LoopID = testLoop
	gen := int64(1)
	partial.LoopGeneration = &gen
	require.ErrorIs(t, partial.Validate(), ErrInvalid)
}

func testTimes() (created, expires time.Time) {
	created = time.Date(2026, 10, 26, 12, 41, 2, 0, time.UTC)
	return created, created.Add(time.Hour)
}

func testTimesOnly() time.Time {
	created, _ := testTimes()
	return created
}

// TestOrchestrationRules covers the orchestration cross-field rules.
func TestOrchestrationRules(t *testing.T) {
	t.Parallel()

	channel := OrchestrationChannel{
		DecisionID: "chd_01JB0000000000000000000010",
		Outcome:    DecisionOutcomeAllow, Global: ChannelModeActive, Link: LinkModeAllow,
	}
	request := OrchestrationRequest{
		TaskType: ModelContractChooseOrchestrationAction,
		Agent: OrchestrationAgent{
			ID: testAgent2, Adapter: AdapterTypeGenericPTY,
			Capabilities: Capabilities{PTYCapture: true},
		},
		State:          OrchestrationState{HumanDraft: DraftStateClosed, AgentPhase: AgentPhaseIdleAtPrompt, PIdle: 0.1, Attention: AttentionStateNone},
		Message:        &PendingObject{Priority: PriorityNormal, Triage: TriageLabelNormal, AgeMS: 1},
		Channel:        channel,
		AllowedActions: []OrchestratorAction{OrchestratorActionQueue, OrchestratorActionDefer},
	}
	require.NoError(t, request.Validate())
	bad := request
	bad.TaskType = ModelContractTriageMessage
	require.ErrorIs(t, bad.Validate(), ErrInvalid)

	both := request
	both.Artifact = &PendingObject{Priority: PriorityNormal, Triage: TriageLabelNormal, AgeMS: 1}
	require.ErrorIs(t, both.Validate(), ErrInvalid)

	off := channel
	off.Global = ChannelModeOff
	require.ErrorIs(t, off.Validate(), ErrInvalid)
	denied := channel
	denied.Link = LinkModeDeny
	require.ErrorIs(t, denied.Validate(), ErrInvalid)

	decision := OrchestratorDecision{
		DecisionID: "01JB0000000000000000000013", AgentID: testAgent2,
		MessageID: testMsgID, StateID: testEventID,
		AllowedActions: []OrchestratorAction{OrchestratorActionQueue},
		SelectedAction: OrchestratorActionQueue,
		HardGuards:     []GuardResult{{Guard: GuardNameSessionConnected, Outcome: GuardOutcomePass}},
		PolicyVersion:  "v1", Channel: channel, CreatedAt: testTimesOnly(),
		ExecutionStatus: ExecutionStatusPending,
	}
	require.NoError(t, decision.Validate())
	badSelect := decision
	badSelect.SelectedAction = OrchestratorActionDefer
	require.ErrorIs(t, badSelect.Validate(), ErrInvalid)
	bothIDs := decision
	bothIDs.ArtifactID = "art-1"
	require.ErrorIs(t, bothIDs.Validate(), ErrInvalid)
	dupGuard := decision
	dupGuard.HardGuards = []GuardResult{
		{Guard: GuardNameSessionConnected, Outcome: GuardOutcomePass},
		{Guard: GuardNameSessionConnected, Outcome: GuardOutcomeFail},
	}
	require.ErrorIs(t, dupGuard.Validate(), ErrInvalid)

	overridden := GuardResult{Guard: GuardNamePIdleThreshold, Outcome: GuardOutcomeOverridden}
	require.NoError(t, overridden.Validate())
	overridden.Guard = GuardNameDeliverable
	require.ErrorIs(t, overridden.Validate(), ErrInvalid)
}

// TestGatewayRules covers the gateway cross-field rules.
func TestGatewayRules(t *testing.T) {
	t.Parallel()

	record := func() GlimmerCallRecord {
		return GlimmerCallRecord{
			CallID: "call-1", Contract: ModelContractTriageMessage,
			InferenceProfile: InferenceProfileBase, BaseModelRepo: "example.invalid/r",
			BaseModelRevision: "v1", Runtime: "vllm", LatencyMS: 1,
			RedactionPolicyVersion: "1", InputSHA256: "0000000000000000000000000000000000000000000000000000000000000000",
			OutputSHA256: "0000000000000000000000000000000000000000000000000000000000000000",
			Status:       GlimmerCallStatusOK, CreatedAt: testTimesOnly(),
		}
	}
	result := InferResult{
		Contract: ModelContractTriageMessage, InferenceProfile: InferenceProfileBase,
		Label:         string(TriageLabelNormal),
		Probabilities: map[string]float64{string(TriageLabelNormal): 1},
		CallRecord:    record(),
	}
	require.NoError(t, result.Validate())
	outside := result
	outside.Label = "bogus"
	require.ErrorIs(t, outside.Validate(), ErrInvalid)
	unbalanced := result
	unbalanced.Probabilities = map[string]float64{string(TriageLabelNormal): 0.5}
	require.ErrorIs(t, unbalanced.Validate(), ErrInvalid)

	profile := GatewayProfile{Contract: ModelContractAnalyzeAgentState, InferenceProfile: InferenceProfileBase}
	require.NoError(t, profile.Validate())
	lora := GatewayProfile{Contract: ModelContractAnalyzeAgentState, InferenceProfile: InferenceProfileAnalystLoRA}
	require.ErrorIs(t, lora.Validate(), ErrInvalid)
	lora.AdapterName, lora.AdapterRevision = testAdapter, "v1"
	require.NoError(t, lora.Validate())

	baseRecord := record()
	baseRecord.AdapterName = testAdapter
	require.ErrorIs(t, baseRecord.Validate(), ErrInvalid)
	loraRecord := record()
	loraRecord.Contract = ModelContractAnalyzeAgentState
	loraRecord.InferenceProfile = InferenceProfileAnalystLoRA
	loraRecord.AdapterName, loraRecord.AdapterRevision = testAdapter, "v1"
	require.ErrorIs(t, loraRecord.Validate(), ErrInvalid)
	loraRecord.AdapterSHA256 = "0000000000000000000000000000000000000000000000000000000000000000"
	require.NoError(t, loraRecord.Validate())
}

// TestFrameRules covers the agent-socket cross-field rules.
func TestFrameRules(t *testing.T) {
	t.Parallel()

	frame := AgentFrame{
		Op: AgentOpHeartbeat, FrameID: 1,
		SentAt: testTimesOnly(),
		Body:   Payload(`{"agent_id":"agent-1"}`),
	}
	require.NoError(t, frame.Validate())
	unknown := frame
	unknown.Op = "bogus"
	require.ErrorIs(t, unknown.Validate(), ErrInvalid)
	noReply := AgentFrame{
		Op: AgentOpRegisterAgentResult, FrameID: 2,
		SentAt: testTimesOnly(),
		Body:   Payload(`{"agent_id":"agent-1","session_id":testSession}`),
	}
	require.ErrorIs(t, noReply.Validate(), ErrInvalid)
	eventResult := AgentFrame{
		Op: AgentOpEmitEventResult, FrameID: 3,
		SentAt: testTimesOnly(),
		Body:   Payload(`{"event_id":"01JB0000000000000000000001","status":"STORED"}`),
	}
	require.ErrorIs(t, eventResult.Validate(), ErrInvalid)
	replyTo := int64(7)
	eventResult.ReplyTo = &replyTo
	require.NoError(t, eventResult.Validate())

	require.NoError(t, (AgentSocketSupport{}).Validate())
	result := EmitEventResult{
		EventID: "01JB0000000000000000000001", Status: EmitEventStatusStored,
	}
	require.NoError(t, result.Validate())
	result.Reason = EmitEventRejectReasonConflict
	require.ErrorIs(t, result.Validate(), ErrInvalid)
	result.Status = EmitEventStatusRejected
	require.NoError(t, result.Validate())
	result.Reason = ""
	require.ErrorIs(t, result.Validate(), ErrInvalid)

	registration := RegisterAgent{
		Manifest: CapabilityManifest{
			SchemaVersion: SchemaVersion, AgentID: testAgentID, Harness: HarnessCodex,
			HarnessVersion: "1", AdapterType: AdapterTypeGenericPTY,
			InputGuardProvider: InputGuardProviderPTYProxy, CaptureMode: CaptureModePTYProxy,
			Deliverable:  true,
			Capabilities: Capabilities{PTYCapture: true},
		},
	}
	require.ErrorIs(t, registration.Validate(), ErrInvalid)
	registration.Process = &ProcessIdentity{PID: 1}
	require.NoError(t, registration.Validate())

	guard := InputGuardReport{
		AgentID: testAgentID, SessionID: testSession, DraftState: DraftStateClosed,
		ActivityState: ActivityStateIdle, DetectionSource: InputGuardProviderPTYProxy, Confidence: 1,
	}
	require.NoError(t, guard.Validate())
	guard.Confidence = 0.5
	require.ErrorIs(t, guard.Validate(), ErrInvalid)

	committed := DeliveryResult{
		DeliveryID: "dlv-1", AgentID: testAgent2, Result: DeliveryResultKindCommitted,
		ObservedAt:      testTimesOnly(),
		ControlVersions: ControlVersions{string(ScopeTypeGlobal): 1},
	}
	require.ErrorIs(t, committed.Validate(), ErrInvalid)
	committed.WriterLeaseID = "lease-1"
	require.NoError(t, committed.Validate())
	cancelled := committed
	cancelled.Result = DeliveryResultKindCancelled
	require.ErrorIs(t, cancelled.Validate(), ErrInvalid)
	failed := committed
	failed.Result = DeliveryResultKindFailed
	require.ErrorIs(t, failed.Validate(), ErrInvalid)

	native := NativeDelivery{
		DeliveryID: "dlv-1", AgentID: testAgent2,
		MessageRef:             MessageRef{MessageID: testMsgID, FromAgent: testAgentID, Priority: PriorityNormal, ThreadID: testThread},
		OrchestratorDecisionID: testEventID, ControlVersions: ControlVersions{string(ScopeTypeGlobal): 1},
	}
	require.NoError(t, native.Validate())
	native.ControlVersions = ControlVersions{}
	require.ErrorIs(t, native.Validate(), ErrInvalid)
}

// TestCaptureRules covers the capture payload cross-field rules.
func TestCaptureRules(t *testing.T) {
	t.Parallel()

	rotated := CaptureRotated{CaptureSessionID: testCapture, ClosedSegmentID: "seg-1", NewSegmentID: "seg-2", Bytes: 10, Compressed: true}
	require.NoError(t, rotated.Validate())
	rotated.NewSegmentID = "seg-1"
	require.ErrorIs(t, rotated.Validate(), ErrInvalid)
	rotated.NewSegmentID = "seg-2"
	compressed := int64(4)
	rotated.BytesCompressed = &compressed
	rotated.Compressed = false
	require.ErrorIs(t, rotated.Validate(), ErrInvalid)

	recreated := CapturePaneRecreated{CaptureSessionID: testCapture, OldPane: "%1", NewPane: "%2"}
	require.NoError(t, recreated.Validate())
	recreated.NewPane = "%1"
	require.ErrorIs(t, recreated.Validate(), ErrInvalid)

	reattached := CaptureReattached{CaptureSessionID: testCapture, GapFromSequence: 1, GapToSequence: 2, GapMS: 3}
	require.NoError(t, reattached.Validate())
	reattached.GapToSequence = 0
	require.ErrorIs(t, reattached.Validate(), ErrInvalid)
}

// TestManifestRules covers the manifest cross-field rules.
func TestManifestRules(t *testing.T) {
	t.Parallel()

	valid := CapabilityManifest{
		SchemaVersion: SchemaVersion, AgentID: testAgentID, Harness: HarnessCodex,
		HarnessVersion: testVersion, AdapterType: AdapterTypeGenericPTY,
		InputGuardProvider: InputGuardProviderPTYProxy, CaptureMode: CaptureModePTYProxy,
		Deliverable:  true,
		Capabilities: Capabilities{PTYCapture: true},
	}
	require.NoError(t, valid.Validate())

	bootstrap := valid
	bootstrap.AdapterType = AdapterTypeNone
	bootstrap.InputGuardProvider = InputGuardProviderNone
	bootstrap.CaptureMode = CaptureModeBootstrapPipePane
	bootstrap.Deliverable = false
	require.NoError(t, bootstrap.Validate())

	bootstrap.Deliverable = true
	require.ErrorIs(t, bootstrap.Validate(), ErrInvalid)
	bootstrap.Deliverable = false

	bootstrap.Capabilities = Capabilities{}
	require.ErrorIs(t, bootstrap.Validate(), ErrInvalid)
	bootstrap.Capabilities = Capabilities{PTYCapture: true}

	native := valid
	native.Capabilities = Capabilities{PTYCapture: true, StructuredEvents: true}
	require.ErrorIs(t, native.Validate(), ErrInvalid)
	native.AdapterType = AdapterTypeNativeAPI
	require.NoError(t, native.Validate())

	guardless := valid
	guardless.InputGuardProvider = InputGuardProviderNone
	require.ErrorIs(t, guardless.Validate(), ErrInvalid)
}

// TestWireEnvelopesConsumeRecipients requires exactly one envelope per
// recipient agent: envelopes are consumed once, in any order.
func TestWireEnvelopesConsumeRecipients(t *testing.T) {
	t.Parallel()

	load := func(t *testing.T) WireEnvelope {
		t.Helper()
		b, err := os.ReadFile("testdata/wire_envelope/valid_none.json")
		require.NoError(t, err)
		var wire WireEnvelope
		require.NoError(t, json.Unmarshal(b, &wire))
		wire.EncryptionProfile = EncryptionProfileGiffordE2EV1
		return wire
	}
	key := func(agent AgentID) RecipientKeyEnvelope {
		return RecipientKeyEnvelope{
			RecipientAgent:  agent,
			EncryptionKeyID: EncryptionKeyID("x25519:" + string(agent) + ":v1"),
			EncapsulatedKey: []byte{1},
			WrappedKey:      []byte{1},
		}
	}

	t.Run("duplicate envelope", func(t *testing.T) {
		t.Parallel()
		wire := load(t)
		wire.RecipientAgents = []AgentID{testAgent2, testAgent3}
		wire.RecipientKeyEnvelopes = []RecipientKeyEnvelope{key(testAgent2), key(testAgent2)}
		require.ErrorIs(t, wire.Validate(), ErrInvalid)
	})

	t.Run("unknown recipient", func(t *testing.T) {
		t.Parallel()
		wire := load(t)
		wire.RecipientAgents = []AgentID{testAgent2, testAgent3}
		wire.RecipientKeyEnvelopes = []RecipientKeyEnvelope{key(testAgent2), key("agent-9")}
		require.ErrorIs(t, wire.Validate(), ErrInvalid)
	})

	t.Run("one per recipient either order", func(t *testing.T) {
		t.Parallel()
		for _, envelopes := range [][]RecipientKeyEnvelope{
			{key(testAgent2), key(testAgent3)},
			{key(testAgent3), key(testAgent2)},
		} {
			wire := load(t)
			wire.RecipientAgents = []AgentID{testAgent2, testAgent3}
			wire.RecipientKeyEnvelopes = envelopes
			require.NoError(t, wire.Validate())
		}
	})
}

// TestFreeTextLimitsCountRunes requires the free-text limits to count
// runes, not bytes.
func TestFreeTextLimitsCountRunes(t *testing.T) {
	t.Parallel()

	t.Run("context refs", func(t *testing.T) {
		t.Parallel()
		b, err := os.ReadFile("testdata/logical_message/valid_minimal.json")
		require.NoError(t, err)
		for _, tc := range []struct {
			name  string
			refs  []string
			valid bool
		}{
			{"200 runes pass", []string{strings.Repeat("é", 200)}, true},
			{"256 runes pass", []string{strings.Repeat("é", 256)}, true},
			{"257 runes fail", []string{strings.Repeat("é", 257)}, false},
			{"empty fails", []string{""}, false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				var message LogicalMessage
				require.NoError(t, json.Unmarshal(b, &message))
				message.ContextRefs = tc.refs
				if tc.valid {
					require.NoError(t, message.Validate())
				} else {
					require.ErrorIs(t, message.Validate(), ErrInvalid)
				}
			})
		}
	})

	t.Run("evidence", func(t *testing.T) {
		t.Parallel()
		valid := InferResult{
			Contract: ModelContractTriageMessage, InferenceProfile: InferenceProfileBase,
			Label:         string(TriageLabelNormal),
			Probabilities: map[string]float64{string(TriageLabelNormal): 1},
			CallRecord: GlimmerCallRecord{
				CallID: "call-1", Contract: ModelContractTriageMessage,
				InferenceProfile: InferenceProfileBase, BaseModelRepo: "example.invalid/r",
				BaseModelRevision: "v1", Runtime: "vllm", LatencyMS: 1,
				RedactionPolicyVersion: "1",
				InputSHA256:            "0000000000000000000000000000000000000000000000000000000000000000",
				OutputSHA256:           "0000000000000000000000000000000000000000000000000000000000000000",
				Status:                 GlimmerCallStatusOK, CreatedAt: testTimesOnly(),
			},
		}
		require.NoError(t, valid.Validate())
		for _, tc := range []struct {
			name     string
			evidence []string
			valid    bool
		}{
			{"120 runes pass", []string{strings.Repeat("é", 120)}, true},
			{"121 runes fail", []string{strings.Repeat("é", 121)}, false},
			{"empty fails", []string{""}, false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				result := valid
				result.Evidence = tc.evidence
				if tc.valid {
					require.NoError(t, result.Validate())
				} else {
					require.ErrorIs(t, result.Validate(), ErrInvalid)
				}
			})
		}
	})
}

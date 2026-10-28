package protocol

// This file holds the contract constants and the root registries. Roots()
// lists every versioned document with a JSON Schema and fixtures; later
// slices append their roots here without touching the v1 entries.

const (
	// SchemaVersion is the schema_version of every versioned document.
	SchemaVersion = 1
	// SchemaDialect is the JSON Schema dialect of schemas/v1/.
	SchemaDialect = "https://json-schema.org/draft/2020-12/schema"
	// SchemaIDBase is the $id prefix of schemas/v1/, built from the real
	// module path in go.mod.
	SchemaIDBase = "https://github.com/smellak/gifford/schemas/v1/"
	// WireProtocol is the protocol name carried in every wire envelope.
	WireProtocol = "gifford-message"
	// WireVersion is the wire envelope version.
	WireVersion = 1
	// WireContentType is the content type of every wire envelope.
	WireContentType = "application/gifford-message+json"
	// JSONRPCVersion is the JSON-RPC version of the Control API.
	JSONRPCVersion = "2.0"
	// MilestoneV1 marks roots frozen at contracts-v1.
	MilestoneV1 = "contracts-v1"
	// MilestoneV11 marks roots added at contracts-v1.1.
	MilestoneV11 = "contracts-v1.1"
)

// Root describes one versioned document: its schema file, its fixture
// directory and a constructor for its zero value.
type Root struct {
	// Name is the schema and fixture name (snake_case of the Go type).
	Name string
	// Since is the milestone that introduced the root.
	Since string
	// New returns a pointer to a zero value of the root type.
	New func() any
}

// Roots returns every registered root, v1 first. The slice is fresh.
func Roots() []Root {
	return []Root{
		{Name: "event_envelope", Since: MilestoneV1, New: func() any { return &EventEnvelope{} }},
		{Name: "capture_attached", Since: MilestoneV1, New: func() any { return &CaptureAttached{} }},
		{Name: "capture_initial_screen", Since: MilestoneV1, New: func() any { return &CaptureInitialScreen{} }},
		{Name: "capture_chunk", Since: MilestoneV1, New: func() any { return &CaptureChunk{} }},
		{Name: "capture_resize", Since: MilestoneV1, New: func() any { return &CaptureResize{} }},
		{Name: "capture_rotated", Since: MilestoneV1, New: func() any { return &CaptureRotated{} }},
		{Name: "capture_pane_recreated", Since: MilestoneV1, New: func() any { return &CapturePaneRecreated{} }},
		{Name: "capture_reattached", Since: MilestoneV1, New: func() any { return &CaptureReattached{} }},
		{Name: "capture_gap_detected", Since: MilestoneV1, New: func() any { return &CaptureGapDetected{} }},
		{Name: "capture_quota_reached", Since: MilestoneV1, New: func() any { return &CaptureQuotaReached{} }},
		{Name: "capture_stopped", Since: MilestoneV1, New: func() any { return &CaptureStopped{} }},
		{Name: "capability_manifest", Since: MilestoneV1, New: func() any { return &CapabilityManifest{} }},
		{Name: "state_snapshot", Since: MilestoneV1, New: func() any { return &StateSnapshot{} }},
		{Name: "logical_message", Since: MilestoneV1, New: func() any { return &LogicalMessage{} }},
		{Name: "wire_envelope", Since: MilestoneV1, New: func() any { return &WireEnvelope{} }},
		{Name: "orchestration_request", Since: MilestoneV1, New: func() any { return &OrchestrationRequest{} }},
		{Name: "orchestrator_decision", Since: MilestoneV1, New: func() any { return &OrchestratorDecision{} }},
		{Name: "infer_request", Since: MilestoneV1, New: func() any { return &InferRequest{} }},
		{Name: "infer_result", Since: MilestoneV1, New: func() any { return &InferResult{} }},
		{Name: "gateway_profile", Since: MilestoneV1, New: func() any { return &GatewayProfile{} }},
		{Name: "activate_adapter_request", Since: MilestoneV1, New: func() any { return &ActivateAdapterRequest{} }},
		{Name: "rollback_to_base_request", Since: MilestoneV1, New: func() any { return &RollbackToBaseRequest{} }},
		{Name: "glimmer_call_record", Since: MilestoneV1, New: func() any { return &GlimmerCallRecord{} }},
		{Name: "agent_frame", Since: MilestoneV1, New: func() any { return &AgentFrame{} }},
		{Name: "register_agent", Since: MilestoneV1, New: func() any { return &RegisterAgent{} }},
		{Name: "register_agent_result", Since: MilestoneV1, New: func() any { return &RegisterAgentResult{} }},
		{Name: "agent_target", Since: MilestoneV1, New: func() any { return &AgentTarget{} }},
		{Name: "input_guard_report", Since: MilestoneV1, New: func() any { return &InputGuardReport{} }},
		{Name: "delivery_result", Since: MilestoneV1, New: func() any { return &DeliveryResult{} }},
		{Name: "health_report", Since: MilestoneV1, New: func() any { return &HealthReport{} }},
		{Name: "message_ack_report", Since: MilestoneV1, New: func() any { return &MessageAckReport{} }},
		{Name: "artifact_event_report", Since: MilestoneV1, New: func() any { return &ArtifactEventReport{} }},
		{Name: "native_delivery", Since: MilestoneV1, New: func() any { return &NativeDelivery{} }},
		{Name: "prepare_delivery", Since: MilestoneV1, New: func() any { return &PrepareDelivery{} }},
		{Name: "commit_delivery", Since: MilestoneV1, New: func() any { return &CommitDelivery{} }},
		{Name: "cancel_delivery", Since: MilestoneV1, New: func() any { return &CancelDelivery{} }},
		{Name: "notify_message_available", Since: MilestoneV1, New: func() any { return &NotifyMessageAvailable{} }},
		{Name: "notify_artifact_available", Since: MilestoneV1, New: func() any { return &NotifyArtifactAvailable{} }},
		{Name: "frame_error", Since: MilestoneV1, New: func() any { return &FrameError{} }},
		{Name: "channel_control", Since: MilestoneV11, New: func() any { return &ChannelControl{} }},
		{Name: "channel_state", Since: MilestoneV11, New: func() any { return &ChannelState{} }},
		{Name: "agent_channel", Since: MilestoneV11, New: func() any { return &AgentChannel{} }},
		{Name: "link_state", Since: MilestoneV11, New: func() any { return &LinkState{} }},
		{Name: "link_list", Since: MilestoneV11, New: func() any { return &LinkList{} }},
		{Name: "loop", Since: MilestoneV11, New: func() any { return &Loop{} }},
		{Name: "loop_list", Since: MilestoneV11, New: func() any { return &LoopList{} }},
		{Name: "approval_request", Since: MilestoneV11, New: func() any { return &ApprovalRequest{} }},
		{Name: "approval_list", Since: MilestoneV11, New: func() any { return &ApprovalList{} }},
		{Name: "effective_view", Since: MilestoneV11, New: func() any { return &EffectiveView{} }},
		{Name: "explanation", Since: MilestoneV11, New: func() any { return &Explanation{} }},
		{Name: "decision_record", Since: MilestoneV11, New: func() any { return &DecisionRecord{} }},
		{Name: "admission_result", Since: MilestoneV11, New: func() any { return &AdmissionResult{} }},
		{Name: "object_control_state", Since: MilestoneV11, New: func() any { return &ObjectControlState{} }},
		{Name: "channel_event_payload", Since: MilestoneV11, New: func() any { return &ChannelEventPayload{} }},
	}
}

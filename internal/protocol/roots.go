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
	// MilestoneV12 marks roots added at contracts-v1.2.
	MilestoneV12 = "contracts-v1.2"
	// MilestoneV13 marks roots added at contracts-v1.3.
	MilestoneV13 = "contracts-v1.3"
	// MilestoneV14 marks roots added at contracts-v1.4.
	MilestoneV14 = "contracts-v1.4"
	// MilestoneV15 marks roots added at contracts-v1.5.
	MilestoneV15 = "contracts-v1.5"
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
	// Title overrides the reflected Go type name when a frozen compatibility
	// view backs an older published root.
	Title string
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
		{Name: "infer_result", Since: MilestoneV1, New: func() any { return &legacyInferResult{} }, Title: "InferResult"},
		{Name: "gateway_profile", Since: MilestoneV1, New: func() any { return &GatewayProfile{} }},
		{Name: "activate_adapter_request", Since: MilestoneV1, New: func() any { return &ActivateAdapterRequest{} }},
		{Name: "rollback_to_base_request", Since: MilestoneV1, New: func() any { return &RollbackToBaseRequest{} }},
		{Name: "glimmer_call_record", Since: MilestoneV1, New: func() any { return &legacyGlimmerCallRecord{} }, Title: "GlimmerCallRecord"},
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
		{Name: "rpc_request", Since: MilestoneV11, New: func() any { return &RPCRequest{} }},
		{Name: "rpc_response", Since: MilestoneV11, New: func() any { return &RPCResponse{} }},
		{Name: "empty_params", Since: MilestoneV11, New: func() any { return &EmptyParams{} }},
		{Name: "node_params", Since: MilestoneV11, New: func() any { return &NodeParams{} }},
		{Name: "set_global_params", Since: MilestoneV11, New: func() any { return &SetGlobalParams{} }},
		{Name: "set_node_params", Since: MilestoneV11, New: func() any { return &SetNodeParams{} }},
		{Name: "effective_params", Since: MilestoneV11, New: func() any { return &EffectiveParams{} }},
		{Name: "explain_params", Since: MilestoneV11, New: func() any { return &ExplainParams{} }},
		{Name: "agent_params", Since: MilestoneV11, New: func() any { return &AgentParams{} }},
		{Name: "set_agent_channel_params", Since: MilestoneV11, New: func() any { return &SetAgentChannelParams{} }},
		{Name: "set_agent_mode_params", Since: MilestoneV11, New: func() any { return &SetAgentModeParams{} }},
		{Name: "link_params", Since: MilestoneV11, New: func() any { return &LinkParams{} }},
		{Name: "list_links_params", Since: MilestoneV11, New: func() any { return &ListLinksParams{} }},
		{Name: "set_link_params", Since: MilestoneV11, New: func() any { return &SetLinkParams{} }},
		{Name: "delete_link_override_params", Since: MilestoneV11, New: func() any { return &DeleteLinkOverrideParams{} }},
		{Name: "create_loop_params", Since: MilestoneV11, New: func() any { return &CreateLoopParams{} }},
		{Name: "loop_params", Since: MilestoneV11, New: func() any { return &LoopParams{} }},
		{Name: "list_loops_params", Since: MilestoneV11, New: func() any { return &ListLoopsParams{} }},
		{Name: "set_loop_mode_params", Since: MilestoneV11, New: func() any { return &SetLoopModeParams{} }},
		{Name: "advance_loop_params", Since: MilestoneV11, New: func() any { return &AdvanceLoopParams{} }},
		{Name: "extend_loop_budget_params", Since: MilestoneV11, New: func() any { return &ExtendLoopBudgetParams{} }},
		{Name: "stop_loop_params", Since: MilestoneV11, New: func() any { return &StopLoopParams{} }},
		{Name: "complete_loop_params", Since: MilestoneV11, New: func() any { return &CompleteLoopParams{} }},
		{Name: "list_approvals_params", Since: MilestoneV11, New: func() any { return &ListApprovalsParams{} }},
		{Name: "approval_params", Since: MilestoneV11, New: func() any { return &ApprovalParams{} }},
		{Name: "resolve_approval_params", Since: MilestoneV11, New: func() any { return &ResolveApprovalParams{} }},
		{Name: "approve_message_params", Since: MilestoneV11, New: func() any { return &ApproveMessageParams{} }},
		{Name: "reject_message_params", Since: MilestoneV11, New: func() any { return &RejectMessageParams{} }},
		{Name: "release_hold_params", Since: MilestoneV11, New: func() any { return &ReleaseHoldParams{} }},
		{Name: "agent_list_params", Since: MilestoneV11, New: func() any { return &AgentListParams{} }},
		{Name: "message_list_params", Since: MilestoneV11, New: func() any { return &MessageListParams{} }},
		{Name: "decision_list_params", Since: MilestoneV11, New: func() any { return &DecisionListParams{} }},
		{Name: "decision_get_params", Since: MilestoneV11, New: func() any { return &DecisionGetParams{} }},
		{Name: "agent_list_result", Since: MilestoneV11, New: func() any { return &AgentListResult{} }},
		{Name: "message_list_result", Since: MilestoneV11, New: func() any { return &MessageListResult{} }},
		{Name: "decision_list_result", Since: MilestoneV11, New: func() any { return &DecisionListResult{} }},
		{Name: "decision_get_result", Since: MilestoneV11, New: func() any { return &DecisionGetResult{} }},
		{Name: "model_status_result", Since: MilestoneV11, New: func() any { return &legacyModelStatusResult{} }, Title: "ModelStatusResult"},
		{Name: "rpc_request_v1_2", Since: MilestoneV12, New: func() any { return &RPCRequestV12{} }},
		{Name: "notification_list_params", Since: MilestoneV12, New: func() any { return &NotificationListParams{} }},
		{Name: "notification_list_item", Since: MilestoneV12, New: func() any { return &NotificationListItem{} }},
		{Name: "notification_list_result", Since: MilestoneV12, New: func() any { return &NotificationListResult{} }},
		{Name: "agent_frame_v1_3", Since: MilestoneV13, New: func() any { return &AgentFrameV13{} }},
		{Name: "state_observation_report", Since: MilestoneV13, New: func() any { return &StateObservationReport{} }},
		{Name: "agent_frame_v1_4", Since: MilestoneV14, New: func() any { return &AgentFrameV14{} }},
		{Name: "rpc_request_v1_4", Since: MilestoneV14, New: func() any { return &RPCRequestV14{} }},
		{Name: "delivery_list_result", Since: MilestoneV14, New: func() any { return &DeliveryListResult{} }},
		{Name: "announce_support", Since: MilestoneV14, New: func() any { return &AgentSocketSupport{} }},
		{Name: "emit_event_result", Since: MilestoneV14, New: func() any { return &EmitEventResult{} }},
		{Name: "rpc_request_v1_5", Since: MilestoneV15, New: func() any { return &RPCRequestV15{} }},
		{Name: "infer_result_v1_5", Since: MilestoneV15, New: func() any { return &InferResult{} }},
		{Name: "glimmer_call_record_v1_5", Since: MilestoneV15, New: func() any { return &GlimmerCallRecord{} }},
		{Name: "model_status_result_v1_5", Since: MilestoneV15, New: func() any { return &ModelStatusResult{} }},
		{Name: "register_model_metadata_request", Since: MilestoneV15, New: func() any { return &RegisterModelMetadataRequest{} }},
		{Name: "model_training_run_metadata", Since: MilestoneV15, New: func() any { return &ModelTrainingRunMetadata{} }},
		{Name: "model_adapter_metadata", Since: MilestoneV15, New: func() any { return &ModelAdapterMetadata{} }},
		{Name: "register_model_pipeline_request", Since: MilestoneV15, New: func() any { return &RegisterModelPipelineRequest{} }},
		{Name: "model_metadata_registration_result", Since: MilestoneV15, New: func() any { return &ModelMetadataRegistrationResult{} }},
		{Name: "model_selection", Since: MilestoneV15, New: func() any { return &ModelSelection{} }},
		{Name: "model_activation_result", Since: MilestoneV15, New: func() any { return &ModelActivationResult{} }},
	}
}

package protocol

import (
	"fmt"
	"time"
)

// This file holds the Core ↔ Proxy/Native Adapter frames of §5.7:
// length-prefixed JSON on agents/<agent_id>.sock. The length prefix is
// transport and not part of this contract.

// AgentOp names an agent-socket operation. The DELIVER_NATIVE_EVENT value
// keeps its longer constant name so the delivery-call guard never matches
// it; the prepare and commit op constants are named the same way.
type AgentOp string

// AgentOp values.
const (
	AgentOpRegisterAgent           AgentOp = "register_agent"
	AgentOpEmitEvent               AgentOp = "emit_event"
	AgentOpHeartbeat               AgentOp = "heartbeat"
	AgentOpReportInputGuard        AgentOp = "report_input_guard"
	AgentOpReportDeliveryResult    AgentOp = "report_delivery_result"
	AgentOpReportHealth            AgentOp = "report_health"
	AgentOpReportStateObservation  AgentOp = "report_state_observation"
	AgentOpReportMessageAck        AgentOp = "report_message_ack"
	AgentOpReportArtifactEvent     AgentOp = "report_artifact_event"
	AgentOpRegisterAgentResult     AgentOp = "register_agent_result"
	AgentOpAnnounceSupport         AgentOp = "announce_support"
	AgentOpEmitEventResult         AgentOp = "emit_event_result"
	AgentOpNativeDelivery          AgentOp = "deliver_native_event"
	AgentOpPrepareDelivery         AgentOp = "prepare_pty_delivery"
	AgentOpCommitDelivery          AgentOp = "commit_pty_delivery"
	AgentOpCancelDelivery          AgentOp = "cancel_delivery"
	AgentOpResync                  AgentOp = "resync"
	AgentOpPauseAutomation         AgentOp = "pause_automation"
	AgentOpResumeAutomation        AgentOp = "resume_automation"
	AgentOpNotifyMessageAvailable  AgentOp = "notify_message_available"
	AgentOpNotifyArtifactAvailable AgentOp = "notify_artifact_available"
	AgentOpError                   AgentOp = "error"
)

// EnumValues returns the ops in table order.
func (AgentOp) EnumValues() []string {
	return []string{string(AgentOpRegisterAgent), string(AgentOpEmitEvent), string(AgentOpHeartbeat), string(AgentOpReportInputGuard), string(AgentOpReportDeliveryResult), string(AgentOpReportHealth), string(AgentOpReportStateObservation), string(AgentOpReportMessageAck), string(AgentOpReportArtifactEvent), string(AgentOpRegisterAgentResult), string(AgentOpAnnounceSupport), string(AgentOpEmitEventResult), string(AgentOpNativeDelivery), string(AgentOpPrepareDelivery), string(AgentOpCommitDelivery), string(AgentOpCancelDelivery), string(AgentOpResync), string(AgentOpPauseAutomation), string(AgentOpResumeAutomation), string(AgentOpNotifyMessageAvailable), string(AgentOpNotifyArtifactAvailable), string(AgentOpError)}
}

// Valid reports whether the value is a known op.
func (v AgentOp) Valid() bool {
	switch v {
	case AgentOpRegisterAgent, AgentOpEmitEvent, AgentOpHeartbeat, AgentOpReportInputGuard,
		AgentOpReportDeliveryResult, AgentOpReportHealth, AgentOpReportStateObservation, AgentOpReportMessageAck,
		AgentOpReportArtifactEvent, AgentOpRegisterAgentResult, AgentOpAnnounceSupport,
		AgentOpEmitEventResult, AgentOpNativeDelivery,
		AgentOpPrepareDelivery, AgentOpCommitDelivery, AgentOpCancelDelivery, AgentOpResync,
		AgentOpPauseAutomation, AgentOpResumeAutomation, AgentOpNotifyMessageAvailable,
		AgentOpNotifyArtifactAvailable, AgentOpError:
		return true
	default:
		return false
	}
}

// AgentOpSpec binds an op to its direction and body type.
type AgentOpSpec struct {
	// Op is the frame op value.
	Op AgentOp
	// FromCore is true for Core-originated ops. Error frames travel both
	// ways; the flag marks the Core side.
	FromCore bool
	// NewBody returns a pointer to a zero value of the op body type.
	NewBody func() any
}

// AgentOpSpecs returns every op spec in table order. The slice is fresh.
func AgentOpSpecs() []AgentOpSpec {
	return []AgentOpSpec{
		{Op: AgentOpRegisterAgent, FromCore: false, NewBody: func() any { return &RegisterAgent{} }},
		{Op: AgentOpEmitEvent, FromCore: false, NewBody: func() any { return &EventEnvelope{} }},
		{Op: AgentOpHeartbeat, FromCore: false, NewBody: func() any { return &AgentTarget{} }},
		{Op: AgentOpReportInputGuard, FromCore: false, NewBody: func() any { return &InputGuardReport{} }},
		{Op: AgentOpReportDeliveryResult, FromCore: false, NewBody: func() any { return &DeliveryResult{} }},
		{Op: AgentOpReportHealth, FromCore: false, NewBody: func() any { return &HealthReport{} }},
		{Op: AgentOpReportStateObservation, FromCore: false, NewBody: func() any { return &StateObservationReport{} }},
		{Op: AgentOpReportMessageAck, FromCore: false, NewBody: func() any { return &MessageAckReport{} }},
		{Op: AgentOpReportArtifactEvent, FromCore: false, NewBody: func() any { return &ArtifactEventReport{} }},
		{Op: AgentOpRegisterAgentResult, FromCore: true, NewBody: func() any { return &RegisterAgentResult{} }},
		{Op: AgentOpAnnounceSupport, FromCore: true, NewBody: func() any { return &AgentSocketSupport{} }},
		{Op: AgentOpEmitEventResult, FromCore: true, NewBody: func() any { return &EmitEventResult{} }},
		{Op: AgentOpNativeDelivery, FromCore: true, NewBody: func() any { return &NativeDelivery{} }},
		{Op: AgentOpPrepareDelivery, FromCore: true, NewBody: func() any { return &PrepareDelivery{} }},
		{Op: AgentOpCommitDelivery, FromCore: true, NewBody: func() any { return &CommitDelivery{} }},
		{Op: AgentOpCancelDelivery, FromCore: true, NewBody: func() any { return &CancelDelivery{} }},
		{Op: AgentOpResync, FromCore: true, NewBody: func() any { return &AgentTarget{} }},
		{Op: AgentOpPauseAutomation, FromCore: true, NewBody: func() any { return &AgentTarget{} }},
		{Op: AgentOpResumeAutomation, FromCore: true, NewBody: func() any { return &AgentTarget{} }},
		{Op: AgentOpNotifyMessageAvailable, FromCore: true, NewBody: func() any { return &NotifyMessageAvailable{} }},
		{Op: AgentOpNotifyArtifactAvailable, FromCore: true, NewBody: func() any { return &NotifyArtifactAvailable{} }},
		{Op: AgentOpError, FromCore: false, NewBody: func() any { return &FrameError{} }},
	}
}

// lookupOpSpec returns the spec for an op, or nil.
func lookupOpSpec(op AgentOp) *AgentOpSpec {
	for _, spec := range AgentOpSpecs() {
		if spec.Op == op {
			return &spec
		}
	}
	return nil
}

// AgentFrame is one length-prefixed agent-socket frame.
type AgentFrame struct {
	Op      AgentOp   `json:"op"`
	FrameID int64     `json:"frame_id" jsonschema:"minimum=1"`
	ReplyTo *int64    `json:"reply_to,omitempty" jsonschema:"minimum=1"`
	SentAt  time.Time `json:"sent_at"`
	Body    Payload   `json:"body"`
}

// AgentFrameV13 is the additive agent-socket envelope published for
// contracts-v1.3. Consumers that may send report_state_observation validate
// frames against agent_frame_v1_3.schema.json. The original agent_frame
// schema remains frozen. Transmission is negotiated by the v1.4 Core support
// announcement: without that announcement a Proxy sends no observation. An
// older Core therefore never receives the additive operation.
type AgentFrameV13 AgentFrame

// Validate applies the current agent-frame rules.
func (f AgentFrameV13) Validate() error { return AgentFrame(f).Validate() }

// AgentFrameV14 is the additive agent-socket envelope published for
// contracts-v1.4. It adds the Core support announcement and durable-event
// result while keeping the v1 and v1.3 schemas frozen.
type AgentFrameV14 AgentFrame

// Validate applies the current agent-frame rules.
func (f AgentFrameV14) Validate() error { return AgentFrame(f).Validate() }

// Validate checks the frame and decodes its body.
func (f AgentFrame) Validate() error {
	if err := checkRoot(&f); err != nil {
		return err
	}
	if f.ReplyTo == nil {
		switch f.Op {
		case AgentOpRegisterAgentResult:
			return &ValidationError{Path: "reply_to", Reason: "required for register_agent_result"}
		case AgentOpEmitEventResult:
			return &ValidationError{Path: "reply_to", Reason: "required for emit_event_result"}
		default:
		}
	}
	spec := lookupOpSpec(f.Op)
	if spec == nil {
		return &ValidationError{Path: "op", Reason: "unknown op"}
	}
	body := spec.NewBody()
	if err := decodeStrict(f.Body, body); err != nil {
		return &ValidationError{Path: "body", Reason: fmt.Sprintf("bad body: %v", err)}
	}
	if v, ok := body.(nestedValidator); ok {
		if err := v.Validate(); err != nil {
			return prefixPath(err, "body")
		}
	}
	return nil
}

// RegisterAgent opens a connection: manifest plus process identity.
type RegisterAgent struct {
	Manifest CapabilityManifest `json:"manifest"`
	Process  *ProcessIdentity   `json:"process,omitempty"`
}

// Validate checks the registration.
func (r RegisterAgent) Validate() error {
	if err := checkRoot(&r); err != nil {
		return err
	}
	if r.Manifest.CaptureMode == CaptureModePTYProxy && r.Process == nil {
		return &ValidationError{Path: familyProcess, Reason: "required with capture mode pty_proxy"}
	}
	return nil
}

// RegisterAgentResult answers a registration.
type RegisterAgentResult struct {
	AgentID   AgentID  `json:"agent_id"`
	SessionID OpaqueID `json:"session_id"`
}

// Validate checks the result.
func (r RegisterAgentResult) Validate() error {
	return checkRoot(&r)
}

// AgentSocketSupport advertises the additive Proxy reports this Core can accept
// on the current connection. The Core sends exactly one announcement,
// immediately after register_agent_result, and sends a fresh one only after a
// reconnect. A Proxy keeps the first announcement and ignores a duplicate
// without answering it. The Core omits the announcement when both functions
// are false; the all-false value remains valid for conservative readers.
type AgentSocketSupport struct {
	ReportStateObservation bool `json:"report_state_observation"`
	DurableEvents          bool `json:"durable_events"`
}

// Validate checks the closed support announcement.
func (s AgentSocketSupport) Validate() error { return checkRoot(&s) }

// EmitEventStatus is the terminal durable outcome for one emitted event.
type EmitEventStatus string

// EmitEventStatus values.
const (
	EmitEventStatusStored        EmitEventStatus = "STORED"
	EmitEventStatusAlreadyStored EmitEventStatus = "ALREADY_STORED"
	EmitEventStatusRejected      EmitEventStatus = "REJECTED"
)

// EnumValues returns durable event outcomes in success-to-rejection order.
func (EmitEventStatus) EnumValues() []string {
	return []string{
		string(EmitEventStatusStored),
		string(EmitEventStatusAlreadyStored),
		string(EmitEventStatusRejected),
	}
}

// Valid reports whether the durable event outcome is known.
func (v EmitEventStatus) Valid() bool {
	return v == EmitEventStatusStored || v == EmitEventStatusAlreadyStored ||
		v == EmitEventStatusRejected
}

// EmitEventRejectReason is a fixed rejection reason. It cannot carry event
// payload, screen content or free-form diagnostics.
type EmitEventRejectReason string

// EmitEventRejectReason values.
const (
	EmitEventRejectReasonNotAllowed EmitEventRejectReason = "NOT_ALLOWED"
	EmitEventRejectReasonConflict   EmitEventRejectReason = "CONFLICT"
)

// EnumValues returns fixed event rejection reasons in table order.
func (EmitEventRejectReason) EnumValues() []string {
	return []string{
		string(EmitEventRejectReasonNotAllowed),
		string(EmitEventRejectReasonConflict),
	}
}

// Valid reports whether the event rejection reason is known.
func (v EmitEventRejectReason) Valid() bool {
	return v == EmitEventRejectReasonNotAllowed || v == EmitEventRejectReasonConflict
}

// EmitEventResult acknowledges one emit_event only after its durable outcome
// is known. REJECTED is terminal for that event but is not a storage ACK.
type EmitEventResult struct {
	EventID ULID                  `json:"event_id"`
	Status  EmitEventStatus       `json:"status"`
	Reason  EmitEventRejectReason `json:"reason,omitempty"`
}

// Validate checks the result and its status-specific fixed reason.
func (r EmitEventResult) Validate() error {
	if err := checkRoot(&r); err != nil {
		return err
	}
	if (r.Status == EmitEventStatusRejected) != (r.Reason != "") {
		return &ValidationError{Path: "reason", Reason: "present iff status is REJECTED"}
	}
	return nil
}

// AgentTarget addresses one agent.
type AgentTarget struct {
	AgentID AgentID `json:"agent_id"`
}

// Validate checks the target.
func (t AgentTarget) Validate() error {
	return checkRoot(&t)
}

// InputGuardReport carries one draft-state observation.
type InputGuardReport struct {
	AgentID           AgentID            `json:"agent_id"`
	SessionID         OpaqueID           `json:"session_id"`
	DraftState        DraftState         `json:"draft_state"`
	ActivityState     ActivityState      `json:"activity_state"`
	DraftGeneration   int64              `json:"draft_generation" jsonschema:"minimum=0"`
	InputGuardVersion int64              `json:"input_guard_version" jsonschema:"minimum=0"`
	LastHumanKeyAt    time.Time          `json:"last_human_key_at,omitzero"`
	DetectionSource   InputGuardProvider `json:"detection_source"`
	Confidence        float64            `json:"confidence" jsonschema:"minimum=0,maximum=1"`
}

// Validate checks the report.
func (r InputGuardReport) Validate() error {
	if err := checkRoot(&r); err != nil {
		return err
	}
	if r.DetectionSource == InputGuardProviderNone {
		return &ValidationError{Path: "detection_source", Reason: "never none in a report"}
	}
	if r.DetectionSource == InputGuardProviderPTYProxy && r.Confidence != 1 {
		return &ValidationError{Path: "confidence", Reason: "pty_proxy reports 1"}
	}
	return nil
}

// DeliveryResult reports what the Proxy did with a delivery.
type DeliveryResult struct {
	DeliveryID                OpaqueID           `json:"delivery_id"`
	AgentID                   AgentID            `json:"agent_id"`
	Result                    DeliveryResultKind `json:"result"`
	ObservedAt                time.Time          `json:"observed_at"`
	ObservedDraftGeneration   int64              `json:"observed_draft_generation" jsonschema:"minimum=0"`
	ObservedInputGuardVersion *int64             `json:"observed_input_guard_version,omitempty" jsonschema:"minimum=0"`
	ControlVersions           ControlVersions    `json:"control_versions"`
	WriterLeaseID             OpaqueID           `json:"writer_lease_id,omitempty"`
	CancelReason              CancelReason       `json:"cancel_reason,omitempty"`
	InjectMode                InjectMode         `json:"inject_mode,omitempty"`
	Error                     string             `json:"error,omitempty" jsonschema:"maxLength=512"`
}

// Validate checks the result.
func (r DeliveryResult) Validate() error {
	if err := checkRoot(&r); err != nil {
		return err
	}
	if r.Result == DeliveryResultKindCommitted && r.WriterLeaseID == "" {
		return &ValidationError{Path: "writer_lease_id", Reason: "required when COMMITTED"}
	}
	if r.Result == DeliveryResultKindCancelled && r.CancelReason == "" {
		return &ValidationError{Path: "cancel_reason", Reason: "required when CANCELLED"}
	}
	if r.Result == DeliveryResultKindFailed && r.Error == "" {
		return &ValidationError{Path: "error", Reason: "required when FAILED"}
	}
	return nil
}

// HealthReport carries one health observation.
type HealthReport struct {
	AgentID     AgentID     `json:"agent_id"`
	HealthState HealthState `json:"health_state"`
	Detail      string      `json:"detail,omitempty" jsonschema:"maxLength=280"`
}

// Validate checks the report.
func (r HealthReport) Validate() error {
	return checkRoot(&r)
}

// MessageAckReport acks one message.
type MessageAckReport struct {
	AgentID   AgentID   `json:"agent_id"`
	MessageID ULID      `json:"message_id"`
	Status    AckStatus `json:"status"`
}

// Validate checks the ack.
func (r MessageAckReport) Validate() error {
	return checkRoot(&r)
}

// ArtifactEventReport reports one artifact state.
type ArtifactEventReport struct {
	AgentID    AgentID       `json:"agent_id"`
	ArtifactID OpaqueID      `json:"artifact_id"`
	Status     ArtifactState `json:"status"`
}

// Validate checks the report.
func (r ArtifactEventReport) Validate() error {
	return checkRoot(&r)
}

// MessageRef points at one admitted message.
type MessageRef struct {
	MessageID ULID     `json:"message_id"`
	FromAgent AgentID  `json:"from_agent"`
	Priority  Priority `json:"priority"`
	ThreadID  ThreadID `json:"thread_id"`
}

// Validate checks the reference.
func (r MessageRef) Validate() error {
	return checkRoot(&r)
}

// ArtifactRef points at one artifact.
type ArtifactRef struct {
	ArtifactID OpaqueID      `json:"artifact_id"`
	FromAgent  AgentID       `json:"from_agent"`
	Purpose    string        `json:"purpose" jsonschema:"minLength=1,maxLength=200"`
	State      ArtifactState `json:"state"`
}

// Validate checks the reference.
func (r ArtifactRef) Validate() error {
	return checkRoot(&r)
}

// NativeDelivery orders a native event delivery.
type NativeDelivery struct {
	DeliveryID             OpaqueID        `json:"delivery_id"`
	AgentID                AgentID         `json:"agent_id"`
	MessageRef             MessageRef      `json:"message_ref"`
	OrchestratorDecisionID ULID            `json:"orchestrator_decision_id"`
	ControlVersions        ControlVersions `json:"control_versions"`
}

// Validate checks the order.
func (d NativeDelivery) Validate() error {
	if err := checkRoot(&d); err != nil {
		return err
	}
	if len(d.ControlVersions) == 0 {
		return &ValidationError{Path: "control_versions", Reason: "non-empty"}
	}
	return nil
}

const approvedDeliveryKeyPrefix = "approved:"

// ApprovedDeliveryKey returns the idempotency key that binds one explicit
// human approval to its exact delivery id on the frozen v1 frame. An invalid
// or oversized id has no such key.
func ApprovedDeliveryKey(id OpaqueID) OpaqueID {
	key := OpaqueID(approvedDeliveryKeyPrefix + string(id))
	if !id.Valid() || !key.Valid() {
		return ""
	}
	return key
}

// PrepareDelivery orders a PTY delivery prepare.
type PrepareDelivery struct {
	DeliveryID                OpaqueID        `json:"delivery_id"`
	AgentID                   AgentID         `json:"agent_id"`
	MessageRef                MessageRef      `json:"message_ref"`
	NoticeLine                NoticeLine      `json:"notice_line"`
	OrchestratorDecisionID    ULID            `json:"orchestrator_decision_id"`
	IdempotencyKey            OpaqueID        `json:"idempotency_key,omitempty"`
	ExpectedStateVersion      int64           `json:"expected_state_version" jsonschema:"minimum=0"`
	ExpectedInputGuardVersion int64           `json:"expected_input_guard_version" jsonschema:"minimum=0"`
	ExpectedDraftGeneration   int64           `json:"expected_draft_generation" jsonschema:"minimum=0"`
	ControlVersions           ControlVersions `json:"control_versions"`
	InjectMode                InjectMode      `json:"inject_mode,omitempty"`
}

// ExplicitlyApproved reports whether the authenticated Core marked this exact
// delivery id as the one human-approved exception to an automation pause.
func (d PrepareDelivery) ExplicitlyApproved() bool {
	key := ApprovedDeliveryKey(d.DeliveryID)
	return key != "" && d.IdempotencyKey == key
}

// Validate checks the order.
func (d PrepareDelivery) Validate() error {
	if err := checkRoot(&d); err != nil {
		return err
	}
	if len(d.ControlVersions) == 0 {
		return &ValidationError{Path: "control_versions", Reason: "non-empty"}
	}
	return nil
}

// CommitDelivery orders a PTY delivery commit.
type CommitDelivery struct {
	DeliveryID    OpaqueID `json:"delivery_id"`
	WriterLeaseID OpaqueID `json:"writer_lease_id"`
}

// Validate checks the order.
func (d CommitDelivery) Validate() error {
	return checkRoot(&d)
}

// CancelDelivery aborts a delivery before commit.
type CancelDelivery struct {
	DeliveryID OpaqueID     `json:"delivery_id"`
	Reason     CancelReason `json:"reason"`
}

// Validate checks the order.
func (d CancelDelivery) Validate() error {
	return checkRoot(&d)
}

// NotifyMessageAvailable announces a message for one agent.
type NotifyMessageAvailable struct {
	MessageRef MessageRef `json:"message_ref"`
}

// Validate checks the announcement.
func (n NotifyMessageAvailable) Validate() error {
	return checkRoot(&n)
}

// NotifyArtifactAvailable announces an artifact for one agent.
type NotifyArtifactAvailable struct {
	ArtifactRef ArtifactRef `json:"artifact_ref"`
}

// Validate checks the announcement.
func (n NotifyArtifactAvailable) Validate() error {
	return checkRoot(&n)
}

// FrameError carries a frame-level error both ways.
type FrameError struct {
	Code    FrameErrorCode `json:"code"`
	Message string         `json:"message" jsonschema:"minLength=1,maxLength=512"`
}

// Validate checks the error.
func (e FrameError) Validate() error {
	return checkRoot(&e)
}

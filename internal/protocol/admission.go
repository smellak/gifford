package protocol

// This file holds the admission result and object control state of §6.6:
// what send_message, reply_to_message and reserve_artifact learn from the
// admission gate. A DENY is not an AdmissionResult: it is returned as the
// ErrorCode of the same name as the reason.

// AdmissionResult is the verdict of admitting one object.
type AdmissionResult struct {
	Result            AdmissionStatus `json:"result"`
	DecisionID        ChannelDecisionID `json:"decision_id"`
	MessageID         ULID            `json:"message_id,omitempty"`
	ArtifactID        OpaqueID        `json:"artifact_id,omitempty"`
	ControlReason     DecisionReason  `json:"control_reason,omitempty"`
	ApprovalRequestID ApprovalID      `json:"approval_request_id,omitempty"`
	ThreadID          ThreadID        `json:"thread_id"`
	LoopID            LoopID          `json:"loop_id,omitempty"`
	LoopGeneration    *int64          `json:"loop_generation,omitempty" jsonschema:"minimum=1"`
	TurnIndex         *int64          `json:"turn_index,omitempty" jsonschema:"minimum=1"`
	ControlVersion    int64           `json:"control_version" jsonschema:"minimum=0"`
}

// Validate checks the admission result.
func (r AdmissionResult) Validate() error {
	if err := checkRoot(&r); err != nil {
		return err
	}
	if (r.MessageID == "") == (r.ArtifactID == "") {
		return &ValidationError{Path: "message_id", Reason: "exactly one of message_id, artifact_id"}
	}
	loopFields := 0
	if r.LoopID != "" {
		loopFields++
	}
	if r.LoopGeneration != nil {
		loopFields++
	}
	if r.TurnIndex != nil {
		loopFields++
	}
	if loopFields != 0 && loopFields != 3 {
		return &ValidationError{Path: "loop_id", Reason: "loop fields all present or all absent"}
	}
	switch r.Result {
	case AdmissionStatusAccepted:
		if r.ControlReason != "" {
			return &ValidationError{Path: "control_reason", Reason: "ACCEPTED carries no reason"}
		}
		if r.ApprovalRequestID != "" {
			return &ValidationError{Path: "approval_request_id", Reason: "only APPROVAL_REQUIRED names a request"}
		}
	case AdmissionStatusAcceptedHeld:
		switch r.ControlReason {
		case DecisionReasonLinkHold, DecisionReasonLoopPaused,
			DecisionReasonLoopBudgetExhausted, DecisionReasonLoopPatternDetected,
			DecisionReasonControlUnavailable:
		default:
			return &ValidationError{Path: "control_reason", Reason: "a hold reason for ACCEPTED_HELD"}
		}
		if r.ApprovalRequestID != "" {
			return &ValidationError{Path: "approval_request_id", Reason: "only APPROVAL_REQUIRED names a request"}
		}
	case AdmissionStatusApprovalRequired:
		switch r.ControlReason {
		case DecisionReasonLinkApproval, DecisionReasonLoopStepTokenRequired,
			DecisionReasonAgentAutoContinueOff:
		default:
			return &ValidationError{Path: "control_reason", Reason: "an approval reason for APPROVAL_REQUIRED"}
		}
		if r.ApprovalRequestID == "" {
			return &ValidationError{Path: "approval_request_id", Reason: "APPROVAL_REQUIRED names the request"}
		}
	default:
		return &ValidationError{Path: "result", Reason: "unknown admission status"}
	}
	return nil
}

// ObjectControlState is the current control state of an admitted object:
// the result of message.approve, message.reject, message.release_hold.
type ObjectControlState struct {
	ObjectType     DecisionObjectType `json:"object_type"`
	ObjectID       OpaqueID           `json:"object_id"`
	ControlState   ControlState       `json:"control_state"`
	ControlReason  DecisionReason     `json:"control_reason"`
	ControlVersion int64              `json:"control_version" jsonschema:"minimum=0"`
	ApprovalID     ApprovalID         `json:"approval_id,omitempty"`
	Cancelled      bool               `json:"cancelled,omitempty"`
}

// Validate checks the object control state.
func (s ObjectControlState) Validate() error {
	if err := checkRoot(&s); err != nil {
		return err
	}
	if s.ObjectType != DecisionObjectTypeMessage && s.ObjectType != DecisionObjectTypeArtifact {
		return &ValidationError{Path: "object_type", Reason: "message or artifact"}
	}
	return nil
}

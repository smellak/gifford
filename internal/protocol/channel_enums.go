package protocol

// This file holds the Channel Control Plane enums of §6.1 (v1.1). The
// ControlMethod enum lands with the Control API in slice 4.

// ScopeType names a control scope.
type ScopeType string

// ScopeType values.
const (
	ScopeTypeGlobal ScopeType = "global"
	ScopeTypeNode   ScopeType = "node"
	ScopeTypeAgent  ScopeType = "agent"
	ScopeTypeLink   ScopeType = "link"
	ScopeTypeThread ScopeType = "thread"
	ScopeTypeLoop   ScopeType = "loop"
)

// EnumValues returns the scopes in table order.
func (ScopeType) EnumValues() []string {
	return []string{string(ScopeTypeGlobal), string(ScopeTypeNode), string(ScopeTypeAgent), string(ScopeTypeLink), string(ScopeTypeThread), string(ScopeTypeLoop)}
}

// Valid reports whether the value is a known scope.
func (v ScopeType) Valid() bool {
	switch v {
	case ScopeTypeGlobal, ScopeTypeNode, ScopeTypeAgent, ScopeTypeLink, ScopeTypeThread, ScopeTypeLoop:
		return true
	default:
		return false
	}
}

// Plane names a delivery plane.
type Plane string

// Plane values.
const (
	PlaneMessage  Plane = "message"
	PlaneArtifact Plane = "artifact"
	PlaneAll      Plane = "all"
)

// EnumValues returns the planes in table order.
func (Plane) EnumValues() []string {
	return []string{string(PlaneMessage), string(PlaneArtifact), string(PlaneAll)}
}

// Valid reports whether the value is a known plane.
func (v Plane) Valid() bool {
	switch v {
	case PlaneMessage, PlaneArtifact, PlaneAll:
		return true
	default:
		return false
	}
}

// ControlMode is the union mode stored on control rows; the legal subset
// depends on the scope (§6.2).
type ControlMode string

// ControlMode values.
const (
	ControlModeActive          ControlMode = "ACTIVE"
	ControlModePaused          ControlMode = "PAUSED"
	ControlModeDraining        ControlMode = "DRAINING"
	ControlModeOff             ControlMode = "OFF"
	ControlModeAllow           ControlMode = "ALLOW"
	ControlModeHold            ControlMode = "HOLD"
	ControlModeRequireApproval ControlMode = "REQUIRE_APPROVAL"
	ControlModeDeny            ControlMode = "DENY"
	ControlModeStopped         ControlMode = "STOPPED"
)

// EnumValues returns the modes in table order.
func (ControlMode) EnumValues() []string {
	return []string{string(ControlModeActive), string(ControlModePaused), string(ControlModeDraining), string(ControlModeOff), string(ControlModeAllow), string(ControlModeHold), string(ControlModeRequireApproval), string(ControlModeDeny), string(ControlModeStopped)}
}

// Valid reports whether the value is a known control mode.
func (v ControlMode) Valid() bool {
	switch v {
	case ControlModeActive, ControlModePaused, ControlModeDraining, ControlModeOff,
		ControlModeAllow, ControlModeHold, ControlModeRequireApproval, ControlModeDeny,
		ControlModeStopped:
		return true
	default:
		return false
	}
}

// ActorType names who wrote a control row.
type ActorType string

// ActorType values.
const (
	ActorTypeHuman       ActorType = "human"
	ActorTypeSystem      ActorType = "system"
	ActorTypePolicy      ActorType = "policy"
	ActorTypeRemoteOwner ActorType = "remote_owner"
)

// EnumValues returns the actors in table order.
func (ActorType) EnumValues() []string {
	return []string{string(ActorTypeHuman), string(ActorTypeSystem), string(ActorTypePolicy), string(ActorTypeRemoteOwner)}
}

// Valid reports whether the value is a known actor.
func (v ActorType) Valid() bool {
	switch v {
	case ActorTypeHuman, ActorTypeSystem, ActorTypePolicy, ActorTypeRemoteOwner:
		return true
	default:
		return false
	}
}

// ControlState is the hold/approval state of a concrete object.
type ControlState string

// ControlState values.
const (
	ControlStateAllowed          ControlState = "ALLOWED"
	ControlStateHeld             ControlState = "HELD"
	ControlStateApprovalRequired ControlState = "APPROVAL_REQUIRED"
	ControlStateDenied           ControlState = "DENIED"
)

// EnumValues returns the states in table order.
func (ControlState) EnumValues() []string {
	return []string{string(ControlStateAllowed), string(ControlStateHeld), string(ControlStateApprovalRequired), string(ControlStateDenied)}
}

// Valid reports whether the value is a known state.
func (v ControlState) Valid() bool {
	switch v {
	case ControlStateAllowed, ControlStateHeld, ControlStateApprovalRequired, ControlStateDenied:
		return true
	default:
		return false
	}
}

// AgentMode names the macro matching one agent's local flags.
type AgentMode string

// AgentMode values.
const (
	AgentModeFull        AgentMode = "FULL"
	AgentModeManual      AgentMode = "MANUAL"
	AgentModeReceiveOnly AgentMode = "RECEIVE_ONLY"
	AgentModeSendOnly    AgentMode = "SEND_ONLY"
	AgentModeOff         AgentMode = "OFF"
)

// EnumValues returns the modes in table order.
func (AgentMode) EnumValues() []string {
	return []string{string(AgentModeFull), string(AgentModeManual), string(AgentModeReceiveOnly), string(AgentModeSendOnly), string(AgentModeOff)}
}

// Valid reports whether the value is a known agent mode.
func (v AgentMode) Valid() bool {
	switch v {
	case AgentModeFull, AgentModeManual, AgentModeReceiveOnly, AgentModeSendOnly, AgentModeOff:
		return true
	default:
		return false
	}
}

// LoopOrigin names what opened a loop.
type LoopOrigin string

// LoopOrigin values.
const (
	LoopOriginHuman        LoopOrigin = "human"
	LoopOriginAgentRequest LoopOrigin = "agent_request"
	LoopOriginWorkflow     LoopOrigin = "workflow"
	LoopOriginImplicit     LoopOrigin = "implicit"
)

// EnumValues returns the origins in table order.
func (LoopOrigin) EnumValues() []string {
	return []string{string(LoopOriginHuman), string(LoopOriginAgentRequest), string(LoopOriginWorkflow), string(LoopOriginImplicit)}
}

// Valid reports whether the value is a known origin.
func (v LoopOrigin) Valid() bool {
	switch v {
	case LoopOriginHuman, LoopOriginAgentRequest, LoopOriginWorkflow, LoopOriginImplicit:
		return true
	default:
		return false
	}
}

// LoopStateReason names why a loop changed state.
type LoopStateReason string

// LoopStateReason values.
const (
	LoopStateReasonManual              LoopStateReason = "MANUAL"
	LoopStateReasonBudgetExhausted     LoopStateReason = "BUDGET_EXHAUSTED"
	LoopStateReasonLoopPatternDetected LoopStateReason = "LOOP_PATTERN_DETECTED"
	LoopStateReasonOwnerStop           LoopStateReason = "OWNER_STOP"
	LoopStateReasonPolicy              LoopStateReason = "POLICY"
)

// EnumValues returns the reasons in table order.
func (LoopStateReason) EnumValues() []string {
	return []string{string(LoopStateReasonManual), string(LoopStateReasonBudgetExhausted), string(LoopStateReasonLoopPatternDetected), string(LoopStateReasonOwnerStop), string(LoopStateReasonPolicy)}
}

// Valid reports whether the value is a known reason.
func (v LoopStateReason) Valid() bool {
	switch v {
	case LoopStateReasonManual, LoopStateReasonBudgetExhausted, LoopStateReasonLoopPatternDetected,
		LoopStateReasonOwnerStop, LoopStateReasonPolicy:
		return true
	default:
		return false
	}
}

// BudgetProfile names a loop budget profile.
type BudgetProfile string

// BudgetProfile values.
const (
	BudgetProfileDemo    BudgetProfile = "demo"
	BudgetProfileDefault BudgetProfile = "default"
	BudgetProfileStrict  BudgetProfile = "strict"
	BudgetProfileCustom  BudgetProfile = "custom"
)

// EnumValues returns the profiles in table order.
func (BudgetProfile) EnumValues() []string {
	return []string{string(BudgetProfileDemo), string(BudgetProfileDefault), string(BudgetProfileStrict), string(BudgetProfileCustom)}
}

// Valid reports whether the value is a known profile.
func (v BudgetProfile) Valid() bool {
	switch v {
	case BudgetProfileDemo, BudgetProfileDefault, BudgetProfileStrict, BudgetProfileCustom:
		return true
	default:
		return false
	}
}

// BudgetDimension names one loop budget dimension.
type BudgetDimension string

// BudgetDimension values.
const (
	BudgetDimensionMaxTurns               BudgetDimension = "max_turns"
	BudgetDimensionMaxMessages            BudgetDimension = "max_messages"
	BudgetDimensionMaxDurationMS          BudgetDimension = "max_duration_ms"
	BudgetDimensionMaxMessagesPerMinute   BudgetDimension = "max_messages_per_minute"
	BudgetDimensionMaxHops                BudgetDimension = "max_hops"
	BudgetDimensionMaxConsecutiveSamePair BudgetDimension = "max_consecutive_same_pair"
	BudgetDimensionMaxRetries             BudgetDimension = "max_retries"
	BudgetDimensionMaxArtifactCount       BudgetDimension = "max_artifact_count"
	BudgetDimensionMaxArtifactBytes       BudgetDimension = "max_artifact_bytes"
)

// EnumValues returns the dimensions in table order.
func (BudgetDimension) EnumValues() []string {
	return []string{string(BudgetDimensionMaxTurns), string(BudgetDimensionMaxMessages), string(BudgetDimensionMaxDurationMS), string(BudgetDimensionMaxMessagesPerMinute), string(BudgetDimensionMaxHops), string(BudgetDimensionMaxConsecutiveSamePair), string(BudgetDimensionMaxRetries), string(BudgetDimensionMaxArtifactCount), string(BudgetDimensionMaxArtifactBytes)}
}

// Valid reports whether the value is a known dimension.
func (v BudgetDimension) Valid() bool {
	switch v {
	case BudgetDimensionMaxTurns, BudgetDimensionMaxMessages, BudgetDimensionMaxDurationMS,
		BudgetDimensionMaxMessagesPerMinute, BudgetDimensionMaxHops, BudgetDimensionMaxConsecutiveSamePair,
		BudgetDimensionMaxRetries, BudgetDimensionMaxArtifactCount, BudgetDimensionMaxArtifactBytes:
		return true
	default:
		return false
	}
}

// DecisionStage names the gate that produced a decision.
type DecisionStage string

// DecisionStage values.
const (
	DecisionStageAdmission    DecisionStage = "admission"
	DecisionStageAcceptance   DecisionStage = "acceptance"
	DecisionStageDelivery     DecisionStage = "delivery"
	DecisionStageContinuation DecisionStage = "continuation"
	DecisionStageRevalidation DecisionStage = "revalidation"
)

// EnumValues returns the stages in table order.
func (DecisionStage) EnumValues() []string {
	return []string{string(DecisionStageAdmission), string(DecisionStageAcceptance), string(DecisionStageDelivery), string(DecisionStageContinuation), string(DecisionStageRevalidation)}
}

// Valid reports whether the value is a known stage.
func (v DecisionStage) Valid() bool {
	switch v {
	case DecisionStageAdmission, DecisionStageAcceptance, DecisionStageDelivery,
		DecisionStageContinuation, DecisionStageRevalidation:
		return true
	default:
		return false
	}
}

// DecisionObjectType names what a decision covers.
type DecisionObjectType string

// DecisionObjectType values.
const (
	DecisionObjectTypeMessage    DecisionObjectType = "message"
	DecisionObjectTypeArtifact   DecisionObjectType = "artifact"
	DecisionObjectTypeLoopAction DecisionObjectType = "loop_action"
)

// EnumValues returns the types in table order.
func (DecisionObjectType) EnumValues() []string {
	return []string{string(DecisionObjectTypeMessage), string(DecisionObjectTypeArtifact), string(DecisionObjectTypeLoopAction)}
}

// Valid reports whether the value is a known type.
func (v DecisionObjectType) Valid() bool {
	switch v {
	case DecisionObjectTypeMessage, DecisionObjectTypeArtifact, DecisionObjectTypeLoopAction:
		return true
	default:
		return false
	}
}

// DecisionReason names why a gate decided. The first six are the allow
// reasons.
type DecisionReason string

// DecisionReason values.
const (
	DecisionReasonAllowed                DecisionReason = "ALLOWED"
	DecisionReasonApprovedOnce           DecisionReason = "APPROVED_ONCE"
	DecisionReasonApprovedThread         DecisionReason = "APPROVED_THREAD"
	DecisionReasonHoldReleased           DecisionReason = "HOLD_RELEASED"
	DecisionReasonStepGranted            DecisionReason = "STEP_GRANTED"
	DecisionReasonBudgetExtended         DecisionReason = "BUDGET_EXTENDED"
	DecisionReasonChannelOff             DecisionReason = "CHANNEL_OFF"
	DecisionReasonChannelPaused          DecisionReason = "CHANNEL_PAUSED"
	DecisionReasonChannelDraining        DecisionReason = "CHANNEL_DRAINING"
	DecisionReasonNodePaused             DecisionReason = "NODE_PAUSED"
	DecisionReasonNodeOff                DecisionReason = "NODE_OFF"
	DecisionReasonNodeDraining           DecisionReason = "NODE_DRAINING"
	DecisionReasonAgentTXDisabled        DecisionReason = "AGENT_TX_DISABLED"
	DecisionReasonAgentRXDisabled        DecisionReason = "AGENT_RX_DISABLED"
	DecisionReasonAgentAutoDeliveryOff   DecisionReason = "AGENT_AUTO_DELIVERY_OFF"
	DecisionReasonAgentAutoContinueOff   DecisionReason = "AGENT_AUTO_CONTINUE_OFF"
	DecisionReasonLinkHold               DecisionReason = "LINK_HOLD"
	DecisionReasonLinkApproval           DecisionReason = "LINK_APPROVAL"
	DecisionReasonLinkDenied             DecisionReason = "LINK_DENIED"
	DecisionReasonLoopPaused             DecisionReason = "LOOP_PAUSED"
	DecisionReasonLoopStepTokenRequired  DecisionReason = DecisionReason("LOOP_STEP_TOKEN_REQUIRED")
	DecisionReasonLoopBudgetExhausted    DecisionReason = "LOOP_BUDGET_EXHAUSTED"
	DecisionReasonLoopPatternDetected    DecisionReason = "LOOP_PATTERN_DETECTED"
	DecisionReasonLoopDraining           DecisionReason = "LOOP_DRAINING"
	DecisionReasonLoopStopped            DecisionReason = "LOOP_STOPPED"
	DecisionReasonObjectHeld             DecisionReason = "OBJECT_HELD"
	DecisionReasonObjectApprovalPending  DecisionReason = "OBJECT_APPROVAL_PENDING"
	DecisionReasonControlVersionConflict DecisionReason = "CONTROL_VERSION_CONFLICT"
	DecisionReasonControlUnavailable     DecisionReason = "CONTROL_UNAVAILABLE"
	DecisionReasonControlChanged         DecisionReason = "CONTROL_CHANGED"
	DecisionReasonPolicyDenied           DecisionReason = "POLICY_DENIED"
)

// EnumValues returns the reasons in table order.
func (DecisionReason) EnumValues() []string {
	return []string{string(DecisionReasonAllowed), string(DecisionReasonApprovedOnce), string(DecisionReasonApprovedThread), string(DecisionReasonHoldReleased), string(DecisionReasonStepGranted), string(DecisionReasonBudgetExtended), string(DecisionReasonChannelOff), string(DecisionReasonChannelPaused), string(DecisionReasonChannelDraining), string(DecisionReasonNodePaused), string(DecisionReasonNodeOff), string(DecisionReasonNodeDraining), string(DecisionReasonAgentTXDisabled), string(DecisionReasonAgentRXDisabled), string(DecisionReasonAgentAutoDeliveryOff), string(DecisionReasonAgentAutoContinueOff), string(DecisionReasonLinkHold), string(DecisionReasonLinkApproval), string(DecisionReasonLinkDenied), string(DecisionReasonLoopPaused), string(DecisionReasonLoopStepTokenRequired), string(DecisionReasonLoopBudgetExhausted), string(DecisionReasonLoopPatternDetected), string(DecisionReasonLoopDraining), string(DecisionReasonLoopStopped), string(DecisionReasonObjectHeld), string(DecisionReasonObjectApprovalPending), string(DecisionReasonControlVersionConflict), string(DecisionReasonControlUnavailable), string(DecisionReasonControlChanged), string(DecisionReasonPolicyDenied)}
}

// Valid reports whether the value is a known reason.
func (v DecisionReason) Valid() bool {
	switch v {
	case DecisionReasonAllowed, DecisionReasonApprovedOnce, DecisionReasonApprovedThread,
		DecisionReasonHoldReleased, DecisionReasonStepGranted, DecisionReasonBudgetExtended,
		DecisionReasonChannelOff, DecisionReasonChannelPaused, DecisionReasonChannelDraining,
		DecisionReasonNodePaused, DecisionReasonNodeOff, DecisionReasonNodeDraining,
		DecisionReasonAgentTXDisabled, DecisionReasonAgentRXDisabled,
		DecisionReasonAgentAutoDeliveryOff, DecisionReasonAgentAutoContinueOff,
		DecisionReasonLinkHold, DecisionReasonLinkApproval, DecisionReasonLinkDenied,
		DecisionReasonLoopPaused, DecisionReasonLoopStepTokenRequired, DecisionReasonLoopBudgetExhausted,
		DecisionReasonLoopPatternDetected, DecisionReasonLoopDraining, DecisionReasonLoopStopped,
		DecisionReasonObjectHeld, DecisionReasonObjectApprovalPending, DecisionReasonControlVersionConflict,
		DecisionReasonControlUnavailable, DecisionReasonControlChanged, DecisionReasonPolicyDenied:
		return true
	default:
		return false
	}
}

// isAllowReason reports whether the reason is one of the first six.
func isAllowReason(reason DecisionReason) bool {
	switch reason {
	case DecisionReasonAllowed, DecisionReasonApprovedOnce, DecisionReasonApprovedThread,
		DecisionReasonHoldReleased, DecisionReasonStepGranted, DecisionReasonBudgetExtended:
		return true
	default:
		return false
	}
}

// ApprovalSource names what opened an approval.
type ApprovalSource string

// ApprovalSource values.
const (
	ApprovalSourceLink         ApprovalSource = "link"
	ApprovalSourceLoopStep     ApprovalSource = "loop_step"
	ApprovalSourceLoopBudget   ApprovalSource = "loop_budget"
	ApprovalSourceLoopPattern  ApprovalSource = "loop_pattern"
	ApprovalSourceOrchestrator ApprovalSource = "orchestrator"
	ApprovalSourceTriage       ApprovalSource = "triage"
	ApprovalSourceAgentRequest ApprovalSource = "agent_request"
)

// EnumValues returns the sources in table order.
func (ApprovalSource) EnumValues() []string {
	return []string{string(ApprovalSourceLink), string(ApprovalSourceLoopStep), string(ApprovalSourceLoopBudget), string(ApprovalSourceLoopPattern), string(ApprovalSourceOrchestrator), string(ApprovalSourceTriage), string(ApprovalSourceAgentRequest)}
}

// Valid reports whether the value is a known source.
func (v ApprovalSource) Valid() bool {
	switch v {
	case ApprovalSourceLink, ApprovalSourceLoopStep, ApprovalSourceLoopBudget, ApprovalSourceLoopPattern,
		ApprovalSourceOrchestrator, ApprovalSourceTriage, ApprovalSourceAgentRequest:
		return true
	default:
		return false
	}
}

// offeredKinds returns the option kinds one approval source may offer.
func offeredKinds(source ApprovalSource) map[string]bool {
	switch source {
	case ApprovalSourceLink:
		return map[string]bool{kindApproveOnce: true, kindApproveThread: true, kindSetLink: true, kindReject: true}
	case ApprovalSourceLoopStep:
		return map[string]bool{kindSteps: true, kindStop: true}
	case ApprovalSourceLoopBudget:
		return map[string]bool{kindExtend: true, kindSetMode: true, kindStop: true}
	case ApprovalSourceLoopPattern:
		return map[string]bool{kindResume: true, kindSetMode: true, kindStop: true}
	case ApprovalSourceOrchestrator, ApprovalSourceTriage:
		return map[string]bool{kindDeliverNow: true, kindHold: true, kindReject: true}
	case ApprovalSourceAgentRequest:
		return map[string]bool{kindExtend: true, kindReject: true}
	default:
		return map[string]bool{}
	}
}

// ApprovalObjectType names what an approval covers.
type ApprovalObjectType string

// ApprovalObjectType values.
const (
	ApprovalObjectTypeMessage    ApprovalObjectType = "message"
	ApprovalObjectTypeArtifact   ApprovalObjectType = "artifact"
	ApprovalObjectTypeLoop       ApprovalObjectType = "loop"
	ApprovalObjectTypeThreadLink ApprovalObjectType = "thread_link"
)

// EnumValues returns the types in table order.
func (ApprovalObjectType) EnumValues() []string {
	return []string{string(ApprovalObjectTypeMessage), string(ApprovalObjectTypeArtifact), string(ApprovalObjectTypeLoop), string(ApprovalObjectTypeThreadLink)}
}

// Valid reports whether the value is a known type.
func (v ApprovalObjectType) Valid() bool {
	switch v {
	case ApprovalObjectTypeMessage, ApprovalObjectTypeArtifact, ApprovalObjectTypeLoop, ApprovalObjectTypeThreadLink:
		return true
	default:
		return false
	}
}

// ApprovalState names an approval lifecycle state.
type ApprovalState string

// ApprovalState values.
const (
	ApprovalStatePending    ApprovalState = "PENDING"
	ApprovalStateApproved   ApprovalState = "APPROVED"
	ApprovalStateRejected   ApprovalState = "REJECTED"
	ApprovalStateExpired    ApprovalState = "EXPIRED"
	ApprovalStateSuperseded ApprovalState = "SUPERSEDED"
)

// EnumValues returns the states in table order.
func (ApprovalState) EnumValues() []string {
	return []string{string(ApprovalStatePending), string(ApprovalStateApproved), string(ApprovalStateRejected), string(ApprovalStateExpired), string(ApprovalStateSuperseded)}
}

// Valid reports whether the value is a known state.
func (v ApprovalState) Valid() bool {
	switch v {
	case ApprovalStatePending, ApprovalStateApproved, ApprovalStateRejected,
		ApprovalStateExpired, ApprovalStateSuperseded:
		return true
	default:
		return false
	}
}

// ExplanationScope names one explanation line scope.
type ExplanationScope string

// ExplanationScope values.
const (
	ExplanationScopePolicy   ExplanationScope = "policy"
	ExplanationScopeControls ExplanationScope = "controls"
	ExplanationScopeGlobal   ExplanationScope = "global"
	ExplanationScopeNode     ExplanationScope = "node"
	ExplanationScopeSender   ExplanationScope = "sender"
	ExplanationScopeReceiver ExplanationScope = "receiver"
	ExplanationScopeLink     ExplanationScope = "link"
	ExplanationScopeThread   ExplanationScope = "thread"
	ExplanationScopeLoop     ExplanationScope = "loop"
	ExplanationScopeObject   ExplanationScope = "object"
)

// EnumValues returns the scopes in table order.
func (ExplanationScope) EnumValues() []string {
	return []string{string(ExplanationScopePolicy), string(ExplanationScopeControls), string(ExplanationScopeGlobal), string(ExplanationScopeNode), string(ExplanationScopeSender), string(ExplanationScopeReceiver), string(ExplanationScopeLink), string(ExplanationScopeThread), string(ExplanationScopeLoop), string(ExplanationScopeObject)}
}

// Valid reports whether the value is a known scope.
func (v ExplanationScope) Valid() bool {
	switch v {
	case ExplanationScopePolicy, ExplanationScopeControls, ExplanationScopeGlobal, ExplanationScopeNode,
		ExplanationScopeSender, ExplanationScopeReceiver, ExplanationScopeLink, ExplanationScopeThread,
		ExplanationScopeLoop, ExplanationScopeObject:
		return true
	default:
		return false
	}
}

// explanationScopeRank orders lines: policy, controls, global, node,
// sender, receiver, link, thread, loop, object.
func explanationScopeRank(scope ExplanationScope) int {
	switch scope {
	case ExplanationScopePolicy:
		return 0
	case ExplanationScopeControls:
		return 1
	case ExplanationScopeGlobal:
		return 2
	case ExplanationScopeNode:
		return 3
	case ExplanationScopeSender:
		return 4
	case ExplanationScopeReceiver:
		return 5
	case ExplanationScopeLink:
		return 6
	case ExplanationScopeThread:
		return 7
	case ExplanationScopeLoop:
		return 8
	case ExplanationScopeObject:
		return 9
	default:
		return 10
	}
}

// AdmissionStatus names an admission gate result.
type AdmissionStatus string

// AdmissionStatus values.
const (
	AdmissionStatusAccepted         AdmissionStatus = "ACCEPTED"
	AdmissionStatusAcceptedHeld     AdmissionStatus = "ACCEPTED_HELD"
	AdmissionStatusApprovalRequired AdmissionStatus = "APPROVAL_REQUIRED"
)

// EnumValues returns the results in table order.
func (AdmissionStatus) EnumValues() []string {
	return []string{string(AdmissionStatusAccepted), string(AdmissionStatusAcceptedHeld), string(AdmissionStatusApprovalRequired)}
}

// Valid reports whether the value is a known result.
func (v AdmissionStatus) Valid() bool {
	switch v {
	case AdmissionStatusAccepted, AdmissionStatusAcceptedHeld, AdmissionStatusApprovalRequired:
		return true
	default:
		return false
	}
}

// ErrorCode names a typed Control API domain error.
type ErrorCode string

// ErrorCode values.
const (
	ErrorCodeNotFound                ErrorCode = "NOT_FOUND"
	ErrorCodeNotRecipient            ErrorCode = "NOT_RECIPIENT"
	ErrorCodeExpired                 ErrorCode = "EXPIRED"
	ErrorCodePolicyDenied            ErrorCode = "POLICY_DENIED"
	ErrorCodeKeyUnavailable          ErrorCode = "KEY_UNAVAILABLE"
	ErrorCodeRecipientNotDeliverable ErrorCode = "RECIPIENT_NOT_DELIVERABLE"
	ErrorCodeChannelOff              ErrorCode = "CHANNEL_OFF"
	ErrorCodeChannelDraining         ErrorCode = "CHANNEL_DRAINING"
	ErrorCodeNodeOff                 ErrorCode = "NODE_OFF"
	ErrorCodeNodeDraining            ErrorCode = "NODE_DRAINING"
	ErrorCodeAgentTXDisabled         ErrorCode = "AGENT_TX_DISABLED"
	ErrorCodeAgentRXDisabled         ErrorCode = "AGENT_RX_DISABLED"
	ErrorCodeLinkDenied              ErrorCode = "LINK_DENIED"
	ErrorCodeLoopStopped             ErrorCode = "LOOP_STOPPED"
	ErrorCodeLoopDraining            ErrorCode = "LOOP_DRAINING"
	ErrorCodeLoopBudgetExhausted     ErrorCode = "LOOP_BUDGET_EXHAUSTED"
	ErrorCodeControlVersionConflict  ErrorCode = "CONTROL_VERSION_CONFLICT"
)

// EnumValues returns the codes in table order.
func (ErrorCode) EnumValues() []string {
	return []string{string(ErrorCodeNotFound), string(ErrorCodeNotRecipient), string(ErrorCodeExpired), string(ErrorCodePolicyDenied), string(ErrorCodeKeyUnavailable), string(ErrorCodeRecipientNotDeliverable), string(ErrorCodeChannelOff), string(ErrorCodeChannelDraining), string(ErrorCodeNodeOff), string(ErrorCodeNodeDraining), string(ErrorCodeAgentTXDisabled), string(ErrorCodeAgentRXDisabled), string(ErrorCodeLinkDenied), string(ErrorCodeLoopStopped), string(ErrorCodeLoopDraining), string(ErrorCodeLoopBudgetExhausted), string(ErrorCodeControlVersionConflict)}
}

// Valid reports whether the value is a known code.
func (v ErrorCode) Valid() bool {
	switch v {
	case ErrorCodeNotFound, ErrorCodeNotRecipient, ErrorCodeExpired, ErrorCodePolicyDenied,
		ErrorCodeKeyUnavailable, ErrorCodeRecipientNotDeliverable, ErrorCodeChannelOff,
		ErrorCodeChannelDraining, ErrorCodeNodeOff, ErrorCodeNodeDraining,
		ErrorCodeAgentTXDisabled, ErrorCodeAgentRXDisabled, ErrorCodeLinkDenied,
		ErrorCodeLoopStopped, ErrorCodeLoopDraining, ErrorCodeLoopBudgetExhausted,
		ErrorCodeControlVersionConflict:
		return true
	default:
		return false
	}
}

// ReceiptType names a federated receipt category.
type ReceiptType string

// ReceiptType values.
const (
	ReceiptTypeControlAccepted         ReceiptType = "CONTROL_ACCEPTED"
	ReceiptTypeControlHeld             ReceiptType = "CONTROL_HELD"
	ReceiptTypeControlApprovalRequired ReceiptType = "CONTROL_APPROVAL_REQUIRED"
	ReceiptTypeControlDenied           ReceiptType = "CONTROL_DENIED"
	ReceiptTypeLoopBudgetExhausted     ReceiptType = "LOOP_BUDGET_EXHAUSTED"
	ReceiptTypeLoopVersionConflict     ReceiptType = "LOOP_VERSION_CONFLICT"
)

// EnumValues returns the categories in table order.
func (ReceiptType) EnumValues() []string {
	return []string{string(ReceiptTypeControlAccepted), string(ReceiptTypeControlHeld), string(ReceiptTypeControlApprovalRequired), string(ReceiptTypeControlDenied), string(ReceiptTypeLoopBudgetExhausted), string(ReceiptTypeLoopVersionConflict)}
}

// Valid reports whether the value is a known category.
func (v ReceiptType) Valid() bool {
	switch v {
	case ReceiptTypeControlAccepted, ReceiptTypeControlHeld, ReceiptTypeControlApprovalRequired,
		ReceiptTypeControlDenied, ReceiptTypeLoopBudgetExhausted, ReceiptTypeLoopVersionConflict:
		return true
	default:
		return false
	}
}

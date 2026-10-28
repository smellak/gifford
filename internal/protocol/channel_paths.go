package protocol

// This file holds the JSON field paths and option kinds repeated across
// the channel types, so the linter sees one literal each. Path constants
// follow the pathXxx convention of event.go; kinds are the text before
// the colon of an ApprovalOption (or the whole token).
const (
	pathScopeType         = "scope_type"
	pathScopeKey          = "scope_key"
	pathPlane             = "plane"
	pathMode              = "mode"
	pathControl           = "control"
	pathMessageControl    = "message_control"
	pathArtifactControl   = "artifact_control"
	pathFromAgent         = "from_agent"
	pathLoopID            = "loop_id"
	pathMessageID         = "message_id"
	pathControlReason     = "control_reason"
	pathApprovalRequestID = "approval_request_id"
	pathResolution        = "resolution"
	pathExpiresAt         = "expires_at"
	pathDecisionID        = "decision_id"
	pathControlState      = "control_state"
	pathApprovalID        = "approval_id"
	pathActorType         = "actor_type"
	pathOutcome           = "outcome"
	pathBlocker           = "blocker"
	pathVersion           = "version"
	pathReason            = "reason"
	pathThenMode          = "then_mode"
	pathExpectedVersions  = "expected_versions"

	reasonMessageOrArtifact = "message or artifact"
	reasonExactlyOneObject  = "exactly one of message_id, artifact_id"
	reasonFromDiffersTo     = "from differs from to"
)

const (
	kindApproveOnce   = "approve_once"
	kindApproveThread = "approve_thread"
	kindSetLink       = "set_link"
	kindReject        = "reject"
	kindSteps         = "steps"
	kindStop          = "stop"
	kindExtend        = "extend"
	kindSetMode       = "set_mode"
	kindResume        = "resume"
	kindDeliverNow    = "deliver_now"
	kindHold          = "hold"
)

package protocol

import (
	"fmt"
	"time"
)

// This file holds the normalised state snapshot of §5.4: what the
// Orchestrator decides from.

// ChannelEffective carries the already-combined channel flags for one
// agent, with the node control epoch the state was read under.
type ChannelEffective struct {
	Global         ChannelMode `json:"global"`
	Node           ChannelMode `json:"node"`
	Tx             bool        `json:"tx"`
	Rx             bool        `json:"rx"`
	AutoDelivery   bool        `json:"auto_delivery"`
	AutoContinue   bool        `json:"auto_continue"`
	ControlVersion int64       `json:"control_version" jsonschema:"minimum=0"`
}

// Validate checks the effective flags.
func (c ChannelEffective) Validate() error {
	return checkRoot(&c)
}

// Confidence carries the analyst probabilities behind a snapshot.
type Confidence struct {
	HumanDraftState float64 `json:"human_draft_state" jsonschema:"minimum=0,maximum=1"`
	AgentPhase      float64 `json:"agent_phase" jsonschema:"minimum=0,maximum=1"`
	AttentionState  float64 `json:"attention_state" jsonschema:"minimum=0,maximum=1"`
	DeliverySafety  float64 `json:"delivery_safety" jsonschema:"minimum=0,maximum=1"`
}

// Validate checks the confidences.
func (c Confidence) Validate() error {
	return checkRoot(&c)
}

// LoopSummary summarises one active loop on a snapshot.
type LoopSummary struct {
	LoopID        LoopID     `json:"loop_id"`
	Mode          LoopMode   `json:"mode"`
	Turns         UsageRatio `json:"turns"`
	Messages      UsageRatio `json:"messages"`
	ElapsedMS     *int64     `json:"elapsed_ms,omitempty" jsonschema:"minimum=0"`
	MaxDurationMS *int64     `json:"max_duration_ms,omitempty" jsonschema:"minimum=1"`
}

// Validate checks the loop summary.
func (l LoopSummary) Validate() error {
	return checkRoot(&l)
}

// StateSnapshot is the normalised agent state the Orchestrator reads.
type StateSnapshot struct {
	StateID            ULID             `json:"state_id"`
	StateVersion       int64            `json:"state_version" jsonschema:"minimum=1"`
	AgentID            AgentID          `json:"agent_id"`
	ObservedAt         time.Time        `json:"observed_at"`
	SessionState       SessionState     `json:"session_state"`
	HumanDraftState    DraftState       `json:"human_draft_state"`
	HumanActivityState ActivityState    `json:"human_activity_state"`
	DraftGeneration    int64            `json:"draft_generation" jsonschema:"minimum=0"`
	InputGuardVersion  int64            `json:"input_guard_version" jsonschema:"minimum=0"`
	LastHumanKeyAt     time.Time        `json:"last_human_key_at,omitzero"`
	AgentPhase         AgentPhase       `json:"agent_phase"`
	AttentionState     AttentionState   `json:"attention_state"`
	HealthState        HealthState      `json:"health_state"`
	DeliverySafety     DeliverySafety   `json:"delivery_safety"`
	BackgroundActivity bool             `json:"background_activity"`
	ChannelEffective   ChannelEffective `json:"channel_effective"`
	ActiveLoops        []LoopSummary    `json:"active_loops" jsonschema:"maxItems=64"`
	PendingApprovals   int64            `json:"pending_approvals" jsonschema:"minimum=0"`
	Confidence         Confidence       `json:"confidence"`
	EvidenceEventIDs   []ULID           `json:"evidence_event_ids" jsonschema:"maxItems=64"`
	AnalystVersion     string           `json:"analyst_version" jsonschema:"minLength=1,maxLength=64"`
	Stale              bool             `json:"stale"`
}

// Validate checks the snapshot.
func (s StateSnapshot) Validate() error {
	if err := checkRoot(&s); err != nil {
		return err
	}
	if s.SessionState == SessionStateObserved && s.HumanDraftState != DraftStateUnknown {
		return &ValidationError{Path: "human_draft_state", Reason: "must be UNKNOWN when session is OBSERVED"}
	}
	seen := make(map[LoopID]bool, len(s.ActiveLoops))
	for i, loop := range s.ActiveLoops {
		if seen[loop.LoopID] {
			return &ValidationError{Path: fmt.Sprintf("active_loops[%d]", i), Reason: "duplicate loop_id"}
		}
		seen[loop.LoopID] = true
	}
	return nil
}

package protocol

import (
	"fmt"
	"time"
)

// This file holds the governed loop types of §6.3.

// LoopBudget is the loop budget: every dimension a loop may consume.
type LoopBudget struct {
	MaxTurns               int64 `json:"max_turns" jsonschema:"minimum=1"`
	MaxMessages            int64 `json:"max_messages" jsonschema:"minimum=1"`
	MaxDurationMS          int64 `json:"max_duration_ms" jsonschema:"minimum=1000"`
	MaxMessagesPerMinute   int64 `json:"max_messages_per_minute" jsonschema:"minimum=1"`
	MaxHops                int64 `json:"max_hops" jsonschema:"minimum=1"`
	MaxConsecutiveSamePair int64 `json:"max_consecutive_same_pair" jsonschema:"minimum=1"`
	MaxRetries             int64 `json:"max_retries" jsonschema:"minimum=0"`
	MaxArtifactCount       int64 `json:"max_artifact_count" jsonschema:"minimum=0"`
	MaxArtifactBytes       int64 `json:"max_artifact_bytes" jsonschema:"minimum=0"`
}

// Validate checks the budget.
func (b LoopBudget) Validate() error {
	return checkRoot(&b)
}

// LoopCounters are the consumed loop counters.
type LoopCounters struct {
	TurnCount     int64 `json:"turn_count" jsonschema:"minimum=0"`
	MessageCount  int64 `json:"message_count" jsonschema:"minimum=0"`
	HopCount      int64 `json:"hop_count" jsonschema:"minimum=0"`
	ArtifactCount int64 `json:"artifact_count" jsonschema:"minimum=0"`
	ArtifactBytes int64 `json:"artifact_bytes" jsonschema:"minimum=0"`
	StepTokens    int64 `json:"step_tokens" jsonschema:"minimum=0"`
}

// Validate checks the counters.
func (c LoopCounters) Validate() error {
	return checkRoot(&c)
}

// Loop is one governed agent-to-agent loop.
type Loop struct {
	LoopID           LoopID        `json:"loop_id"`
	ThreadID         ThreadID      `json:"thread_id"`
	LoopGeneration   int64         `json:"loop_generation" jsonschema:"minimum=1"`
	OwnerNode        NodeID        `json:"owner_node"`
	Origin           LoopOrigin    `json:"origin"`
	Participants     []AgentID     `json:"participants" jsonschema:"minItems=2,maxItems=32,uniqueItems=true"`
	Mode             LoopMode      `json:"mode"`
	LocalOverride    ControlMode   `json:"local_override,omitempty"`
	EffectiveMode    LoopMode      `json:"effective_mode"`
	StateReason      LoopStateReason `json:"state_reason,omitempty"`
	BudgetProfile    BudgetProfile `json:"budget_profile"`
	RootMessageID    ULID          `json:"root_message_id,omitempty"`
	Budget           LoopBudget    `json:"budget"`
	Counters         LoopCounters  `json:"counters"`
	LastSenderAgent  AgentID       `json:"last_sender_agent,omitempty"`
	ControlVersion   int64         `json:"control_version" jsonschema:"minimum=1"`
	StartedAt        time.Time     `json:"started_at"`
	ExpiresAt        time.Time     `json:"expires_at"`
	LastProgressAt   time.Time     `json:"last_progress_at,omitzero"`
	StoppedAt        time.Time     `json:"stopped_at,omitzero"`
	CreatedBy        string        `json:"created_by" jsonschema:"minLength=1,maxLength=128"`
	UpdatedAt        time.Time     `json:"updated_at"`
}

// Validate checks the loop.
func (l Loop) Validate() error {
	if err := checkRoot(&l); err != nil {
		return err
	}
	if l.LocalOverride != "" && l.LocalOverride != ControlModePaused && l.LocalOverride != ControlModeStopped {
		return &ValidationError{Path: "local_override", Reason: "PAUSED or STOPPED"}
	}
	if want := effectiveLoopMode(l.Mode, l.LocalOverride); l.EffectiveMode != want {
		return &ValidationError{Path: "effective_mode", Reason: fmt.Sprintf("must be %s", want)}
	}
	if want := l.StartedAt.Add(time.Duration(l.Budget.MaxDurationMS) * time.Millisecond); !l.ExpiresAt.Equal(want) {
		return &ValidationError{Path: "expires_at", Reason: "must equal started_at plus max_duration_ms"}
	}
	if l.Mode == LoopModeStopped && l.StoppedAt.IsZero() {
		return &ValidationError{Path: "stopped_at", Reason: "required when mode is STOPPED"}
	}
	if l.LastSenderAgent != "" {
		found := false
		for _, p := range l.Participants {
			if p == l.LastSenderAgent {
				found = true
				break
			}
		}
		if !found {
			return &ValidationError{Path: "last_sender_agent", Reason: "must be a participant"}
		}
	}
	return nil
}

// effectiveLoopMode computes the effective mode from the owner mode and
// the local override.
func effectiveLoopMode(mode LoopMode, override ControlMode) LoopMode {
	switch {
	case mode == LoopModeStopped || mode == LoopModeCompleted:
		return mode
	case override == ControlModeStopped:
		return LoopModeStopped
	case override == ControlModePaused:
		return LoopModePaused
	default:
		return mode
	}
}

// LoopList lists loops.
type LoopList struct {
	Loops []Loop `json:"loops"`
}

// Validate checks the list.
func (l LoopList) Validate() error {
	return checkRoot(&l)
}

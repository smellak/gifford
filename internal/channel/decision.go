package channel

import (
	"fmt"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

// Outcome constants, so that executors can write d.Outcome() == channel.Allow.
const (
	Allow    = protocol.DecisionOutcomeAllow
	Hold     = protocol.DecisionOutcomeHold
	Approval = protocol.DecisionOutcomeApproval
	Deny     = protocol.DecisionOutcomeDeny
)

// restrictionClass orders restrictions from weakest to strongest. It is
// declared here and used by the evaluation.
type restrictionClass int

const (
	restrictionNone restrictionClass = iota
	restrictionApproval
	restrictionHold
	restrictionDeny
)

// decision is the unexported state of a Decision: the inert record plus
// what a record does not carry, the blocker's class and scope.
type decision struct {
	rec      protocol.DecisionRecord
	class    restrictionClass
	blocker  protocol.ExplanationScope
	delivery deliveryAuthority
}

// deliveryAuthority binds a delivery-stage Decision to the queue generation
// and node epoch that are not fields of the frozen protocol DecisionRecord.
// A zero generation means the Decision carries no PTY delivery authority.
type deliveryAuthority struct {
	generation int64
	epoch      int64
}

// Decision is the immutable result of a gate. Only this package builds one;
// the zero value is not a decision and is never Allowed.
type Decision struct {
	d *decision // the only field; nil in the zero value
}

// ID returns the gate-minted decision id, empty on the zero Decision.
func (x Decision) ID() protocol.ChannelDecisionID {
	if x.d == nil {
		return ""
	}
	return x.d.rec.DecisionID
}

// Stage returns the gate stage, empty on the zero Decision.
func (x Decision) Stage() protocol.DecisionStage {
	if x.d == nil {
		return ""
	}
	return x.d.rec.Stage
}

// Plane returns the decision plane, empty on the zero Decision.
func (x Decision) Plane() protocol.Plane {
	if x.d == nil {
		return ""
	}
	return x.d.rec.Plane
}

// Object returns the decided object type and id, empty on the zero Decision.
func (x Decision) Object() (protocol.DecisionObjectType, protocol.OpaqueID) {
	if x.d == nil {
		return "", ""
	}
	return x.d.rec.ObjectType, x.d.rec.ObjectID
}

// From returns the sender, empty on the zero Decision.
func (x Decision) From() protocol.AgentID {
	if x.d == nil {
		return ""
	}
	return x.d.rec.FromAgent
}

// To returns the receiver, empty on the zero Decision.
func (x Decision) To() protocol.AgentID {
	if x.d == nil {
		return ""
	}
	return x.d.rec.ToAgent
}

// Outcome returns the gate outcome, empty on the zero Decision.
func (x Decision) Outcome() protocol.DecisionOutcome {
	if x.d == nil {
		return ""
	}
	return x.d.rec.Outcome
}

// Reason returns the typed reason, empty on the zero Decision.
func (x Decision) Reason() protocol.DecisionReason {
	if x.d == nil {
		return ""
	}
	return x.d.rec.Reason
}

// Allowed reports whether the outcome is Allow, false on the zero Decision.
func (x Decision) Allowed() bool {
	return x.Outcome() == Allow
}

// IsZero reports whether this is the zero Decision.
func (x Decision) IsZero() bool {
	return x.d == nil
}

// MatchedControlIDs returns a copy of the matched control ids, nil on the
// zero Decision.
func (x Decision) MatchedControlIDs() []protocol.ControlID {
	if x.d == nil {
		return nil
	}
	return append([]protocol.ControlID(nil), x.d.rec.MatchedControlIDs...)
}

// Effective returns a copy of the effective control.
func (x Decision) Effective() protocol.EffectiveControl {
	if x.d == nil {
		return protocol.EffectiveControl{}
	}
	return x.d.rec.EffectiveControl
}

// Versions returns a copy of the version vector, nil on the zero Decision.
func (x Decision) Versions() protocol.ControlVersions {
	if x.d == nil {
		return nil
	}
	vers := make(protocol.ControlVersions, len(x.d.rec.ControlVersions))
	for key, version := range x.d.rec.ControlVersions {
		vers[key] = version
	}
	return vers
}

// Record returns a deep copy of the inert record.
func (x Decision) Record() protocol.DecisionRecord {
	if x.d == nil {
		return protocol.DecisionRecord{}
	}
	return copyRecord(x.d.rec)
}

// DecidedAt returns when the decision was taken, zero on the zero Decision.
func (x Decision) DecidedAt() time.Time {
	if x.d == nil {
		return time.Time{}
	}
	return x.d.rec.CreatedAt
}

// newDecision is the only constructor: it validates the record, keeps a
// deep copy and returns the record's error, wrapped, if any.
func newDecision(rec protocol.DecisionRecord, class restrictionClass, blocker protocol.ExplanationScope) (Decision, error) {
	if err := rec.Validate(); err != nil {
		return Decision{}, fmt.Errorf("invalid decision record: %w", err)
	}
	return Decision{d: &decision{rec: copyRecord(rec), class: class, blocker: blocker}}, nil
}

// withDeliveryAuthority returns an immutable copy bound to one queue
// generation and control epoch.
func withDeliveryAuthority(x Decision, generation, epoch int64) Decision {
	bound := *x.d
	bound.delivery = deliveryAuthority{generation: generation, epoch: epoch}
	return Decision{d: &bound}
}

// copyRecord deep-copies a decision record: the matched ids, the version
// vector and the explanation with its lines and generation pointers.
func copyRecord(rec protocol.DecisionRecord) protocol.DecisionRecord {
	out := rec
	out.MatchedControlIDs = make([]protocol.ControlID, len(rec.MatchedControlIDs))
	copy(out.MatchedControlIDs, rec.MatchedControlIDs)
	vers := make(protocol.ControlVersions, len(rec.ControlVersions))
	for key, version := range rec.ControlVersions {
		vers[key] = version
	}
	out.ControlVersions = vers
	lines := make([]protocol.ExplanationLine, len(rec.Explanation.Lines))
	for i, line := range rec.Explanation.Lines {
		lines[i] = line
		if line.Version != nil {
			version := *line.Version
			lines[i].Version = &version
		}
	}
	out.Explanation.Lines = lines
	if rec.Explanation.LoopGeneration != nil {
		generation := *rec.Explanation.LoopGeneration
		out.Explanation.LoopGeneration = &generation
	}
	return out
}

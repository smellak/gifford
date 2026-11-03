package fallback

import (
	"time"

	"github.com/smellak/gifford/internal/decide"
)

// AnalysisDeadline is how long a notice waits for Glimmer's analysis
// before the detector decides. A slower answer counts as ModelLate.
const AnalysisDeadline = 10 * time.Second

// EscalateAfter is how long a message may stay unread before the operator is
// told, once per message.
const EscalateAfter = 30 * time.Minute

// schedule holds the notice slots, measured from the moment the message
// became pending: the first notice, then the re-notices at 5 min, 15 min,
// 45 min, 2 h and 4 h. Nothing is re-sent after the last slot.
var schedule = [...]time.Duration{
	0,
	5 * time.Minute,
	15 * time.Minute,
	45 * time.Minute,
	2 * time.Hour,
	4 * time.Hour,
}

// ModelOutcome is what Glimmer gave for this notice. The zero value is
// ModelOff, so a caller that never asked the model gets the detector.
type ModelOutcome uint8

// valid reports whether the outcome is a known enum value. Anything else
// is an unknown state: it retains the attempt and never notifies.
func (m ModelOutcome) valid() bool {
	switch m {
	case ModelOff, ModelFailed, ModelCircuitOpen, ModelLate, ModelNotify, ModelDefer:
		return true
	}
	return false
}

// Model outcomes. Only ModelNotify and ModelDefer are answers; every
// other value hands the state judgement to the detector.
const (
	ModelOff ModelOutcome = iota
	ModelFailed
	ModelCircuitOpen
	ModelLate
	ModelNotify
	ModelDefer
)

// PaneState is the detector's reading of the agent's pane. The zero value
// is PaneUnknown, which never writes.
type PaneState uint8

// Pane states. PaneIdle means the detector is certain the agent is not
// working and its input box is empty.
const (
	PaneUnknown PaneState = iota
	PaneIdle
	PaneBusy
)

// Controls are the channel controls on this communication. Any of them
// set holds the notice whatever the model or the detector say.
type Controls struct {
	Off             bool
	Hold            bool
	Manual          bool
	Rejected        bool
	Expired         bool
	BudgetExhausted bool
	ApprovalPending bool
}

// Facts is everything one decision needs. GuardsPassed is false by
// default so a caller that did not evaluate the guards never writes.
type Facts struct {
	Now          time.Time
	PendingSince time.Time
	Read         bool
	Controls     Controls
	GuardsPassed bool
	Model        ModelOutcome
	Pane         PaneState
	// Notices are the times of notices whose result was certain.
	Notices []time.Time
	// Uncertain is set while an earlier attempt awaits reconciliation.
	Uncertain bool
	// Escalated is set once the operator has been alerted about this message.
	Escalated bool
}

// Action is what the caller does with the pane.
type Action uint8

// Actions. Hold is a legitimate stop that never counts as a failure;
// Wait means look again later; Drop ends the attempt: rejection is
// terminal, with no further slot and no escalation.
const (
	ActionWait Action = iota
	ActionNotify
	ActionHold
	ActionDrop
)

// Source names who judged the agent's state.
type Source uint8

// Sources. SourceNone means no state judgement was needed.
const (
	SourceNone Source = iota
	SourceModel
	SourceDetector
)

// Reason is a content-free code explaining a Decision.
type Reason string

// Reasons, one per exit of Decide.
const (
	ReasonInvalidFacts    Reason = "INVALID_FACTS"
	ReasonInvalidModel    Reason = "INVALID_MODEL"
	ReasonRead            Reason = "READ"
	ReasonControlOff      Reason = "CONTROL_OFF"
	ReasonControlHold     Reason = "CONTROL_HOLD"
	ReasonControlManual   Reason = "CONTROL_MANUAL"
	ReasonControlRejected Reason = "CONTROL_REJECTED"
	ReasonControlExpired  Reason = "CONTROL_EXPIRED"
	ReasonControlBudget   Reason = "CONTROL_BUDGET"
	ReasonApprovalPending Reason = "APPROVAL_PENDING"
	ReasonUncertain       Reason = "UNCERTAIN_UNRECONCILED"
	ReasonScheduleDone    Reason = "SCHEDULE_DONE"
	ReasonNotDue          Reason = "NOT_DUE"
	ReasonGuardFailed     Reason = "GUARD_FAILED"
	ReasonModelDefer      Reason = "MODEL_DEFER"
	ReasonAgentBusy       Reason = "AGENT_BUSY"
	ReasonStateUnknown    Reason = "STATE_UNKNOWN"
	ReasonNotify          Reason = "NOTIFY"
)

// Decision is the outcome of Decide. Escalate is independent of Action: a
// notice can wait while the operator is alerted. Next is the next notice slot
// after Now, or zero once the schedule is exhausted.
type Decision struct {
	Action   Action
	Escalate bool
	Source   Source
	Reason   Reason
	Next     time.Time
}

// Decide applies the schedule around a single verdict source: the write
// verdict for controls, guards and state comes from decide, so the order
// is defined once. Around it, this package owns the input contract
// (validity, read), the uncertain-attempt rule, the notice schedule and
// the escalation. A legitimate hold is not an outage: no notice and no
// escalation.
func Decide(f Facts) Decision {
	if f.PendingSince.IsZero() || f.Now.IsZero() || f.Now.Before(f.PendingSince) {
		return Decision{Action: ActionHold, Reason: ReasonInvalidFacts}
	}
	if f.Read {
		return Decision{Action: ActionWait, Reason: ReasonRead}
	}
	if !f.Model.valid() {
		// Unknown model state: retain the attempt and never notify,
		// like invalid input. It fails closed and silent.
		return Decision{Action: ActionHold, Reason: ReasonInvalidModel}
	}
	d := Decision{
		Escalate: !f.Escalated && f.Now.Sub(f.PendingSince) >= EscalateAfter,
		Next:     nextSlot(f.PendingSince, f.Now),
	}
	if f.Uncertain {
		d.Action, d.Reason = ActionHold, ReasonUncertain
		return d
	}
	slot, last := dueSlot(f.PendingSince, f.Now)
	if f.Controls.Rejected {
		// Rejection ends the attempt even when the slot is covered: the
		// schedule never outranks a terminal control, and a dropped
		// attempt neither renotifies nor escalates.
		return Decision{Action: ActionDrop, Reason: ReasonControlRejected}
	}
	switch {
	case covered(f.Notices, slot, f.Now) && last:
		d.Action, d.Reason = ActionWait, ReasonScheduleDone
		return d
	case covered(f.Notices, slot, f.Now):
		d.Action, d.Reason = ActionWait, ReasonNotDue
		return d
	}
	v := decide.Decide(controlOf(f.Controls), decideGuard(f), decideFree(f))
	if v.Outcome == decide.Drop {
		// Rejection ends the attempt: decide wins over the hold, terminal.
		return Decision{Action: ActionDrop, Reason: ReasonControlRejected}
	}
	if v.Outcome == decide.Hold {
		if reason, held := controlReason(f.Controls); held {
			return Decision{Action: ActionHold, Reason: reason}
		}
	}
	switch v.Outcome {
	case decide.Notify:
		d.Action, d.Reason = ActionNotify, ReasonNotify
		if f.Model == ModelNotify {
			d.Source = SourceModel
		} else {
			d.Source = SourceDetector
		}
		return d
	case decide.Uncertain:
		d.Action, d.Reason = ActionWait, ReasonStateUnknown
		if !f.GuardsPassed {
			d.Reason = ReasonGuardFailed
		}
		return d
	default:
		d.Action, d.Reason = ActionWait, ReasonAgentBusy
		if f.Model == ModelDefer {
			d.Source = SourceModel
			d.Reason = ReasonModelDefer
		}
		return d
	}
}

// controlOf maps the schedule controls onto decide's verdicts. Rejection
// ends the attempt; any other set control retains it; no control lets it
// through. It carries no order of its own: the precedence lives in decide.
func controlOf(c Controls) decide.Control {
	if c.Rejected {
		return decide.ControlDeny
	}
	switch {
	case c.Off, c.Hold, c.Manual, c.Expired, c.BudgetExhausted, c.ApprovalPending:
		return decide.ControlHold
	}
	return decide.ControlAllow
}

// controlReason names the set control for the record. The hold verdict
// comes from decide; this only attributes it and opens no approval.
// Denial ends the attempt, so it is named first.
func controlReason(c Controls) (Reason, bool) {
	switch {
	case c.Rejected:
		return ReasonControlRejected, true
	case c.Off:
		return ReasonControlOff, true
	case c.Hold:
		return ReasonControlHold, true
	case c.Manual:
		return ReasonControlManual, true
	case c.Expired:
		return ReasonControlExpired, true
	case c.BudgetExhausted:
		return ReasonControlBudget, true
	case c.ApprovalPending:
		return ReasonApprovalPending, true
	}
	return "", false
}

// decideGuard maps the evaluated guards onto decide's guard. A passed
// evaluation passes every guard; anything else stays uncertain and never
// notifies. A model answer stands in for the detector's certainty.
func decideGuard(f Facts) decide.Guard {
	if !f.GuardsPassed || (f.Pane == PaneUnknown && f.Model != ModelNotify) {
		return decide.Guard{}
	}
	return decide.Guard{DetectorCertain: true, RegistryValid: true, IsCLI: true}
}

// decideFree maps the state judgement onto decide's detector verdict. A
// model notify frees the attempt even on an unreadable pane; a model
// defer never does; otherwise only a certain idle pane frees it.
func decideFree(f Facts) bool {
	if f.Model == ModelNotify {
		return true
	}
	return f.Model != ModelDefer && f.Pane == PaneIdle
}

// dueSlot returns the start of the latest slot already reached and
// whether it is the last one. Slots missed while the agent was busy
// collapse into that single slot, so they cost one notice, not a burst.
func dueSlot(since, now time.Time) (time.Time, bool) {
	elapsed := now.Sub(since)
	latest := 0
	for i, offset := range schedule {
		if offset > elapsed {
			break
		}
		latest = i
	}
	return since.Add(schedule[latest]), latest == len(schedule)-1
}

// covered reports whether a certain notice was sent at or after the slot
// start. Notices stamped in the future are ignored rather than trusted.
func covered(notices []time.Time, slot, now time.Time) bool {
	for _, at := range notices {
		if !at.Before(slot) && !at.After(now) {
			return true
		}
	}
	return false
}

// nextSlot returns the first slot strictly after now, or zero.
func nextSlot(since, now time.Time) time.Time {
	for _, offset := range schedule {
		if at := since.Add(offset); at.After(now) {
			return at
		}
	}
	return time.Time{}
}

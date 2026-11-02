package fallback

import "time"

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
// Wait means look again later.
const (
	ActionWait Action = iota
	ActionNotify
	ActionHold
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

// Decide applies the precedence of controls, then the uncertain-attempt
// rule, then the schedule, then the guards, then the state judgement.
func Decide(f Facts) Decision {
	if f.PendingSince.IsZero() || f.Now.IsZero() || f.Now.Before(f.PendingSince) {
		return Decision{Action: ActionHold, Reason: ReasonInvalidFacts}
	}
	if f.Read {
		return Decision{Action: ActionWait, Reason: ReasonRead}
	}
	if reason, held := controlReason(f.Controls); held {
		// A legitimate hold is not an outage: no notice and no escalation.
		return Decision{Action: ActionHold, Reason: reason}
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
	switch {
	case covered(f.Notices, slot, f.Now) && last:
		d.Action, d.Reason = ActionWait, ReasonScheduleDone
		return d
	case covered(f.Notices, slot, f.Now):
		d.Action, d.Reason = ActionWait, ReasonNotDue
		return d
	case !f.GuardsPassed:
		d.Action, d.Reason = ActionWait, ReasonGuardFailed
		return d
	}
	d.Action, d.Source, d.Reason = judge(f.Model, f.Pane)
	return d
}

// controlReason reports the first control set, in precedence order.
func controlReason(c Controls) (Reason, bool) {
	switch {
	case c.Off:
		return ReasonControlOff, true
	case c.Hold:
		return ReasonControlHold, true
	case c.Manual:
		return ReasonControlManual, true
	case c.Rejected:
		return ReasonControlRejected, true
	case c.Expired:
		return ReasonControlExpired, true
	case c.BudgetExhausted:
		return ReasonControlBudget, true
	case c.ApprovalPending:
		return ReasonApprovalPending, true
	}
	return "", false
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

// judge gives the state judgement: the model when it answered, the
// detector otherwise. Only a certain idle pane notifies without a model.
func judge(model ModelOutcome, pane PaneState) (Action, Source, Reason) {
	switch model {
	case ModelNotify:
		return ActionNotify, SourceModel, ReasonNotify
	case ModelDefer:
		return ActionWait, SourceModel, ReasonModelDefer
	case ModelOff, ModelFailed, ModelCircuitOpen, ModelLate:
	}
	switch pane {
	case PaneIdle:
		return ActionNotify, SourceDetector, ReasonNotify
	case PaneBusy:
		return ActionWait, SourceDetector, ReasonAgentBusy
	case PaneUnknown:
	}
	return ActionWait, SourceDetector, ReasonStateUnknown
}

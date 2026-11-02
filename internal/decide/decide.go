package decide

// Control is the authorized and revalidated control verdict for one
// delivery attempt, highest precedence.
type Control int

const (
	// ControlAllow lets the attempt reach the guards.
	ControlAllow Control = iota
	// ControlHold retains the attempt: OFF or HOLD, MANUAL without a
	// deliver-now approval, expired, out of budget, or waiting approval.
	ControlHold
	// ControlDeny ends the attempt: rejected or policy-denied.
	ControlDeny
	// ControlChanged cancels the attempt: a control moved between
	// authorization and commit and is audited as CONTROL_CHANGED.
	ControlChanged
)

// Guard is one fresh guard snapshot for the attached-pane mode.
type Guard struct {
	// DetectorCertain is false when the classifier supports no verdict.
	DetectorCertain bool
	// Busy reports the harness working.
	Busy bool
	// BoxBusy reports text in the input box.
	BoxBusy bool
	// HumanActive reports recent keyboard activity.
	HumanActive bool
	// RegistryValid reports the pane registration still matches.
	RegistryValid bool
	// IsCLI reports the pane runs the registered harness CLI.
	IsCLI bool
}

// Outcome is what the attempt may do next.
type Outcome int

const (
	// Notify writes the content-free notice line.
	Notify Outcome = iota
	// Hold writes nothing and stays registered for a later attempt.
	Hold
	// Drop ends the attempt.
	Drop
	// Uncertain writes nothing and records the attempt as uncertain
	// pending reconciliation: never re-inject, never wipe another's text.
	Uncertain
)

// Decision is one ordered verdict. Reason is a content-free code.
type Decision struct {
	Outcome Outcome
	Reason  string
}

// Reasons name the deciding row without pane content.
const (
	ReasonControlsUnknown = "controls-unknown"
	ReasonControlsHold    = "controls-hold"
	ReasonControlsDeny    = "controls-deny"
	ReasonControlsChanged = "controls-changed"
	ReasonDetectorUnknown = "detector-unknown"
	ReasonAgentBusy       = "agent-busy"
	ReasonBoxBusy         = "box-busy"
	ReasonHumanActive     = "human-active"
	ReasonRegistryInvalid = "registry-invalid"
	ReasonNotCLI          = "not-cli"
	ReasonAgentFree       = "agent-free"
)

// Decide orders one attempt: controls, then guards, then detector state.
// A failing control never reaches the guards; an uncertain detector never
// notifies. free is the detector's free verdict, read only when every
// guard above it passes.
func Decide(c Control, g Guard, free bool) Decision {
	switch c {
	case ControlAllow:
	case ControlDeny:
		return Decision{Outcome: Drop, Reason: ReasonControlsDeny}
	case ControlChanged:
		return Decision{Outcome: Drop, Reason: ReasonControlsChanged}
	case ControlHold:
		return Decision{Outcome: Hold, Reason: ReasonControlsHold}
	default:
		return Decision{Outcome: Hold, Reason: ReasonControlsUnknown}
	}
	if !g.DetectorCertain {
		return Decision{Outcome: Uncertain, Reason: ReasonDetectorUnknown}
	}
	if g.Busy {
		return Decision{Outcome: Hold, Reason: ReasonAgentBusy}
	}
	if g.BoxBusy {
		return Decision{Outcome: Hold, Reason: ReasonBoxBusy}
	}
	if g.HumanActive {
		return Decision{Outcome: Hold, Reason: ReasonHumanActive}
	}
	if !g.RegistryValid {
		return Decision{Outcome: Hold, Reason: ReasonRegistryInvalid}
	}
	if !g.IsCLI {
		return Decision{Outcome: Hold, Reason: ReasonNotCLI}
	}
	if !free {
		return Decision{Outcome: Hold, Reason: ReasonAgentBusy}
	}
	return Decision{Outcome: Notify, Reason: ReasonAgentFree}
}

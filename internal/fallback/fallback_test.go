package fallback

import (
	"testing"
	"time"

	"github.com/smellak/gifford/internal/decide"
)

var t0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

// idle is a pending message the detector may notify right now.
func idle(now time.Duration) Facts {
	return Facts{
		Now: t0.Add(now), PendingSince: t0, GuardsPassed: true,
		Model: ModelOff, Pane: PaneIdle,
	}
}

// A rejection ends the attempt even when the slot is covered: the
// schedule never outranks a terminal control, and a dropped attempt
// never escalates.
func TestDecideRejectedEndsCoveredSlot(t *testing.T) {
	t.Parallel()

	f := idle(60 * time.Minute)
	f.Notices = []time.Time{t0.Add(50 * time.Minute)}
	f.Controls = Controls{Rejected: true}
	got := Decide(f)
	if got.Action != ActionDrop || got.Reason != ReasonControlRejected {
		t.Fatalf("got %+v, want drop on rejected covered slot", got)
	}
	if got.Escalate {
		t.Fatalf("dropped attempt escalates: %+v", got)
	}
}

func TestDecideDetectorReplacesEveryModelFailure(t *testing.T) {
	t.Parallel()
	for _, model := range []ModelOutcome{ModelOff, ModelFailed, ModelCircuitOpen, ModelLate} {
		f := idle(0)
		f.Model = model
		got := Decide(f)
		if got.Action != ActionNotify || got.Source != SourceDetector {
			t.Fatalf("model %d: got %+v, want detector notify", model, got)
		}
		f.Pane = PaneBusy
		if got := Decide(f); got.Action != ActionWait || got.Reason != ReasonAgentBusy {
			t.Fatalf("model %d busy: got %+v, want AGENT_BUSY wait", model, got)
		}
		f.Pane = PaneUnknown
		if got := Decide(f); got.Action != ActionWait || got.Reason != ReasonStateUnknown {
			t.Fatalf("model %d unknown: got %+v, want STATE_UNKNOWN wait", model, got)
		}
	}
}

func TestDecideModelAnswerWinsOverDetector(t *testing.T) {
	t.Parallel()
	f := idle(0)
	f.Model = ModelDefer
	if got := Decide(f); got.Action != ActionWait || got.Source != SourceModel {
		t.Fatalf("got %+v, want model defer", got)
	}
	f.Model, f.Pane = ModelNotify, PaneBusy
	if got := Decide(f); got.Action != ActionNotify || got.Source != SourceModel {
		t.Fatalf("got %+v, want model notify", got)
	}
}

// A Glimmer failure never releases a human hold: every control holds,
// in precedence order, and none of them escalates.
func TestDecideControlsOutrankEverything(t *testing.T) {
	t.Parallel()
	cases := []struct {
		controls Controls
		action   Action
		want     Reason
	}{
		{Controls{Off: true, Hold: true}, ActionHold, ReasonControlOff},
		{Controls{Hold: true, Manual: true}, ActionHold, ReasonControlHold},
		{Controls{Manual: true}, ActionHold, ReasonControlManual},
		{Controls{Rejected: true}, ActionDrop, ReasonControlRejected},
		{Controls{Expired: true}, ActionHold, ReasonControlExpired},
		{Controls{BudgetExhausted: true}, ActionHold, ReasonControlBudget},
		{Controls{ApprovalPending: true}, ActionHold, ReasonApprovalPending},
	}
	for _, tc := range cases {
		f := idle(time.Hour)
		f.Controls = tc.controls
		f.Model = ModelCircuitOpen
		got := Decide(f)
		if got.Action != tc.action || got.Reason != tc.want || got.Escalate {
			t.Fatalf("%+v: got %+v, want %d %s without escalation", tc.controls, got, tc.action, tc.want)
		}
	}
}

func TestDecideGuardsOutrankJudgement(t *testing.T) {
	t.Parallel()
	f := idle(0)
	f.GuardsPassed = false
	f.Model = ModelNotify
	if got := Decide(f); got.Action != ActionWait || got.Reason != ReasonGuardFailed {
		t.Fatalf("got %+v, want GUARD_FAILED wait", got)
	}
	if got := Decide(Facts{Now: t0, PendingSince: t0, Pane: PaneIdle}); got.Action == ActionNotify {
		t.Fatalf("zero GuardsPassed notified: %+v", got)
	}
}

func TestDecideRenoticeSchedule(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		now     time.Duration
		notices []time.Duration
		want    Action
		reason  Reason
		next    time.Duration
	}{
		{"first notice", 0, nil, ActionNotify, ReasonNotify, 5 * time.Minute},
		{"covered before 5m", 4 * time.Minute, []time.Duration{0}, ActionWait, ReasonNotDue, 5 * time.Minute},
		{"renotice at 5m", 5 * time.Minute, []time.Duration{0}, ActionNotify, ReasonNotify, 15 * time.Minute},
		{"renotice at 15m", 16 * time.Minute, []time.Duration{0, 5 * time.Minute}, ActionNotify, ReasonNotify, 45 * time.Minute},
		{"renotice at 45m", 45 * time.Minute, []time.Duration{0, 5 * time.Minute, 15 * time.Minute}, ActionNotify, ReasonNotify, 2 * time.Hour},
		{"renotice at 2h", 2 * time.Hour, []time.Duration{45 * time.Minute}, ActionNotify, ReasonNotify, 4 * time.Hour},
		{"renotice at 4h", 4 * time.Hour, []time.Duration{2 * time.Hour}, ActionNotify, ReasonNotify, 0},
		{"schedule done", 9 * time.Hour, []time.Duration{4 * time.Hour}, ActionWait, ReasonScheduleDone, 0},
		// Slots missed while busy collapse: one notice at 50m covers 5, 15 and 45.
		{"missed slots collapse", 50 * time.Minute, nil, ActionNotify, ReasonNotify, 2 * time.Hour},
		{"one notice covers", 60 * time.Minute, []time.Duration{50 * time.Minute}, ActionWait, ReasonNotDue, 2 * time.Hour},
	}
	for _, tc := range cases {
		f := idle(tc.now)
		f.Escalated = true
		for _, n := range tc.notices {
			f.Notices = append(f.Notices, t0.Add(n))
		}
		got := Decide(f)
		if got.Action != tc.want || got.Reason != tc.reason {
			t.Fatalf("%s: got %+v, want %d %s", tc.name, got, tc.want, tc.reason)
		}
		want := time.Time{}
		if tc.next != 0 {
			want = t0.Add(tc.next)
		}
		if !got.Next.Equal(want) {
			t.Fatalf("%s: next %v, want %v", tc.name, got.Next, want)
		}
	}
}

func TestDecideFutureNoticeIsNotTrusted(t *testing.T) {
	t.Parallel()
	f := idle(5 * time.Minute)
	f.Notices = []time.Time{t0.Add(time.Hour)}
	if got := Decide(f); got.Action != ActionNotify {
		t.Fatalf("got %+v, want notify despite a future-stamped notice", got)
	}
}

func TestDecideEscalatesOnceAtThirtyMinutes(t *testing.T) {
	t.Parallel()
	f := idle(29 * time.Minute)
	f.Pane = PaneBusy
	if got := Decide(f); got.Escalate {
		t.Fatalf("escalated before 30m: %+v", got)
	}
	f.Now = t0.Add(30 * time.Minute)
	got := Decide(f)
	if !got.Escalate || got.Action != ActionWait {
		t.Fatalf("got %+v, want escalate while waiting on a busy agent", got)
	}
	f.Escalated = true
	if got := Decide(f); got.Escalate {
		t.Fatalf("escalated twice: %+v", got)
	}
}

// An uncertain attempt is never re-injected, but it may still escalate:
// a message stuck behind it is exactly what the operator must hear about.
func TestDecideUncertainHoldsWithoutReinjecting(t *testing.T) {
	t.Parallel()
	f := idle(31 * time.Minute)
	f.Uncertain = true
	f.Model = ModelNotify
	got := Decide(f)
	if got.Action != ActionHold || got.Reason != ReasonUncertain || !got.Escalate {
		t.Fatalf("got %+v, want uncertain hold with escalation", got)
	}
}

func TestDecideReadAndInvalidFacts(t *testing.T) {
	t.Parallel()
	f := idle(time.Hour)
	f.Read = true
	if got := Decide(f); got.Action != ActionWait || got.Reason != ReasonRead || got.Escalate {
		t.Fatalf("read: got %+v", got)
	}
	for _, bad := range []Facts{
		{Now: t0, GuardsPassed: true, Pane: PaneIdle},
		{PendingSince: t0, GuardsPassed: true, Pane: PaneIdle},
		{Now: t0.Add(-time.Second), PendingSince: t0, GuardsPassed: true, Pane: PaneIdle},
	} {
		if got := Decide(bad); got.Action != ActionHold || got.Reason != ReasonInvalidFacts {
			t.Fatalf("%+v: got %+v, want INVALID_FACTS hold", bad, got)
		}
	}
}

// No exit of Decide opens an approval or notifies without a certain
// state: across every combination, a notice needs guards, no control,
// no uncertainty and either a model notify or a certain idle pane.
func TestDecideNeverNotifiesWithoutGrounds(t *testing.T) {
	t.Parallel()
	models := []ModelOutcome{ModelOff, ModelFailed, ModelCircuitOpen, ModelLate, ModelNotify, ModelDefer}
	panes := []PaneState{PaneUnknown, PaneIdle, PaneBusy}
	for _, model := range models {
		for _, pane := range panes {
			for _, guards := range []bool{false, true} {
				for _, uncertain := range []bool{false, true} {
					for _, hold := range []bool{false, true} {
						f := Facts{
							Now: t0, PendingSince: t0, GuardsPassed: guards, Model: model,
							Pane: pane, Uncertain: uncertain, Controls: Controls{Hold: hold},
						}
						got := Decide(f)
						grounds := model == ModelNotify || (model != ModelDefer && pane == PaneIdle)
						allowed := guards && !uncertain && !hold && grounds
						if (got.Action == ActionNotify) != allowed {
							t.Fatalf("%+v: got %+v, notify allowed=%v", f, got, allowed)
						}
					}
				}
			}
		}
	}
}

// The write verdict comes from decide: denial ends the attempt even with
// everything else green, and a deny outranks every other set control.
func TestDecideVerdictComesFromDecide(t *testing.T) {
	t.Parallel()
	f := idle(0)
	f.Controls = Controls{Rejected: true, Off: true}
	f.Model, f.Pane = ModelNotify, PaneIdle
	if got := Decide(f); got.Action != ActionDrop || got.Reason != ReasonControlRejected {
		t.Fatalf("got %+v, want terminal drop with the rejection named", got)
	}
	if got := controlOf(Controls{}); got != decide.ControlAllow {
		t.Fatalf("no control must let the attempt through, got %d", got)
	}
}

// A model outcome outside the enum is an unknown state: it retains the
// attempt and never notifies, even on a certain idle pane past escalation.
func TestDecideInvalidModelNeverNotifies(t *testing.T) {
	t.Parallel()
	f := idle(31 * time.Minute)
	f.Model = ModelOutcome(99)
	f.Pane = PaneIdle
	if got := Decide(f); got.Action != ActionHold || got.Reason != ReasonInvalidModel {
		t.Fatalf("got %+v, want invalid-model hold", got)
	}
	if got := Decide(f); got.Action == ActionNotify {
		t.Fatalf("invalid model notified: %+v", got)
	}
}

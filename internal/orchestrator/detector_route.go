package orchestrator

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/detect"
	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/protocol"
)

// DeterministicPTYPolicy is the policy of a PTY delivery the detector
// alone decided, without an orchestration model. Its decisions carry it in
// policy_version and its guard detail names it.
const DeterministicPTYPolicy = "deterministic-pty-v1"

// CorroboratedPTYPolicy is the policy of a PTY delivery the analyst found
// idle below its calibrated threshold and the detector corroborated: the
// model still chooses, on the detector's quiet screen.
const CorroboratedPTYPolicy = DeterministicPTYPolicy + "+analyst"

const (
	// detectorQuietWindow is how long the same verdict on the same screen
	// must have held: the tmux route's output quiet window.
	detectorQuietWindow = 5 * time.Second
	// detectorFreshness bounds the age of the newest verdict. The Proxy
	// cuts a quiet session again every second, so a verdict this old means
	// the cuts stopped.
	detectorFreshness = 3 * time.Second
)

// detectorReference is the screen a detector decision read: tx C and the
// commit go ahead only on the same unbroken quiet window.
type detectorReference struct {
	quietSince   int64
	screenDigest protocol.Digest
	guardVersion int64
}

func detectorReferenceOf(d *agentlink.DetectorEvidence) detectorReference {
	if d == nil {
		return detectorReference{}
	}
	return detectorReference{
		quietSince: d.QuietSince.UnixNano(), screenDigest: d.Verdict.ScreenDigest,
		guardVersion: d.InputGuardVersion,
	}
}

const (
	reasonNotMeasured = "harness build is not measured"
	reasonNoVerdict   = "no detector verdict"
	reasonNoManifest  = "session manifest is unavailable"
	reasonQuestion    = "agent asked a question"
)

// detectorWaits reports whether a refusal of DeterministicPTYPolicy can
// clear by itself: the harness is measured and its Proxy sends verdicts,
// so a busy or not yet quiet screen is waited out rather than handed to
// the human.
func detectorWaits(reason string) bool {
	return reason != "" && reason != reasonNotMeasured && reason != reasonNoVerdict &&
		reason != reasonNoManifest
}

// detectorEligible applies DeterministicPTYPolicy to the link's newest
// verdict: a measured profile for the session's exact harness build, a
// verdict on the current guard version that is certain, not busy, with an
// empty composer and the harness in front, received within
// detectorFreshness and unchanged for detectorQuietWindow. The reason is a
// fixed phrase, never screen text.
func (e *Executor) detectorEligible(snapshot agentlink.Snapshot, manifestJSON string, now time.Time) (bool, string) {
	var manifest protocol.CapabilityManifest
	if json.Unmarshal([]byte(manifestJSON), &manifest) != nil || manifest.Validate() != nil {
		return false, reasonNoManifest
	}
	version := profiles.ParseVersion(manifest.HarnessVersion)
	profile, err := e.processProfiles.Profile(string(manifest.Harness), version)
	if version == "" || err != nil || profile.Harness != string(manifest.Harness) || !profile.Measured {
		return false, reasonNotMeasured
	}
	d := snapshot.Detector
	switch {
	case d == nil:
		return false, reasonNoVerdict
	case !snapshot.HasReport || d.InputGuardVersion != snapshot.LastReport.InputGuardVersion:
		return false, "detector verdict is for another guard version"
	case now.Sub(d.ReceivedAt) > detectorFreshness:
		return false, "detector verdict is not fresh"
	case !d.Verdict.Certain:
		return false, "detector is not certain"
	case !d.Verdict.IsCLI:
		return false, "harness is not in front"
	case d.Verdict.Busy && d.Verdict.Detail == detect.DetailQuestionVeto:
		return false, reasonQuestion
	case d.Verdict.Busy:
		return false, "harness is busy"
	case d.Verdict.BoxBusy:
		return false, "input box holds text"
	case now.Sub(d.QuietSince) < detectorQuietWindow:
		return false, "screen is not quiet yet"
	}
	return true, ""
}

// corroborable reports whether an analyst state misses eligibility only on
// its calibrated threshold: every earlier check passed, the analyst reads
// the agent idle at its prompt, and the same call's calibrated probability of
// WAITING_APPROVAL plus ASKING_QUESTION is below the configured limit, and
// the measured profile of the session's harness build sets corroboration.
// The detector may then corroborate it. Without a limit, or for a harness
// whose question screens the analyst has not been checked on, nothing
// corroborates.
//
// The producer derives a PTY state's delivery safety from the same
// calibrated idle probability, so below the threshold it is never SAFE, only
// UNCERTAIN; the detector's verdict then supplies the safety evidence. UNSAFE,
// which names human input, never corroborates.
func (e *Executor) corroborable(state StateRead, answered bool) bool {
	if state.AutomaticEligible || state.EligibilityReason != StateEligibilityBelowThreshold ||
		state.Snapshot.AgentPhase != protocol.AgentPhaseIdleAtPrompt {
		return false
	}
	if e.unsafePhaseLimit <= 0 || !state.unsafePhaseValid || state.unsafePhaseMass >= e.unsafePhaseLimit && !answered {
		return false
	}
	if e.corroborates == nil || !e.corroborates(state.harness, state.harnessVersion) {
		return false
	}
	return state.Snapshot.DeliverySafety == protocol.DeliverySafetySafe ||
		state.Snapshot.DeliverySafety == protocol.DeliverySafetyUncertain
}

// profileCorroborates reads the corroboration switch of the measured profile
// for a harness build; an unknown or unmeasured build never corroborates.
func profileCorroborates(loaded profiles.Profiles) func(protocol.Harness, string) bool {
	return func(harness protocol.Harness, version string) bool {
		parsed := profiles.ParseVersion(version)
		profile, err := loaded.Profile(string(harness), parsed)
		return parsed != "" && err == nil && profile.Harness == string(harness) &&
			profile.Measured && profile.Corroboration
	}
}

// corroborationGuard records, in the decision's guards, that the detector
// corroborated an analyst state below its threshold. A passing guard
// carries no detail; the link's detector verdict says whether an answered
// question set the unsafe-phase limit aside.
func (e *Executor) corroborationGuard(state StateRead) protocol.GuardResult {
	return guard(protocol.GuardNameDetectorFallback, true, fmt.Sprintf(
		"p_waiting_approval+p_asking_question %.6f < unsafe_phase_limit %.6f; p_idle %.6f; profile %s corroboration",
		state.unsafePhaseMass, e.unsafePhaseLimit, state.IdleProbability, state.harness))
}

// questionAnswered reports whether the link's newest verdict says the
// human answered the agent's last question: the analyst's reading of that
// question on the same screen is then not a reason to hold.
func questionAnswered(snapshot agentlink.Snapshot) bool {
	return snapshot.Detector != nil && snapshot.Detector.Verdict.Detail == detect.DetailQuestionAnswered
}

// detectorGuard is p_idle_threshold under DeterministicPTYPolicy. Its
// failure keeps the historical detail first and names the policy's reason.
func detectorGuard(eligible bool, reason string) protocol.GuardResult {
	return guard(protocol.GuardNamePIdleThreshold, eligible,
		"no orchestration model is available; "+DeterministicPTYPolicy+" is not eligible: "+reason)
}

// detectorHolds is the commit's check of a detector decision, after the
// prepare: the policy still holds on the same unbroken quiet window the
// decision read.
func (e *Executor) detectorHolds(ctx context.Context, tx *sql.Tx, plan executionPlan) bool {
	snapshot, ok := e.linkSnapshot(ctx, tx, plan.record.AgentID)
	if !ok || snapshot.State != agentlink.StateUp || snapshot.SessionID != plan.record.sessionID {
		return false
	}
	eligible, _ := e.detectorEligible(snapshot, plan.automatic.manifest, e.clock.Now())
	return eligible && detectorReferenceOf(snapshot.Detector) == plan.automatic.detector
}

// shouldWaitForDetector recognizes a DEFER of the detector route that only
// waits on the harness or its human: the policy is eligible or refuses for
// a reason detectorWaits accepts, and every failed guard is that
// p_idle_threshold, descendants still settling, or the human draft or
// activity. The rolled-back pass leaves no decision and no
// approval, so the next worker pass decides again; expiry still ends it.
func shouldWaitForDetector(view automaticView, rule allowedRule) bool {
	if len(rule.actions) != 1 || rule.actions[0] != protocol.OrchestratorActionDefer ||
		!view.detectorOK && !detectorWaits(view.detectorReason) ||
		rule.note != firstFailedGuard(view.guards) {
		return false
	}
	for _, result := range view.guards {
		if result.Outcome == protocol.GuardOutcomePass {
			continue
		}
		if result.Outcome != protocol.GuardOutcomeFail {
			return false
		}
		switch result.Guard {
		case protocol.GuardNamePIdleThreshold, protocol.GuardNameHumanDraftClosed,
			protocol.GuardNameHumanActivityNotActive:
		case protocol.GuardNameNoNewChildOrTool:
			if !strings.HasPrefix(result.Detail, untrustedProcessDetail+settlingDetail) {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// detectorWaitNotes holds, per agent, why the detector route last waited:
// a guard name and fixed detail, never screen text. It is cleared by the
// next pass that decides.
type detectorWaitNotes struct {
	mu    sync.Mutex
	notes map[protocol.AgentID]string
}

func (w *detectorWaitNotes) set(agent protocol.AgentID, note string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.notes == nil {
		w.notes = make(map[protocol.AgentID]string)
	}
	w.notes[agent] = note
}

func (w *detectorWaitNotes) forget(agent protocol.AgentID) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.notes, agent)
}

// DetectorWait reports why the detector route is waiting to deliver to
// agent, if it is.
func (e *Executor) DetectorWait(agent protocol.AgentID) (string, bool) {
	e.detectorWaits.mu.Lock()
	defer e.detectorWaits.mu.Unlock()
	note, ok := e.detectorWaits.notes[agent]
	return note, ok
}

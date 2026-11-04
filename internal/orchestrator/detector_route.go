package orchestrator

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/smellak/gifford/internal/daemon/agentlink"
	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/protocol"
)

// DeterministicPTYPolicy is the policy of a PTY delivery the detector
// alone decided, without an orchestration model. Its decisions carry it in
// policy_version and its guard detail names it.
const DeterministicPTYPolicy = "deterministic-pty-v1"

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

// detectorEligible applies DeterministicPTYPolicy to the link's newest
// verdict: a measured profile for the session's exact harness build, a
// verdict on the current guard version that is certain, not busy, with an
// empty composer and the harness in front, received within
// detectorFreshness and unchanged for detectorQuietWindow. The reason is a
// fixed phrase, never screen text.
func (e *Executor) detectorEligible(snapshot agentlink.Snapshot, manifestJSON string, now time.Time) (bool, string) {
	var manifest protocol.CapabilityManifest
	if json.Unmarshal([]byte(manifestJSON), &manifest) != nil || manifest.Validate() != nil {
		return false, "session manifest is unavailable"
	}
	version := profiles.ParseVersion(manifest.HarnessVersion)
	profile, err := e.processProfiles.Profile(string(manifest.Harness), version)
	if version == "" || err != nil || profile.Harness != string(manifest.Harness) || !profile.Measured {
		return false, "harness build is not measured"
	}
	d := snapshot.Detector
	switch {
	case d == nil:
		return false, "no detector verdict"
	case !snapshot.HasReport || d.InputGuardVersion != snapshot.LastReport.InputGuardVersion:
		return false, "detector verdict is for another guard version"
	case now.Sub(d.ReceivedAt) > detectorFreshness:
		return false, "detector verdict is not fresh"
	case !d.Verdict.Certain:
		return false, "detector is not certain"
	case !d.Verdict.IsCLI:
		return false, "harness is not in front"
	case d.Verdict.Busy:
		return false, "harness is busy"
	case d.Verdict.BoxBusy:
		return false, "input box holds text"
	case now.Sub(d.QuietSince) < detectorQuietWindow:
		return false, "screen is not quiet yet"
	}
	return true, ""
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

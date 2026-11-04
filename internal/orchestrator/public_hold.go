package orchestrator

import "github.com/smellak/gifford/internal/protocol"

// detectorReasons are DeterministicPTYPolicy's fixed refusal reasons.
var detectorReasons = []string{
	reasonNotMeasured, reasonNoVerdict, reasonNoManifest,
	"detector verdict is for another guard version", "detector verdict is not fresh",
	"detector is not certain", "harness is not in front", "harness is busy",
	"input box holds text", "screen is not quiet yet",
}

// publicDetails are the guard details that are fixed phrases, never text
// that came from a process, a path or an error. Only these may leave the
// node's decision rows for an operator.
var publicDetails = func() map[string]bool {
	details := map[string]bool{
		"session is not CONNECTED":                                       true,
		"recovery session does not match the uncertain attempt":          true,
		"session is not a deliverable PTY Proxy":                         true,
		"agent link has no fresh high-water report":                      true,
		"human draft is not CLOSED":                                      true,
		"human activity is ACTIVE":                                       true,
		"draft state is unavailable":                                     true,
		"activity state is unavailable":                                  true,
		"process state is unavailable":                                   true,
		"delivery expired":                                               true,
		"agent has another unresolved UNCERTAIN commit":                  true,
		"durable COMMITTED evidence exists for this recipient delivery":  true,
		"no orchestration model is available":                            true,
		untrustedProcessDetail:                                           true,
		untrustedProcessDetail + settlingDetail:                          true,
		untrustedProcessDetail + settlingDetail + ": screen not at rest": true,
		untrustedProcessDetail + settlingDetail + ": screen changed":     true,
		untrustedProcessDetail + settlingDetail + ": human key":          true,
	}
	for _, reason := range detectorReasons {
		details[detectorGuard(false, reason).Detail] = true
	}
	return details
}()

// PublicHold names why a guard held, for an operator: the guard and, only
// when it is a fixed phrase, its detail.
func PublicHold(result protocol.GuardResult) string {
	if publicDetails[result.Detail] {
		return string(result.Guard) + ": " + result.Detail
	}
	return string(result.Guard)
}

// firstPublicHold is PublicHold of the first failed guard, or empty.
func firstPublicHold(guards []protocol.GuardResult) string {
	for _, result := range guards {
		if result.Outcome == protocol.GuardOutcomeFail {
			return PublicHold(result)
		}
	}
	return ""
}

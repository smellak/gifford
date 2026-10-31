package gateway

import (
	"encoding/json"
	"strings"
	"unicode/utf8"

	"github.com/smellak/gifford/internal/protocol"
)

// Reading limits of the letter answers, TRIAGE_MESSAGE and
// CHOOSE_ORCHESTRATION_ACTION.
const (
	// minLetterMass is the least probability the contract's letters must
	// hold at the label position: the bar the endpoint already sets for
	// ANALYZE_AGENT_STATE.
	minLetterMass = 0.5
	// maxReasonRunes bounds the one reason of a
	// CHOOSE_ORCHESTRATION_ACTION answer.
	maxReasonRunes = 80
)

// readAnswer reads a decoded answer against its request and returns the
// evidence its result may carry. A letter mass present on any answer must
// lie in [0, 1]; beyond that, an ANALYZE_AGENT_STATE answer is not read
// here. A TRIAGE_MESSAGE or CHOOSE_ORCHESTRATION_ACTION answer is readable
// only if it carries a letter mass of at least minLetterMass and its label
// is the single most probable one. A CHOOSE_ORCHESTRATION_ACTION answer
// must also name only actions the request allowed and give exactly one
// reason of 1 to maxReasonRunes characters. That reason is read and
// dropped: it may echo the screen, so no result carries it.
func readAnswer(req protocol.InferRequest, wire wireResponse) ([]string, bool) {
	if wire.LetterMass != nil && !inUnit(*wire.LetterMass) {
		return nil, false
	}
	switch req.Contract {
	case protocol.ModelContractTriageMessage:
		return wire.Evidence, enoughMass(wire.LetterMass) && soleArgmax(wire.Label, wire.Probabilities)
	case protocol.ModelContractChooseOrchestrationAction:
		allowed, ok := allowedActions(req.Payload)
		if !ok || !enoughMass(wire.LetterMass) || !soleArgmax(wire.Label, wire.Probabilities) ||
			!oneReason(wire.Evidence) {
			return nil, false
		}
		for action := range wire.Probabilities {
			if !allowed[action] {
				return nil, false
			}
		}
		return nil, true
	default:
		return wire.Evidence, true
	}
}

// inUnit reports whether p lies in [0, 1]. NaN does not.
func inUnit(p float64) bool { return p >= 0 && p <= 1 }

// enoughMass reports whether a letter mass is present and at least
// minLetterMass.
func enoughMass(mass *float64) bool { return mass != nil && *mass >= minLetterMass }

// soleArgmax reports whether label is in probs and more probable than
// every other label there: a tie at the top is no answer.
func soleArgmax(label string, probs map[string]float64) bool {
	top, ok := probs[label]
	if !ok {
		return false
	}
	for other, p := range probs {
		if other != label && p >= top {
			return false
		}
	}
	return true
}

// oneReason reports whether evidence is exactly one reason of 1 to
// maxReasonRunes characters that is not only white space.
func oneReason(evidence []string) bool {
	return len(evidence) == 1 && strings.TrimSpace(evidence[0]) != "" &&
		utf8.RuneCountInString(evidence[0]) <= maxReasonRunes
}

// allowedActions is the action set of a CHOOSE_ORCHESTRATION_ACTION
// request, which InferRequest.Validate has already decoded strictly.
func allowedActions(payload protocol.Payload) (map[string]bool, bool) {
	var req protocol.OrchestrationRequest
	if json.Unmarshal(payload, &req) != nil {
		return nil, false
	}
	set := make(map[string]bool, len(req.AllowedActions))
	for _, action := range req.AllowedActions {
		set[string(action)] = true
	}
	return set, len(set) > 0
}

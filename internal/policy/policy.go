package policy

import (
	"path"

	"github.com/smellak/gifford/internal/protocol"
)

// Effect is the result a matching rule contributes.
type Effect string

// Policy effects.
const (
	EffectAllow Effect = "allow"
	EffectDeny  Effect = "deny"
)

// Action is one operation a remote node may request.
type Action string

// Actions understood by policy v1.
const (
	ActionDeliverMessage Action = "deliver_message"
	ActionAck            Action = "ack"
	ActionRevoke         Action = "revoke"
	ActionFetchKeys      Action = "fetch_keys"
	ActionFetchArtifact  Action = "fetch_artifact"
	ActionListAgents     Action = "list_agents"
	ActionReadPolicies   Action = "read_policies"
)

// Decision is the fail-closed result of policy evaluation.
type Decision string

// Policy decisions.
const (
	DecisionAllow Decision = "ALLOW"
	DecisionDeny  Decision = "DENY"
)

// Rule is the canonical, validated form of one policy rule.
type Rule struct {
	ID     string `json:"id"`
	Effect Effect `json:"effect"`
	Peer   string `json:"peer"`
	Action Action `json:"action"`
	Agent  string `json:"agent"`
}

// Policy is a completely validated ruleset. Rules are kept private so a
// caller cannot mutate the compiled policy after its digest is calculated.
type Policy struct {
	Version      string
	Digest       string
	CompiledJSON string
	rules        []Rule
}

// Rules returns a copy of the canonical rules ordered by id.
func (policy *Policy) Rules() []Rule {
	if policy == nil {
		return nil
	}
	return append([]Rule(nil), policy.rules...)
}

// Evaluation explains the rule and policy version behind a decision. RuleID
// is empty when no rule matched.
type Evaluation struct {
	Decision Decision
	RuleID   string
	Version  string
}

// Evaluate decides one authenticated peer action without reading storage,
// certificates, or the network. Any invalid input or missing match denies.
func Evaluate(
	policy *Policy,
	peer protocol.NodeID,
	organizationID string,
	action Action,
	agent protocol.AgentID,
) Evaluation {
	result := Evaluation{Decision: DecisionDeny}
	if policy == nil {
		return result
	}
	result.Version = policy.Version
	if !peer.Valid() || !agent.Valid() || !validAction(action) {
		return result
	}
	var allowRule string
	for _, rule := range policy.rules {
		if rule.Action != action || !peerMatches(rule.Peer, peer, organizationID) {
			continue
		}
		matched, err := path.Match(rule.Agent, string(agent))
		if err != nil || !matched {
			continue
		}
		if rule.Effect == EffectDeny {
			result.RuleID = rule.ID
			return result
		}
		if allowRule == "" {
			allowRule = rule.ID
		}
	}
	if allowRule != "" {
		result.Decision = DecisionAllow
		result.RuleID = allowRule
	}
	return result
}

func peerMatches(rulePeer string, peer protocol.NodeID, organizationID string) bool {
	switch {
	case rulePeer == "*":
		return true
	case len(rulePeer) > len("org:") && rulePeer[:len("org:")] == "org:":
		return rulePeer[len("org:"):] == organizationID
	default:
		return rulePeer == string(peer)
	}
}

func validAction(action Action) bool {
	switch action {
	case ActionDeliverMessage, ActionAck, ActionRevoke, ActionFetchKeys,
		ActionFetchArtifact, ActionListAgents, ActionReadPolicies:
		return true
	default:
		return false
	}
}

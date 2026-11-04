package config

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/smellak/gifford/internal/protocol"
)

// ErrInvalidEscalateAfter means escalate_after is not a duration string
// between MinEscalateAfter and MaxEscalateAfter.
var ErrInvalidEscalateAfter = errors.New("invalid config escalate_after")

const (
	// MaxOperatorAgents bounds operator_agents.
	MaxOperatorAgents = 8
	// MinEscalateAfter is the shortest escalate_after.
	MinEscalateAfter = time.Minute
	// MaxEscalateAfter is the longest escalate_after: the day an
	// operator notice stays deliverable.
	MaxEscalateAfter = 24 * time.Hour
)

// Operators returns the configured operators in order: operator_agents,
// or operator_agent alone, or none. The first owns the incident register.
func (c Config) Operators() []protocol.AgentID {
	if len(c.OperatorAgents) > 0 {
		return slices.Clone(c.OperatorAgents)
	}
	if c.OperatorAgent != "" {
		return []protocol.AgentID{c.OperatorAgent}
	}
	return nil
}

// applyOperatorAgents reads operator_agents: a list of 1 to
// MaxOperatorAgents distinct agent ids, set only when operator_agent is
// not. An absent key leaves the list empty.
func applyOperatorAgents(n *yaml.Node, single bool, dst *[]protocol.AgentID) error {
	if n.Kind == 0 {
		return nil
	}
	if single {
		return fmt.Errorf("set operator_agent or operator_agents, not both: %w", ErrInvalidOperatorAgent)
	}
	if n.Kind != yaml.SequenceNode || len(n.Content) == 0 || len(n.Content) > MaxOperatorAgents {
		return fmt.Errorf("operator_agents must list 1 to %d agent ids: %w", MaxOperatorAgents, ErrInvalidOperatorAgent)
	}
	operators := make([]protocol.AgentID, 0, len(n.Content))
	for _, item := range n.Content {
		agent := protocol.AgentID(item.Value)
		if item.Kind != yaml.ScalarNode || item.ShortTag() != tagStr || !agent.Valid() {
			return fmt.Errorf("operator_agents: %w", ErrInvalidOperatorAgent)
		}
		if slices.Contains(operators, agent) {
			return fmt.Errorf("operator_agents lists %s twice: %w", agent, ErrInvalidOperatorAgent)
		}
		operators = append(operators, agent)
	}
	*dst = operators
	return nil
}

// applyEscalateAfter reads escalate_after. An absent key keeps the node
// default.
func applyEscalateAfter(n *yaml.Node, dst *time.Duration) error {
	if n.Kind == 0 {
		return nil
	}
	if n.Kind != yaml.ScalarNode || n.ShortTag() != tagStr {
		return fmt.Errorf("escalate_after must be a duration string: %w", ErrInvalidEscalateAfter)
	}
	after, err := time.ParseDuration(n.Value)
	if err != nil || after < MinEscalateAfter || after > MaxEscalateAfter {
		return fmt.Errorf("escalate_after must be between %s and %s: %w",
			MinEscalateAfter, MaxEscalateAfter, ErrInvalidEscalateAfter)
	}
	*dst = after
	return nil
}

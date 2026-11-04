package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

// nodeOperator is the operator the config fixtures name first.
const nodeOperator = protocol.AgentID("node-operator")

func TestLoadOperatorAgent(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, body string
		want       protocol.AgentID
		invalid    bool
	}{
		{"omitted", "# No operator is configured.\n", "", false},
		{"configured", "operator_agent: node-operator\n", nodeOperator, false},
		{"empty", "operator_agent: ''\n", "", true},
		{"null", "operator_agent: null\n", "", true},
		{"number", "operator_agent: 123\n", "", true},
		{"malformed", "operator_agent: Bad Agent\n", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "config.yaml")
			require.NoError(t, os.WriteFile(path, []byte(tc.body), 0o600))
			cfg, err := Load(path)
			if tc.invalid {
				require.ErrorIs(t, err, ErrInvalidOperatorAgent)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, cfg.OperatorAgent)
		})
	}
}

func TestLoadOperatorAgents(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, body string
		want       []protocol.AgentID
		invalid    bool
	}{
		{"no operator", "# No operator is configured.\n", nil, false},
		{"single key", "operator_agent: node-operator\n", []protocol.AgentID{nodeOperator}, false},
		{
			"list in order", "operator_agents: [node-operator, second-operator]\n",
			[]protocol.AgentID{nodeOperator, "second-operator"},
			false,
		},
		{"one entry", "operator_agents: [node-operator]\n", []protocol.AgentID{nodeOperator}, false},
		{"both keys", "operator_agent: node-operator\noperator_agents: [second-operator]\n", nil, true},
		{"empty list", "operator_agents: []\n", nil, true},
		{"scalar", "operator_agents: node-operator\n", nil, true},
		{"null list", "operator_agents: null\n", nil, true},
		{"twice", "operator_agents: [node-operator, node-operator]\n", nil, true},
		{"number entry", "operator_agents: [node-operator, 123]\n", nil, true},
		{"malformed entry", "operator_agents: [Bad Agent]\n", nil, true},
		{"too many", "operator_agents: [a1, a2, a3, a4, a5, a6, a7, a8, a9]\n", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "config.yaml")
			require.NoError(t, os.WriteFile(path, []byte(tc.body), 0o600))
			cfg, err := Load(path)
			if tc.invalid {
				require.ErrorIs(t, err, ErrInvalidOperatorAgent)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, cfg.Operators())
		})
	}
}

func TestLoadEscalateAfter(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, body string
		want       time.Duration
		invalid    bool
	}{
		{"no deadline", "# The node default applies.\n", 0, false},
		{"ten minutes", "escalate_after: 10m\n", 10 * time.Minute, false},
		{"lower bound", "escalate_after: 1m\n", time.Minute, false},
		{"upper bound", "escalate_after: 24h\n", 24 * time.Hour, false},
		{"too short", "escalate_after: 59s\n", 0, true},
		{"too long", "escalate_after: 25h\n", 0, true},
		{"not a duration", "escalate_after: soon\n", 0, true},
		{"number", "escalate_after: 600\n", 0, true},
		{"null duration", "escalate_after: null\n", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "config.yaml")
			require.NoError(t, os.WriteFile(path, []byte(tc.body), 0o600))
			cfg, err := Load(path)
			if tc.invalid {
				require.ErrorIs(t, err, ErrInvalidEscalateAfter)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, cfg.EscalateAfter)
		})
	}
}

package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

func TestLoadOperatorAgent(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, body string
		want       protocol.AgentID
		invalid    bool
	}{
		{"omitted", "# No operator is configured.\n", "", false},
		{"configured", "operator_agent: node-operator\n", "node-operator", false},
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

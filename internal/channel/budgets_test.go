package channel

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
)

// expectedBudgets holds the §9 values per profile.
func expectedBudgets() map[protocol.BudgetProfile]protocol.LoopBudget {
	shared := protocol.LoopBudget{
		MaxDurationMS:          600000,
		MaxHops:                24,
		MaxConsecutiveSamePair: 6,
		MaxRetries:             3,
		MaxArtifactCount:       3,
		MaxArtifactBytes:       104857600,
	}
	demo := shared
	demo.MaxTurns = 12
	demo.MaxMessages = 24
	demo.MaxMessagesPerMinute = 12
	strict := shared
	strict.MaxTurns = 6
	strict.MaxMessages = 12
	strict.MaxMessagesPerMinute = 6
	return map[protocol.BudgetProfile]protocol.LoopBudget{
		protocol.BudgetProfileDemo:    demo,
		protocol.BudgetProfileDefault: demo,
		protocol.BudgetProfileStrict:  strict,
	}
}

// TestBudgets proves the embedded profiles match §9 exactly, unknown
// profiles fail, and the parser names the bad file and key.
func TestBudgets(t *testing.T) {
	t.Parallel()

	budgets, err := LoadBudgets()
	require.NoError(t, err)
	for profile, want := range expectedBudgets() {
		got, err := budgets.Profile(profile)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}

	_, err = budgets.Profile(protocol.BudgetProfileCustom)
	require.ErrorIs(t, err, ErrUnknownBudgetProfile)
	_, err = budgets.Profile("bogus")
	require.ErrorIs(t, err, ErrUnknownBudgetProfile)

	cases := []struct {
		name string
		yaml string
	}{
		{name: "unknown key", yaml: "max_turns: 12\nbogus: 1\n"},
		{name: "missing key", yaml: "max_turns: 12\n"},
		{name: "zero value", yaml: "max_turns: 0\nmax_messages: 24\nmax_duration_ms: 600000\nmax_messages_per_minute: 12\nmax_hops: 24\nmax_consecutive_same_pair: 6\nmax_retries: 3\nmax_artifact_count: 3\nmax_artifact_bytes: 104857600\n"},
		{name: "second document", yaml: "max_turns: 12\nmax_messages: 24\nmax_duration_ms: 600000\nmax_messages_per_minute: 12\nmax_hops: 24\nmax_consecutive_same_pair: 6\nmax_retries: 3\nmax_artifact_count: 3\nmax_artifact_bytes: 104857600\n---\nmax_turns: 12\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := parseBudgetFile("demo", []byte(tc.yaml))
			require.ErrorIs(t, err, ErrInvalidBudget)
		})
	}
}

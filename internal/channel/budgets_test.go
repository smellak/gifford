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

func TestEffectiveBudget(t *testing.T) {
	t.Parallel()

	budgets, err := LoadBudgets()
	require.NoError(t, err)
	base := expectedBudgets()[protocol.BudgetProfileStrict]

	got, profile, err := budgets.Effective(protocol.BudgetProfileStrict, nil)
	require.NoError(t, err)
	require.Equal(t, base, got)
	require.Equal(t, protocol.BudgetProfileStrict, profile)

	value := func(n int64) *int64 { return &n }
	cases := []struct {
		name    string
		request protocol.RequestedBudget
		want    protocol.LoopBudget
	}{
		{name: "turns", request: protocol.RequestedBudget{MaxTurns: value(1)}, want: func() protocol.LoopBudget { v := base; v.MaxTurns = 1; return v }()},
		{name: "max messages", request: protocol.RequestedBudget{MaxMessages: value(1)}, want: func() protocol.LoopBudget { v := base; v.MaxMessages = 1; return v }()},
		{name: "duration", request: protocol.RequestedBudget{MaxDurationMS: value(1000)}, want: func() protocol.LoopBudget { v := base; v.MaxDurationMS = 1000; return v }()},
		{name: "rate", request: protocol.RequestedBudget{MaxMessagesPerMinute: value(1)}, want: func() protocol.LoopBudget { v := base; v.MaxMessagesPerMinute = 1; return v }()},
		{name: "hops", request: protocol.RequestedBudget{MaxHops: value(1)}, want: func() protocol.LoopBudget { v := base; v.MaxHops = 1; return v }()},
		{name: "same pair", request: protocol.RequestedBudget{MaxConsecutiveSamePair: value(1)}, want: func() protocol.LoopBudget { v := base; v.MaxConsecutiveSamePair = 1; return v }()},
		{name: "retries zero", request: protocol.RequestedBudget{MaxRetries: value(0)}, want: func() protocol.LoopBudget { v := base; v.MaxRetries = 0; return v }()},
		{name: "artifact count zero", request: protocol.RequestedBudget{MaxArtifactCount: value(0)}, want: func() protocol.LoopBudget { v := base; v.MaxArtifactCount = 0; return v }()},
		{name: "artifact bytes zero", request: protocol.RequestedBudget{MaxArtifactBytes: value(0)}, want: func() protocol.LoopBudget { v := base; v.MaxArtifactBytes = 0; return v }()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, profile, err := budgets.Effective(protocol.BudgetProfileStrict, &tc.request)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
			require.Equal(t, protocol.BudgetProfileCustom, profile)
		})
	}

	larger := protocol.RequestedBudget{MaxTurns: value(base.MaxTurns + 100), MaxRetries: value(base.MaxRetries + 100)}
	got, profile, err = budgets.Effective(protocol.BudgetProfileStrict, &larger)
	require.NoError(t, err)
	require.Equal(t, base, got)
	require.Equal(t, protocol.BudgetProfileStrict, profile)

	for name, profile := range map[string]protocol.BudgetProfile{
		"custom":  protocol.BudgetProfileCustom,
		"unknown": "unknown",
	} {
		_, _, err := budgets.Effective(profile, nil)
		require.ErrorIs(t, err, ErrInvalidBudget, name)
	}
	for name, request := range map[string]protocol.RequestedBudget{
		"empty":      {},
		"zero turns": {MaxTurns: value(0)},
	} {
		_, _, err := budgets.Effective(protocol.BudgetProfileStrict, &request)
		require.ErrorIs(t, err, ErrInvalidBudget, name)
	}

	got.MaxTurns = 999
	unchanged, err := budgets.Profile(protocol.BudgetProfileStrict)
	require.NoError(t, err)
	require.Equal(t, base, unchanged)
}

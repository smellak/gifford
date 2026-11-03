//go:build linux

package tmuxattach

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseChildProcessStatUsesPIDAndFieldTwentyTwo(t *testing.T) {
	t.Parallel()

	child, err := parseChildProcessStat(
		731,
		[]byte("731 (child ) name) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 987654\n"),
	)
	require.NoError(t, err)
	require.Equal(t, PaneChild{PID: 731, Start: 987654}, child)
}

func TestParseChildProcessStatRejectsInvalidIdentity(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name string
		pid  int
		stat string
	}{
		{name: "pid", pid: 0, stat: "0 (child) S"},
		{name: "closing parenthesis", pid: 1, stat: "1 child S"},
		{name: "truncated", pid: 1, stat: "1 (child) S 1 2"},
		{name: "start time", pid: 1, stat: "1 (child) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 0"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			_, err := parseChildProcessStat(testCase.pid, []byte(testCase.stat))
			require.Error(t, err)
		})
	}
}

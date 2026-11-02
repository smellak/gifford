package detect

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Bench over the 12 sanitized Codex 0.154.0 captures from the bank
// (testdata/codex, 120x40, alphanumeric-masked, ANSI intact). Truth comes
// with the captures; the criteria are the mapper's: idle Certain and
// free, typed box occupied, working Busy, dialogs never writable.
func TestCodexBenchCaptures(t *testing.T) {
	cases := []struct {
		file          string
		certain       bool
		busy, boxBusy bool
	}{
		{"idle-01.ansi", true, false, false},
		{"idle-02.ansi", true, false, false},
		{"idle-03.ansi", true, false, false},
		{"typed-01.ansi", true, false, true},
		{"typed-02.ansi", true, false, true},
		{"typed-03.ansi", true, false, true},
		{"working-01.ansi", true, true, false},
		{"working-02.ansi", true, true, false},
		{"working-03.ansi", true, true, false},
		{"dialog-01.ansi", true, false, true},
		{"dialog-02.ansi", true, false, true},
		{"dialog-03.ansi", true, false, true},
	}
	for _, tc := range cases {
		raw, err := os.ReadFile("testdata/codex/" + tc.file)
		require.NoError(t, err)
		st := Classify(HarnessCodex, DefaultTable(), raw, 120, 40, 100, codexTree())
		require.Equal(t, tc.certain, st.Certain, "%s certain (detail %s)", tc.file, st.Detail)
		require.Equal(t, tc.busy, st.Busy, "%s busy", tc.file)
		require.Equal(t, tc.boxBusy, st.BoxBusy, "%s box (detail %s)", tc.file, st.Detail)
		require.True(t, st.IsCLI, "%s runs the CLI", tc.file)
		// Only idle screens may allow a write: certain, free box,
		// and not working.
		writable := st.Certain && !st.BoxBusy && !st.Busy
		require.Equal(t, strings.HasPrefix(tc.file, "idle-"), writable,
			"%s write gate (detail %s)", tc.file, st.Detail)
	}
}

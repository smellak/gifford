package session

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/proxy/draft"
	"github.com/smellak/gifford/internal/vt"
)

// museFrame draws a Muse frame whose prompt row is "❯ " followed by styled
// text, with the cursor on the column after "❯ ", as Muse 1.4.14 draws
// its prompt: the arrow in indexed 75, a hint in indexed 242 on the
// default background, a draft in the normal colour.
func museFrame(t *testing.T, styled string) vt.Snapshot {
	t.Helper()
	emu, err := vt.New(120, 10)
	require.NoError(t, err)
	t.Cleanup(func() { _ = emu.Close() })
	rows := []string{
		"Ordinary public transcript output", "",
		"\x1b[2m" + strings.Repeat("─", 120) + "\x1b[22m",
		"\x1b[0m\x1b[38;5;75m❯ " + styled + "\x1b[0m",
		"\x1b[2m" + strings.Repeat("─", 120) + "\x1b[22m",
		"  muse-model · high · ~/project",
	}
	_, err = emu.Write([]byte(strings.Join(rows, "\r\n") + "\x1b[4;3H"))
	require.NoError(t, err)
	require.NoError(t, emu.Flush())
	return emu.Snapshot()
}

// Muse's rotating prompt hint, every cell in the measured hint colour, reads
// as an empty input line: the screen goes to the analyst and a delivery may
// proceed. The bare prompt still reads empty, and text in any other colour,
// the hint's own words included, is the human's draft.
func TestMusePromptHintReadsEmpty(t *testing.T) {
	t.Parallel()
	loaded, err := profiles.Load()
	require.NoError(t, err)
	p, err := loaded.Profile("muse-code", "1.4.14-R7309.1")
	require.NoError(t, err)
	require.True(t, p.InputPlaceholderAnyText)
	for name, tc := range map[string]struct {
		styled  string
		text    string
		ok      bool
		reason  string
		hasRows bool
	}{
		"bare prompt":        {"", "", true, "", true},
		"hint":               {"\x1b[38;5;242m/goal pins a session objective with a progress bar\x1b[39m", "", true, "", true},
		"other hint":         {"\x1b[38;5;242mAsk to monitor a running build or log\x1b[39m", "", true, "", true},
		"draft":              {"\x1b[39mfix the tests", "fix the tests", true, LiveScreenInputNotEmpty, false},
		"draft of hint text": {"\x1b[39m/goal pins a session objective with a progress bar", "/goal pins a session objective with a progress bar", true, LiveScreenInputNotEmpty, false},
		"hint then typed":    {"\x1b[38;5;242m/goal\x1b[39m x", "/goal x", true, LiveScreenInputNotEmpty, false},
		"faint not indexed":  {"\x1b[2m/goal pins\x1b[22m", "/goal pins", true, LiveScreenInputNotEmpty, false},
		"hint on background": {"\x1b[38;5;242;48;5;234m/goal pins\x1b[0m", "/goal pins", true, LiveScreenInputNotEmpty, false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			snap := museFrame(t, tc.styled)
			text, ok := draft.CursorInput(p, snap)
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.text, text)
			rows, reason := LiveScreen(p, snap)
			require.Equal(t, tc.reason, reason)
			require.Equal(t, tc.hasRows, len(rows) > 0)
		})
	}
}

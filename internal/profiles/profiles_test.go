package profiles

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// bs is one backslash. YAML unicode escapes below go through bs so the
// source never holds a literal control character.
var bs = string(rune(92))

func TestLoadDefaults(t *testing.T) {
	t.Parallel()

	ps, err := Load()
	require.NoError(t, err)

	for _, name := range []string{"claude", "codex", "muse", "opencode", "antigravity"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			p, err := ps.Profile(name)
			require.NoError(t, err)
			require.Equal(t, name, p.Harness)
			// Defaults are xterm standard guesses, not measurements.
			require.False(t, p.Measured)
			require.Equal(t, "\r", p.Submit)
			require.Equal(t, "\n", p.InsertNewline)
			require.Equal(t, "\x15", p.ClearLine)
			require.Equal(t, "\x03", p.Cancel)
			require.False(t, p.BracketedPaste)
			require.Equal(t, ExtendedNone, p.ExtendedKeyboard)
			require.NotNil(t, p.InputLine)
		})
	}
}

func TestProfileUnknown(t *testing.T) {
	t.Parallel()

	ps, err := Load()
	require.NoError(t, err)
	_, err = ps.Profile("no-such-harness")
	require.ErrorIs(t, err, ErrUnknownHarness)
}

func validYAML() string {
	return strings.Join([]string{
		`harness: synth`,
		`measured: true`,
		`version_range: ">=2"`,
		`submit: "\r"`,
		`insert_newline: "\n"`,
		`clear_line: "` + bs + `u0015"`,
		`cancel: "` + bs + `u0003"`,
		`bracketed_paste: true`,
		`extended_keyboard: csi_u`,
		`input_line: "^` + bs + bs + `s*>` + bs + bs + `s?(.*)$"`,
		"",
	}, "\n")
}

func TestParseFileValid(t *testing.T) {
	t.Parallel()

	p, err := ParseFile("synth", []byte(validYAML()))
	require.NoError(t, err)
	require.Equal(t, "synth", p.Harness)
	require.True(t, p.Measured)
	require.Equal(t, ">=2", p.VersionRange)
	require.Equal(t, ExtendedCSIU, p.ExtendedKeyboard)
	require.Equal(t, `^\s*>\s?(.*)$`, p.InputLinePattern)

	draft, ok := p.Draft("> hello world")
	require.True(t, ok)
	require.Equal(t, "hello world", draft)
	_, ok = p.Draft("some status line")
	require.False(t, ok)
}

func TestParseFileInvalid(t *testing.T) {
	t.Parallel()

	cases := map[string]func(string) string{
		"missing harness":         func(s string) string { return dropLine(s, "harness:") },
		"harness mismatch":        func(s string) string { return strings.Replace(s, "harness: synth", "harness: other", 1) },
		"missing measured":        func(s string) string { return dropLine(s, "measured:") },
		"missing version_range":   func(s string) string { return dropLine(s, "version_range:") },
		"missing submit":          func(s string) string { return dropLine(s, "submit:") },
		"empty cancel":            func(s string) string { return dropLine(s, "cancel:") + "cancel: ''" },
		"nul in clear_line":       func(s string) string { return strings.Replace(s, "u0015", "u0000", 1) },
		"missing bracketed_paste": func(s string) string { return dropLine(s, "bracketed_paste:") },
		"missing ext keyboard":    func(s string) string { return dropLine(s, "extended_keyboard:") },
		"bad ext keyboard": func(s string) string {
			return strings.Replace(s, "extended_keyboard: csi_u", "extended_keyboard: carrier-pigeon", 1)
		},
		"missing input_line": func(s string) string { return dropLine(s, "input_line:") },
		"bad input_line": func(s string) string {
			return strings.Replace(s, "(.*)", "([a", 1)
		},
		"input_line no group": func(s string) string {
			return strings.Replace(s, "(.*)", ".*", 1)
		},
		"input_line two groups": func(s string) string {
			return strings.Replace(s, "(.*)", "(>)(.*)", 1)
		},
		"unknown key":   func(s string) string { return s + "telepathy: true" },
		"two documents": func(s string) string { return strings.Replace(s, "measured:", "---\nmeasured:", 1) },
		"not yaml":      func(string) string { return "{{{{" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := ParseFile("synth", []byte(mutate(validYAML())))
			require.ErrorIs(t, err, ErrInvalidProfile)
		})
	}
}

func dropLine(s, prefix string) string {
	var kept []string
	for _, line := range strings.Split(s, "\n") {
		if !strings.HasPrefix(line, prefix) {
			kept = append(kept, line)
		}
	}
	return strings.Join(kept, "\n")
}

// TestDraftAgainstSyntheticScreen drives Draft with lines a synthetic TUI
// would show: prompt lines yield the draft, status lines do not match. No
// real harness runs here.
func TestDraftAgainstSyntheticScreen(t *testing.T) {
	t.Parallel()

	p, err := ParseFile("synth", []byte(validYAML()))
	require.NoError(t, err)

	screen := []string{
		"working… tokens 12/128k",
		"> fix the flaky test",
		"  continued draft line",
		"esc to cancel - enter to send",
		">",
	}
	var drafts []string
	for _, line := range screen {
		if draft, ok := p.Draft(line); ok {
			drafts = append(drafts, draft)
		}
	}
	require.Equal(t, []string{"fix the flaky test", ""}, drafts)
}

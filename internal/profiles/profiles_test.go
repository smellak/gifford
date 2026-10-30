package profiles

import (
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	testSynth   = "synth"
	testGeneric = "generic"
	testOther   = "other"
)

// bs is one backslash. YAML unicode escapes below go through bs so the
// source never holds a literal control character.
var bs = string(rune(92))

func TestLoadDefaults(t *testing.T) {
	t.Parallel()

	ps, err := Load()
	require.NoError(t, err)

	for _, name := range []string{"claude-code", "codex", "muse-code", "opencode", "antigravity", testGeneric} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			p, err := ps.Profile(name, "9.9.9")
			require.NoError(t, err)
			require.Equal(t, name, p.Harness)
			// Defaults are xterm standard guesses, not measurements.
			require.False(t, p.Measured)
			// Unmeasured profiles yield no draft evidence, even when the
			// guess pattern would match.
			_, ok := p.Draft("> anything at all")
			require.False(t, ok)
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

// TestMeasuredInputLinesRejectBlankRows is a repository guard, not a loader
// rule: keycap may write a local measurement while its real harness prompt
// pattern is still unknown, but that unsafe combination must not enter the
// embedded profile bank. A blank cursor row is not evidence that a draft held
// on another row was submitted or cleared.
func TestMeasuredInputLinesRejectBlankRows(t *testing.T) {
	t.Parallel()

	safe := func(profile Profile) bool {
		if !profile.Measured {
			return true
		}
		return !profile.InputLine.MatchString("") && !profile.InputLine.MatchString("   ")
	}

	ps, err := Load()
	require.NoError(t, err)
	for _, name := range harnessNames {
		profile := ps.byName[name]
		require.Truef(t, safe(profile), "%s: measured input_line must reject blank rows", name)
	}

	for _, tc := range []struct {
		name    string
		pattern string
		want    bool
	}{
		{name: "reject catch-all", pattern: `^(.*)$`, want: false},
		{name: "accept prompt prefix", pattern: `^\s*>\s?(.*)$`, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			profile := Profile{Measured: true, InputLine: regexp.MustCompile(tc.pattern)}
			require.Equal(t, tc.want, safe(profile))
		})
	}
}

func TestProfileUnknown(t *testing.T) {
	t.Parallel()

	ps, err := Load()
	require.NoError(t, err)
	_, err = ps.Profile("no-such-harness", "1.0")
	require.ErrorIs(t, err, ErrUnknownHarness)
}

func TestParseVersion(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		line string
		want string
	}{
		{line: "2.1.280 (Claude Code)", want: "2.1.280"},
		{line: "codex-cli 0.154.0", want: "0.154.0"},
		{line: "fakecap 1.2.3 (nightly)", want: "1.2.3"},
		{line: "v2.10", want: "2.10"},
		{line: "no version here"},
		{line: ""},
	} {
		t.Run(test.line, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, test.want, ParseVersion(test.line))
		})
	}
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

	p, err := ParseFile(testSynth, []byte(validYAML()))
	require.NoError(t, err)
	require.Equal(t, testSynth, p.Harness)
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
		"missing harness":       func(s string) string { return dropLine(s, "harness:") },
		"harness mismatch":      func(s string) string { return strings.Replace(s, "harness: "+testSynth, "harness: "+testOther, 1) },
		"missing measured":      func(s string) string { return dropLine(s, "measured:") },
		"missing version_range": func(s string) string { return dropLine(s, "version_range:") },
		"empty version_range": func(s string) string {
			return strings.Replace(s, `version_range: ">=2"`, `version_range: ""`, 1)
		},
		"version_range empty seg": func(s string) string {
			return strings.Replace(s, `version_range: ">=2"`, `version_range: ">=1..0"`, 1)
		},
		"version_range empty cond": func(s string) string {
			return strings.Replace(s, `version_range: ">=2"`, `version_range: ","`, 1)
		},
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

			_, err := ParseFile(testSynth, []byte(mutate(validYAML())))
			require.ErrorIs(t, err, ErrInvalidProfile)
		})
	}
}

// TestProfileVersionFallback covers version resolution: inside the range
// the harness profile wins, outside (or with an unknown version) the
// generic profile applies, and "*" matches everything.
func TestProfileVersionFallback(t *testing.T) {
	t.Parallel()

	rangedRaw := strings.Replace(validYAML(), `version_range: ">=2"`, `version_range: ">=2.0.0,<3.0.0"`, 1)
	ranged, err := ParseFile(testSynth, []byte(rangedRaw))
	require.NoError(t, err)
	genericRaw := strings.Replace(validYAML(), "harness: "+testSynth, "harness: generic", 1)
	generic, err := ParseFile(testGeneric, []byte(genericRaw))
	require.NoError(t, err)
	ps := Profiles{byName: map[string]Profile{testSynth: ranged, testGeneric: generic}}

	for _, tc := range []struct {
		version string
		want    string
	}{
		{"2.0.0", testSynth},
		{"2.5", testSynth},
		{"2.0.0.1", testSynth},
		{"1.9.9", testGeneric},
		{"3.0.0", testGeneric},
		{"", testGeneric},
	} {
		t.Run("version "+tc.version, func(t *testing.T) {
			t.Parallel()

			p, err := ps.Profile(testSynth, tc.version)
			require.NoError(t, err)
			require.Equal(t, tc.want, p.Harness)
		})
	}

	star, err := ps.Profile(testGeneric, "")
	require.NoError(t, err)
	require.Equal(t, testGeneric, star.Harness)

	pinnedRaw := strings.Replace(validYAML(), `version_range: ">=2"`, `version_range: "1.2"`, 1)
	pinned, err := ParseFile(testSynth, []byte(pinnedRaw))
	require.NoError(t, err)
	psPinned := Profiles{byName: map[string]Profile{testSynth: pinned, testGeneric: generic}}
	for _, tc := range []struct {
		version string
		want    string
	}{
		{"1.2", testSynth},
		{"1.2.0", testSynth},
		{"1.2.1", testGeneric},
	} {
		p, err := psPinned.Profile(testSynth, tc.version)
		require.NoError(t, err)
		require.Equal(t, tc.want, p.Harness)
	}

	exactRaw := strings.Replace(validYAML(), `version_range: ">=2"`, `version_range: "2.1.280"`, 1)
	exact, err := ParseFile(testSynth, []byte(exactRaw))
	require.NoError(t, err)
	psExact := Profiles{byName: map[string]Profile{testSynth: exact, testGeneric: generic}}
	selected, err := psExact.Profile(testSynth, ParseVersion("2.1.280 (Claude Code)"))
	require.NoError(t, err)
	require.Equal(t, testSynth, selected.Harness)
	require.True(t, selected.Measured)
	selected, err = psExact.Profile(testSynth, ParseVersion("2.1.281 (Claude Code)"))
	require.NoError(t, err)
	require.Equal(t, testGeneric, selected.Harness)
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

	p, err := ParseFile(testSynth, []byte(validYAML()))
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

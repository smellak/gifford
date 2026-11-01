package profiles

import (
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	testSynth      = "synth"
	testGeneric    = "generic"
	testOther      = "other"
	testNull       = "null"
	testUnmeasured = "unmeasured"

	testVersionBeta = "2.1.280-beta.1"
	testVersionRC   = "2.1.280-rc.1"
	testVersionDev  = "2.1.280-dev"

	testMuseStableVersion = "1.4.13"
	testMuseBuildVersion  = "1.4.13-R6996.1"
	testMuseVersionLine   = "Muse Code 1.4.13 (1.4.13-R6996.1)"
	testClaudeVersion     = "2.1.280"
	testClaudeVersionLine = testClaudeVersion + " (Claude Code)"
	testCodexVersion      = "0.154.0"
	testCodexVersionLine  = "codex-cli " + testCodexVersion

	testHorizontalBorderPattern = `^─+$`
)

// bs is one backslash. YAML unicode escapes below go through bs so the
// source never holds a literal control character.
var bs = string(rune(92))

func TestLoadDefaults(t *testing.T) {
	t.Parallel()

	ps, err := Load()
	require.NoError(t, err)

	for _, name := range []string{testGeneric} {
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
			require.Empty(t, p.InputPlaceholder)
			require.False(t, p.InputBounds.Configured())
			require.Equal(t, InputAreaUnknown, p.InputArea, "no harness declares a single-line area")
		})
	}
}

func TestLoadMeasuredProfiles(t *testing.T) {
	t.Parallel()

	ps, err := Load()
	require.NoError(t, err)

	tests := []struct {
		name             string
		version          string
		otherVersion     string
		insertNewline    string
		inputLine        string
		inputPlaceholder string
		above            []InputBoundary
		below            []InputBoundary
		matchingDraftRow string
	}{
		{
			name:             "claude-code",
			version:          testClaudeVersion,
			otherVersion:     "2.1.281",
			insertNewline:    "\n",
			inputLine:        "^❯\u00a0(.*)$",
			above:            []InputBoundary{{Pattern: testHorizontalBorderPattern}},
			below:            []InputBoundary{{Pattern: testHorizontalBorderPattern}},
			matchingDraftRow: "❯\u00a0draft",
		},
		{
			name:             "codex",
			version:          testCodexVersion,
			otherVersion:     "0.154.1",
			insertNewline:    "\n",
			inputLine:        `^› (.*)$`,
			inputPlaceholder: "Ask Codex to do anything",
			above: []InputBoundary{
				{Pattern: `^$`},
				{Pattern: `^$`},
				{Pattern: `^$`, Negated: true},
			},
			below: []InputBoundary{
				{Pattern: `^$`},
				{Pattern: `^  \S.* · .* · .+$`},
			},
			matchingDraftRow: "› draft",
		},
		{
			name:             "muse-code",
			version:          testMuseBuildVersion,
			otherVersion:     testMuseStableVersion,
			insertNewline:    "\x1b\r",
			inputLine:        `^❯ ?(.*)$`,
			above:            []InputBoundary{{Pattern: testHorizontalBorderPattern}},
			below:            []InputBoundary{{Pattern: testHorizontalBorderPattern}},
			matchingDraftRow: "❯ draft",
		},
		{
			name:          "opencode",
			version:       "1.18.29",
			otherVersion:  "1.18.30",
			insertNewline: "\n",
			inputLine:     `^ *┃(?:  (.*))?$`,
			above: []InputBoundary{
				{Pattern: `^ *┃$`},
				{Pattern: `^ *┃.*$`, Negated: true},
			},
			below: []InputBoundary{
				{Pattern: `^ *┃$`},
				{Pattern: `^ *┃  \S.*$`},
				{Pattern: `^ *╹▀+$`},
			},
			matchingDraftRow: "  ┃  draft",
		},
		{
			name:             "antigravity",
			version:          "1.4.8",
			otherVersion:     "1.4.9",
			insertNewline:    "\n",
			inputLine:        `^> ?(.*)$`,
			above:            []InputBoundary{{Pattern: testHorizontalBorderPattern}},
			below:            []InputBoundary{{Pattern: testHorizontalBorderPattern}},
			matchingDraftRow: "> draft",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			profile, err := ps.Profile(test.name, test.version)
			require.NoError(t, err)
			require.Equal(t, test.name, profile.Harness)
			require.True(t, profile.Measured)
			require.Equal(t, test.version, profile.VersionRange)
			require.Equal(t, "\r", profile.Submit)
			require.Equal(t, test.insertNewline, profile.InsertNewline)
			require.Equal(t, "\x15", profile.ClearLine)
			require.Equal(t, "\x03", profile.Cancel)
			require.True(t, profile.BracketedPaste)
			require.Equal(t, ExtendedNone, profile.ExtendedKeyboard)
			require.Equal(t, test.inputLine, profile.InputLinePattern)
			require.Equal(t, test.inputPlaceholder, profile.InputPlaceholder)
			require.Equal(t, InputAreaUnknown, profile.InputArea)
			requireBoundaries(t, test.above, profile.InputBounds.Above)
			requireBoundaries(t, test.below, profile.InputBounds.Below)

			draft, ok := profile.Draft(test.matchingDraftRow)
			require.True(t, ok)
			require.Equal(t, "draft", draft)

			fallback, err := ps.Profile(test.name, test.otherVersion)
			require.NoError(t, err)
			require.Equal(t, testGeneric, fallback.Harness)
			require.False(t, fallback.Measured)
		})
	}
}

func requireBoundaries(t *testing.T, want, got []InputBoundary) {
	t.Helper()
	require.Len(t, got, len(want))
	for index := range want {
		require.Equal(t, want[index].Pattern, got[index].Pattern)
		require.Equal(t, want[index].Negated, got[index].Negated)
	}
}

func TestOpenCodeMeasuredBoxLayouts(t *testing.T) {
	t.Parallel()
	loaded, err := Load()
	require.NoError(t, err)
	profile, err := loaded.Profile("opencode", "1.18.29")
	require.NoError(t, err)
	require.Equal(t, 2, profile.InputBounds.Box.LeftColumn)
	require.Equal(t, []InputBoxLayout{
		{Columns: 80, RightMargin: 2, BottomMargin: 3},
		{Columns: 100, RightMargin: 2, BottomMargin: 2},
		{Columns: 120, RightMargin: 2, BottomMargin: 2},
		{Columns: 140, RightMargin: 44, BottomMargin: 2},
		{Columns: 160, RightMargin: 44, BottomMargin: 2},
		{Columns: 180, RightMargin: 44, BottomMargin: 2},
		{Columns: 200, RightMargin: 44, BottomMargin: 2},
		{Columns: 220, RightMargin: 44, BottomMargin: 2},
	}, profile.InputBounds.Box.Layouts)
}

// TestMeasuredProfilesDeclareInputBounds is a repository guard rather than a
// loader rule: keycap may write a local measurement before its hand-tuned box
// bounds are known, but such a profile may not enter the embedded bank. A
// single_line declaration is deliberately not an alternative for editors that
// accept insert_newline.
func TestMeasuredProfilesDeclareInputBounds(t *testing.T) {
	t.Parallel()

	safe := func(profile Profile) bool {
		return !profile.Measured || profile.InputBounds.Complete()
	}
	ps, err := Load()
	require.NoError(t, err)
	for _, name := range harnessNames {
		profile := ps.byName[name]
		require.Truef(t, safe(profile), "%s: measured profile must declare input_bounds above and below", name)
	}

	boundary := []InputBoundary{{Pattern: "^border$"}}
	for _, tc := range []struct {
		name    string
		profile Profile
		want    bool
	}{
		{name: testUnmeasured, profile: Profile{}, want: true},
		{name: "both sides", profile: Profile{Measured: true, InputBounds: InputBounds{Above: boundary, Below: boundary}}, want: true},
		{name: "above only", profile: Profile{Measured: true, InputBounds: InputBounds{Above: boundary}}},
		{name: "below only", profile: Profile{Measured: true, InputBounds: InputBounds{Below: boundary}}},
		{name: "single line is no bypass", profile: Profile{Measured: true, InputArea: InputAreaSingleLine}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, safe(tc.profile))
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
		{line: testClaudeVersionLine, want: testClaudeVersion},
		{line: testCodexVersionLine, want: testCodexVersion},
		{line: "2.1.281 (Claude Code)", want: "2.1.281"},
		{line: testMuseVersionLine, want: testMuseBuildVersion},
		{line: "Muse Code 1.4.13 (1.4.13)", want: testMuseStableVersion},
		{line: "Muse Code 1.4.13(1.4.13-R6996.1)", want: testMuseBuildVersion},
		{line: "Muse Code v1.4.13 (V1.4.13-R6996.1)", want: testMuseBuildVersion},
		{line: "Muse Code 1.4.13\t(1.4.13+R6996.1)", want: "1.4.13+R6996.1"},
		{line: testVersionBeta, want: testVersionBeta},
		{line: testVersionRC, want: testVersionRC},
		{line: testVersionDev, want: testVersionDev},
		{line: "release 2.1.280+build.7", want: "2.1.280+build.7"},
		{line: "fakecap 1.2.3 (nightly)", want: "1.2.3"},
		{line: "v2.10", want: "2.10"},
		{line: "2.1.280 and protocol 1.0"},
		{line: "2.1.280 or 2.1.281"},
		{line: "Muse Code 1.4.13 (1.4.14-R6996.1)"},
		{line: "Muse Code 1.4.13 (1.4.13.1)"},
		{line: "Muse Code 1.4.13 build (1.4.13-R6996.1)"},
		{line: "Muse Code 1.4.13\n(1.4.13-R6996.1)"},
		{line: "Muse Code 1.4.13 1.4.13-R6996.1"},
		{line: "Muse Code 1.4.13 ( 1.4.13-R6996.1)"},
		{line: "Muse Code 1.4.13 (1.4.13-R6996.1 )"},
		{line: "Muse Code 1.4.13 (1.4.13-R6996.1"},
		{line: "Muse Code 1.4.13 (1.4.13-R6996.1 dirty)"},
		{line: "Muse Code 1.4.13 (1.4.13-R6996.1 2.0)"},
		{line: "Muse Code 1.4.13 (1.4.13-R6996.1) protocol 2.0"},
		{line: "Muse1.4.13 (1.4.13-R6996.1)"},
		{line: "Muse Code 1.4.13 (1.4.13-R6996.1_dirty)"},
		{line: "2.1.280-beta..1"},
		{line: "product2.1.280"},
		{line: "2.1.280_beta"},
		{line: strings.Repeat("1.", 33) + "1"},
		{line: strings.Repeat("x", maxVersionOutputBytes+1)},
		{line: "no version here"},
		{line: ""},
	} {
		name := test.line
		if len(name) > 80 {
			name = "output exceeds parser bound"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, test.want, ParseVersion(test.line))
		})
	}
}

func TestParseVersionParenthesizedBounds(t *testing.T) {
	t.Parallel()

	firstAtLimit := "1." + strings.Repeat("1", maxVersionRunes-2)
	secondAtLimit := "1.2-" + strings.Repeat("a", maxVersionRunes-4)
	secondOverLimit := secondAtLimit + "a"
	firstOverLimit := "1." + strings.Repeat("1", maxVersionRunes-1)

	for _, test := range []struct {
		name string
		line string
		want string
	}{
		{name: "both tokens at limit", line: firstAtLimit + " (" + firstAtLimit + ")", want: firstAtLimit},
		{name: "second token at limit", line: "1.2 (" + secondAtLimit + ")", want: secondAtLimit},
		{name: "first token over limit", line: firstOverLimit + " (" + firstOverLimit + ")"},
		{name: "second token over limit", line: "1.2 (" + secondOverLimit + ")"},
	} {
		t.Run(test.name, func(t *testing.T) {
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

func TestParseFileInputPlaceholder(t *testing.T) {
	t.Parallel()

	const placeholder = "Ask Codex to do anything"
	base := strings.Replace(validYAML(), `version_range: ">=2"`, `version_range: "2.1.280"`, 1)
	p, err := ParseFile(testSynth, []byte(withInputPlaceholder(base, `"`+placeholder+`"`)))
	require.NoError(t, err)
	require.Equal(t, placeholder, p.InputPlaceholder)
	draft, ok := p.Draft("> " + placeholder)
	require.True(t, ok)
	require.Equal(t, placeholder, draft, "Profile.Draft must not special-case the placeholder")
	for name, character := range map[string]string{"ASCII": "x", "multibyte": "é"} {
		t.Run(name+" at rune limit", func(t *testing.T) {
			t.Parallel()

			atLimit := strings.Repeat(character, maxInputPlaceholderRunes)
			p, err := ParseFile(testSynth, []byte(withInputPlaceholder(base, `"`+atLimit+`"`)))
			require.NoError(t, err)
			require.Equal(t, atLimit, p.InputPlaceholder)
		})
	}

	for name, raw := range map[string]string{
		"empty":                     withInputPlaceholder(base, `""`),
		testNull:                    withInputPlaceholder(base, testNull),
		"non-string":                withInputPlaceholder(base, `42`),
		testUnmeasured:              strings.Replace(withInputPlaceholder(base, `"placeholder"`), "measured: true", "measured: false", 1),
		"wildcard range":            strings.Replace(withInputPlaceholder(base, `"placeholder"`), `version_range: "2.1.280"`, `version_range: "*"`, 1),
		"non-exact range":           strings.Replace(withInputPlaceholder(base, `"placeholder"`), `version_range: "2.1.280"`, `version_range: ">=2"`, 1),
		"leading space":             withInputPlaceholder(base, `" placeholder"`),
		"trailing space":            withInputPlaceholder(base, `"placeholder "`),
		"C0 control":                withInputPlaceholder(base, `"place\u001bholder"`),
		"DEL control":               withInputPlaceholder(base, `"place\u007fholder"`),
		"C1 control":                withInputPlaceholder(base, `"place\u0085holder"`),
		"ASCII over rune limit":     withInputPlaceholder(base, `"`+strings.Repeat("x", maxInputPlaceholderRunes+1)+`"`),
		"multibyte over rune limit": withInputPlaceholder(base, `"`+strings.Repeat("é", maxInputPlaceholderRunes+1)+`"`),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := ParseFile(testSynth, []byte(raw))
			require.ErrorIs(t, err, ErrInvalidProfile)
			require.ErrorContains(t, err, "input_placeholder")
		})
	}

	t.Run("invalid UTF-8", func(t *testing.T) {
		t.Parallel()

		raw := append([]byte(base), []byte("input_placeholder: \"")...)
		raw = append(raw, 0xff)
		raw = append(raw, '"', '\n')
		_, err := ParseFile(testSynth, raw)
		require.ErrorIs(t, err, ErrInvalidProfile)
	})
}

func withInputPlaceholder(profile, yamlValue string) string {
	return profile + "input_placeholder: " + yamlValue + "\n"
}

func TestParseFileInputBounds(t *testing.T) {
	t.Parallel()

	base := strings.Replace(validYAML(), `version_range: ">=2"`, `version_range: "2.1.280"`, 1)
	valid := base + `input_bounds:
  above:
    - "^near-above$"
    - not: "^box-row.*$"
  below:
    - "^near-below$"
    - "^bottom-border$"
`
	p, err := ParseFile(testSynth, []byte(valid))
	require.NoError(t, err)
	require.True(t, p.InputBounds.Complete())
	require.Equal(t, []string{"^near-above$", "^box-row.*$"}, []string{
		p.InputBounds.Above[0].Pattern, p.InputBounds.Above[1].Pattern,
	})
	require.True(t, p.InputBounds.Above[0].Matches("near-above"))
	require.False(t, p.InputBounds.Above[0].Matches("x near-above"), "a boundary must cover the whole row")
	require.True(t, p.InputBounds.Above[1].Negated)
	require.False(t, p.InputBounds.Above[1].Matches("box-row content"))
	require.True(t, p.InputBounds.Above[1].Matches(""), "the explicit outer row may be blank")

	validBox := strings.TrimSuffix(valid, "\n") + `
  box:
    left_column: 2
    layouts:
      - {columns: 80, right_margin: 2, bottom_margin: 3}
`
	p, err = ParseFile(testSynth, []byte(validBox))
	require.NoError(t, err)
	require.Equal(t, InputBox{
		LeftColumn: 2,
		Layouts:    []InputBoxLayout{{Columns: 80, RightMargin: 2, BottomMargin: 3}},
	}, p.InputBounds.Box)

	boxBounds := base + `input_bounds:
  above: ["^top$"]
  below: ["^bottom$"]
  box:
`

	for name, raw := range map[string]string{
		"not a mapping":       base + "input_bounds: []\n",
		testNull:              base + "input_bounds: null\n",
		"missing above":       base + "input_bounds:\n  below: [\"^bottom$\"]\n",
		"missing below":       base + "input_bounds:\n  above: [\"^top$\"]\n",
		"empty above":         base + "input_bounds:\n  above: []\n  below: [\"^bottom$\"]\n",
		"above not a list":    base + "input_bounds:\n  above: \"^top$\"\n  below: [\"^bottom$\"]\n",
		"expression not text": base + "input_bounds:\n  above: [123]\n  below: [\"^bottom$\"]\n",
		"invalid expression":  base + "input_bounds:\n  above: [\"(\"]\n  below: [\"^bottom$\"]\n",
		"not before final":    base + "input_bounds:\n  above: [{not: \"^box$\"}, \"^top$\"]\n  below: [\"^bottom$\"]\n",
		"not is not text":     base + "input_bounds:\n  above: [{not: 123}]\n  below: [\"^bottom$\"]\n",
		"unknown expression":  base + "input_bounds:\n  above: [{except: \"^box$\"}]\n  below: [\"^bottom$\"]\n",
		"multiple expression keys": base +
			"input_bounds:\n  above: [{not: \"^box$\", extra: \"^x$\"}]\n  below: [\"^bottom$\"]\n",
		"blank top border":    base + "input_bounds:\n  above: [\"^ *$\"]\n  below: [\"^bottom$\"]\n",
		"blank bottom border": base + "input_bounds:\n  above: [\"^top$\"]\n  below: [\"^ *$\"]\n",
		"unknown side":        base + "input_bounds:\n  above: [\"^top$\"]\n  below: [\"^bottom$\"]\n  sideways: [\"^x$\"]\n",
		"box not a mapping":   strings.TrimSuffix(boxBounds, "  box:\n") + "  box: []\n",
		"box missing left":    boxBounds + "    layouts: [{columns: 80, right_margin: 2, bottom_margin: 3}]\n",
		"box empty layouts":   boxBounds + "    left_column: 2\n    layouts: []\n",
		"box negative margin": boxBounds + "    left_column: 2\n    layouts: [{columns: 80, right_margin: -1, bottom_margin: 3}]\n",
		"box narrow edge":     boxBounds + "    left_column: 2\n    layouts: [{columns: 4, right_margin: 1, bottom_margin: 0}]\n",
		"box duplicate width": boxBounds + "    left_column: 2\n    layouts: [{columns: 80, right_margin: 2, bottom_margin: 3}, {columns: 80, right_margin: 2, bottom_margin: 2}]\n",
		testUnmeasured:        strings.Replace(valid, "measured: true", "measured: false", 1),
		"wildcard range":      strings.Replace(valid, `version_range: "2.1.280"`, `version_range: "*"`, 1),
		"non-exact range":     strings.Replace(valid, `version_range: "2.1.280"`, `version_range: ">=2"`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := ParseFile(testSynth, []byte(raw))
			require.ErrorIs(t, err, ErrInvalidProfile)
			require.ErrorContains(t, err, "input_bounds")
		})
	}
}

func TestParseFileInputArea(t *testing.T) {
	t.Parallel()

	unmeasured := func(s string) string { return strings.Replace(s, "measured: true", "measured: false", 1) }
	for name, raw := range map[string]string{
		"absent":             validYAML(),
		"unknown":            withInputArea(validYAML(), "unknown"),
		"unknown unmeasured": unmeasured(withInputArea(validYAML(), "unknown")),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			p, err := ParseFile(testSynth, []byte(raw))
			require.NoError(t, err)
			require.Equal(t, InputAreaUnknown, p.InputArea)
		})
	}

	// A key that breaks the line inside the draft admits drafts of more
	// lines, and insert_newline is required: no profile loads single_line.
	const notAnArea = "must be single_line or unknown"
	for name, c := range map[string]struct{ raw, reason string }{
		"single_line with insert_newline": {withInputArea(validYAML(), "single_line"), "single_line requires no insert_newline"},
		"single_line unmeasured":          {unmeasured(withInputArea(validYAML(), "single_line")), "single_line requires measured: true"},
		"another area":                    {withInputArea(validYAML(), "multi_line"), notAnArea},
		"empty":                           {withInputArea(validYAML(), `""`), notAnArea},
		testNull:                          {withInputArea(validYAML(), testNull), notAnArea},
		"non-string":                      {withInputArea(validYAML(), "42"), notAnArea},
		"tagged":                          {withInputArea(validYAML(), "!area unknown"), notAnArea},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := ParseFile(testSynth, []byte(c.raw))
			require.ErrorIs(t, err, ErrInvalidProfile)
			require.ErrorContains(t, err, "input_area: "+c.reason)
		})
	}
}

func withInputArea(profile, yamlValue string) string {
	return profile + "input_area: " + yamlValue + "\n"
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
	genericRaw = strings.Replace(genericRaw, "measured: true", "measured: false", 1)
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
	selected, err := psExact.Profile(testSynth, ParseVersion(testClaudeVersionLine))
	require.NoError(t, err)
	require.Equal(t, testSynth, selected.Harness)
	require.True(t, selected.Measured)
	selected, err = psExact.Profile(testSynth, ParseVersion("2.1.281 (Claude Code)"))
	require.NoError(t, err)
	require.Equal(t, testGeneric, selected.Harness)
	for _, line := range []string{
		testVersionBeta,
		testVersionRC,
		testVersionDev,
		"2.1.280 or 2.1.281",
		"2.1.280..1",
	} {
		selected, err = psExact.Profile(testSynth, ParseVersion(line))
		require.NoError(t, err)
		require.Equal(t, testGeneric, selected.Harness, line)
		require.False(t, selected.Measured, line)
	}
}

func TestMuseBuildVersionSelectsExactProfile(t *testing.T) {
	t.Parallel()

	exactRaw := strings.Replace(validYAML(), `version_range: ">=2"`, `version_range: "`+testMuseBuildVersion+`"`, 1)
	exact, err := ParseFile(testSynth, []byte(exactRaw))
	require.NoError(t, err)
	require.Equal(t, testMuseBuildVersion, exact.VersionRange)

	genericRaw := strings.Replace(validYAML(), "harness: "+testSynth, "harness: "+testGeneric, 1)
	genericRaw = strings.Replace(genericRaw, "measured: true", "measured: false", 1)
	generic, err := ParseFile(testGeneric, []byte(genericRaw))
	require.NoError(t, err)
	ps := Profiles{byName: map[string]Profile{testSynth: exact, testGeneric: generic}}

	for _, test := range []struct {
		name        string
		line        string
		wantVersion string
		wantHarness string
	}{
		{name: "exact build", line: testMuseVersionLine, wantVersion: testMuseBuildVersion, wantHarness: testSynth},
		{name: "stable", line: "Muse Code 1.4.13 (1.4.13)", wantVersion: testMuseStableVersion, wantHarness: testGeneric},
		{name: "different build", line: "Muse Code 1.4.13 (1.4.13-R6997.1)", wantVersion: "1.4.13-R6997.1", wantHarness: testGeneric},
		{name: "Claude", line: testClaudeVersionLine, wantVersion: testClaudeVersion, wantHarness: testGeneric},
		{name: "Codex", line: testCodexVersionLine, wantVersion: testCodexVersion, wantHarness: testGeneric},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			version := ParseVersion(test.line)
			require.Equal(t, test.wantVersion, version)
			selected, err := ps.Profile(testSynth, version)
			require.NoError(t, err)
			require.Equal(t, test.wantHarness, selected.Harness)
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

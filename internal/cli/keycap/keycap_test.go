package keycap

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/proxy/input"
)

// Shared arrow sequences for the verify table and the tour.
const (
	arrowUp   = "\x1b[A"
	arrowDown = "\x1b[B"
)

// testVersion is the fake harness version every test measures.
const testVersion = "1.2.3"

func TestCommandLine(t *testing.T) {
	t.Parallel()

	cases := []struct {
		args string
		code int
	}{
		{"keycap", exitcode.Usage},
		{"keycap --harness", exitcode.Usage},
		{"keycap --harness ''", exitcode.Usage},
		{"keycap --harness 'A B'", exitcode.Usage},
		{"keycap --harness no-such-harness-xyz", exitcode.Usage},
		{"keycap --harness x stray", exitcode.Usage},
	}
	for _, tc := range cases {
		t.Run(tc.args, func(t *testing.T) {
			t.Parallel()
			cmd := NewCmd(&deps.Deps{})
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)
			cmd.SetIn(strings.NewReader(""))
			cmd.SetArgs(strings.Fields(tc.args))
			var ec *deps.ExitError
			require.ErrorAs(t, cmd.Execute(), &ec)
			require.Equal(t, tc.code, ec.Code)
		})
	}
}

// TestNonTerminalStdio reaches the terminal check with a valid command
// line: without a real terminal the flow refuses before starting.
func TestNonTerminalStdio(t *testing.T) {
	t.Parallel()

	cmd := NewCmd(&deps.Deps{})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetIn(strings.NewReader(""))
	cmd.SetArgs([]string{"--harness", "x", "--", "/bin/true"})
	var ec *deps.ExitError
	require.ErrorAs(t, cmd.Execute(), &ec)
	require.Equal(t, exitcode.Error, ec.Code)
}

func TestVerify(t *testing.T) {
	t.Parallel()

	cases := []struct {
		step StepKind
		unit string
		want bool
	}{
		{StepSubmit, "\r", true},
		{StepSubmit, "\n", true},
		{StepSubmit, "a", false},
		{StepSubmit, arrowUp, true},
		{StepSubmit, "\x1b[200~x\x1b[201~", false},
		{StepSubmit, "", false},
		{StepCancel, "\x03", true},
		{StepClearLine, "\x15", true},
		{StepEsc, "\x1b", true},
		{StepEsc, "\x1b[27u", true},
		{StepEsc, "a", false},
		{StepCtrlC, "\x03", true},
		{StepCtrlC, "\x1b", false},
		{StepBackspace, "\x7f", true},
		{StepBackspace, "\x08", true},
		{StepTab, "\t", true},
		{StepArrowUp, arrowUp, true},
		{StepArrowDown, arrowDown, true},
		{StepArrowLeft, "\x1b[D", true},
		{StepArrowRight, "\x1b[C", true},
		{StepArrowUp, "\x1bOA", true},
		{StepArrowUp, "\x1b[B", false},
		{StepArrowUp, "a", false},
		{StepHome, "\x1b[H", true},
		{StepHome, "\x1b[1~", true},
		{StepEnd, "\x1b[F", true},
		{StepPaste, "anything", true},
		{StepPaste, "", false},
	}
	for _, tc := range cases {
		t.Run(string(tc.step)+"/"+tc.unit, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, verify(tc.step, []byte(tc.unit)))
		})
	}
}

func TestCompleteStep(t *testing.T) {
	t.Parallel()

	unit, rest, ok := completeStep([]byte("\r"))
	require.True(t, ok)
	require.Equal(t, []byte("\r"), unit)
	require.Empty(t, rest)

	unit, rest, ok = completeStep([]byte(arrowUp + "x"))
	require.True(t, ok)
	require.Equal(t, []byte("\x1b[A"), unit)
	require.Equal(t, []byte("x"), rest)

	for _, incomplete := range []string{"\x1b", "\x1b[", "\x1b[2", "\x1bO", ""} {
		_, _, ok = completeStep([]byte(incomplete))
		require.False(t, ok, "incomplete %q must wait", incomplete)
	}
}

func TestShapeOf(t *testing.T) {
	t.Parallel()

	require.Equal(t, shapeXterm, shapeOf([]byte(arrowUp)))
	require.Equal(t, shapeXterm, shapeOf([]byte("\r")))
	require.Equal(t, shapeCSIU, shapeOf([]byte("\x1b[9u")))
	require.Equal(t, shapeCSIU, shapeOf([]byte("\x1b[97;5u")))
	require.Equal(t, shapeModifyOther, shapeOf([]byte("\x1b[27;5;65~")))
}

func TestParseVersion(t *testing.T) {
	t.Parallel()

	require.Equal(t, testVersion, parseVersion("fakecap "+testVersion+" (nightly)"))
	require.Equal(t, "2.10", parseVersion("v2.10"))
	require.Empty(t, parseVersion("no version here"))
	require.Empty(t, parseVersion(""))
}

func TestYAMLEscape(t *testing.T) {
	t.Parallel()

	require.Equal(t, `\r`, yamlEscape("\r"))
	require.Equal(t, `\n`, yamlEscape("\n"))
	require.Equal(t, `\x1b`, yamlEscape("\x1b"))
	require.Equal(t, `\x01`, yamlEscape("\x01"))
	require.Equal(t, `\x7f`, yamlEscape("\x7f"))
	require.Equal(t, `a\\b\"c`, yamlEscape(`a\b"c`))
	require.Equal(t, `abc`, yamlEscape("abc"))
	require.Equal(t, `\u00e9`, yamlEscape("é"))
}

// TestRenderedProfileLoads renders one measurement and loads it back
// with production rules: the YAML carries the captures, and the parser
// replays them to the right events.
func TestRenderedProfileLoads(t *testing.T) {
	t.Parallel()

	m := &measurement{version: testVersion}
	m.record(StepSubmit, []byte("\r"))
	m.record(StepInsertNewline, []byte("\n"))
	m.record(StepClearLine, []byte("\x15"))
	m.record(StepCancel, []byte("\x03"))
	m.record(StepArrowUp, []byte("\x1b[A"))
	wrapped := true
	m.pasteWrapped = &wrapped

	ext, reason := m.extended()
	require.Empty(t, reason)
	content := renderProfile("codex", m, "^(.*)$", m.version, ext)
	prof, err := profiles.ParseFile("codex", []byte(content))
	require.NoError(t, err)
	require.True(t, prof.Measured)
	require.Equal(t, testVersion, prof.VersionRange)
	require.Equal(t, profiles.ExtendedNone, prof.ExtendedKeyboard)

	require.NoError(t, validateCandidate("codex", content, m))

	parser := input.NewParser(prof)
	submit := parser.Feed([]byte("\r"))
	require.Len(t, submit, 1)
	require.Equal(t, input.KindSubmit, submit[0].Kind)
	cancel := parser.Feed([]byte("\x03"))
	require.Len(t, cancel, 1)
	require.Equal(t, input.KindCancel, cancel[0].Kind)
}

// TestValidateCandidateRejectsBadSequences renders unmeasurable bytes
// and duplicated sequences: neither may reach the disk.
func TestValidateCandidateRejectsBadSequences(t *testing.T) {
	t.Parallel()

	nul := &measurement{version: testVersion}
	nul.record(StepSubmit, []byte{0x00})
	nul.record(StepInsertNewline, []byte("\n"))
	nul.record(StepClearLine, []byte("\x15"))
	nul.record(StepCancel, []byte("\x03"))
	wrapped := true
	nul.pasteWrapped = &wrapped
	require.Error(t, validateCandidate("codex",
		renderProfile("codex", nul, "^(.*)$", testVersion, profiles.ExtendedNone), nul))

	dup := &measurement{version: testVersion}
	dup.record(StepSubmit, []byte("\r"))
	dup.record(StepInsertNewline, []byte("\n"))
	dup.record(StepClearLine, []byte("\x15"))
	dup.record(StepCancel, []byte("\r"))
	dup.pasteWrapped = &wrapped
	require.Error(t, validateCandidate("codex",
		renderProfile("codex", dup, "^(.*)$", testVersion, profiles.ExtendedNone), dup))
}

func TestObserver(t *testing.T) {
	t.Parallel()

	var o observer
	o.scan([]byte("hello \x1b[?200"))
	require.Nil(t, o.paste)
	o.scan([]byte("4h world"))
	require.NotNil(t, o.paste)
	require.True(t, *o.paste)
	o.scan([]byte("\x1b[>1u"))
	require.True(t, o.kitty)
	require.False(t, o.modifyKeys)
	o.scan([]byte("\x1b[>4;2m"))
	require.True(t, o.modifyKeys)
	o.scan([]byte("\x1b[?2004l"))
	require.False(t, *o.paste)
	require.Contains(t, o.describe(), "desactivado")
}

// TestObserverPopAndReset clears pushed modes when the harness pops or
// resets them itself: nothing stays active, so teardown restores nothing.
func TestObserverPopAndReset(t *testing.T) {
	t.Parallel()

	var o observer
	o.scan([]byte("\x1b[?2004h\x1b[>1u\x1b[>4;2m"))
	require.True(t, *o.paste)
	require.True(t, o.kitty)
	require.True(t, o.modifyKeys)
	o.scan([]byte("\x1b[?2004l\x1b[<1u\x1b[>4;0m"))
	require.False(t, *o.paste)
	require.False(t, o.kitty)
	require.False(t, o.modifyKeys)
}

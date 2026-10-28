package redact

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// panicRule is a test-only rule that always panics, proving the methods
// recover a rule failure into ErrInternal.
type panicRule struct{}

func (panicRule) find([]string) []finding { panic("test panic") }

// TestFailClosed pins the fail-closed engine: a panicking rule set, a span
// off a rune boundary or past its line end, an empty span, a span on a
// missing line, an unknown kind, a bundle without rules and nil or zero
// Redactors all return ErrInternal with the zero result on both methods,
// including on empty input; C1 control text fails input validation with
// ErrInvalidInput before any rule runs.
func TestFailClosed(t *testing.T) {
	t.Parallel()

	lines := func() []string { return []string{"hello"} }

	t.Run("panicking rule set", func(t *testing.T) {
		t.Parallel()
		r := &Redactor{rules: &ruleSet{version: testVersion, rules: []rule{panicRule{}}}}
		screen, err := r.RedactScreen(Screen{Rows: lines()})
		require.ErrorIs(t, err, ErrInternal)
		require.Equal(t, ScreenResult{}, screen)
		text, err := r.RedactText("hello")
		require.ErrorIs(t, err, ErrInternal)
		require.Equal(t, TextResult{}, text)
	})

	bad := []struct {
		name    string
		finding finding
	}{
		{"past the line end", finding{kind: KindAPIKey, parts: []span{{line: 0, start: 0, end: 99}}}},
		{"off a rune boundary", finding{kind: KindAPIKey, parts: []span{{line: 0, start: 2, end: 3}}}},
		{"empty span", finding{kind: KindAPIKey, parts: []span{{line: 0, start: 2, end: 2}}}},
		{"missing line", finding{kind: KindAPIKey, parts: []span{{line: 4, start: 0, end: 1}}}},
		{"unknown kind", finding{kind: Kind("nope"), parts: []span{{line: 0, start: 0, end: 1}}}},
		{"no spans", finding{kind: KindAPIKey, parts: nil}},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := testRedactor(stubRule{kind: tc.finding.kind, report: func([]string) []finding {
				return []finding{tc.finding}
			}})
			screen, err := r.RedactScreen(Screen{Rows: []string{"h\u00e9llo"}})
			require.ErrorIs(t, err, ErrInternal)
			require.Equal(t, ScreenResult{}, screen)
			text, err := r.RedactText("h\u00e9llo")
			require.ErrorIs(t, err, ErrInternal)
			require.Equal(t, TextResult{}, text)
		})
	}

	t.Run("no rules", func(t *testing.T) {
		t.Parallel()
		empty := &Redactor{rules: &ruleSet{version: testVersion}}
		var zero Redactor
		var nilRedactor *Redactor
		for i, r := range []*Redactor{empty, &zero, nilRedactor} {
			screen, err := r.RedactScreen(Screen{Rows: lines()})
			require.ErrorIs(t, err, ErrInternal, "redactor %d", i)
			require.Equal(t, ScreenResult{}, screen, "redactor %d", i)
			text, err := r.RedactText("hello")
			require.ErrorIs(t, err, ErrInternal, "redactor %d", i)
			require.Equal(t, TextResult{}, text, "redactor %d", i)
		}
	})

	t.Run("empty input without rules", func(t *testing.T) {
		t.Parallel()
		empty := &Redactor{rules: &ruleSet{version: testVersion}}
		var zero Redactor
		var nilRedactor *Redactor
		screens := []Screen{{}, {Rows: []string{}}}
		for i, r := range []*Redactor{empty, &zero, nilRedactor} {
			text, err := r.RedactText("")
			require.ErrorIs(t, err, ErrInternal, "redactor %d", i)
			require.Equal(t, TextResult{}, text, "redactor %d", i)
			for j, s := range screens {
				screen, err := r.RedactScreen(s)
				require.ErrorIs(t, err, ErrInternal, "redactor %d screen %d", i, j)
				require.Equal(t, ScreenResult{}, screen, "redactor %d screen %d", i, j)
			}
		}
	})

	t.Run("c1 text fails before rules", func(t *testing.T) {
		t.Parallel()
		quiet := testRedactor(stubRule{kind: KindAPIKey, report: func([]string) []finding { return nil }})
		var nilRedactor *Redactor
		for _, control := range []rune{0x80, 0x85, 0x9b, 0x9f} {
			input := "public" + string(control) + "text"
			for i, r := range []*Redactor{nilRedactor, quiet} {
				result, err := r.RedactText(input)
				require.ErrorIs(t, err, ErrInvalidInput, "U+%04X redactor %d", control, i)
				require.Equal(t, TextResult{}, result, "U+%04X redactor %d", control, i)
			}
			var calls int
			spy := testRedactor(stubRule{kind: KindAPIKey, report: func([]string) []finding {
				calls++
				return nil
			}})
			_, err := spy.RedactText(input)
			require.ErrorIs(t, err, ErrInvalidInput, "U+%04X", control)
			require.Zero(t, calls, "U+%04X: rules ran on invalid input", control)
		}
	})
}

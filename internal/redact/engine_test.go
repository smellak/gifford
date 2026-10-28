package redact

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// stubRule is a test-only rule reporting fixed findings. It stands in for
// the v1 rules, which arrive with New in the next PR.
type stubRule struct {
	kind   Kind
	report func(lines []string) []finding
}

func (s stubRule) find(lines []string) []finding { return s.report(lines) }

// fixed returns a rule reporting the same spans on every call.
func fixed(kind Kind, parts ...span) stubRule {
	return stubRule{kind: kind, report: func([]string) []finding {
		return []finding{{kind: kind, parts: parts}}
	}}
}

// testVersion is the version every test-only bundle reports.
const testVersion = "test-1"

// wrapRow and shortRow are the two screen rows the wrap tests join and
// split: a full row ending in a digit joins, short rows never do.
const (
	wrapRow  = "0123456789"
	shortRow = "abcdef"
)

// testRedactor builds a Redactor over one versioned test bundle.
func testRedactor(rules ...rule) *Redactor {
	return &Redactor{rules: &ruleSet{version: testVersion, rules: rules}}
}

// requireErrorHoldsNoInput requires that err carries no 4-rune substring
// of the refused input, so a failure cannot echo what it rejected.
func requireErrorHoldsNoInput(t *testing.T, err error, input string) {
	t.Helper()
	require.Error(t, err)
	runes := []rune(input)
	for i := 0; i+4 <= len(runes); i++ {
		require.NotContains(t, err.Error(), string(runes[i:i+4]))
	}
}

// TestEngine pins the §5 and §7 engine over test-only rule sets: width
// keeping masks, priority overlap counting, multi-span findings, soft-wrap
// joins, the cluster edges and the bundle version.
func TestEngine(t *testing.T) {
	t.Parallel()

	t.Run("cell width masking", func(t *testing.T) {
		t.Parallel()
		r := testRedactor(fixed(KindAPIKey, span{line: 0, start: 2, end: 8}))
		got, err := r.RedactText("ab密码cd")
		require.NoError(t, err)
		require.Equal(t, "ab••••cd", got.Text)
		require.Equal(t, testVersion, got.PolicyVersion)
		require.Equal(t, Counts{Total: 1, ByKind: map[Kind]int64{KindAPIKey: 1}}, got.Counts)
	})

	t.Run("overlap counts once", func(t *testing.T) {
		t.Parallel()
		r := testRedactor(
			fixed(KindConnectionString, span{line: 0, start: 0, end: 5}),
			fixed(KindAPIKey, span{line: 0, start: 3, end: 8}),
		)
		got, err := r.RedactText("0123456789")
		require.NoError(t, err)
		require.Equal(t, "••••••••89", got.Text)
		require.Equal(t, Counts{Total: 1, ByKind: map[Kind]int64{KindConnectionString: 1}}, got.Counts)
	})

	t.Run("multi span finding counts once", func(t *testing.T) {
		t.Parallel()
		r := testRedactor(stubRule{kind: KindJWT, report: func([]string) []finding {
			return []finding{{kind: KindJWT, parts: []span{
				{line: 0, start: 0, end: 2},
				{line: 0, start: 5, end: 7},
			}}}
		}})
		got, err := r.RedactText("abcdefgh")
		require.NoError(t, err)
		require.Equal(t, "••cde••h", got.Text)
		require.Equal(t, Counts{Total: 1, ByKind: map[Kind]int64{KindJWT: 1}}, got.Counts)
	})

	t.Run("soft wrap joins", func(t *testing.T) {
		t.Parallel()
		r := testRedactor(fixed(KindAPIKey, span{line: 0, start: 8, end: 12}))
		got, err := r.RedactScreen(Screen{Rows: []string{wrapRow, shortRow}, Cols: 10})
		require.NoError(t, err)
		require.Equal(t, []string{"01234567••", "••cdef"}, got.Rows)
		require.Equal(t, testVersion, got.PolicyVersion)
	})

	t.Run("frame blocks join", func(t *testing.T) {
		t.Parallel()
		r := testRedactor(fixed(KindAPIKey, span{line: 1, start: 0, end: 3}))
		got, err := r.RedactScreen(Screen{Rows: []string{"012345678│", shortRow}, Cols: 10})
		require.NoError(t, err)
		require.Equal(t, []string{"012345678│", "•••def"}, got.Rows)
	})

	t.Run("unknown cols never joins", func(t *testing.T) {
		t.Parallel()
		r := testRedactor(fixed(KindAPIKey, span{line: 0, start: 0, end: 10}))
		got, err := r.RedactScreen(Screen{Rows: []string{wrapRow, shortRow}})
		require.NoError(t, err)
		require.Equal(t, []string{"••••••••••", shortRow}, got.Rows)
	})

	t.Run("trailing mark absorbed", func(t *testing.T) {
		t.Parallel()
		r := testRedactor(fixed(KindSecretAssignment, span{line: 0, start: 4, end: 7}))
		got, err := r.RedactText("key e\u0301!")
		require.NoError(t, err)
		require.Equal(t, "key •!", got.Text)
		require.Equal(t, CellWidth("key e\u0301!"), CellWidth(got.Text))
	})

	t.Run("cluster edges fail closed", func(t *testing.T) {
		t.Parallel()
		zwj := "a\u200db"
		vsAfter := "ab\uFE0F"
		zwjInside := "a\u200db"
		cases := []struct {
			name  string
			input string
			start int
			end   int
		}{
			{"joiner before", zwj, 4, 5},
			{"extender after", vsAfter, 0, 2},
			{"joiner inside", zwjInside, 0, 4},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				r := testRedactor(fixed(KindAPIKey, span{line: 0, start: tc.start, end: tc.end}))
				got, err := r.RedactText(tc.input)
				require.ErrorIs(t, err, ErrUnsupportedInput)
				require.Equal(t, TextResult{}, got)
				requireErrorHoldsNoInput(t, err, tc.input)
				screen, err := r.RedactScreen(Screen{Rows: []string{tc.input}})
				require.ErrorIs(t, err, ErrUnsupportedInput)
				require.Equal(t, ScreenResult{}, screen)
				requireErrorHoldsNoInput(t, err, tc.input)
			})
		}
	})

	t.Run("flag inside fails", func(t *testing.T) {
		t.Parallel()
		flag := string([]rune{0x1f1e6, 0x1f1f8})
		r := testRedactor(fixed(KindAPIKey, span{line: 0, start: 0, end: len(flag)}))
		got, err := r.RedactText(flag)
		require.ErrorIs(t, err, ErrUnsupportedInput)
		require.Equal(t, TextResult{}, got)
		requireErrorHoldsNoInput(t, err, flag)
		screen, err := r.RedactScreen(Screen{Rows: []string{flag}})
		require.ErrorIs(t, err, ErrUnsupportedInput)
		require.Equal(t, ScreenResult{}, screen)
		requireErrorHoldsNoInput(t, err, flag)
	})

	t.Run("flag and emoji next to span kept", func(t *testing.T) {
		t.Parallel()
		flag := string([]rune{0x1f1e6, 0x1f1f8})
		emoji := "\U0001F600"
		input := flag + "secret" + emoji
		r := testRedactor(fixed(KindAPIKey, span{line: 0, start: len(flag), end: len(flag) + 6}))
		got, err := r.RedactText(input)
		require.NoError(t, err)
		require.Equal(t, flag+"••••••"+emoji, got.Text)
		screen, err := r.RedactScreen(Screen{Rows: []string{input}})
		require.NoError(t, err)
		require.Equal(t, []string{flag + "••••••" + emoji}, screen.Rows)
	})

	t.Run("flag next to span kept", func(t *testing.T) {
		t.Parallel()
		flag := string([]rune{0x1f1e6, 0x1f1f8})
		input := flag + "secret" + flag
		r := testRedactor(fixed(KindAPIKey, span{line: 0, start: len(flag), end: len(flag) + 6}))
		got, err := r.RedactText(input)
		require.NoError(t, err)
		require.Equal(t, flag+"••••••"+flag, got.Text)
		require.True(t, strings.HasPrefix(got.Text, flag))
	})
}

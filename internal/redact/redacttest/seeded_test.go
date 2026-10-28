package redacttest_test

import (
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/redact"
	"github.com/smellak/gifford/internal/redact/redacttest"
)

// mustScreens loads the seeded screens, failing the test on any error.
func mustScreens(t *testing.T) []redacttest.SeededScreen {
	t.Helper()
	screens, err := redacttest.Screens()
	require.NoError(t, err)
	return screens
}

// mustTexts loads the seeded texts, failing the test on any error.
func mustTexts(t *testing.T) []redacttest.SeededText {
	t.Helper()
	texts, err := redacttest.Texts()
	require.NoError(t, err)
	return texts
}

// mustFixtureRedactor builds the v1 redactor over the fixture paths.
func mustFixtureRedactor(t *testing.T) *redact.Redactor {
	t.Helper()
	r, err := redact.New(redacttest.Options())
	require.NoError(t, err)
	return r
}

// TestSeededScreens replays the five harness screens through the real
// rules: rows, version and counts equal Want, and no secret fragment
// survives in the output.
func TestSeededScreens(t *testing.T) {
	t.Parallel()
	r := mustFixtureRedactor(t)
	for _, s := range mustScreens(t) {
		t.Run(s.Name, func(t *testing.T) {
			t.Parallel()
			got, err := r.RedactScreen(s.Input)
			require.NoError(t, err)
			require.Equal(t, s.Want, got.Rows)
			require.Equal(t, "1", got.PolicyVersion)
			require.Equal(t, s.Counts, got.Counts)
			for _, secret := range s.Secrets {
				for _, row := range got.Rows {
					require.NotContains(t, row, secret, "screen %s", s.Name)
				}
			}
		})
	}
}

// TestSeededTexts replays the four message entries through the real
// rules.
func TestSeededTexts(t *testing.T) {
	t.Parallel()
	r := mustFixtureRedactor(t)
	for _, x := range mustTexts(t) {
		t.Run(x.Name, func(t *testing.T) {
			t.Parallel()
			got, err := r.RedactText(x.Input)
			require.NoError(t, err)
			require.Equal(t, x.Want, got.Text)
			require.Equal(t, "1", got.PolicyVersion)
			require.Equal(t, x.Counts, got.Counts)
			for _, secret := range x.Secrets {
				require.NotContains(t, got.Text, secret, "text %s", x.Name)
			}
		})
	}
}

// TestFixtureCoverage pins the §9.3 inventory: five screens with their
// harnesses and dimensions, four texts, 19 screen substitutions and 7
// text ones, every kind covered, and the called-out rows and headers.
func TestFixtureCoverage(t *testing.T) {
	t.Parallel()
	screens := mustScreens(t)
	texts := mustTexts(t)

	wantScreens := []struct {
		name    string
		harness protocol.Harness
		cols    int
		rows    int
	}{
		{"claude-code", protocol.HarnessClaudeCode, 100, 24},
		{"codex", protocol.HarnessCodex, 120, 26},
		{"muse-code", protocol.HarnessMuseCode, 100, 30},
		{"opencode", protocol.HarnessOpencode, 80, 24},
		{"antigravity", protocol.HarnessAntigravity, 90, 20},
	}
	require.Len(t, screens, len(wantScreens))
	screenTotal := 0
	kinds := map[redact.Kind]bool{}
	for i, want := range wantScreens {
		s := screens[i]
		require.Equal(t, want.name, s.Name)
		require.Equal(t, want.harness, s.Harness)
		require.Equal(t, want.cols, s.Input.Cols)
		require.Len(t, s.Input.Rows, want.rows)
		require.Len(t, s.Want, want.rows)
		for _, row := range s.Input.Rows {
			require.Equal(t, want.cols, redact.CellWidth(row), "screen %s", s.Name)
		}
		screenTotal += int(s.Counts.Total)
		for k := range s.Counts.ByKind {
			kinds[k] = true
		}
		for _, secret := range s.Secrets {
			require.GreaterOrEqual(t, utf8.RuneCountInString(secret), 4)
		}
	}
	require.Equal(t, 19, screenTotal)

	wantTexts := []string{"m1-service-account", "m2-basic-auth", "m3-registry-tokens", "m4-digest-auth"}
	require.Len(t, texts, len(wantTexts))
	textTotal := 0
	for i, want := range wantTexts {
		require.Equal(t, want, texts[i].Name)
		textTotal += int(texts[i].Counts.Total)
		for k := range texts[i].Counts.ByKind {
			kinds[k] = true
		}
	}
	require.Equal(t, 7, textTotal)

	for _, k := range redact.Kind("").EnumValues() {
		require.True(t, kinds[redact.Kind(k)], "kind %s covered", k)
	}

	t.Run("codex full row", func(t *testing.T) {
		t.Parallel()
		rows := screens[1].Input.Rows
		found := false
		for _, row := range rows {
			if redact.CellWidth(row) != screens[1].Input.Cols {
				continue
			}
			runes := []rune(row)
			if len(runes) > 0 && runes[len(runes)-1] != ' ' {
				found = true
			}
		}
		require.True(t, found, "codex has a full row ending in a non-blank rune")
	})

	t.Run("antigravity input box", func(t *testing.T) {
		t.Parallel()
		found := false
		for _, row := range screens[4].Input.Rows {
			runes := []rune(row)
			if len(runes) == 90 && runes[0] == '│' && runes[89] == '│' {
				found = true
			}
		}
		require.True(t, found, "antigravity has the input box with frames at 0 and 89")
	})

	t.Run("digest headers", func(t *testing.T) {
		t.Parallel()
		var m4 string
		for _, x := range texts {
			if x.Name == "m4-digest-auth" {
				m4 = x.Input
			}
		}
		require.Contains(t, m4, "Authorization: Digest ")
		require.Contains(t, m4, `"Authorization": "Digest `)
	})
}

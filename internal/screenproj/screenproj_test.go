package screenproj

import (
	"encoding/json"
	"errors"
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/redact"
	"github.com/smellak/gifford/internal/redact/redacttest"
)

// All screens and names below are invented.
const (
	canaryDraft = "HUMAN-CANARY-q7Lm draft"
	listedName  = "SentinelCorp"
	childCargo  = "cargo"
	childGoTest = "go test"
	childNpm    = "npm install"
)

// shown is a cursor known and shown on row 2.
var shown = Cursor{Row: 2, Known: true, Visible: true}

func realRedactor(t *testing.T) *redact.Redactor {
	t.Helper()
	red, err := redact.New(redact.Options{})
	require.NoError(t, err)
	return red
}

func githubPAT(t *testing.T) string {
	t.Helper()
	secret, err := redacttest.Value("github_pat")
	require.NoError(t, err)
	return secret
}

func policies(t *testing.T, lines int) Policies {
	t.Helper()
	return Policies{Lines: lines, Redactor: realRedactor(t), Mask: NoMask()}
}

func closedInput(rows ...string) Input {
	return Input{Rows: rows, Cols: 80, Draft: protocol.DraftStateClosed}
}

// requireNoFragment fails if any part of secret is in out.
func requireNoFragment(t *testing.T, out []string, secret string) {
	t.Helper()
	joined := strings.Join(out, "\n")
	for _, part := range []string{secret[:10], secret[10:20], secret[20:], secret[len(secret)-10:]} {
		require.NotContains(t, joined, part, "no fragment of the secret is left")
	}
}

func TestProjectKeepsAClosedScreen(t *testing.T) {
	t.Parallel()
	in := closedInput("step 3 of 7: compiling", "tests: 12 passed, 0 failed")
	got, err := Project(in, policies(t, MaxLines))
	require.NoError(t, err)
	require.Equal(t, []string{"step 3 of 7: compiling", "tests: 12 passed, 0 failed"}, got.Lines)
	require.Equal(t, MaxLines, got.N)
	require.Equal(t, Versions{HumanInput: HumanInputPolicyV1, Redaction: redact.PolicyVersion, DomainMask: DomainMaskNone}, got.Versions)
	require.Zero(t, got.Redactions)
	again, err := Project(in, policies(t, MaxLines))
	require.NoError(t, err)
	require.Equal(t, got, again, "the same screen and policies, the same projection")
}

func TestProjectExcludesTheHumanInput(t *testing.T) {
	t.Parallel()
	screen := []string{"compiling step 3", "> " + canaryDraft, "  second line of it", "  hint below"}
	for _, draft := range []protocol.DraftState{
		protocol.DraftStateOpen, protocol.DraftStatePasting, protocol.DraftStateSubmitting, protocol.DraftStateUnknown, "",
	} {
		got, err := Project(Input{Rows: screen, Cols: 80, Cursor: shown, Draft: draft}, policies(t, MaxLines))
		require.NoError(t, err, "draft %q", draft)
		require.Equal(t, []string{}, got.Lines, "draft %q: the whole screen is blanked, then cut to nothing", draft)
		require.Equal(t, HumanInputPolicyV1, got.Versions.HumanInput)
	}
	require.Equal(t, "> "+canaryDraft, screen[1], "the input is not changed")

	// A multi-line paste taller than any guessed block: no row of it
	// is left, the first ones included.
	tall := make([]string, 0, 60)
	for i := range 60 {
		tall = append(tall, fmt.Sprintf("  %s pasted row %d", canaryDraft, i))
	}
	got, err := Project(Input{Rows: tall, Cols: 80, Cursor: Cursor{Row: 59, Known: true, Visible: true}, Draft: protocol.DraftStatePasting}, policies(t, MaxLines))
	require.NoError(t, err)
	require.NotContains(t, strings.Join(got.Lines, "\n"), canaryDraft)

	// Unknown, hidden or off the screen: nothing is projected.
	for _, c := range []Cursor{{}, {Row: 2, Visible: true}, {Row: 2, Known: true}, {Row: -1, Known: true, Visible: true}, {Row: 4, Known: true, Visible: true}} {
		got, err := Project(Input{Rows: screen, Cols: 80, Cursor: c, Draft: protocol.DraftStateOpen}, policies(t, MaxLines))
		require.ErrorIs(t, err, ErrNoCursor, "%+v", c)
		require.Nil(t, got.Lines, "%+v", c)
		require.NotContains(t, err.Error(), "CANARY")
	}
	got, err = Project(Input{Rows: screen, Cols: 80, Draft: protocol.DraftStateClosed}, policies(t, MaxLines))
	require.NoError(t, err, "a closed draft needs no cursor")
	require.Len(t, got.Lines, 4)
}

func TestProjectCleansBeforeTheRedactor(t *testing.T) {
	t.Parallel()
	secret := githubPAT(t)
	// A control rune inside the secret would hide it from the
	// redactor, and the redactor refuses control runes.
	in := closedInput(
		"\x1b[1mbold\x1b[0m\ttab\a bell  ",
		"token "+secret[:12]+"\a"+secret[12:]+"\r",
		"ok",
	)
	got, err := Project(in, policies(t, MaxLines))
	require.NoError(t, err, "a screen with control runes does not fail the redactor")
	require.Equal(t, "[1mbold[0mtab bell", got.Lines[0], "control runes dropped, trailing blanks trimmed")
	require.True(t, strings.HasPrefix(got.Lines[1], "token ••••"), "the secret is masked: %q", got.Lines[1])
	requireNoFragment(t, got.Lines, secret)
	require.Equal(t, int64(1), got.Redactions)
}

func TestProjectRedactsWithTheWidth(t *testing.T) {
	t.Parallel()
	// The userinfo fills its row to the width, so only the width says
	// the next row continues it.
	user, err := redacttest.Value("db_userinfo")
	require.NoError(t, err)
	in := Input{Rows: []string{"postgres://app_rw:", user[7:] + "@db.example.invalid:5432/app"}, Cols: 18, Draft: protocol.DraftStateClosed}
	got, err := Project(in, policies(t, MaxLines))
	require.NoError(t, err)
	require.NotContains(t, strings.Join(got.Lines, "\n"), user[7:], "the wrapped password is masked")
	require.Equal(t, int64(1), got.Redactions)
}

func TestProjectCutsLast(t *testing.T) {
	t.Parallel()
	secret := githubPAT(t)
	filler := func(n int) []string {
		rows := make([]string, n)
		for i := range rows {
			rows[i] = fmt.Sprintf("log line %02d", i)
		}
		return rows
	}

	t.Run("trailing empty rows", func(t *testing.T) {
		t.Parallel()
		got, err := Project(closedInput("a", "", "b", "", "   ", "\t", "\a"), policies(t, MaxLines))
		require.NoError(t, err)
		require.Equal(t, []string{"a", "", "b"}, got.Lines, "an inner empty row stays")
	})
	t.Run("more than N rows", func(t *testing.T) {
		t.Parallel()
		rows := filler(50)
		got, err := Project(closedInput(rows...), policies(t, MaxLines))
		require.NoError(t, err)
		require.Equal(t, rows[10:], got.Lines, "the last 40")
		got, err = Project(closedInput(rows...), policies(t, 3))
		require.NoError(t, err)
		require.Equal(t, rows[47:], got.Lines, "the last N")
		require.Equal(t, 3, got.N)
	})
	t.Run("a row over 512 runes", func(t *testing.T) {
		t.Parallel()
		got, err := Project(closedInput(strings.Repeat("é", 600), "ok"), policies(t, MaxLines))
		require.NoError(t, err)
		require.Equal(t, strings.Repeat("é", MaxRowRunes), got.Lines[0], "runes, not bytes")
		require.Equal(t, "ok", got.Lines[1])
	})
	t.Run("the rune cut cannot split a secret", func(t *testing.T) {
		t.Parallel()
		got, err := Project(closedInput(strings.Repeat("x", MaxRowRunes-11)+" "+secret), policies(t, MaxLines))
		require.NoError(t, err)
		requireNoFragment(t, got.Lines, secret)
		require.Equal(t, int64(1), got.Redactions)
	})
	t.Run("the row cut cannot split a secret", func(t *testing.T) {
		t.Parallel()
		// A token wrapped across a frame whose head row falls just
		// outside the last 40 rows.
		rows := append([]string{"tok " + secret[:20], "│ " + secret[20:]}, filler(MaxLines-1)...)
		got, err := Project(closedInput(rows...), policies(t, MaxLines))
		require.NoError(t, err)
		require.Len(t, got.Lines, MaxLines)
		requireNoFragment(t, got.Lines, secret)
		require.Equal(t, int64(1), got.Redactions)
	})
	t.Run("an empty screen", func(t *testing.T) {
		t.Parallel()
		for _, rows := range [][]string{nil, {}, {"", "  "}} {
			got, err := Project(closedInput(rows...), policies(t, MaxLines))
			require.NoError(t, err)
			b, err := json.Marshal(got.Lines)
			require.NoError(t, err)
			require.JSONEq(t, `[]`, string(b), "never null")
		}
	})
}

func TestProjectMasksAfterRedacting(t *testing.T) {
	t.Parallel()
	secret := githubPAT(t)
	mask, err := NewMask([]string{listedName, "North Wind"})
	require.NoError(t, err)
	p := policies(t, MaxLines)
	p.Mask = mask
	got, err := Project(closedInput(
		"deploy to SENTINELCORP staging",
		"sentinelcorporate and x_sentinelcorp stay",
		"north wind ltd, token "+secret,
	), p)
	require.NoError(t, err)
	require.Equal(t, "deploy to [domain] staging", got.Lines[0])
	require.Equal(t, "sentinelcorporate and x_sentinelcorp stay", got.Lines[1], "whole words only")
	require.True(t, strings.HasPrefix(got.Lines[2], "[domain] ltd, token ••••"), got.Lines[2])
	require.Equal(t, mask.Version(), got.Versions.DomainMask)
	requireNoFragment(t, got.Lines, secret)
}

func TestMask(t *testing.T) {
	t.Parallel()
	const acme = "AcmeBank"
	names, err := ParseMaskList(strings.NewReader("# private list\n\n  AcmeBank  \nacme\nNorth Wind\n"))
	require.NoError(t, err)
	require.Equal(t, []string{acme, "acme", "North Wind"}, names)
	m, err := NewMask(names)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(m.Version(), "domain-mask-v1:"))
	require.Len(t, m.Version(), len("domain-mask-v1:")+12)
	require.Equal(t, []string{
		"login to [domain] portal", "[domain]-tools and acmebank_x and acmex", "[domain] ltd, [domain].", "",
	}, m.apply([]string{"login to ACMEBANK portal", "acme-tools and acmebank_x and acmex", "north wind ltd, North Wind.", ""}),
		"case-insensitive, whole words only")

	// The version names the list, not its order or case.
	same, err := NewMask([]string{"north wind", "ACME", "acmebank", "Acme"})
	require.NoError(t, err)
	require.Equal(t, m.Version(), same.Version())
	other, err := NewMask([]string{"acme"})
	require.NoError(t, err)
	require.NotEqual(t, m.Version(), other.Version())

	require.Equal(t, DomainMaskNone, NoMask().Version())
	require.Equal(t, []string{acme}, NoMask().apply([]string{acme}))
	require.Empty(t, Mask{}.Version(), "the zero Mask names nothing")

	for name, in := range map[string]string{
		"empty":     "",
		"comments":  "# only\n\n",
		"control":   "acme\nbad\x07name\n",
		"too long":  strings.Repeat("n", 129) + "\n",
		"too large": strings.Repeat("name\n", (1<<20)/5+1),
		"too many":  strings.Repeat("n\n", maxMaskNames+1),
	} {
		_, err := ParseMaskList(strings.NewReader(in))
		require.ErrorIs(t, err, ErrMaskList, name)
		require.NotContains(t, err.Error(), "bad", name)
	}
	for name, list := range map[string][]string{
		"none":    nil,
		"empty":   {""},
		"control": {"bad\x07name"},
		"utf-8":   {"\xff"},
	} {
		_, err := NewMask(list)
		require.ErrorIs(t, err, ErrMaskList, name)
		require.NotContains(t, err.Error(), "bad", name)
	}
}

// fakeRedactor answers with a fixed result.
type fakeRedactor struct {
	res redact.ScreenResult
	err error
}

func (f fakeRedactor) RedactScreen(redact.Screen) (redact.ScreenResult, error) { return f.res, f.err }

func TestProjectFailsClosed(t *testing.T) {
	t.Parallel()
	in := closedInput("tests: 9 passed")
	red := realRedactor(t)
	for name, p := range map[string]Policies{
		"no lines":       {Redactor: red, Mask: NoMask()},
		"too many lines": {Lines: MaxLines + 1, Redactor: red, Mask: NoMask()},
		"no redactor":    {Lines: MaxLines, Mask: NoMask()},
		"no mask chosen": {Lines: MaxLines, Redactor: red},
	} {
		got, err := Project(in, p)
		require.ErrorIs(t, err, ErrInvalidPolicies, name)
		require.Nil(t, got.Lines, name)
		require.ErrorIs(t, p.Validate(), ErrInvalidPolicies, name)
	}
	require.NoError(t, policies(t, 1).Validate())

	for name, bad := range map[string]Input{
		"no width":     {Rows: in.Rows, Draft: protocol.DraftStateClosed},
		"too wide":     {Rows: in.Rows, Cols: redact.MaxCols + 1, Draft: protocol.DraftStateClosed},
		"too many row": {Rows: make([]string, redact.MaxRows+1), Cols: 80, Draft: protocol.DraftStateClosed},
	} {
		_, err := Project(bad, policies(t, MaxLines))
		require.ErrorIs(t, err, ErrInvalidInput, name)
	}

	sentinel := errors.New("sentinel redactor detail")
	for name, red := range map[string]Redactor{
		"refused":      fakeRedactor{err: sentinel},
		"rows lost":    fakeRedactor{res: redact.ScreenResult{PolicyVersion: "1"}},
		"no version":   fakeRedactor{res: redact.ScreenResult{Rows: []string{"tests: 9 passed"}}},
		"negative sum": fakeRedactor{res: redact.ScreenResult{Rows: []string{"x"}, PolicyVersion: "1", Counts: redact.Counts{Total: -1}}},
	} {
		got, err := Project(in, Policies{Lines: MaxLines, Redactor: red, Mask: NoMask()})
		require.ErrorIs(t, err, ErrRedaction, name)
		require.NotErrorIs(t, err, sentinel, "%s: the redactor's own error may describe the screen", name)
		require.NotContains(t, err.Error(), "sentinel", name)
		require.Nil(t, got.Lines, name)
	}
	_, err := Project(closedInput(strings.Repeat("x", redact.MaxInputBytes+1)), policies(t, MaxLines))
	require.ErrorIs(t, err, ErrRedaction, "an oversize screen is refused, never cut unredacted")
}

func TestChildProcess(t *testing.T) {
	t.Parallel()
	for _, verb := range []string{
		"build", "check", "compile", "exec", "fmt", "format", "generate", "install",
		"lint", "list", "run", "status", "test", "update", "vet",
	} {
		require.True(t, ChildProcess("tool "+verb), verb)
	}
	for _, ok := range []string{"go", childCargo, childNpm, "g++", "python3.12", "a_b-c", strings.Repeat("g", 32)} {
		require.True(t, ChildProcess(ok), ok)
	}
	for _, bad := range []string{
		"", " go", "go ", "go  test", "-go", ".go", "..go", "bin/go", "/usr/bin/go test", `tools\go`,
		"go ./pkg", "curl https://example.invalid", "nc example.invalid:80", "go -race", "npm --global",
		"go test pkg", "python secretToken123", "bash Alice", "python main.py", "go TEST", "go tests",
		"go Test", "GOFLAGS=x go", "go\ttest", string([]byte{'g', 0xff}), "gö", strings.Repeat("g", 33),
	} {
		require.False(t, ChildProcess(bad), "%q", bad)
	}
}

func TestModelChildProcesses(t *testing.T) {
	t.Parallel()
	require.Nil(t, ModelChildProcesses(nil), "not observed stays unknown")
	require.Equal(t, []string{}, ModelChildProcesses([]string{}), "observed none stays none")
	in := []string{childNpm, childGoTest, childGoTest, childCargo}
	require.Equal(t, []string{childCargo, childGoTest, childNpm}, ModelChildProcesses(in))
	require.Equal(t, []string{childNpm, childGoTest, childGoTest, childCargo}, in, "the input is not changed")
	nine := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"}
	require.Len(t, ModelChildProcesses(nine[:8]), MaxChildProcesses)
	for name, list := range map[string][]string{
		"too many":        nine,
		"a 33-char base":  {"go", strings.Repeat("g", 33)},
		"a verb not kept": {"go", "go tests"},
		"a leading dash":  {"-rf"},
		"argv":            {"python secret_script.py --token abc"},
	} {
		require.Nil(t, ModelChildProcesses(list), name)
	}
}

// TestImports pins that the projection stays pure: its non-test files
// import only the standard library, internal/protocol and internal/redact.
func TestImports(t *testing.T) {
	t.Parallel()
	allowed := map[string]bool{
		"github.com/smellak/gifford/internal/protocol": true,
		"github.com/smellak/gifford/internal/redact":   true,
	}
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	checked := 0
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		checked++
		src, err := os.ReadFile(file)
		require.NoError(t, err)
		parsed, err := parser.ParseFile(token.NewFileSet(), file, src, parser.ImportsOnly)
		require.NoError(t, err)
		for _, imp := range parsed.Imports {
			path := strings.Trim(imp.Path.Value, `"`)
			if allowed[path] {
				continue
			}
			first, _, _ := strings.Cut(path, "/")
			require.NotContains(t, first, ".", "non-stdlib import %s in %s", path, file)
			require.NotContains(t, []string{"os", "net", "net/http", "time", "io/fs", "path/filepath", "syscall"}, path,
				"%s in %s: no disk, network or clock", path, file)
		}
	}
	require.Positive(t, checked)
}

package eval

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
)

func predictions(profile string) string {
	var b strings.Builder
	for i, truth := range []string{"A", "B", "D", "E"} {
		probs := make([]string, 0, 7)
		for _, l := range []string{"A", "B", "C", "D", "E", "F", "G"} {
			v := 0
			if l == truth {
				v = 1
			}
			probs = append(probs, fmt.Sprintf("%q:%d", l, v))
		}
		fmt.Fprintf(&b, `{"window_id":"01JB%022d","split":"test","harness":"codex","harness_version":"1.0","session_id":"sess-1","source_quality":"pty_screen","label":%q,"probabilities":{%s},"valid_mass":1,"invalid":false,"latency_ms":120,"profile":%q}`+"\n",
			i+1, truth, strings.Join(probs, ","), profile)
	}
	return b.String()
}

const flagProfile = "--profile"

func write(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(p, []byte(content), 0o600))
	return p
}

func exec(t *testing.T, args ...string) (string, error) {
	t.Helper()
	cmd := NewCmd(&deps.Deps{})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(io.Discard)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

func TestEvalWritesStableReport(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	base := write(t, dir, "base.jsonl", predictions("base"))
	lora := write(t, dir, "lora.jsonl", predictions("analyst-lora"))
	out := filepath.Join(dir, "RESULTS.md")
	_, err := exec(t, flagProfile, "base="+base, flagProfile, "analyst-lora="+lora, "--out", out)
	require.NoError(t, err)
	first, err := os.ReadFile(out)
	require.NoError(t, err)
	// The same model twice does not improve: criterion 1 fails the gate.
	require.Contains(t, string(first), "## Gate: FAIL")

	stdout, err := exec(t, flagProfile, "base="+base, flagProfile, "analyst-lora="+lora)
	require.NoError(t, err)
	require.Equal(t, string(first), stdout, "same inputs, same bytes")
}

func TestEvalFailsClosed(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	base := write(t, dir, "base.jsonl", predictions("base"))
	lora := write(t, dir, "lora.jsonl", predictions("analyst-lora"))
	short := write(t, dir, "short.jsonl", strings.SplitAfter(predictions("analyst-lora"), "\n")[0])
	nan := write(t, dir, "nan.jsonl", strings.Replace(predictions("base"), `"valid_mass":1`, `"valid_mass":NaN`, 1))

	for name, tc := range map[string]struct {
		args []string
		code int
	}{
		"no base":           {[]string{flagProfile, "analyst-lora=" + lora}, exitcode.Usage},
		"unknown profile":   {[]string{flagProfile, "big=" + base}, exitcode.Usage},
		"profile twice":     {[]string{flagProfile, "base=" + base, flagProfile, "base=" + base}, exitcode.Usage},
		"bad bins":          {[]string{flagProfile, "base=" + base, "--bins", "0"}, exitcode.Usage},
		"different windows": {[]string{flagProfile, "base=" + base, flagProfile, "analyst-lora=" + short}, exitcode.Error},
		"NaN":               {[]string{flagProfile, "base=" + nan}, exitcode.Error},
		"swapped profiles":  {[]string{flagProfile, "base=" + lora}, exitcode.Error},
	} {
		_, err := exec(t, tc.args...)
		require.Error(t, err, name)
		var ec *deps.ExitError
		if tc.code == exitcode.Usage {
			require.ErrorAs(t, err, &ec, name)
			require.Equal(t, exitcode.Usage, ec.Code, name)
		} else {
			require.False(t, errorsAsUsage(err), name)
		}
	}
}

func errorsAsUsage(err error) bool {
	var ec *deps.ExitError
	return errors.As(err, &ec) && ec.Code == exitcode.Usage
}

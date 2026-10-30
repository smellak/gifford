//go:build unix

package fuzz

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
)

// brokenHarness makes the fake harness quit at once, so every scenario
// fails.
const brokenHarness = "GIFFORD_FUZZ_TEST_BROKEN_HARNESS"

// TestMain runs this test binary as the fake harness when gifford fuzz
// runs it as gifford: fuzz harness <record>.
func TestMain(m *testing.M) {
	if len(os.Args) > 2 && os.Args[1] == "fuzz" && os.Args[2] == harnessUse {
		if os.Getenv(brokenHarness) != "" {
			os.Exit(exitcode.Error)
		}
		cmd := NewCmd(&deps.Deps{})
		cmd.SilenceUsage, cmd.SilenceErrors = true, true
		cmd.SetArgs(os.Args[2:])
		if err := cmd.Execute(); err != nil {
			fmt.Fprintln(os.Stderr, "Error:", err)
			var ec *deps.ExitError
			if errors.As(err, &ec) {
				os.Exit(ec.Code)
			}
			os.Exit(exitcode.Error)
		}
		os.Exit(exitcode.OK)
	}
	// A race-enabled harness sleeps a second at exit for late race
	// reports, and every scenario waits for its harness to exit.
	_ = os.Setenv("GORACE", strings.TrimSpace(os.Getenv("GORACE")+" atexit_sleep_ms=0"))
	os.Exit(m.Run())
}

// fuzzCmd runs gifford fuzz with args and returns what it printed.
func fuzzCmd(t *testing.T, args string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	cmd := NewCmd(&deps.Deps{})
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(strings.Fields(args))
	err := cmd.ExecuteContext(t.Context())
	return out.String(), err
}

// TestFuzzRunsABatch runs a small batch with its baseline: every
// scenario passes, and the summary gives the seed and every count.
func TestFuzzRunsABatch(t *testing.T) {
	t.Parallel()
	out, err := fuzzCmd(t, "--scenarios 8 --baseline --seed 42")
	require.NoError(t, err, out)
	for _, want := range []string{
		"gifford fuzz: seed 42, 8 scenarios\n",
		"proxy: 8 scenarios, 0 failed, ",
		"  mixed lines: 0\n",
		"  commits with the draft open or unknown: 0 of ",
		"  lost keys: 0\n",
		"  notices received other than once: 0\n",
		"baseline: 8 scenarios, 0 failed\n",
		"  corrupt lines: ",
	} {
		require.Contains(t, out, want)
	}
	require.Regexp(t, `notices: [1-9]\d* delivered, \d+ requeued, of \d+ \(\d+ attempts\)`, out)
	require.NotContains(t, out, "FAIL")
}

// TestFuzzDrawsAndPrintsASeed runs without --seed: a seed is drawn and
// printed, and no baseline runs.
func TestFuzzDrawsAndPrintsASeed(t *testing.T) {
	t.Parallel()
	out, err := fuzzCmd(t, "--scenarios 2")
	require.NoError(t, err, out)
	m := regexp.MustCompile(`^gifford fuzz: seed (\d+), 2 scenarios\n`).FindStringSubmatch(out)
	require.NotNil(t, m, out)
	_, err = strconv.ParseUint(m[1], 10, 64)
	require.NoError(t, err)
	require.NotContains(t, out, "baseline:")
}

// TestFuzzFailsWithSeedAndIndex runs on a harness that quits at once:
// the command fails, naming the seed and index of every scenario and
// where its files are.
func TestFuzzFailsWithSeedAndIndex(t *testing.T) {
	t.Setenv(brokenHarness, "1")
	t.Setenv("TMPDIR", t.TempDir())
	out, err := fuzzCmd(t, "--scenarios 3 --seed 9")
	var ec *deps.ExitError
	require.ErrorAs(t, err, &ec)
	require.Equal(t, exitcode.Error, ec.Code)
	require.Contains(t, err.Error(), "3 scenarios failed with seed 9")
	require.Contains(t, out, "proxy: 3 scenarios, 3 failed, ")
	for i := range 3 {
		require.Contains(t, out, fmt.Sprintf("FAIL proxy scenario: seed 9 index %d\n", i))
	}
	require.Contains(t, out, "the harness ended before the scenario")
	require.Contains(t, out, "  kept in "+os.Getenv("TMPDIR"))
}

// TestFuzzUsage refuses a command line without scenarios, with a bad
// flag or a stray argument, and a harness mode without one record.
func TestFuzzUsage(t *testing.T) {
	t.Parallel()
	for _, args := range []string{
		"", "--scenarios 0", "--scenarios -3", "--scenarios x", "--scenarios 1 --seed -1",
		"--scenarios 1 extra", "--bogus", "harness", "harness a b",
	} {
		t.Run(args, func(t *testing.T) {
			t.Parallel()
			_, err := fuzzCmd(t, args)
			var ec *deps.ExitError
			require.ErrorAs(t, err, &ec)
			require.Equal(t, exitcode.Usage, ec.Code)
		})
	}
}

// TestHarnessModeIsHidden keeps the fake harness out of the help, while
// gifford fuzz harness still runs it.
func TestHarnessModeIsHidden(t *testing.T) {
	t.Parallel()
	out, err := fuzzCmd(t, "--help")
	require.NoError(t, err)
	require.Contains(t, out, "--seed")
	require.NotRegexp(t, `(?m)^\s+`+harnessUse+`\s`, out, "no command line lists the harness")
	cmd := NewCmd(&deps.Deps{})
	found, _, err := cmd.Find([]string{harnessUse})
	require.NoError(t, err)
	require.Equal(t, harnessUse, found.Name())
	require.True(t, found.Hidden)
}

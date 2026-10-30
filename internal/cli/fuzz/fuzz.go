// Package fuzz implements `gifford fuzz`, the keyboard-versus-injection
// race fuzzer, and the fake harness it races against: a hidden mode the
// fuzzer starts by running its own binary again.
package fuzz

import (
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/smellak/gifford/internal/cli/deps"
	"github.com/smellak/gifford/internal/cli/exitcode"
	"github.com/smellak/gifford/internal/fuzz"
)

// harnessUse names the hidden fake harness mode: gifford fuzz harness
// <record>.
const harnessUse = "harness"

// NewCmd returns the gifford fuzz command.
func NewCmd(_ *deps.Deps) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "fuzz",
		Short: "Run the keyboard-vs-injection race fuzzer",
		Long: `Race a synthetic human's keyboard against automatic deliveries, over
--scenarios scenarios, and check that the human is never typed over.

Each scenario runs a fake harness, this binary again, under a real session
on its own terminal. The human types lines with backspace, Ctrl-U, Ctrl-C,
bracketed paste and Enter, at changing rhythms; long pauses pass on the
session's clock, not in real time. An injector with nobody to stop it
delivers notices through the session at the worst instants it can find,
and sends back to its queue each one the session refuses.

A scenario fails when the harness gets a line that is neither exactly a
notice nor exactly what the human meant, when a human key never arrives as
sent, when a delivery starts with the draft open or unknown, or when a
notice arrives other than once when committed, or at all when not. Every
scenario has a time limit; one that hangs fails.

--seed draws the scenarios, so the same seed repeats the batch's scripts;
without it a seed is drawn and printed. --baseline runs the same scenarios
again without the Proxy: the human and the injector write to the same
terminal, and the summary gives the share of lines that came out corrupt.
Scenarios run in parallel, one per CPU at most. A failed scenario keeps its
files in a directory the summary names; everything else is removed.

Exit status: 0 when every scenario passed, 1 when any failed or the batch
was interrupted, 2 on a usage error, 3 where the fuzzer cannot run.`,
		Args: cobra.ArbitraryArgs,
		RunE: run,
	}
	cmd.Flags().Int("scenarios", 0, "number of race scenarios to run")
	cmd.Flags().Bool("baseline", false, "also run the naive-injection baseline")
	cmd.Flags().Uint64("seed", 0, "seed that draws the scenarios (default: drawn and printed)")
	cmd.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
		return usage(err)
	})
	cmd.AddCommand(newHarnessCmd())
	return cmd
}

// usage is a command line error.
func usage(err error) error {
	return &deps.ExitError{Code: exitcode.Usage, Err: fmt.Errorf("gifford fuzz: %w", err)}
}

// run runs one batch and prints its summary.
func run(cmd *cobra.Command, args []string) error {
	if len(args) > 0 {
		return usage(fmt.Errorf("unexpected argument %q", args[0]))
	}
	scenarios, err := cmd.Flags().GetInt("scenarios")
	if err != nil {
		return usage(err)
	}
	if scenarios < 1 {
		return usage(errors.New("--scenarios must be at least 1"))
	}
	baseline, err := cmd.Flags().GetBool("baseline")
	if err != nil {
		return usage(err)
	}
	seed, err := cmd.Flags().GetUint64("seed")
	if err != nil {
		return usage(err)
	}
	if !cmd.Flags().Changed("seed") {
		seed = rand.Uint64() // #nosec G404 -- a seed to print and repeat, not a secret
	}
	exe, err := os.Executable()
	if err != nil {
		return &deps.ExitError{Code: exitcode.Error, Err: fmt.Errorf("gifford fuzz: harness: %w", err)}
	}
	ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	out := cmd.OutOrStdout()
	if _, err := fmt.Fprintf(out, "gifford fuzz: seed %d, %d scenarios\n", seed, scenarios); err != nil {
		return &deps.ExitError{Code: exitcode.Error, Err: fmt.Errorf("gifford fuzz: %w", err)}
	}
	sum, err := fuzz.Run(ctx, fuzz.Options{
		Scenarios: scenarios,
		Seed:      seed,
		Baseline:  baseline,
		Harness:   []string{exe, cmd.Name(), harnessUse},
	})
	switch {
	case errors.Is(err, errors.ErrUnsupported):
		return &deps.ExitError{Code: exitcode.NotImplemented, Err: fmt.Errorf("gifford fuzz: %w", err)}
	case err != nil && !errors.Is(err, fuzz.ErrInterrupted):
		return &deps.ExitError{Code: exitcode.Error, Err: fmt.Errorf("gifford fuzz: %w", err)}
	}
	if rerr := report(out, sum); rerr != nil {
		return &deps.ExitError{Code: exitcode.Error, Err: fmt.Errorf("gifford fuzz: %w", rerr)}
	}
	if err != nil {
		return &deps.ExitError{Code: exitcode.Error, Err: fmt.Errorf("gifford fuzz: %w", err)}
	}
	if n := len(sum.Failures); n > 0 {
		return &deps.ExitError{
			Code: exitcode.Error,
			Err:  fmt.Errorf("gifford fuzz: %d scenarios failed with seed %d, kept in %s", n, sum.Seed, sum.Dir),
		}
	}
	return nil
}

// report prints a batch's summary, then one entry per failed scenario
// with its seed and index.
func report(w io.Writer, sum fuzz.Summary) error {
	var b strings.Builder
	p := sum.Proxy
	fmt.Fprintf(&b, "proxy: %d scenarios, %d failed, %d at a time\n", p.Scenarios, p.Failed, sum.Parallel)
	fmt.Fprintf(&b, "  lines received: %d\n", p.Lines)
	fmt.Fprintf(&b, "  mixed lines: %d\n", p.Mixed)
	fmt.Fprintf(&b, "  commits with the draft open or unknown: %d of %d\n", p.OpenCommits, p.Commits)
	fmt.Fprintf(&b, "  lost keys: %d\n", p.LostKeys)
	fmt.Fprintf(&b, "  notices: %d delivered, %d requeued, of %d (%d attempts)\n", p.Delivered, p.Requeued, p.Notices, p.Attempts)
	fmt.Fprintf(&b, "  notices received other than once: %d\n", p.NoticeFaults)
	fmt.Fprintf(&b, "  enters pressed before the draft showed: %d\n", p.Blind)
	if base := sum.Baseline; base != nil {
		fmt.Fprintf(&b, "baseline: %d scenarios, %d failed\n", base.Scenarios, base.Failed)
		fmt.Fprintf(&b, "  lines received: %d\n", base.Lines)
		fmt.Fprintf(&b, "  corrupt lines: %d (%.2f%%)\n", base.Mixed, base.CorruptPercent())
		fmt.Fprintf(&b, "  lost keys: %d\n", base.LostKeys)
		fmt.Fprintf(&b, "  notices received whole: %d of %d\n", base.Delivered, base.Notices)
	}
	fmt.Fprintf(&b, "duration: %s\n", sum.Duration.Round(10*time.Millisecond))
	for _, f := range sum.Failures {
		run := "proxy"
		if f.Baseline {
			run = "baseline"
		}
		fmt.Fprintf(&b, "FAIL %s scenario: seed %d index %d\n", run, f.Seed, f.Index)
		for _, r := range f.Reasons {
			fmt.Fprintf(&b, "  %s\n", r)
		}
		fmt.Fprintf(&b, "  kept in %s\n", f.Dir)
	}
	_, err := io.WriteString(w, b.String())
	return err //nolint:wrapcheck // Writing to stdout has nothing to add.
}

// newHarnessCmd returns the hidden fake harness mode.
func newHarnessCmd() *cobra.Command {
	return &cobra.Command{
		Use:    harnessUse + " <record>",
		Short:  "Run the fake harness the fuzzer races against",
		Hidden: true,
		Args:   cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return usage(errors.New("harness: expected one record file"))
			}
			in, ok := cmd.InOrStdin().(*os.File)
			if !ok {
				return &deps.ExitError{Code: exitcode.Error, Err: errors.New("gifford fuzz harness: stdin is not a terminal")}
			}
			if err := fuzz.Harness(in, cmd.OutOrStdout(), args[0]); err != nil {
				return &deps.ExitError{Code: exitcode.Error, Err: fmt.Errorf("gifford fuzz: %w", err)}
			}
			return nil
		},
	}
}

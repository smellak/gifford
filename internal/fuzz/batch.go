package fuzz

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/smellak/gifford/internal/clock"
)

// DefaultTimeout bounds one scenario when Options.Timeout is zero: a
// scenario takes well under a second, so reaching it means a hang.
const DefaultTimeout = time.Minute

// Batch errors.
var (
	// ErrNoScenarios means Options.Scenarios is below one.
	ErrNoScenarios = errors.New("fuzz: at least one scenario")
	// ErrNoHarness means Options.Harness is empty.
	ErrNoHarness = errors.New("fuzz: no harness command")
	// ErrInterrupted means the batch stopped before its last scenario.
	ErrInterrupted = errors.New("fuzz: batch interrupted")
)

// Options configures one batch.
type Options struct {
	// Scenarios is how many scenarios run, indexes 0 to Scenarios-1.
	Scenarios int
	// Seed draws every scenario's script.
	Seed uint64
	// Baseline runs the same scenarios again without the Proxy.
	Baseline bool
	// Parallel caps the scenarios running at once; zero or more than
	// the CPUs means one per CPU.
	Parallel int
	// Harness starts the fake harness; each scenario appends its
	// record file.
	Harness []string
	// Dir holds the batch directory; "" is the system temp directory.
	Dir string
	// Timeout bounds each scenario; zero is DefaultTimeout.
	Timeout time.Duration
}

// Totals add up one run of a batch, through the Proxy or without it.
type Totals struct {
	Scenarios int
	// Failed counts the scenarios that broke an invariant or did not
	// finish; a baseline scenario fails only when it does not finish.
	Failed int
	// Lines counts the lines the harness got.
	Lines int
	// Mixed counts lines neither exactly a notice nor exactly what the
	// human meant there: through the Proxy, an invariant; without it,
	// the corrupt lines.
	Mixed int
	// LostKeys counts the human keys that never arrived as sent.
	LostKeys int
	// Commits counts deliveries that started writing; OpenCommits the
	// ones that started with the draft not closed.
	Commits     int
	OpenCommits int
	// Notices counts the notices to deliver; Delivered the ones
	// committed and received once, Requeued the ones still queued at
	// the end.
	Notices   int
	Delivered int
	Requeued  int
	// Attempts counts every prepare the injector made.
	Attempts int
	// NoticeFaults counts notices received other than once when
	// committed, or at all when not.
	NoticeFaults int
	// Blind counts the times the human pressed Enter without seeing its
	// draft on the screen.
	Blind int
}

// CorruptPercent is the share of the lines the harness got that were
// mixed.
func (t Totals) CorruptPercent() float64 {
	if t.Lines == 0 {
		return 0
	}
	return 100 * float64(t.Mixed) / float64(t.Lines)
}

// Failure is one scenario that failed, enough to find it again.
type Failure struct {
	// Baseline marks a baseline scenario.
	Baseline bool
	Index    int
	Seed     uint64
	// Reasons says what failed, one line each.
	Reasons []string
	// Dir keeps the scenario's files.
	Dir string
}

// Summary is what one batch found.
type Summary struct {
	Seed     uint64
	Parallel int
	Proxy    Totals
	// Baseline is set when the batch ran it.
	Baseline *Totals
	Failures []Failure
	// Dir keeps the failed scenarios' directories; empty when none
	// failed.
	Dir      string
	Duration time.Duration
}

// now reads the real clock, for the batch's duration.
func now() time.Time { return clock.System{}.Now() }

// config is what every scenario of a batch shares.
type config struct {
	dir     string
	harness []string
	timeout time.Duration
}

// Run runs a batch: every scenario through a real session, then, with
// Baseline, the same scenarios without the Proxy. Scenarios run in
// parallel, each bounded by its timeout. A failed scenario keeps its
// directory under Summary.Dir; everything else is removed.
func Run(ctx context.Context, opts Options) (Summary, error) {
	if opts.Scenarios < 1 {
		return Summary{}, ErrNoScenarios
	}
	if len(opts.Harness) == 0 {
		return Summary{}, ErrNoHarness
	}
	if !supported {
		return Summary{}, fmt.Errorf("fuzz: %w", errors.ErrUnsupported)
	}
	parallel := opts.Parallel
	if parallel <= 0 || parallel > runtime.NumCPU() {
		parallel = runtime.NumCPU()
	}
	parallel = min(parallel, opts.Scenarios)
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	dir, err := os.MkdirTemp(opts.Dir, "gifford-fuzz-")
	if err != nil {
		return Summary{}, fmt.Errorf("fuzz: batch dir: %w", err)
	}
	cfg := config{dir: dir, harness: opts.Harness, timeout: timeout}
	sum := Summary{Seed: opts.Seed, Parallel: parallel}
	start := now()
	var failures []Failure
	sum.Proxy, failures = runBatch(ctx, cfg, opts, parallel, false)
	sum.Failures = append(sum.Failures, failures...)
	if opts.Baseline && ctx.Err() == nil {
		base, failures := runBatch(ctx, cfg, opts, parallel, true)
		sum.Baseline = &base
		sum.Failures = append(sum.Failures, failures...)
	}
	sum.Duration = now().Sub(start)
	if len(sum.Failures) == 0 {
		if err := os.RemoveAll(dir); err != nil {
			return sum, fmt.Errorf("fuzz: batch dir: %w", err)
		}
	} else {
		sum.Dir = dir
	}
	if err := ctx.Err(); err != nil {
		return sum, fmt.Errorf("%w: %w", ErrInterrupted, err)
	}
	return sum, nil
}

// runBatch runs every scenario of one run, parallel at a time, and adds
// them up.
func runBatch(ctx context.Context, cfg config, opts Options, parallel int, baseline bool) (Totals, []Failure) {
	indexes := make(chan int)
	results := make(chan result)
	var wg sync.WaitGroup
	for range parallel {
		wg.Go(func() {
			for i := range indexes {
				results <- runScenario(ctx, cfg, NewScript(opts.Seed, i), baseline)
			}
		})
	}
	go func() {
		defer close(indexes)
		for i := range opts.Scenarios {
			select {
			case indexes <- i:
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() {
		wg.Wait()
		close(results)
	}()
	var t Totals
	var failures []Failure
	for r := range results {
		t.add(r, baseline)
		if r.failed(baseline) {
			failures = append(failures, Failure{
				Baseline: baseline, Index: r.index, Seed: opts.Seed, Reasons: r.reasons(), Dir: r.dir,
			})
		}
	}
	sort.Slice(failures, func(i, j int) bool { return failures[i].Index < failures[j].Index })
	return t, failures
}

// result is what one scenario found.
type result struct {
	index                  int
	lines, mixed, lostKeys int
	commits, openCommits   int
	notices, delivered     int
	requeued, attempts     int
	noticeFaults, blind    int
	// err is a scenario that did not finish or could not be checked.
	err    error
	detail []string
	dir    string
}

// failed reports a scenario that broke an invariant or did not finish;
// without the Proxy, mixed lines are what the baseline measures.
func (r result) failed(baseline bool) bool {
	if r.err != nil {
		return true
	}
	return !baseline && (r.mixed > 0 || r.lostKeys > 0 || r.openCommits > 0 || r.noticeFaults > 0)
}

// reasons says why a scenario failed, its error first.
func (r result) reasons() []string {
	var out []string
	if r.err != nil {
		out = append(out, r.err.Error())
	}
	return append(out, r.detail...)
}

// add counts one scenario.
func (t *Totals) add(r result, baseline bool) {
	t.Scenarios++
	if r.failed(baseline) {
		t.Failed++
	}
	t.Lines += r.lines
	t.Mixed += r.mixed
	t.LostKeys += r.lostKeys
	t.Commits += r.commits
	t.OpenCommits += r.openCommits
	t.Notices += r.notices
	t.Delivered += r.delivered
	t.Requeued += r.requeued
	t.Attempts += r.attempts
	t.NoticeFaults += r.noticeFaults
	t.Blind += r.blind
}

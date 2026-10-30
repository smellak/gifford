//go:build unix

package fuzz

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// batchSeed is the seed of the tests' batch.
const batchSeed = 20261030

// TestBatchNeverTramplesTheHuman runs 50 scenarios through a real
// session, then the same 50 without the Proxy. Through the Proxy no
// line is mixed, no key is lost, no delivery starts with the draft open
// and every notice arrives once or stays queued; most are delivered.
// Without it, lines come out corrupt. Nothing is left on disk.
func TestBatchNeverTramplesTheHuman(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	sum, err := Run(t.Context(), Options{
		Scenarios: 50, Seed: batchSeed, Baseline: true, Harness: testHarness(t, harnessArg), Dir: dir,
	})
	require.NoError(t, err)
	require.Empty(t, sum.Failures)
	require.Equal(t, uint64(batchSeed), sum.Seed)
	require.Positive(t, sum.Duration)

	p := sum.Proxy
	require.Equal(t, 50, p.Scenarios)
	require.Zero(t, p.Failed)
	require.Zero(t, p.Mixed)
	require.Zero(t, p.LostKeys)
	require.Zero(t, p.OpenCommits)
	require.Zero(t, p.NoticeFaults)
	require.Positive(t, p.Lines)
	require.Positive(t, p.Commits)
	require.Equal(t, p.Commits, p.Delivered, "every commit delivered its notice once")
	require.Equal(t, p.Notices, p.Delivered+p.Requeued)
	require.Greater(t, p.Delivered, p.Notices/2)
	require.Greater(t, p.Attempts, p.Delivered, "the session refused some attempts")

	require.NotNil(t, sum.Baseline)
	b := *sum.Baseline
	require.Equal(t, 50, b.Scenarios)
	require.Zero(t, b.Failed)
	require.Equal(t, p.Notices, b.Notices, "the same scenarios")
	require.Positive(t, b.Mixed, "without the Proxy lines come out corrupt")
	require.Positive(t, b.CorruptPercent())

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries, "a batch with no failure leaves nothing")
}

// TestHangFailsWithItsSeedAndIndex runs scenarios on a harness that
// draws its prompt and then never answers: each one fails once its time
// is up, with its seed and index and its files kept, and the batch
// ends.
func TestHangFailsWithItsSeedAndIndex(t *testing.T) {
	t.Parallel()
	start := time.Now()
	sum, err := Run(t.Context(), Options{
		Scenarios: 3, Seed: batchSeed, Harness: testHarness(t, deafArg), Dir: t.TempDir(), Timeout: 2 * time.Second,
	})
	require.NoError(t, err)
	require.Less(t, time.Since(start), 30*time.Second, "the batch never waits on a hang")
	require.Len(t, sum.Failures, 3)
	require.Equal(t, 3, sum.Proxy.Failed)
	for i, f := range sum.Failures {
		require.Equal(t, i, f.Index)
		require.Equal(t, uint64(batchSeed), f.Seed)
		require.False(t, f.Baseline)
		require.Contains(t, f.Reasons[0], "scenario timed out after 2s")
		require.DirExists(t, f.Dir)
		require.FileExists(t, filepath.Join(f.Dir, scriptFile))
		require.FileExists(t, filepath.Join(f.Dir, verdictFile))
		require.Equal(t, filepath.Dir(f.Dir), sum.Dir)
	}
}

// TestRunNeedsScenariosAndAHarness refuses a batch with nothing to run
// or nothing to run it on.
func TestRunNeedsScenariosAndAHarness(t *testing.T) {
	t.Parallel()
	_, err := Run(t.Context(), Options{Harness: testHarness(t, harnessArg)})
	require.ErrorIs(t, err, ErrNoScenarios)
	_, err = Run(t.Context(), Options{Scenarios: 1})
	require.ErrorIs(t, err, ErrNoHarness)
}

// TestInterruptedBatchSaysSo stops a batch before it starts: it says it
// was interrupted.
func TestInterruptedBatchSaysSo(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := Run(ctx, Options{Scenarios: 5, Seed: batchSeed, Harness: testHarness(t, harnessArg), Dir: t.TempDir()})
	require.ErrorIs(t, err, ErrInterrupted)
}

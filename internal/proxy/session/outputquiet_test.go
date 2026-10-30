//go:build unix

package session

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy"
)

// quietNotice is the line the output quiet tests deliver.
const quietNotice = "[GIFFORD] quiet-k8d1 pending"

// writePaintHarnessScript drops the raw harness that first leaves the
// path of its terminal in $2: a test that writes there paints on the
// harness's screen as the harness would, with no key reaching it.
func writePaintHarnessScript(t *testing.T) string {
	t.Helper()
	script := filepath.Join(t.TempDir(), "paintharness")
	body := "#!/bin/sh\nOUT=\"$1\"\nstty -icanon -echo min 1 time 0\ntty > \"$2\"\necho deliver-ready\nexec cat >> \"$OUT\"\n"
	require.NoError(t, os.WriteFile(script, []byte(body), 0o700))
	return script
}

// paintRun is a delivery session over the paint harness on a step
// clock, with its agent socket and a registered Core.
type paintRun struct {
	*deliveryRun
	core  *fakeCore
	clock *stepClock
	tty   string
}

// paintSession runs a paint run past the quiet period of its ready
// line, with the edits given on its options.
func paintSession(t *testing.T, id string, edits ...func(*Options)) *paintRun {
	t.Helper()
	clk := &stepClock{now: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)}
	tty := filepath.Join(t.TempDir(), "harness.tty")
	dr, k, _ := startSocketDelivery(t, id, newWindowClock(false), func(o *Options) {
		o.Command[0] = writePaintHarnessScript(t)
		o.Command = append(o.Command, tty)
		o.Clock = clk
		for _, edit := range edits {
			edit(o)
		}
	})
	name, err := os.ReadFile(tty)
	require.NoError(t, err)
	return &paintRun{deliveryRun: dr, core: k, clock: clk, tty: strings.TrimSpace(string(name))}
}

// write puts raw on the harness's terminal, as the harness would.
func (p *paintRun) write(t *testing.T, raw string) {
	t.Helper()
	f, err := os.OpenFile(p.tty, os.O_WRONLY|unix.O_NOCTTY, 0)
	require.NoError(t, err)
	_, err = f.WriteString(raw)
	require.NoError(t, errors.Join(err, f.Close()))
}

// paint writes one line on the harness's screen and returns once the
// loop has folded it.
func (p *paintRun) paint(t *testing.T, text string) {
	t.Helper()
	p.write(t, text+"\n")
	waitScreen(t, p.session, "the loop never folded the paint", below(text))
}

// TestOutputHoldsThePrepare paints on the harness's screen, then
// prepares once the session's clock has moved: within the quiet period
// the session and the socket refuse it as the harness painting and
// write nothing; from its end on the delivery goes through.
func TestOutputHoldsThePrepare(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		after time.Duration
		quiet bool
	}{
		{name: "100 ms after the output", after: 100 * time.Millisecond},
		{name: "1 ms before the quiet period ends", after: outputQuietPeriod - time.Millisecond},
		{name: "as the quiet period ends", after: outputQuietPeriod, quiet: true},
		{name: "400 ms after the output", after: 400 * time.Millisecond, quiet: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := paintSession(t, "quiet-prepare")
			dr := p.deliveryRun
			p.paint(t, "painting")
			p.clock.advance(tc.after)
			if !tc.quiet {
				require.Equal(t, DeliveryAbortedHumanPriorityOutcome, prepareNow(t, dr.session, "dlv-busy"))
				gen, ver := guardAt(dr.session)
				r := p.core.order(protocol.AgentOpPrepareDelivery, sockPrepare("dlv-sock", quietNotice, gen, ver))
				require.Equal(t, protocol.DeliveryResultKindAbortedHumanPriority, r.Result, "the node is refused too")
				require.Equal(t, []string{reasonOutputBusy, reasonOutputBusy}, abortReasons(dr))
				require.Empty(t, sinkPayloads(dr.sink, protocol.EventTypeInputDeliveryPrepared))
				endRawSession(t, dr)
				require.Empty(t, harnessOut(t, dr.out), "no byte reached the harness")
				return
			}
			require.Equal(t, DeliveryPreparedOutcome, prepareNow(t, dr.session, "dlv-quiet"))
			rep, err := dr.session.CommitDelivery("dlv-quiet", quietNotice, protocol.InjectModeTyped)
			require.NoError(t, err)
			require.Equal(t, DeliveryCommittedOutcome, rep.Outcome)
			waitHarness(t, dr, quietNotice+"\n", "the notice never reached the harness")
			require.Empty(t, abortReasons(dr))
			endRawSession(t, dr)
		})
	}
}

// TestOutputBetweenPrepareAndCommit paints on the harness's screen
// between a PREPARED and its commit: the commit aborts before it takes
// the lease and writes no byte, and once the screen has been quiet for
// the period a new delivery goes through.
func TestOutputBetweenPrepareAndCommit(t *testing.T) {
	t.Parallel()
	p := paintSession(t, "quiet-commit")
	dr := p.deliveryRun
	const canary = "canary-q7m2"
	gen, ver := guardAt(dr.session)
	require.Equal(t, protocol.DeliveryResultKindPrepared,
		p.core.order(protocol.AgentOpPrepareDelivery, sockPrepare("dlv-early", "[GIFFORD] "+canary+" pending", gen, ver)).Result)
	p.paint(t, "painting")
	r := p.core.order(protocol.AgentOpCommitDelivery, sockCommit("dlv-early"))
	require.Equal(t, protocol.DeliveryResultKindAbortedHumanPriority, r.Result)
	require.Equal(t, []string{reasonOutputBusy}, abortReasons(dr))
	require.Empty(t, sinkPayloads(dr.sink, protocol.EventTypeInputDeliveryRevalidated), "no lease taken")
	require.Empty(t, sinkPayloads(dr.sink, protocol.EventTypeInputDeliveryStarted))
	dr.session.mu.Lock()
	lease, committing := dr.session.lease, dr.session.committing
	dr.session.mu.Unlock()
	require.Nil(t, lease)
	require.False(t, committing, "the aborted commit left no reservation")

	p.clock.advance(outputQuietPeriod)
	require.Equal(t, DeliveryPreparedOutcome, prepareNow(t, dr.session, "dlv-late"))
	rep, err := dr.session.CommitDelivery("dlv-late", quietNotice, protocol.InjectModeTyped)
	require.NoError(t, err)
	require.Equal(t, DeliveryCommittedOutcome, rep.Outcome)
	waitHarness(t, dr, quietNotice+"\n", "the notice never reached the harness")
	endRawSession(t, dr)
	out := harnessOut(t, dr.out)
	require.Equal(t, quietNotice+"\n", out, "only the delivery after the quiet period")
	require.NotContains(t, out, canary, "no byte of the aborted notice")
}

// noticeBuffer keeps the daemon notice race-clean.
type noticeBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *noticeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *noticeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestNoticeIsNoOutput runs a silent harness under a session that
// writes the daemon notice: the notice is the Proxy's, not the
// harness's, so no output time is kept, and a delivery prepares and
// commits with the session's clock never moved.
func TestNoticeIsNoOutput(t *testing.T) {
	t.Parallel()
	out := filepath.Join(t.TempDir(), "harness.out")
	ready := filepath.Join(t.TempDir(), "harness.ready")
	script := filepath.Join(t.TempDir(), "silentharness")
	body := "#!/bin/sh\nOUT=\"$1\"\nstty -icanon -echo min 1 time 0\n: > \"$2\"\nexec cat >> \"$OUT\"\n"
	require.NoError(t, os.WriteFile(script, []byte(body), 0o700))
	master, slave := testPTY(t)
	sink := &chanSink{}
	notice := &noticeBuffer{}
	opts := testOptions(t.TempDir(), "quiet-notice", slave, slave, sink, []string{script, out, ready})
	opts.Profile = deliveryProfile()
	opts.Notice = notice
	opts.Clock = &stepClock{now: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)}
	opts.SubmitDelay = 0
	s, err := NewSession(opts)
	require.NoError(t, err)
	dr := &deliveryRun{session: s, master: master, sink: sink, out: out, done: runAsync(t, s)}
	waitRaw(t, slave)
	require.Eventually(t, func() bool {
		_, err := os.Stat(ready)
		return err == nil
	}, 10*time.Second, time.Millisecond, "the harness never got ready")

	require.Equal(t, proxy.DaemonNotice+"\n", notice.String(), "the session wrote the daemon notice")
	s.mu.Lock()
	kept := s.lastOutput
	s.mu.Unlock()
	require.True(t, kept.IsZero(), "the notice is no harness output")
	require.True(t, publishedSnapshot(t, s).LastOutput.IsZero())
	require.Equal(t, DeliveryPreparedOutcome, prepareNow(t, s, "dlv-notice"))
	rep, err := s.CommitDelivery("dlv-notice", quietNotice, protocol.InjectModeTyped)
	require.NoError(t, err)
	require.Equal(t, DeliveryCommittedOutcome, rep.Outcome)
	waitHarness(t, dr, quietNotice+"\n", "the notice line never reached the harness")
	endRawSession(t, dr)
}

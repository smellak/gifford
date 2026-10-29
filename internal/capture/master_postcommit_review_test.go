//go:build masterreview

package capture

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// Inject only a scheduled, synthetic readiness observation. The next
// clock read occurs after attach releases its exclusive session lock.
type masterPostCommitClock struct {
	base  *advClock
	after func()
}

func (c *masterPostCommitClock) Now() time.Time {
	if c.after != nil {
		f := c.after
		c.after = nil
		f()
	}
	return c.base.Now()
}

// CAP-A2 section7 step11: an injected post-commit row-cache failure
// remains ErrAttach even if the matching ingest becomes observable.
func TestMasterPostCommitFailureDoesNotBecomeSuccess(t *testing.T) {
	skipUnlessAttach(t)
	data := t.TempDir()
	runner := &fakeRunner{
		serverOut: attachServer(),
		paneBy:    map[string]paneFacts{attachTarget: attachPane()},
		openOut:   openOutcome{opened: true, snapshot: []byte("synthetic snapshot")},
	}
	clk := &masterPostCommitClock{base: attachClock()}
	deps := attachDeps(runner, data, clk.base)
	deps.Clock = clk
	var holder *os.File
	t.Cleanup(func() {
		if holder != nil {
			_ = releaseLock(holder)
		}
	})
	deps.CommitRow = func(_ context.Context, row SessionRow) error {
		nonce, ok := parseBoundaryRef(row.CaptureSessionID, 1, row.InitialScreenBlobRef)
		if !ok {
			t.Fatal("invalid fixture committed boundary")
		}
		clk.after = func() {
			root, err := openCaptureRoot(data, false)
			if err != nil {
				t.Fatal(err)
			}
			defer root.Close()
			holder, err = tryLock(root, row.AgentID+"/"+row.CaptureSessionID+"/ingest.lock", true)
			if err != nil {
				t.Fatalf("readiness attempted before handover released lock: %v", err)
			}
			if err := writeLockLine(holder, fmt.Sprintf("ingest %d %s", deps.PID+1, nonce)); err != nil {
				t.Fatal(err)
			}
			pane := runner.paneBy[attachTarget]
			pane.pipe = true
			runner.paneBy[attachTarget] = pane
		}
		return errors.New("synthetic post-commit row failure")
	}
	result, err := Attach(context.Background(), deps, attachOpts())
	t.Logf("injected committed row failure, then valid matching readiness: result=%+v error=%v", result, err)
	if !errors.Is(err, ErrAttach) || !strings.Contains(err.Error(), "capture committed; readiness unconfirmed") {
		t.Fatal("post-commit row-cache failure became success instead of the required committed/unconfirmed ErrAttach")
	}
}

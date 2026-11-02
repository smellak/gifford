//go:build linux

package proxy

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/creack/pty"
	"golang.org/x/sys/unix"
)

const ptyGCStressDuration = 3 * time.Second

// TestPTYOpenMatchesMasterUnderGCStackShrink keeps worker stacks eligible for
// shrinking while pty.Open runs. A stale ioctl pointer can leave TIOCGPTN at
// zero and make Open return a slave that does not belong to its master.
func TestPTYOpenMatchesMasterUnderGCStackShrink(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping PTY GC stress in short mode")
	}

	// Keep one pair open so a clean devpts cannot legitimately reuse index zero
	// while the stressed Open calls run.
	anchorMaster, anchorTTY, err := pty.Open()
	if err != nil {
		t.Fatalf("open anchor PTY: %v", err)
	}
	t.Cleanup(func() {
		_ = anchorTTY.Close()
		_ = anchorMaster.Close()
	})

	var stop atomic.Bool
	var gcWorkers sync.WaitGroup
	gcWorkers.Add(1)
	go func() {
		defer gcWorkers.Done()
		for !stop.Load() {
			runtime.GC()
		}
	}()

	timer := time.AfterFunc(ptyGCStressDuration, func() { stop.Store(true) })
	defer timer.Stop()

	var (
		iterations   atomic.Int64
		workers      sync.WaitGroup
		failureOnce  sync.Once
		firstFailure error
	)
	for range 8 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for !stop.Load() {
				_ = withPTYStackDepth(2000, func() {})
				iterations.Add(1)
				if err := checkPTYPair(); err != nil {
					failureOnce.Do(func() {
						firstFailure = err
						stop.Store(true)
					})
				}
			}
		}()
	}

	workers.Wait()
	stop.Store(true)
	gcWorkers.Wait()

	if firstFailure != nil {
		t.Fatalf("PTY pair mismatch after %d opens: %v", iterations.Load(), firstFailure)
	}
	t.Logf("%d PTY pairs matched under GC stack pressure", iterations.Load())
}

func checkPTYPair() error {
	master, tty, err := pty.Open()
	if err != nil {
		return fmt.Errorf("open PTY: %w", err)
	}
	defer func() {
		_ = tty.Close()
		_ = master.Close()
	}()

	masterIndex, err := unix.IoctlGetUint32(int(master.Fd()), unix.TIOCGPTN)
	if err != nil {
		return fmt.Errorf("read master PTY index: %w", err)
	}
	var stat unix.Stat_t
	if err := unix.Fstat(int(tty.Fd()), &stat); err != nil {
		return fmt.Errorf("stat tty: %w", err)
	}
	ttyIndex := unix.Minor(stat.Rdev)
	if ttyIndex != masterIndex {
		return fmt.Errorf("master index %d, tty minor %d", masterIndex, ttyIndex)
	}
	return nil
}

//go:noinline
func withPTYStackDepth(depth int, fn func()) byte {
	var pad [8]byte
	pad[depth&7] = byte(depth)
	if depth > 0 {
		pad[0] += withPTYStackDepth(depth-1, fn)
	} else {
		fn()
	}
	return pad[depth&7]
}

package capture

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const (
	testSessionA = "cap-01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testSessionB = "cap-01ARZ3NDEKTSV4RRFFQ69G5FAW"
	testSegment  = "seg-01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testAgent    = "miner-1"
)

// openTestRoot opens an empty capture root under a temp directory.
func openTestRoot(t *testing.T) (*os.Root, string) {
	t.Helper()
	data := t.TempDir()
	if err := os.MkdirAll(filepath.Join(data, "captures"), 0o700); err != nil {
		t.Fatal(err)
	}
	root, err := openCaptureRoot(data, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	return root, data
}

// makeSession creates an agent session directory.
func makeSession(t *testing.T, root *os.Root, agent, session string) {
	t.Helper()
	if err := root.Mkdir(agent, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		t.Fatal(err)
	}
	if err := root.Mkdir(agent+"/"+session, 0o700); err != nil {
		t.Fatal(err)
	}
}

// putFile publishes a control or data file under dir.
func putFile(t *testing.T, root *os.Root, dir, name string, b []byte) {
	t.Helper()
	if err := publishFile(root, dir, name, b, false); err != nil {
		t.Fatal(err)
	}
}

// skipUnlessLocks skips lock behavior tests where flock is unavailable.
func skipUnlessLocks(t *testing.T, root *os.Root) {
	t.Helper()
	if err := ensureLock(root, "quota.lock"); err != nil {
		if errors.Is(err, errors.ErrUnsupported) {
			t.Skip("locks unsupported on this platform")
		}
		t.Fatal(err)
	}
}

func TestEnsureLockIdempotent(t *testing.T) {
	root, _ := openTestRoot(t)
	skipUnlessLocks(t, root)
	if err := ensureLock(root, "quota.lock"); err != nil {
		t.Fatal(err)
	}
	fi, err := root.Lstat("quota.lock")
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("lock mode = %o, want 600", fi.Mode().Perm())
	}
}

func TestWithLockPropagatesFnError(t *testing.T) {
	root, _ := openTestRoot(t)
	skipUnlessLocks(t, root)
	errBoom := errors.New("boom")
	err := withLock(root, "quota.lock", true, time.Second, func() error {
		return errBoom
	})
	if !errors.Is(err, errBoom) {
		t.Fatalf("withLock = %v, want boom", err)
	}
}

func TestWithLockExclusiveBlocks(t *testing.T) {
	root, _ := openTestRoot(t)
	skipUnlessLocks(t, root)
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = withLock(root, "quota.lock", true, 5*time.Second, func() error {
			close(release)
			time.Sleep(300 * time.Millisecond)
			return nil
		})
	}()
	<-release
	err := withLock(root, "quota.lock", true, 50*time.Millisecond, func() error {
		return nil
	})
	if !errors.Is(err, ErrBusy) {
		t.Fatalf("second exclusive = %v, want ErrBusy", err)
	}
	<-done
}

func TestLockLiveProbe(t *testing.T) {
	root, _ := openTestRoot(t)
	skipUnlessLocks(t, root)
	live, err := lockLive(root, "quota.lock")
	if err != nil {
		t.Fatal(err)
	}
	if live {
		t.Fatal("live with no holder")
	}
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = withLock(root, "quota.lock", true, 5*time.Second, func() error {
			close(release)
			time.Sleep(200 * time.Millisecond)
			return nil
		})
	}()
	<-release
	live, err = lockLive(root, "quota.lock")
	if err != nil {
		t.Fatal(err)
	}
	if !live {
		t.Fatal("not live under exclusive holder")
	}
	<-done
	live, err = lockLive(root, "quota.lock")
	if err != nil {
		t.Fatal(err)
	}
	if live {
		t.Fatal("live after release")
	}
	live, err = lockLive(root, "missing.lock")
	if err != nil {
		t.Fatal(err)
	}
	if live {
		t.Fatal("live for missing file")
	}
}

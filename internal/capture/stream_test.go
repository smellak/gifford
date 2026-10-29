package capture

import (
	"os"
	"path/filepath"
	"testing"
)

func logRun(root *os.Root, agent, session string) *ingestRun {
	return &ingestRun{
		root: root,
		opts: IngestOptions{Agent: agent, Session: session},
		lim:  ingestLimits(),
		deps: IngestDeps{PID: 4242, Clock: attachClock()},
	}
}

func TestOpenLogSymlink(t *testing.T) {
	data := t.TempDir()
	root, err := openCaptureRoot(data, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	makeSession(t, root, storeTestAgent, storeTestSession)
	outside := filepath.Join(data, "outside.log")
	if err := os.WriteFile(outside, []byte("sentinel"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(data, "captures", storeTestAgent, storeTestSession, "ingest.log")); err != nil {
		t.Fatal(err)
	}
	run := logRun(root, storeTestAgent, storeTestSession)
	if err := run.openLog(); err == nil {
		t.Fatal("openLog followed a symlink")
	}
	kept, err := os.ReadFile(outside)
	if err != nil {
		t.Fatal(err)
	}
	if string(kept) != "sentinel" {
		t.Fatalf("outside file = %q, want untouched", kept)
	}
}

func TestOpenLogCreateAndAppend(t *testing.T) {
	data := t.TempDir()
	root, err := openCaptureRoot(data, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	makeSession(t, root, storeTestAgent, storeTestSession)
	run := logRun(root, storeTestAgent, storeTestSession)
	if err := run.openLog(); err != nil {
		t.Fatal(err)
	}
	run.logLine("hello")
	if run.log == nil {
		t.Fatal("no log opened")
	}
	fi, err := os.Stat(filepath.Join(data, "captures", storeTestAgent, storeTestSession, "ingest.log"))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 || !fi.Mode().IsRegular() {
		t.Fatalf("log mode = %v", fi.Mode())
	}
}

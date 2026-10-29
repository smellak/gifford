//go:build tmux

package capturetest

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/smellak/gifford/internal/capture"
)

// setup runs one gated counter pane of n lines and attaches it. The
// gate opens after attach returns, so every line flows through the
// pipe. It returns the server, deps, result and the raw byte total.
func setupCounter(t *testing.T, agent string, n int) (*testServer, *testDeps, capture.AttachResult, int64) {
	t.Helper()
	srv := newServer(t)
	d := &testDeps{sock: srv.sock, data: t.TempDir(), mono: testMono{start: time.Now()}}
	gate := filepath.Join(t.TempDir(), "gate")
	pane := srv.newPane("gated", gate, itoa(n))
	result, err := capture.Attach(context.Background(), d.attachDeps(), d.attachOpts(agent, pane))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(gate, []byte("go"), 0o600); err != nil {
		t.Fatal(err)
	}
	var total int64
	for i := 0; i < n; i++ {
		total += int64(len(rawLine(i)))
	}
	return srv, d, result, total
}

// burstCommitted is the committed image of a gburst total: every
// newline the pane prints crosses the pipe as CR LF.
func burstCommitted(total int64) int64 {
	var nls int64
	for i := int64(64); i < total; i += 65 {
		nls++
	}
	return total + nls
}

// TestAttachIngestStatus runs a 20 MiB burst through one pane: the
// segment bytes equal the produced stream exactly, and status shows
// bytes and last activity.
func TestAttachIngestStatus(t *testing.T) {
	const total = int64(20 << 20)
	want := burstCommitted(total)
	srv := newServer(t)
	d := &testDeps{sock: srv.sock, data: t.TempDir(), mono: testMono{start: time.Now()}}
	gate := filepath.Join(t.TempDir(), "gate")
	pane := srv.newPane("gburst", gate, itoa(int(total)))
	// Wide chunks keep the 20 MiB commit count low; exactness is
	// unaffected.
	opts := d.attachOpts("a-ml", pane)
	opts.Limits = capture.Limits{ChunkMaxBytes: 1 << 20, ChunkMaxAge: capture.Duration(time.Second)}
	res, err := capture.Attach(context.Background(), d.attachDeps(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(gate, []byte("go"), 0o600); err != nil {
		t.Fatal(err)
	}
	waitRaw(t, d, res.Session, want, 3*time.Minute)

	var segBytes int64
	segs := dbSegments(t, d.data, res.Session)
	if len(segs) == 0 {
		t.Fatal("no segments committed")
	}
	for _, seg := range segs {
		segBytes += seg.bytes
	}
	if segBytes != want {
		t.Fatalf("segment bytes = %d, want %d", segBytes, want)
	}
	st := dbSession(t, d.data, res.Session)
	if !st.found || st.state != "ATTACHED" {
		t.Fatalf("session row = %+v, want ATTACHED", st)
	}
	line := findStatus(t, d, "a-ml")
	if line.Capture != res.Session || line.Pane == "" || line.Bytes != want {
		t.Fatalf("status line = %+v, want %d bytes", line, want)
	}
	if line.LastActivity == "" || line.LastActivity == "-" {
		t.Fatalf("status last activity = %q", line.LastActivity)
	}
	if got := srv.panePipe(res.Pane); got != "1" {
		t.Fatalf("pane_pipe = %s, want 1", got)
	}
}

// TestSnapshotJunction checks fact 14: the snapshot's last line plus
// the first chunk's bytes continue the count with no gap and no
// overlap.
func TestSnapshotJunction(t *testing.T) {
	const n1, n2 = 40, 100
	srv := newServer(t)
	d := &testDeps{sock: srv.sock, data: t.TempDir(), mono: testMono{start: time.Now()}}
	gate := filepath.Join(t.TempDir(), "gate")
	pane := srv.newPane("junction", itoa(n1), gate, itoa(n2))
	res, err := capture.Attach(context.Background(), d.attachDeps(), d.attachOpts("a-ml", pane))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(gate, []byte("go"), 0o600); err != nil {
		t.Fatal(err)
	}
	var total int64
	for i := 0; i < n2; i++ {
		total += int64(len(rawLine(n1 + i)))
	}
	waitRaw(t, d, res.Session, total, time.Minute)

	snaps := dbSnapshots(t, d.data, "a-ml", res.Session)
	if len(snaps) != 1 {
		t.Fatalf("snapshots = %v, want exactly one initial", snaps)
	}
	snapLines := snapshotLines(t, d.data, "a-ml", res.Session, snaps[0])
	if len(snapLines) == 0 {
		t.Fatal("snapshot has no visible lines")
	}
	last := snapLines[len(snapLines)-1]
	if last != strings.TrimSuffix(visibleLine(counterLine(n1-1)), "\n") {
		t.Fatalf("snapshot last line = %q, want %q", last, visibleLine(counterLine(n1-1)))
	}
	var chunks string
	for _, ev := range dbEvents(t, d.data, res.Session) {
		if ev.typ != "capture.chunk" {
			continue
		}
		chunks += decodeRun(t, rawSlice(t, d.data, "a-ml", res.Session, chunkPayload(t, ev.payload)))
	}
	first := strings.SplitN(chunks, "\n", 2)[0]
	if first != strings.TrimSuffix(visibleLine(counterLine(n1)), "\n") {
		t.Fatalf("first chunk line = %q, want %q", first, visibleLine(counterLine(n1)))
	}
	wantChunks := ""
	for i := n1; i < n1+n2; i++ {
		wantChunks += visibleLine(counterLine(i))
	}
	if chunks != wantChunks {
		t.Fatalf("chunk stream = %d runes, want %d: gap or overlap at the junction", len(chunks), len(wantChunks))
	}
}

// TestRefuseAndReplace opens a day-0 `cat >>` pipe on the pane:
// Attach returns ErrPipeExists while the file keeps growing; with
// Replace the pipe is replaced, the day-0 file keeps its size and
// bytes, and ours grows.
func TestRefuseAndReplace(t *testing.T) {
	srv := newServer(t)
	d := &testDeps{sock: srv.sock, data: t.TempDir(), mono: testMono{start: time.Now()}}
	day0 := filepath.Join(t.TempDir(), "day0.raw")
	pane := srv.newPane("paced", itoa(400), "20")
	srv.run("pipe-pane", "-t", pane, "-o", "cat >> "+day0)

	_, err := capture.Attach(context.Background(), d.attachDeps(), d.attachOpts("a-ml", pane))
	if err == nil {
		t.Fatal("attach over day-0 pipe succeeded, want ErrPipeExists")
	}
	if !isPipeExists(err) {
		t.Fatalf("attach error = %v, want ErrPipeExists", err)
	}
	waitFileGrows(t, day0, time.Minute)
	before, err := os.ReadFile(day0)
	if err != nil {
		t.Fatal(err)
	}
	opts := d.attachOpts("a-ml", pane)
	opts.Replace = true
	res, err := capture.Attach(context.Background(), d.attachDeps(), opts)
	if err != nil {
		t.Fatalf("replace attach: %v", err)
	}
	frozenSize := int64(len(before))
	frozenHash := fileHash(t, day0)
	deadline := time.Now().Add(time.Minute)
	for {
		if st := dbSession(t, d.data, res.Session); st.chunkSum > 0 {
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatal("our pipe committed nothing after replace")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if fi, err := os.Stat(day0); err != nil || fi.Size() != frozenSize {
		t.Fatalf("day-0 size changed after replace")
	}
	if got := fileHash(t, day0); got != frozenHash {
		t.Fatal("day-0 bytes changed after replace")
	}
}

// TestConcurrentAttachesOnePipe races eight attaches of the same pane
// and expects exactly one live row and one open pipe.
func TestConcurrentAttachesOnePipe(t *testing.T) {
	srv := newServer(t)
	d := &testDeps{sock: srv.sock, data: t.TempDir(), mono: testMono{start: time.Now()}}
	gate := filepath.Join(t.TempDir(), "gate")
	pane := srv.newPane("gated", gate, itoa(50))
	var wg sync.WaitGroup
	var mu sync.Mutex
	var results []capture.AttachResult
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(agent string) {
			defer wg.Done()
			res, err := capture.Attach(context.Background(), d.attachDeps(), d.attachOpts(agent, pane))
			if err != nil {
				return
			}
			mu.Lock()
			results = append(results, res)
			mu.Unlock()
		}(agentName(i))
	}
	wg.Wait()
	if len(results) == 0 {
		t.Fatal("all eight attaches refused")
	}
	if err := os.WriteFile(gate, []byte("go"), 0o600); err != nil {
		t.Fatal(err)
	}
	live := 0
	for _, res := range results {
		if st := dbSession(t, d.data, res.Session); st.found && st.state == "ATTACHED" {
			live++
		}
	}
	if live == 0 {
		t.Fatal("no live row survived the race")
	}
	n, err := countLive(t, d)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("live rows = %d, want 1", n)
	}
	if got := srv.panePipe(pane); got != "1" {
		t.Fatalf("pane_pipe = %s, want 1", got)
	}
}

// agentName assigns each racer of the concurrent test a distinct agent.
func agentName(i int) string {
	return "agent-" + string(rune('a'+i))
}

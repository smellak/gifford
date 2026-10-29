//go:build tmux

package capturetest

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/smellak/gifford/internal/capture"
)

// openGate creates a gate file.
func openGate(t *testing.T, gate string) {
	t.Helper()
	if err := os.WriteFile(gate, []byte("go"), 0o600); err != nil {
		t.Fatal(err)
	}
}

// readMarker returns the ingest pid and nonce from ingest.lock.
func readMarker(t *testing.T, data, agent, session string) (string, string) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(data, "captures", agent, session, "ingest.lock"))
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Fields(string(raw))
	if len(parts) != 3 || parts[0] != "ingest" {
		t.Fatalf("bad marker %q", string(raw))
	}
	return parts[1], parts[2]
}

// killPid SIGKILLs one process.
func killPid(t *testing.T, pid string) {
	t.Helper()
	if err := exec.Command("kill", "-9", pid).Run(); err != nil {
		t.Fatal(err)
	}
}

// waitChunks polls until one session committed n chunk events.
func waitChunks(t *testing.T, data, session string, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if got := countEvents(t, data, session, "capture.chunk"); got >= n {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("chunks = %d, want %d", countEvents(t, data, session, "capture.chunk"), n)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// waitStop polls until the session row stops with the reason.
func waitStop(t *testing.T, d *testDeps, session, reason string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		st := dbSession(t, d.data, session)
		if st.found && st.state == "STOPPED" {
			if got := stoppedReason(t, d.data, session); got != reason {
				t.Fatalf("stopped reason = %q, want %q", got, reason)
			}
			return
		}
		superviseOnce(t, d)
		if !time.Now().Before(deadline) {
			t.Fatalf("session %s not stopped: %+v", session, st)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// countState counts capture rows in one state.
func countState(t *testing.T, d *testDeps, state string) int {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(d.data, "gifford.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM capture_sessions WHERE state = ?`, state).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// TestForeignPipeLastBoundary opens a foreign `cat >>` pipe between
// the claim facts and the open: the attach is refused and the foreign
// file keeps growing.
func TestForeignPipeLastBoundary(t *testing.T) {
	srv := newServer(t)
	d := &testDeps{sock: srv.sock, data: t.TempDir(), mono: testMono{start: time.Now()}}
	foreign := filepath.Join(t.TempDir(), "foreign.raw")
	pane := srv.newPane("paced", itoa(400), "20")
	hook := func() {
		srv.run("pipe-pane", "-t", pane, "-o", "cat >> "+foreign)
	}
	capture.TestHookBeforeOpen = func(ctx context.Context) { hook() }
	defer func() { capture.TestHookBeforeOpen = nil }()

	_, err := capture.Attach(context.Background(), d.attachDeps(), d.attachOpts("a-ml", pane))
	if !isPipeExists(err) {
		t.Fatalf("attach error = %v, want ErrPipeExists", err)
	}
	if n := countSessions(t, d, "a-ml"); n != 0 {
		t.Fatalf("sessions = %d, want 0 after refusal", n)
	}
	waitFileGrows(t, foreign, time.Minute)
	first, err := os.Stat(foreign)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	second, err := os.Stat(foreign)
	if err != nil {
		t.Fatal(err)
	}
	if second.Size() <= first.Size() {
		t.Fatal("foreign file stopped growing after refusal")
	}
}

// restartServer kills the server on sock and recreates session test
// with a producing %1, returning the new pane id.
func restartServer(t *testing.T, sock, oldPane, cmd string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	run := func(args ...string) string {
		t.Helper()
		cmd := exec.CommandContext(ctx, "tmux", append([]string{"-S", sock}, args...)...)
		cmd.Env = withoutTMUX()
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("tmux %s: %v", strings.Join(args, " "), err)
		}
		return strings.TrimSpace(string(out))
	}
	run("kill-server")
	// kill-server returns before the socket is gone: wait for the
	// server to be unreachable before recreating it.
	deadline := time.Now().Add(30 * time.Second)
	for {
		cmd := exec.CommandContext(ctx, "tmux", "-S", sock, "ls")
		cmd.Env = withoutTMUX()
		if err := cmd.Run(); err != nil {
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatal("old server never released the socket")
		}
		time.Sleep(50 * time.Millisecond)
	}
	run("new-session", "-d", "-s", "test", "-x", "200", "-y", "50")
	newPane := run("new-window", "-d", "-P", "-F", "#{pane_id}", "-t", "test:", cmd)
	if newPane != oldPane {
		t.Fatalf("new pane = %s, want reuse of %s", newPane, oldPane)
	}
	return newPane
}

// TestServerRestartReusedPane follows CAP-S12: a hook restarts the
// server between the facts and the open and the new pane reuses the
// %N. In normal attach, in replace attach and in a supervisor
// reattach, the new pane never gets Gifford's pipe; a foreign pipe
// there is preserved and no event is bound to it.
func TestServerRestartReusedPane(t *testing.T) {
	for _, mode := range []string{"normal", "replace", "reattach"} {
		t.Run(mode, func(t *testing.T) {
			srv := newServer(t)
			d := &testDeps{sock: srv.sock, data: t.TempDir(), mono: testMono{start: time.Now()}}
			foreign := filepath.Join(t.TempDir(), "foreign.raw")
			pane := srv.newPane("paced", itoa(600), "50")
			var session string
			if mode == "reattach" {
				res, err := capture.Attach(context.Background(), d.attachDeps(), d.attachOpts("a-ml", pane))
				if err != nil {
					t.Fatal(err)
				}
				session = res.Session
				waitChunks(t, d.data, session, 1, time.Minute)
			} else {
				capture.TestHookBeforeOpen = func(context.Context) {
					restartServer(t, srv.sock, pane, self(t)+" helper paced 600 50")
					srv.run("pipe-pane", "-t", pane, "-o", "cat >> "+foreign)
				}
				defer func() { capture.TestHookBeforeOpen = nil }()
				opts := d.attachOpts("a-ml", pane)
				opts.Replace = mode == "replace"
				_, err := capture.Attach(context.Background(), d.attachDeps(), opts)
				if err == nil {
					t.Fatal("attach over a replaced server succeeded")
				}
			}
			if mode == "reattach" {
				restartServer(t, srv.sock, pane, self(t)+" helper paced 600 50")
				srv.run("pipe-pane", "-t", pane, "-o", "cat >> "+foreign)
				before := dbSession(t, d.data, session).chunkSum
				superviseOnce(t, d)
				if n := countSessions(t, d, "a-ml"); n != 1 {
					t.Fatalf("sessions = %d, want 1", n)
				}
				if snaps := dbSnapshots(t, d.data, "a-ml", session); len(snaps) != 1 {
					t.Fatalf("snapshots = %d, want 1 (no reattach)", len(snaps))
				}
				if got := dbSession(t, d.data, session).chunkSum; got != before {
					t.Fatalf("chunk bytes moved on the impostor pane: %d vs %d", got, before)
				}
			} else if n := countSessions(t, d, "a-ml"); n != 0 {
				t.Fatalf("sessions = %d, want 0 after refusal", n)
			}
			// The foreign pipe survived on the new pane and keeps
			// growing; Gifford bound no event to it.
			waitFileGrows(t, foreign, time.Minute)
			if got := srv.panePipe(pane); got != "1" {
				t.Fatalf("pane_pipe = %s, want 1 (foreign pipe preserved)", got)
			}
			before, err := os.Stat(foreign)
			if err != nil {
				t.Fatal(err)
			}
			time.Sleep(500 * time.Millisecond)
			after, err := os.Stat(foreign)
			if err != nil {
				t.Fatal(err)
			}
			if after.Size() <= before.Size() {
				t.Fatal("foreign file stopped growing")
			}
		})
	}
}

// TestStallDetachesPipe blocks the writer with the hook: after the
// overflow #{pane_pipe} becomes 0 within 1 s while the helper's
// counter keeps going, and no event refers to unflushed bytes.
func TestStallDetachesPipe(t *testing.T) {
	srv := newServer(t)
	d := &testDeps{sock: srv.sock, data: t.TempDir(), mono: testMono{start: time.Now()}}
	gate := filepath.Join(t.TempDir(), "gate")
	pane := srv.newPane("stallfeed", gate, itoa(20<<20), "1000000")
	release := make(chan struct{})
	block := func() { <-release }
	capture.TestHookBeforeCommit.Store(&block)
	defer capture.TestHookBeforeCommit.Store(nil)
	defer close(release)
	res, err := capture.Attach(context.Background(), d.attachDeps(), d.attachOpts("a-ml", pane))
	if err != nil {
		t.Fatal(err)
	}
	openGate(t, gate)
	srv.waitPipe(pane, "0", 2*time.Minute)
	// The counter keeps going after the detach.
	first, _ := srv.try("capture-pane", "-p", "-t", pane)
	time.Sleep(2 * time.Second)
	second, _ := srv.try("capture-pane", "-p", "-t", pane)
	if len(second) <= len(first) {
		t.Fatal("helper stopped after the detach")
	}
	// Every committed chunk resolves to persisted bytes: nothing
	// refers to the unflushed remainder.
	for _, ev := range dbEvents(t, d.data, res.Session) {
		if ev.typ != "capture.chunk" {
			continue
		}
		rawSlice(t, d.data, "a-ml", res.Session, chunkPayload(t, ev.payload))
	}
}

// TestStaleReadinessIdlePane follows CAP-S15: an idle helper pane and
// an ingest that exits right after writing its marker. Attach does
// not report ready.
//
// Attach holds ingest.lock exclusive until just before the readiness
// loop. The test kills the fresh ingest as soon as its process
// appears (found by /proc scan, SIGKILL): the marker it may have
// written is stale — its pid is verified dead — so the readiness
// loop never confirms and Attach reports unconfirmed.
func TestStaleReadinessIdlePane(t *testing.T) {
	srv := newServer(t)
	d := &testDeps{sock: srv.sock, data: t.TempDir(), mono: testMono{start: time.Now()}}
	pane := srv.newPane("idle")
	deps := d.attachDeps()
	deps.ReadyPoll = 5 * time.Second
	deps.ReadyTimeout = 20 * time.Second
	type result struct {
		res capture.AttachResult
		err error
	}
	done := make(chan result, 1)
	go func() {
		res, err := capture.Attach(context.Background(), deps, d.attachOpts("a-ml", pane))
		done <- result{res, err}
	}()
	pid := waitIngestProc(t, 30*time.Second)
	killProc(t, pid)
	res := <-done
	if res.err == nil {
		t.Fatalf("attach reported ready with a dead ingest: %+v", res.res)
	}
	// Committed but unconfirmed: the row stays ATTACHED with no
	// chunks and no marker behind it.
	if n := countState(t, d, "ATTACHED"); n != 1 {
		t.Fatalf("attached rows = %d, want 1 committed row", n)
	}
	sessions := sessionsOf(t, d)
	if len(sessions) != 1 {
		t.Fatalf("sessions = %v", sessions)
	}
	if n := countEvents(t, d.data, sessions[0], "capture.chunk"); n != 0 {
		t.Fatalf("chunks = %d, want 0", n)
	}
	raw, err := os.ReadFile(filepath.Join(d.data, "captures", "a-ml", sessions[0], "ingest.lock"))
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Fields(string(raw))
	if len(parts) != 3 || parts[0] != "ingest" {
		t.Fatalf("no marker behind the unconfirmed attach: %q", string(raw))
	}
	// The marker is stale: its pid is dead (reaped or zombie), so
	// no probe could have observed a live holder.
	var mpid int
	if _, err := fmt.Sscanf(parts[1], "%d", &mpid); err != nil {
		t.Fatal(err)
	}
	if !procDead(mpid) {
		t.Fatalf("marker pid %d still running", mpid)
	}
}

// waitIngestProc polls /proc for this suite's ingest child: any
// process whose command line re-executes this binary in ingest mode.
func waitIngestProc(t *testing.T, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if pid := findIngestProc(); pid > 0 {
			return pid
		}
		if !time.Now().Before(deadline) {
			t.Fatal("ingest process never appeared")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// findIngestProc scans /proc for a `capture ingest` re-execution.
func findIngestProc() int {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return -1
	}
	self := os.Getpid()
	for _, e := range entries {
		var pid int
		if _, err := fmt.Sscanf(e.Name(), "%d", &pid); err != nil {
			continue
		}
		if pid == self || pid <= 1 {
			continue
		}
		raw, err := os.ReadFile(filepath.Join("/proc", e.Name(), "cmdline"))
		if err != nil {
			continue
		}
		argv := strings.Split(string(raw), "\x00")
		for i := 0; i+1 < len(argv); i++ {
			if argv[i] == "capture" && argv[i+1] == "ingest" {
				return pid
			}
		}
	}
	return 0
}

// procDead reports a reaped or zombie process.
func procDead(pid int) bool {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return true
	}
	if err := proc.Signal(syscall.Signal(0)); err != nil {
		return true
	}
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return true
	}
	fields := strings.Fields(string(raw))
	return len(fields) > 2 && fields[2] == "Z"
}

// killProc SIGKILLs a process and verifies its death.
func killProc(t *testing.T, pid int) {
	t.Helper()
	proc, err := os.FindProcess(pid)
	if err != nil {
		t.Fatal(err)
	}
	if err := proc.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if err := proc.Signal(syscall.Signal(0)); err != nil {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("pid %d survived SIGKILL", pid)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestIngestDeathReattach kills its own ingest and expects
// gap_detected{ingest_died}, a new initial screen and reattached,
// while the helper's counter never stops.
func TestIngestDeathReattach(t *testing.T) {
	srv := newServer(t)
	d := &testDeps{sock: srv.sock, data: t.TempDir(), mono: testMono{start: time.Now()}}
	pane := srv.newPane("paced", itoa(1200), "20")
	res, err := capture.Attach(context.Background(), d.attachDeps(), d.attachOpts("a-ml", pane))
	if err != nil {
		t.Fatal(err)
	}
	waitChunks(t, d.data, res.Session, 1, time.Minute)
	before := dbSession(t, d.data, res.Session).chunkSum
	if before == 0 {
		t.Fatal("no bytes committed before the kill")
	}
	pid, _ := readMarker(t, d.data, "a-ml", res.Session)
	killPid(t, pid)
	srv.waitPipe(pane, "0", 30*time.Second)
	superviseOnce(t, d)
	if causes := gapCauses(t, d.data, res.Session); !hasCause(causes, "ingest_died") {
		t.Fatalf("gap causes = %v, want ingest_died", causes)
	}
	waitSnapshots(t, d.data, "a-ml", res.Session, 2, time.Minute)
	if n := countEvents(t, d.data, res.Session, "capture.reattached"); n == 0 {
		t.Fatal("no reattached event after ingest death")
	}
	if r := dbSession(t, d.data, res.Session).reattach; r < 1 {
		t.Fatalf("reattach count = %d", r)
	}
	// The counter never stopped: committed bytes grow past the
	// pre-kill total through the new pipe.
	deadline := time.Now().Add(time.Minute)
	for {
		if got := dbSession(t, d.data, res.Session).chunkSum; got > before {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatal("no new bytes after reattach")
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// sessionsOf lists one agent's session ids.
func sessionsOf(t *testing.T, d *testDeps) []string {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(d.data, "gifford.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	rows, err := db.Query(`SELECT capture_session_id FROM capture_sessions WHERE agent_id = 'a-ml' ORDER BY capture_session_id`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// hasCause reports one gap cause in the list.
func hasCause(causes []string, want string) bool {
	for _, c := range causes {
		if c == want {
			return true
		}
	}
	return false
}

// TestAttachSuperviseRace runs a supervisor every 50 ms while Attach
// runs: one pipe, no gap event and no ERROR.
func TestAttachSuperviseRace(t *testing.T) {
	srv := newServer(t)
	d := &testDeps{sock: srv.sock, data: t.TempDir(), mono: testMono{start: time.Now()}}
	gate := filepath.Join(t.TempDir(), "gate")
	pane := srv.newPane("gated", gate, itoa(100))
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(50 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				_ = capture.Supervise(context.Background(), d.superviseDeps(),
					capture.SuperviseOptions{Data: d.data, Once: true})
			}
		}
	}()
	res, err := capture.Attach(context.Background(), d.attachDeps(), d.attachOpts("a-ml", pane))
	if err != nil {
		close(stop)
		wg.Wait()
		t.Fatal(err)
	}
	openGate(t, gate)
	var total int64
	for i := 0; i < 100; i++ {
		total += int64(len(rawLine(i)))
	}
	waitRaw(t, d, res.Session, total, time.Minute)
	close(stop)
	wg.Wait()
	if got := srv.panePipe(pane); got != "1" {
		t.Fatalf("pane_pipe = %s, want 1", got)
	}
	if causes := gapCauses(t, d.data, res.Session); len(causes) != 0 {
		t.Fatalf("gap events = %v during a clean race", causes)
	}
	if n := countState(t, d, "ERROR"); n != 0 {
		t.Fatalf("ERROR rows = %d", n)
	}
	raw, err := os.ReadFile(filepath.Join(d.data, "captures", "a-ml", res.Session, "ingest.log"))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.Contains(line, " error:") {
			t.Fatalf("ingest log holds an error: %q", line)
		}
	}
}

// TestPaneRecreated kills the pane and opens a new pane at the same
// slot: the supervisor reports pane_recreated and reattaches. A pane
// at another slot is not taken.
func TestPaneRecreated(t *testing.T) {
	srv := newServer(t)
	d := &testDeps{sock: srv.sock, data: t.TempDir(), mono: testMono{start: time.Now()}}
	pane := srv.newPane("paced", itoa(1200), "20")
	res, err := capture.Attach(context.Background(), d.attachDeps(), d.attachOpts("a-ml", pane))
	if err != nil {
		t.Fatal(err)
	}
	waitChunks(t, d.data, res.Session, 1, time.Minute)
	// Kill the pane: its window closes and the recorded %N is gone.
	// The lowest free window index is reused, so the next window
	// lands on the same slot with a new %N.
	srv.run("kill-pane", "-t", pane)
	fresh := srv.newPane("paced", itoa(1200), "20")
	if fresh == pane {
		t.Fatalf("new pane reused %s", pane)
	}
	deadline := time.Now().Add(2 * time.Minute)
	for {
		superviseOnce(t, d)
		if countEvents(t, d.data, res.Session, "capture.reattached") > 0 {
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatal("no reattach after recreation")
		}
		time.Sleep(200 * time.Millisecond)
	}
	if n := countEvents(t, d.data, res.Session, "capture.pane_recreated"); n != 1 {
		t.Fatalf("pane_recreated events = %d, want 1", n)
	}
	waitSnapshots(t, d.data, "a-ml", res.Session, 2, time.Minute)
	// A pane at another slot is not taken: no new session appears
	// and it never gets a pipe.
	other := srv.newPane("paced", itoa(100), "20")
	superviseOnce(t, d)
	if n := countSessions(t, d, "a-ml"); n != 1 {
		t.Fatalf("sessions = %d, want 1", n)
	}
	if got := srv.panePipe(other); got != "0" {
		t.Fatalf("other pane pipe = %s, want 0", got)
	}
}

// TestStopAndRestart stops a live ingest: STOPPED stays, a later
// Supervise never reattaches, and pane_pipe becomes 0 at the next
// output.
func TestStopAndRestart(t *testing.T) {
	srv := newServer(t)
	d := &testDeps{sock: srv.sock, data: t.TempDir(), mono: testMono{start: time.Now()}}
	gate1 := filepath.Join(t.TempDir(), "gate1")
	gate2 := filepath.Join(t.TempDir(), "gate2")
	pane := srv.newPane("gated", gate1, itoa(50), gate2, itoa(50))
	res, err := capture.Attach(context.Background(), d.attachDeps(), d.attachOpts("a-ml", pane))
	if err != nil {
		t.Fatal(err)
	}
	openGate(t, gate1)
	var first int64
	for i := 0; i < 50; i++ {
		first += int64(len(rawLine(i)))
	}
	waitRaw(t, d, res.Session, first, time.Minute)
	line, err := capture.Stop(context.Background(), d.stopDeps(), capture.StopOptions{Data: d.data, Agent: "a-ml"})
	if err != nil {
		t.Fatalf("stop: %v", err)
	}
	t.Logf("stop: %s", line)
	if st := dbSession(t, d.data, res.Session); !st.found || st.state != "STOPPED" {
		t.Fatalf("row = %+v, want STOPPED", st)
	}
	if got := stoppedReason(t, d.data, res.Session); got != "manual" {
		t.Fatalf("stopped reason = %q, want manual", got)
	}
	superviseOnce(t, d)
	if st := dbSession(t, d.data, res.Session); st.state != "STOPPED" {
		t.Fatalf("row = %+v after supervise", st)
	}
	if n := countEvents(t, d.data, res.Session, "capture.reattached"); n != 0 {
		t.Fatal("supervise reattached a stopped session")
	}
	openGate(t, gate2)
	srv.waitPipe(pane, "0", 30*time.Second)
	if got := dbSession(t, d.data, res.Session).chunkSum; got != first {
		t.Fatalf("bytes moved after stop: %d vs %d", got, first)
	}
}

// TestRotationAndBudgetSmall attaches with tiny limits: segments
// rotate and the run stops on quota.
func TestRotationAndBudgetSmall(t *testing.T) {
	srv := newServer(t)
	d := &testDeps{sock: srv.sock, data: t.TempDir(), mono: testMono{start: time.Now()}}
	// Paced input the writer can drain: a burst would overflow the
	// small queue into a stall instead of reaching quota.
	pane := srv.newPane("paced", itoa(20000), "1")
	opts := d.attachOpts("a-ml", pane)
	opts.Limits = capture.Limits{
		ChunkMaxBytes: 1024, FrameMaxBytes: 4096, SegmentMaxBytes: 8192,
		SegmentMaxAge: capture.Duration(time.Hour), ChunkMaxAge: capture.Duration(time.Second),
		FlushInterval: capture.Duration(10 * time.Millisecond), QueueMaxBytes: 65536,
		ResizeInterval: capture.Duration(time.Second), ReserveBlockBytes: 6144,
		SnapshotMaxBytes: 65536, LogMaxBytes: 4096,
		AgentQuotaBytes: 215040, NodeQuotaBytes: 280576,
	}
	res, err := capture.Attach(context.Background(), d.attachDeps(), opts)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Minute)
	for {
		superviseOnce(t, d)
		if st := dbSession(t, d.data, res.Session); st.found && st.state == "STOPPED" {
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatal("quota stop never arrived")
		}
		time.Sleep(200 * time.Millisecond)
	}
	if n := countEvents(t, d.data, res.Session, "capture.rotated"); n == 0 {
		t.Fatal("no rotation under tiny segments")
	}
	if got := stoppedReason(t, d.data, res.Session); got != "quota" {
		t.Fatalf("stopped reason = %q, want quota", got)
	}
}

// TestEscapedDataDir runs the flow end to end under a data path with
// a space, a quote, a hash, a percent and a semicolon, status
// included.
func TestEscapedDataDir(t *testing.T) {
	srv := newServer(t)
	data := filepath.Join(t.TempDir(), "a b'#%;x")
	if err := os.MkdirAll(data, 0o700); err != nil {
		t.Fatal(err)
	}
	d := &testDeps{sock: srv.sock, data: data, mono: testMono{start: time.Now()}}
	const n = 50
	gate := filepath.Join(t.TempDir(), "gate")
	pane := srv.newPane("gated", gate, itoa(n))
	res, err := capture.Attach(context.Background(), d.attachDeps(), d.attachOpts("a-ml", pane))
	if err != nil {
		t.Fatal(err)
	}
	openGate(t, gate)
	var total int64
	for i := 0; i < n; i++ {
		total += int64(len(rawLine(i)))
	}
	waitRaw(t, d, res.Session, total, time.Minute)
	line := findStatus(t, d, "a-ml")
	if line.Capture != res.Session || line.Bytes != total {
		t.Fatalf("status line = %+v", line)
	}
}

//go:build tmux

package capturetest

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/smellak/gifford/internal/capture"
	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/protocol"

	_ "modernc.org/sqlite"
)

// testDeps wires the capture entry points to one private server.
type testDeps struct {
	sock string
	data string
	mono testMono
}

// attachDeps builds AttachDeps over the test server and data dir.
func (d *testDeps) attachDeps() capture.AttachDeps {
	exe, err := os.Executable()
	if err != nil {
		panic(err)
	}
	return capture.AttachDeps{
		Runner:   capture.Dial(d.sock),
		Discover: func(context.Context) (string, error) { return d.sock, nil },
		Exe:      func() (string, error) { return exe, nil },
		PID:      os.Getpid(),
		Clock:    clock.System{},
		Mono:     d.mono,
		DataDir:  func() string { return d.data },
	}
}

// superviseDeps builds SuperviseDeps over the test server and data dir.
func (d *testDeps) superviseDeps() capture.SuperviseDeps {
	exe, err := os.Executable()
	if err != nil {
		panic(err)
	}
	return capture.SuperviseDeps{
		RunnerFor: capture.Dial,
		Exe:       func() (string, error) { return exe, nil },
		PID:       os.Getpid(),
		Clock:     clock.System{},
		Mono:      d.mono,
		DataDir:   func() string { return d.data },
	}
}

// statusDeps builds StatusDeps.
func (d *testDeps) statusDeps() capture.StatusDeps {
	return capture.StatusDeps{
		RunnerFor: capture.Dial,
		DataDir:   func() string { return d.data },
	}
}

// stopDeps builds StopDeps with a bounded poll.
func (d *testDeps) stopDeps() capture.StopDeps {
	return capture.StopDeps{
		PID:     os.Getpid(),
		Clock:   clock.System{},
		Mono:    d.mono,
		DataDir: func() string { return d.data },
		Timeout: 20 * time.Second,
		Poll:    100 * time.Millisecond,
	}
}

// attachOpts builds AttachOptions for one agent and pane.
func (d *testDeps) attachOpts(agent, pane string) capture.AttachOptions {
	return capture.AttachOptions{
		Agent:   agent,
		Harness: protocol.HarnessMuseCode,
		Target:  pane,
		Socket:  d.sock,
		Data:    d.data,
	}
}

// rowEvent is one committed event in sequence order.
type rowEvent struct {
	seq     int64
	typ     string
	payload string
}

// dbEvents reads one session's stream through its own sqlite handle.
func dbEvents(t *testing.T, data, session string) []rowEvent {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(data, "gifford.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	rows, err := db.Query(`SELECT sequence, event_type, payload_json FROM events WHERE session_id = ? ORDER BY sequence`, session)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var out []rowEvent
	for rows.Next() {
		var ev rowEvent
		if err := rows.Scan(&ev.seq, &ev.typ, &ev.payload); err != nil {
			t.Fatal(err)
		}
		out = append(out, ev)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// rowState is the cached session row plus repair facts.
type rowState struct {
	found     bool
	state     string
	bytes     int64
	reattach  int64
	ref       string
	pane      string
	cols      int64
	rows      int64
	sessions  int64
	chunkSum  int64
	chunkLast int64
	rotated   int64
	stopped   bool
}

// dbSession reads the cached row and folds the stream facts the
// tests assert: chunk bytes, last chunk sequence, rotations, stop.
func dbSession(t *testing.T, data, session string) rowState {
	t.Helper()
	var st rowState
	db, err := sql.Open("sqlite", filepath.Join(data, "gifford.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var ref sql.NullString
	err = db.QueryRow(`SELECT state, bytes_captured, reattach_count, initial_screen_blob_ref, tmux_pane, pane_cols, pane_rows FROM capture_sessions WHERE capture_session_id = ?`, session).Scan(&st.state, &st.bytes, &st.reattach, &ref, &st.pane, &st.cols, &st.rows)
	if err == sql.ErrNoRows {
		return st
	}
	if err != nil {
		t.Fatal(err)
	}
	st.found = true
	if ref.Valid {
		st.ref = ref.String
	}
	for _, ev := range dbEvents(t, data, session) {
		switch ev.typ {
		case "capture.chunk":
			var chunk struct {
				ByteCount int64 `json:"byte_count"`
			}
			if err := json.Unmarshal([]byte(ev.payload), &chunk); err != nil {
				t.Fatal(err)
			}
			st.chunkSum += chunk.ByteCount
			st.chunkLast = ev.seq
			st.sessions++
		case "capture.rotated":
			st.rotated++
		case "capture.stopped":
			st.stopped = true
		}
	}
	return st
}

// dbSnapshots lists the committed snapshot names of one session.
func dbSnapshots(t *testing.T, data, agent, session string) []string {
	t.Helper()
	dir := filepath.Join(data, "captures", agent, session)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		name := e.Name()
		if len(name) > 5 && name[:8] == "initial-" {
			out = append(out, name)
		}
	}
	return out
}

// findStatus returns one agent's status line.
func findStatus(t *testing.T, d *testDeps, agent string) capture.StatusLine {
	t.Helper()
	lines, err := capture.Status(context.Background(), d.statusDeps(), capture.StatusOptions{Data: d.data, Agent: agent})
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range lines {
		if line.Agent == agent {
			return line
		}
	}
	t.Fatalf("no status line for agent %s", agent)
	return capture.StatusLine{}
}

//go:build tmux

package capturetest

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/smellak/gifford/internal/capture"
	"github.com/smellak/gifford/internal/protocol"
)

// itoa formats a test integer.
func itoa(n int) string {
	return strconv.Itoa(n)
}

// waitRaw polls the committed chunk bytes until they reach the exact
// raw total the helper emitted.
func waitRaw(t *testing.T, d *testDeps, session string, want int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if got := dbSession(t, d.data, session).chunkSum; got >= want {
			if got != want {
				t.Fatalf("committed bytes = %d, want exactly %d", got, want)
			}
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("committed bytes = %d, want %d", dbSession(t, d.data, session).chunkSum, want)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// drainTerminal runs supervise passes until the session row stops.
func drainTerminal(t *testing.T, d *testDeps, session string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last error
	for {
		last = capture.Supervise(context.Background(), d.superviseDeps(),
			capture.SuperviseOptions{Data: d.data, Once: true})
		if st := dbSession(t, d.data, session); st.found && st.state == "STOPPED" {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("session %s not stopped after supervise: %v", session, last)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// chunkPayload decodes one capture.chunk payload.
func chunkPayload(t *testing.T, payload string) protocol.CaptureChunk {
	t.Helper()
	var chunk protocol.CaptureChunk
	if err := json.Unmarshal([]byte(payload), &chunk); err != nil {
		t.Fatal(err)
	}
	return chunk
}

// rawSlice resolves one chunk to its raw bytes via its blob ref
// (blob://capture/<session>/<segment>/<offset>).
func rawSlice(t *testing.T, data, agent, session string, chunk protocol.CaptureChunk) []byte {
	t.Helper()
	parts := strings.Split(string(chunk.RawBlobRef), "/")
	if len(parts) != 6 || parts[0] != "blob:" || parts[3] != session {
		t.Fatalf("bad blob ref %q", chunk.RawBlobRef)
	}
	offset, err := strconv.Atoi(parts[5])
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(data, "captures", agent, session, parts[4]+".raw"))
	if err != nil {
		t.Fatal(err)
	}
	end := int64(offset) + chunk.ByteCount
	if int64(len(raw)) < end {
		t.Fatalf("segment short: ref %q", chunk.RawBlobRef)
	}
	return raw[offset:end]
}

// decodeRun decodes an ANSI+VTE run to its visible text with the same
// vt-normalizer the ingest path applies before diffing: printable
// runes and newlines kept, escape sequences and controls dropped.
func decodeRun(t *testing.T, raw []byte) string {
	t.Helper()
	var out strings.Builder
	i := 0
	for i < len(raw) {
		c := raw[i]
		switch {
		case c == 0x1b:
			i++
			if i < len(raw) && raw[i] == '[' {
				i++
				for i < len(raw) && (raw[i] < '@' || raw[i] > '~') {
					i++
				}
				i++
			} else if i < len(raw) {
				i++
			}
		case c == '\n' || (c >= 0x20 && c < 0x7f):
			out.WriteByte(c)
			i++
		default:
			i++
		}
	}
	return out.String()
}

// countLines counts newline-terminated lines in s.
func countLines(s string) int {
	return strings.Count(s, "\n")
}

// visibleLine strips the ANSI color wrapper of one counter line.
func visibleLine(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "\x1b[32m", ""), "\x1b[0m", "")
}

// snapshotLines decodes one committed snapshot to its non-empty
// visible lines.
func snapshotLines(t *testing.T, data, agent, session, name string) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(data, "captures", agent, session, name))
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, line := range strings.Split(decodeRun(t, raw), "\n") {
		if line != "" {
			out = append(out, line)
		}
	}
	return out
}

// isPipeExists reports ErrPipeExists without importing the sentinel.
func isPipeExists(err error) bool {
	return errors.Is(err, capture.ErrPipeExists)
}

// countLive counts live capture rows across agents.
func countLive(t *testing.T, d *testDeps) (int, error) {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(d.data, "gifford.db"))
	if err != nil {
		return 0, err
	}
	defer func() { _ = db.Close() }()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM capture_sessions WHERE state = 'ATTACHED'`).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// segRow is one segment row.
type segRow struct {
	id      string
	from    int64
	to      int64
	bytes   int64
	storage string
}

// dbSegments reads one session's segment rows in id order.
func dbSegments(t *testing.T, data, session string) []segRow {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(data, "gifford.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	rows, err := db.Query(`SELECT segment_id, seq_from, seq_to, bytes_raw, storage_path FROM capture_segments WHERE capture_session_id = ? ORDER BY segment_id`, session)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var out []segRow
	for rows.Next() {
		var seg segRow
		if err := rows.Scan(&seg.id, &seg.from, &seg.to, &seg.bytes, &seg.storage); err != nil {
			t.Fatal(err)
		}
		out = append(out, seg)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// countEvents counts one session's events of one type.
func countEvents(t *testing.T, data, session, typ string) int {
	t.Helper()
	n := 0
	for _, ev := range dbEvents(t, data, session) {
		if ev.typ == typ {
			n++
		}
	}
	return n
}

// gapCauses decodes every capture.gap_detected cause of one session.
func gapCauses(t *testing.T, data, session string) []string {
	t.Helper()
	var out []string
	for _, ev := range dbEvents(t, data, session) {
		if ev.typ != "capture.gap_detected" {
			continue
		}
		var gap protocol.CaptureGapDetected
		if err := json.Unmarshal([]byte(ev.payload), &gap); err != nil {
			t.Fatal(err)
		}
		out = append(out, string(gap.Cause))
	}
	return out
}

// stoppedReason decodes the capture.stopped reason of one session.
func stoppedReason(t *testing.T, data, session string) string {
	t.Helper()
	for _, ev := range dbEvents(t, data, session) {
		if ev.typ != "capture.stopped" {
			continue
		}
		var stopped struct {
			Reason string `json:"reason"`
		}
		if err := json.Unmarshal([]byte(ev.payload), &stopped); err != nil {
			t.Fatal(err)
		}
		return stopped.Reason
	}
	return ""
}

// countSessions counts all capture rows for one agent.
func countSessions(t *testing.T, d *testDeps, agent string) int {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(d.data, "gifford.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM capture_sessions WHERE agent_id = ?`, agent).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// waitSnapshots polls until one session has n snapshots.
func waitSnapshots(t *testing.T, data, agent, session string, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if got := len(dbSnapshots(t, data, agent, session)); got >= n {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("snapshots = %d, want %d", len(dbSnapshots(t, data, agent, session)), n)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// fileHash returns the sha256 of a file.
func fileHash(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// superviseOnce runs one supervisor pass over the data dir.
func superviseOnce(t *testing.T, d *testDeps) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := capture.Supervise(ctx, d.superviseDeps(), capture.SuperviseOptions{Data: d.data, Once: true}); err != nil {
		t.Fatal(err)
	}
}

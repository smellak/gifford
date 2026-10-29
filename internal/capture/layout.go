package capture

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/protocol"
)

var (
	captureSessionPattern = regexp.MustCompile(`^cap-[0-9A-HJKMNP-TV-Z]{26}$`)
	segmentIDPattern      = regexp.MustCompile(`^seg-[0-9A-HJKMNP-TV-Z]{26}$`)
	noncePattern          = regexp.MustCompile(`^[0-9a-f]{32}$`)
)

// ulidAlphabet is Crockford base32 without I, L, O, U.
const ulidAlphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// newULID returns a timestamped random id: milliseconds from the clock,
// then 80 bits of crypto/rand, Crockford base32.
func newULID(clk clock.Clock) (string, error) {
	var entropy [10]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", fmt.Errorf("ulid: %w", err)
	}
	return encodeULID(uint64(clk.Now().UnixMilli()), entropy), nil
}

// encodeULID renders 48 timestamp bits and 80 entropy bits as 26
// Crockford characters: the 130-bit string holds two leading zero bits
// and then the 16 bytes MSB-first, five bits per character.
func encodeULID(ms uint64, entropy [10]byte) string {
	var buf [16]byte
	buf[0] = byte((ms >> 40) & 0xff)
	buf[1] = byte((ms >> 32) & 0xff)
	buf[2] = byte((ms >> 24) & 0xff)
	buf[3] = byte((ms >> 16) & 0xff)
	buf[4] = byte((ms >> 8) & 0xff)
	buf[5] = byte(ms & 0xff)
	copy(buf[6:], entropy[:])
	var out [26]byte
	for i := range 26 {
		var v byte
		for k := range 5 {
			if d := i*5 + k - 2; d >= 0 {
				if (buf[d/8]>>(7-(d%8)))&1 == 1 {
					v |= 1 << (4 - k)
				}
			}
		}
		out[i] = ulidAlphabet[v]
	}
	return string(out[:])
}

// newCaptureSessionID returns a cap- id.
func newCaptureSessionID(clk clock.Clock) (string, error) {
	u, err := newULID(clk)
	if err != nil {
		return "", err
	}
	return "cap-" + u, nil
}

// newSegmentID returns a seg- id.
func newSegmentID(clk clock.Clock) (string, error) {
	u, err := newULID(clk)
	if err != nil {
		return "", err
	}
	return "seg-" + u, nil
}

// validateCaptureSessionID checks the id before any path is built.
func validateCaptureSessionID(id string) error {
	if !captureSessionPattern.MatchString(id) {
		return fmt.Errorf("%w: bad capture session id", ErrUnsafePath)
	}
	return nil
}

// validateSegmentID checks the id before any path is built.
func validateSegmentID(id string) error {
	if !segmentIDPattern.MatchString(id) {
		return fmt.Errorf("%w: bad segment id", ErrUnsafePath)
	}
	return nil
}

// newNonce returns 16 crypto/rand bytes in lowercase hex.
func newNonce() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("nonce: %w", err)
	}
	const hex = "0123456789abcdef"
	var sb strings.Builder
	sb.Grow(32)
	for _, c := range b {
		sb.WriteByte(hex[c>>4])
		sb.WriteByte(hex[c&15])
	}
	return sb.String(), nil
}

// validateNonce checks the 32 lowercase hex characters.
func validateNonce(nonce string) error {
	if !noncePattern.MatchString(nonce) {
		return fmt.Errorf("%w: bad attempt nonce", ErrUnsafePath)
	}
	return nil
}

// parseSequence parses a decimal sequence 0 to 2^63-1.
func parseSequence(s string) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("%w: empty sequence", ErrUsage)
	}
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return 0, fmt.Errorf("%w: bad sequence", ErrUsage)
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: bad sequence", ErrUsage)
	}
	return n, nil
}

// openCaptureRoot opens the capture root with the §4.3 checks. When
// create is set, mutating entry points may create a missing root,
// fsyncing the data directory; readers never create anything.
func openCaptureRoot(data string, create bool) (*os.Root, error) {
	rootPath := data + "/captures"
	fi, err := os.Lstat(rootPath)
	if err != nil {
		if !create || !os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: capture root: %w", ErrUnsafePath, err)
		}
		if err := os.Mkdir(rootPath, 0o700); err != nil {
			return nil, fmt.Errorf("%w: capture root: %w", ErrUnsafePath, err)
		}
		if err := fsyncDir(data); err != nil {
			return nil, err
		}
		fi, err = os.Lstat(rootPath)
		if err != nil {
			return nil, fmt.Errorf("%w: capture root: %w", ErrUnsafePath, err)
		}
	}
	if err := checkDir(fi); err != nil {
		return nil, fmt.Errorf("%w: capture root: %w", ErrUnsafePath, err)
	}
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return nil, fmt.Errorf("%w: capture root: %w", ErrUnsafePath, err)
	}
	dot, err := root.Open(".")
	if err != nil {
		_ = root.Close()
		return nil, fmt.Errorf("%w: capture root: %w", ErrUnsafePath, err)
	}
	dfi, err := dot.Stat()
	_ = dot.Close()
	if err != nil || !os.SameFile(fi, dfi) {
		_ = root.Close()
		return nil, fmt.Errorf("%w: capture root swapped", ErrUnsafePath)
	}
	return root, nil
}

// fsyncDir fsyncs a directory by path.
func fsyncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("%w: fsync dir: %w", ErrUnsafePath, err)
	}
	defer func() { _ = d.Close() }()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("%w: fsync dir: %w", ErrUnsafePath, err)
	}
	return nil
}

// checkOwner enforces ownership by the effective uid.
func checkOwner(fi os.FileInfo) error {
	euid := os.Geteuid()
	if euid < 0 {
		return fmt.Errorf("file owner unknown")
	}
	uid, _, ok := fileIdentity(fi)
	if !ok || int64(uid) != int64(euid) {
		return fmt.Errorf("foreign file owner")
	}
	return nil
}

// checkDir enforces the directory checks: a real directory, mode
// exactly 0o700, owned by the effective uid.
func checkDir(fi os.FileInfo) error {
	if !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("not a directory")
	}
	if fi.Mode().Perm() != 0o700 {
		return fmt.Errorf("bad directory mode %o", fi.Mode().Perm())
	}
	return checkOwner(fi)
}

// checkFile enforces the file checks: regular, mode exactly 0o600,
// link count 1, owned by the effective uid.
func checkFile(fi os.FileInfo) error {
	if !fi.Mode().IsRegular() {
		return fmt.Errorf("not a regular file")
	}
	if fi.Mode().Perm() != 0o600 {
		return fmt.Errorf("bad file mode %o", fi.Mode().Perm())
	}
	if err := checkOwner(fi); err != nil {
		return err
	}
	if _, single, ok := fileIdentity(fi); !ok || !single {
		return fmt.Errorf("file has more than one link")
	}
	return nil
}

// openChecked opens one path through the root with the full checks,
// comparing the opened file with its Lstat: a mismatch means the entry
// was swapped.
func openChecked(root *os.Root, name string, wantDir bool) (*os.File, error) {
	fail := func(err error) (*os.File, error) {
		return nil, fmt.Errorf("%w: %s: %w", ErrUnsafePath, name, err)
	}
	fi, err := root.Lstat(name)
	if err != nil {
		return fail(err)
	}
	if wantDir {
		if err := checkDir(fi); err != nil {
			return fail(err)
		}
	} else if err := checkFile(fi); err != nil {
		return fail(err)
	}
	f, err := root.Open(name)
	if err != nil {
		return fail(err)
	}
	gfi, err := f.Stat()
	if err != nil || !os.SameFile(fi, gfi) {
		_ = f.Close()
		return fail(fmt.Errorf("entry swapped"))
	}
	return f, nil
}

// SessionMeta is session.json, schema gifford.capture.session.v1,
// written once by attach and never rewritten.
type SessionMeta struct {
	Schema           string      `json:"schema"`
	CaptureSessionID string      `json:"capture_session_id"`
	AgentID          string      `json:"agent_id"`
	Harness          string      `json:"harness"`
	NodeID           string      `json:"node_id"`
	Tmux             SessionTmux `json:"tmux"`
	Limits           Limits      `json:"limits"`
}

// SessionTmux pins the server instance and the logical slot.
type SessionTmux struct {
	Socket          string `json:"socket"`
	ServerPID       int    `json:"server_pid"`
	ServerStartTime int64  `json:"server_start_time"`
	SessionID       string `json:"session_id"`
	SessionName     string `json:"session_name"`
	WindowIndex     int    `json:"window_index"`
	PaneIndex       int    `json:"pane_index"`
}

// controlFile is a strict control document: its schema name, its exact
// top-level fields, its nested objects' fields, and its value checks.
type controlFile interface {
	controlSchema() string
	controlFields() []string
	controlNested() map[string][]string
	validate() error
}

// sessionSchema is the session.json schema name.
const sessionSchema = "gifford.capture.session.v1"

// schemaKey is the JSON schema field every control document carries.
const schemaKey = "schema"

func (m *SessionMeta) controlSchema() string { return sessionSchema }

func (m *SessionMeta) controlFields() []string {
	return []string{schemaKey, "capture_session_id", "agent_id", "harness", "node_id", "tmux", "limits"}
}

func (m *SessionMeta) controlNested() map[string][]string {
	return map[string][]string{
		"tmux": {"socket", "server_pid", "server_start_time", "session_id", "session_name", "window_index", "pane_index"},
		"limits": {
			"chunk_max_bytes", "frame_max_bytes", "segment_max_bytes", "segment_max_age", "chunk_max_age",
			"flush_interval", "queue_max_bytes", "resize_interval", "reserve_block_bytes", "snapshot_max_bytes",
			"log_max_bytes", "agent_quota_bytes", "node_quota_bytes",
		},
	}
}

// validate checks §4.5: ids, harness, node, tmux facts and limits.
func (m *SessionMeta) validate() error {
	if err := validateCaptureSessionID(m.CaptureSessionID); err != nil {
		return err
	}
	if !protocol.AgentID(m.AgentID).Valid() {
		return fmt.Errorf("%w: bad agent id", ErrUnsafePath)
	}
	if !protocol.Harness(m.Harness).Valid() {
		return fmt.Errorf("%w: bad harness", ErrUnsafePath)
	}
	if !nodeIDPattern.MatchString(m.NodeID) {
		return fmt.Errorf("%w: bad node id", ErrUnsafePath)
	}
	t := m.Tmux
	if !strings.HasPrefix(t.Socket, "/") || len(t.Socket) > 128 || strings.IndexFunc(t.Socket, unicode.IsControl) >= 0 {
		return fmt.Errorf("%w: bad tmux socket", ErrUnsafePath)
	}
	if t.ServerPID <= 1 || t.ServerStartTime <= 0 {
		return fmt.Errorf("%w: bad tmux server identity", ErrUnsafePath)
	}
	if !tmuxSessionPattern.MatchString(t.SessionID) {
		return fmt.Errorf("%w: bad tmux session id", ErrUnsafePath)
	}
	if len(t.SessionName) < 1 || len(t.SessionName) > 128 || strings.IndexFunc(t.SessionName, unicode.IsControl) >= 0 {
		return fmt.Errorf("%w: bad tmux session name", ErrUnsafePath)
	}
	if t.WindowIndex < 0 || t.WindowIndex > 999999 || t.PaneIndex < 0 || t.PaneIndex > 999999 {
		return fmt.Errorf("%w: bad pane slot", ErrUnsafePath)
	}
	if err := m.Limits.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrUnsafePath, err)
	}
	return nil
}

// quotaLedger is quota.json: the session's budget ledger.
type quotaLedger struct {
	Schema        string `json:"schema"`
	ReservedBytes int64  `json:"reserved_bytes"`
}

func (q *quotaLedger) controlSchema() string { return "gifford.capture.quota.v1" }

func (q *quotaLedger) controlFields() []string { return []string{schemaKey, "reserved_bytes"} }

func (q *quotaLedger) controlNested() map[string][]string { return nil }

func (q *quotaLedger) validate() error {
	if q.ReservedBytes < 0 {
		return fmt.Errorf("%w: negative reservation", ErrUnsafePath)
	}
	return nil
}

// stopRequest is stop.request: the durable stop intent.
type stopRequest struct {
	Schema      string `json:"schema"`
	Reason      string `json:"reason"`
	Detail      string `json:"detail"`
	RequestedAt string `json:"requested_at"`
}

func (s *stopRequest) controlSchema() string { return "gifford.capture.stop.v1" }

func (s *stopRequest) controlFields() []string {
	return []string{schemaKey, "reason", "detail", "requested_at"}
}

func (s *stopRequest) controlNested() map[string][]string { return nil }

func (s *stopRequest) validate() error {
	if s.Reason != "manual" && s.Reason != "relaunched_with_run" {
		return fmt.Errorf("%w: bad stop reason", ErrUnsafePath)
	}
	if len(s.Detail) > 280 || strings.IndexFunc(s.Detail, unicode.IsControl) >= 0 {
		return fmt.Errorf("%w: bad stop detail", ErrUnsafePath)
	}
	if _, err := time.Parse(time.RFC3339, s.RequestedAt); err != nil {
		return fmt.Errorf("%w: bad stop time", ErrUnsafePath)
	}
	return nil
}

// ingestExit is ingest.exit: the last exit record, with its attempt.
type ingestExit struct {
	Schema  string `json:"schema"`
	Cause   string `json:"cause"`
	At      string `json:"at"`
	PID     int    `json:"pid"`
	Attempt string `json:"attempt"`
}

func (e *ingestExit) controlSchema() string { return "gifford.capture.exit.v1" }

func (e *ingestExit) controlFields() []string {
	return []string{schemaKey, "cause", "at", "pid", "attempt"}
}

func (e *ingestExit) controlNested() map[string][]string { return nil }

func (e *ingestExit) validate() error {
	switch e.Cause {
	case "eof", "stall", "quota", "stop", ExitError, "refused":
	default:
		return fmt.Errorf("%w: bad exit cause", ErrUnsafePath)
	}
	if _, err := time.Parse(time.RFC3339, e.At); err != nil {
		return fmt.Errorf("%w: bad exit time", ErrUnsafePath)
	}
	if e.PID <= 0 {
		return fmt.Errorf("%w: bad exit pid", ErrUnsafePath)
	}
	if err := validateNonce(e.Attempt); err != nil {
		return err
	}
	return nil
}

// readControl reads one strict control file: checked as in §4.3, at
// most limit + 1 bytes, one JSON object with exactly the expected
// fields, the exact schema, and nothing after it.
func readControl(root *os.Root, name string, limit int64, v controlFile) error {
	fail := func(err error) error {
		return fmt.Errorf("%w: %s: %w", ErrUnsafePath, name, err)
	}
	f, err := openChecked(root, name, false)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return fail(err)
	}
	if int64(len(data)) > limit {
		return fail(fmt.Errorf("oversized control file"))
	}
	if err := decodeControl(data, v); err != nil {
		return fail(err)
	}
	return nil
}

// decodeControl parses one strict control document.
func decodeControl(data []byte, v controlFile) error {
	var m map[string]json.RawMessage
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		return fmt.Errorf("bad control document: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return fmt.Errorf("trailing control data")
	}
	if len(m) != len(v.controlFields()) {
		return fmt.Errorf("bad control fields")
	}
	for _, k := range v.controlFields() {
		if _, ok := m[k]; !ok {
			return fmt.Errorf("missing control field %q", k)
		}
	}
	var schema struct {
		Schema string `json:"schema"`
	}
	if err := json.Unmarshal(data, &schema); err != nil || schema.Schema != v.controlSchema() {
		return fmt.Errorf("bad control schema")
	}
	for name, keys := range v.controlNested() {
		var sub map[string]json.RawMessage
		if err := json.Unmarshal(m[name], &sub); err != nil {
			return fmt.Errorf("bad control section %q", name)
		}
		if len(sub) != len(keys) {
			return fmt.Errorf("bad control section %q", name)
		}
		for _, k := range keys {
			if _, ok := sub[k]; !ok {
				return fmt.Errorf("missing control field %q", k)
			}
		}
	}
	if err := json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("bad control document: %w", err)
	}
	return v.validate()
}

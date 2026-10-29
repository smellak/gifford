package capture

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/smellak/gifford/internal/protocol"
)

// Node and control allowances in bytes: N covers the node-level files,
// C covers every control name's final, one transient temp and remnants.
const (
	nodeAllowance  int64 = 65536
	controlRetain  int64 = 131072
	quotaLockBound       = 2 * time.Second
)

// footprint sums a session directory: control and log files apart from
// the data files. Every entry must be expected, regular and single
// linked; anything else fails the budget closed.
func footprint(root *os.Root, agent, session string) (control, log, data int64, err error) {
	fail := func(e error) (int64, int64, int64, error) {
		return 0, 0, 0, fmt.Errorf("%w: footprint %s/%s: %w", ErrUnsafePath, agent, session, e)
	}
	d, err := root.Open(agent + "/" + session)
	if err != nil {
		return fail(err)
	}
	defer func() { _ = d.Close() }()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return fail(err)
	}
	for _, name := range names {
		kind, err := classifySessionFile(name)
		if err != nil {
			return fail(err)
		}
		li, err := root.Lstat(agent + "/" + session + "/" + name)
		if err != nil {
			return fail(err)
		}
		if !li.Mode().IsRegular() {
			return fail(fmt.Errorf("not a regular file"))
		}
		if _, single, ok := fileIdentity(li); !ok || !single {
			return fail(fmt.Errorf("file link"))
		}
		switch kind {
		case fileControl:
			control += li.Size()
		case fileLog:
			log += li.Size()
		default:
			data += li.Size()
		}
	}
	return control, log, data, nil
}

// fileKind classes a session file: control, log or data.
type fileKind int

const (
	fileControl fileKind = iota
	fileLog
	fileData
)

// controlNames are the exact control and lock names of a session.
var controlNames = map[string]bool{
	"session.json": true, "quota.json": true, "ingest.exit": true,
	"stop.request": true, "ingest.lock": true, "stop.lock": true,
}

// classifySessionFile sorts one session entry: exact control names,
// the log, publication temps, snapshots and segment parts. Anything
// else is unexpected.
func classifySessionFile(name string) (fileKind, error) {
	if controlNames[name] {
		return fileControl, nil
	}
	if name == "ingest.log" {
		return fileLog, nil
	}
	if i := strings.Index(name, ".tmp-"); i >= 0 {
		if isHex16(name[i+5:]) && !strings.Contains(name[:i], "/") {
			return fileControl, nil
		}
		return fileData, errors.New("bad temp name")
	}
	if body, ok := strings.CutSuffix(name, ".ansi"); ok {
		inner, ok := strings.CutPrefix(body, "initial-")
		if !ok {
			return fileData, errors.New("bad snapshot name")
		}
		i := strings.LastIndex(inner, "-")
		if i < 0 {
			return fileData, errors.New("bad snapshot name")
		}
		if _, err := parseSequence(inner[:i]); err != nil {
			return fileData, err
		}
		if err := validateNonce(inner[i+1:]); err != nil {
			return fileData, err
		}
		return fileData, nil
	}
	for _, suffix := range []string{".raw", ".idx", ".zst", ".frames"} {
		if stem, ok := strings.CutSuffix(name, suffix); ok {
			if err := validateSegmentID(stem); err != nil {
				return fileData, err
			}
			return fileData, nil
		}
	}
	return fileData, errors.New("unexpected entry")
}

// isHex16 reports 16 lowercase hex characters.
func isHex16(s string) bool {
	if len(s) != 16 {
		return false
	}
	for i := range len(s) {
		c := s[i]
		if c >= '0' && c <= '9' || c >= 'a' && c <= 'f' {
			continue
		}
		return false
	}
	return true
}

// readLedger reads one session ledger strictly.
func readLedger(root *os.Root, agent, session string) (int64, error) {
	// The ledger lives in the session directory; readControl needs a
	// root-relative name, so sessions are addressed by directory.
	var q quotaLedger
	if err := readControlAt(root, agent+"/"+session+"/quota.json", 4096, &q); err != nil {
		return 0, err
	}
	return q.ReservedBytes, nil
}

// readControlAt reads a strict control file by root-relative path: the
// entry and its parent chain are checked through the root.
func readControlAt(root *os.Root, path string, limit int64, v controlFile) error {
	dir, _ := splitDir(path)
	if dir != "" {
		if _, err := openChecked(root, dir, true); err != nil {
			return err
		}
	}
	return readControl(root, path, limit, v)
}

// splitDir cuts the last path element.
func splitDir(path string) (string, string) {
	if i := strings.LastIndex(path, "/"); i >= 0 {
		return path[:i], path[i+1:]
	}
	return "", path
}

// agentUsage sums the agent's ledgers; a session directory without a
// ledger counts its footprint.
func agentUsage(root *os.Root, agent string) (int64, error) {
	if !protocol.AgentID(agent).Valid() {
		return 0, fmt.Errorf("%w: bad agent id", ErrUnsafePath)
	}
	d, err := root.Open(agent)
	if err != nil {
		return 0, fmt.Errorf("%w: usage %s: %w", ErrUnsafePath, agent, err)
	}
	defer func() { _ = d.Close() }()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return 0, fmt.Errorf("%w: usage %s: %w", ErrUnsafePath, agent, err)
	}
	var total int64
	for _, session := range names {
		if err := validateCaptureSessionID(session); err != nil {
			return 0, fmt.Errorf("%w: usage %s: %w", ErrUnsafePath, agent, err)
		}
		reserved, err := readLedger(root, agent, session)
		if err == nil {
			total += reserved
			continue
		}
		c, l, dt, ferr := footprint(root, agent, session)
		if ferr != nil {
			return 0, ferr
		}
		total += c + l + dt
	}
	return total, nil
}

// nodeUsage sums every ledger under the root plus the node allowance.
func nodeUsage(root *os.Root) (int64, error) {
	d, err := root.Open(".")
	if err != nil {
		return 0, fmt.Errorf("%w: node usage: %w", ErrUnsafePath, err)
	}
	defer func() { _ = d.Close() }()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return 0, fmt.Errorf("%w: node usage: %w", ErrUnsafePath, err)
	}
	total := nodeAllowance
	for _, agent := range names {
		fi, err := root.Lstat(agent)
		if err != nil {
			return 0, fmt.Errorf("%w: node usage: %w", ErrUnsafePath, err)
		}
		if !fi.IsDir() {
			if !isNodeFile(agent) {
				return 0, fmt.Errorf("%w: unexpected node entry", ErrUnsafePath)
			}
			continue
		}
		if !protocol.AgentID(agent).Valid() {
			return 0, fmt.Errorf("%w: unexpected node entry", ErrUnsafePath)
		}
		sd, err := root.Open(agent)
		if err != nil {
			return 0, fmt.Errorf("%w: node usage: %w", ErrUnsafePath, err)
		}
		sessions, err := sd.Readdirnames(-1)
		_ = sd.Close()
		if err != nil {
			return 0, fmt.Errorf("%w: node usage: %w", ErrUnsafePath, err)
		}
		for _, session := range sessions {
			if err := validateCaptureSessionID(session); err != nil {
				return 0, fmt.Errorf("%w: unexpected node entry: %w", ErrUnsafePath, err)
			}
			reserved, err := readLedger(root, agent, session)
			if err == nil {
				total += reserved
				continue
			}
			c, l, dt, ferr := footprint(root, agent, session)
			if ferr != nil {
				return 0, ferr
			}
			total += c + l + dt
		}
	}
	return total, nil
}

// isNodeFile reports the expected node-level files and their temps.
func isNodeFile(name string) bool {
	switch name {
	case "node-id", "claims.lock", "quota.lock", "supervise.lock":
		return true
	}
	if i := strings.Index(name, ".tmp-"); i >= 0 {
		return isHex16(name[i+5:])
	}
	return false
}

// quotaFits reports whether n bytes fit both quotas. Anything
// unreadable or over a cap is unavailable: false, never an error.
func quotaFits(root *os.Root, agent string, n int64, lim Limits) bool {
	au, err := agentUsage(root, agent)
	if err != nil {
		return false
	}
	nu, err := nodeUsage(root)
	if err != nil {
		return false
	}
	return au+n <= lim.AgentQuotaBytes && nu+n <= lim.NodeQuotaBytes
}

// reserve grants n bytes to a session when both quotas allow it,
// writing the new ledger. Unreadable ledgers, a missing lock bound and
// a refused replacement are unavailable: not granted, no error.
func reserve(root *os.Root, agent, session string, n int64, lim Limits) (bool, error) {
	if n < 0 {
		return false, fmt.Errorf("negative reservation")
	}
	if err := ensureLock(root, "quota.lock"); err != nil {
		return false, nil //nolint:nilerr // An unavailable budget is not granted, no error.
	}
	granted := false
	// tryReserve folds every unavailable cause into false; the lock
	// closure itself never fails, so nilerr has nothing to report.
	tryReserve := func() bool {
		if !quotaFits(root, agent, n, lim) {
			return false
		}
		current, err := readLedger(root, agent, session)
		if err != nil {
			return false
		}
		if err := writeLedger(root, agent, session, current+n); err != nil {
			return false
		}
		return true
	}
	err := withLock(root, "quota.lock", true, quotaLockBound, func() error {
		granted = tryReserve()
		return nil
	})
	if err != nil {
		return false, nil //nolint:nilerr // A missed lock bound is not granted, no error.
	}
	return granted, nil
}

// reserveNew ledgers a fresh session without graduated history: the
// caps check runs under quota.lock, then the first ledger is written.
// Anything unavailable is not granted, no error.
func reserveNew(root *os.Root, agent, session string, n int64, lim Limits) (bool, error) {
	if n < 0 {
		return false, fmt.Errorf("negative reservation")
	}
	if err := ensureLock(root, "quota.lock"); err != nil {
		return false, nil //nolint:nilerr // An unavailable budget is not granted, no error.
	}
	// grant folds every unavailable cause into false; the lock
	// closure itself never fails, so nilerr has nothing to report.
	grant := func() bool {
		if !quotaFits(root, agent, n, lim) {
			return false
		}
		if err := writeLedger(root, agent, session, n); err != nil {
			return false
		}
		return true
	}
	granted := false
	err := withLock(root, "quota.lock", true, quotaLockBound, func() error {
		granted = grant()
		return nil
	})
	if err != nil {
		return false, nil //nolint:nilerr // A missed lock bound is not granted, no error.
	}
	return granted, nil
}

// lowerLedger moves the ledger down toward target, never up, ignoring
// failures: a stale ledger over-counts usage, which fails closed. It
// serves the abort paths, where the mapping error outranks the books.
func lowerLedger(root *os.Root, agent, session string, target int64) {
	current, err := readLedger(root, agent, session)
	if err != nil || target >= current {
		return
	}
	_ = writeLedger(root, agent, session, target)
}

// writeLedger replaces a session ledger atomically.
func writeLedger(root *os.Root, agent, session string, reserved int64) error {
	data, err := json.Marshal(&quotaLedger{Schema: "gifford.capture.quota.v1", ReservedBytes: reserved})
	if err != nil {
		return fmt.Errorf("%w: ledger: %w", ErrUnsafePath, err)
	}
	return publishFile(root, agent+"/"+session, "quota.json", data, false)
}

// settle lowers D to the data footprint, plus at most one block while
// live. F is never lowered here; the caller retains it while a late
// writer is possible.
func settle(root *os.Root, agent, session string, lim Limits, terminal bool) error {
	current, err := readLedger(root, agent, session)
	if err != nil {
		return err
	}
	_, _, data, err := footprint(root, agent, session)
	if err != nil {
		return err
	}
	target := controlRetain + lim.LogMaxBytes + data
	if !terminal {
		target += lim.ReserveBlockBytes
	}
	if target >= current {
		return nil
	}
	return writeLedger(root, agent, session, target)
}

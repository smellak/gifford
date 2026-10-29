//go:build mapperreview

package capture

// Mapper's synthetic review probes for 28912fa: the pending CAP-I05..I09
// corrections of the 665de758 review. Never committed. Fake tmux, temp
// directories and synthetic bytes only. Entry points (Attach, Ingest)
// wherever reachable; pure helpers only where the original diagnostic
// was one.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"

	"github.com/smellak/gifford/internal/protocol"
)

const (
	zeOther  = "cap-01JC00000000000000000000EE"
	zeOther2 = "cap-01JC00000000000000000000FF"
	zeSeg    = "seg-01JC00000000000000000000AA"
	zeHex16  = "0123456789abcdef"
)

func zeSessionDir(data, agent, session string) string {
	return filepath.Join(data, "captures", agent, session)
}

func zeChunks(t *testing.T, data string) (n int, total, maxLen int64) {
	t.Helper()
	store, err := OpenStore(context.Background(), filepath.Join(data, "gifford.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	events, err := readAllEvents(context.Background(), store, storeTestSession)
	if err != nil {
		t.Fatal(err)
	}
	for i := range events {
		if events[i].EventType != protocol.EventTypeCaptureChunk {
			continue
		}
		var c protocol.CaptureChunk
		if err := decodePayload(&events[i], &c); err != nil {
			t.Fatal(err)
		}
		n++
		total += c.ByteCount
		maxLen = max(maxLen, c.ByteCount)
	}
	return n, total, maxLen
}

// CAP-I05 (a): retained F is not data credit. The session ledger holds
// only controlRetain + log_max_bytes and the agent quota has no room
// for a block or the chunk: the ingest must not write the chunk.
func TestZERetainedFNotSpendable(t *testing.T) {
	skipUnlessAttach(t)
	data := t.TempDir()
	lim := ingestLimits()
	ledger := controlRetain + lim.LogMaxBytes
	root := ingestFixture(t, data, lim, ledger)
	makeSession(t, root, storeTestAgent, zeOther)
	if err := writeLedger(root, storeTestAgent, zeOther, lim.AgentQuotaBytes-ledger-10); err != nil {
		t.Fatal(err)
	}
	cause, err := runIngest(t, ingestDeps(), ingestOpts(data, nil), []byte("hello"))
	n, total, _ := zeChunks(t, data)
	t.Logf("ledger=F only, agent headroom 10 B: cause=%s err=%v chunks=%d bytes=%d", cause, err, n, total)
	if n != 0 {
		t.Errorf("a chunk was written on retained F with no data allowance")
	}
}

// CAP-I05 (b): a temp belongs to its final artifact's bucket.
func TestZEDataTempStaysData(t *testing.T) {
	cases := map[string]fileKind{
		zeSeg + ".zst.tmp-" + zeHex16:                     fileData,
		zeSeg + ".frames.tmp-" + zeHex16:                  fileData,
		zeSeg + ".raw":                                    fileData,
		"initial-1-" + testNonce + ".ansi.tmp-" + zeHex16: fileData,
		"quota.json.tmp-" + zeHex16:                       fileControl,
		"ingest.exit.tmp-" + zeHex16:                      fileControl,
	}
	for name, want := range cases {
		got, err := classifySessionFile(name)
		t.Logf("%s -> kind %v err=%v", name, got, err)
		if err != nil || got != want {
			t.Errorf("%s classified %v (err %v), want %v", name, got, err, want)
		}
	}
}

// CAP-I05 (c): a present but malformed ledger fails closed.
func TestZECorruptLedgerFailsClosed(t *testing.T) {
	data := t.TempDir()
	root, err := openCaptureRoot(data, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.Close() }()
	makeSession(t, root, storeTestAgent, zeOther)
	if err := os.WriteFile(filepath.Join(zeSessionDir(data, storeTestAgent, zeOther), "quota.json"), []byte("{\"schema\":"), 0o600); err != nil {
		t.Fatal(err)
	}
	a, aerr := agentUsage(root, storeTestAgent)
	n, nerr := nodeUsage(root)
	t.Logf("corrupt ledger: agentUsage=%d err=%v; nodeUsage=%d err=%v", a, aerr, n, nerr)
	if aerr == nil || nerr == nil {
		t.Errorf("a corrupt ledger is accepted")
	}
}

// CAP-I05 (d): only the abandoned-bootstrap shape (empty lock files,
// no ledger) may fall back to footprint accounting. A ledger-less
// directory holding data is malformed and must fail, also at Attach.
func TestZEMissingLedgerNeedsBootstrapShape(t *testing.T) {
	skipUnlessAttach(t)
	data := t.TempDir()
	root, err := openCaptureRoot(data, true)
	if err != nil {
		t.Fatal(err)
	}
	makeSession(t, root, "miner-1", zeOther)
	if err := ensureLock(root, "miner-1/"+zeOther+"/ingest.lock"); err != nil {
		t.Fatal(err)
	}
	a, aerr := agentUsage(root, "miner-1")
	t.Logf("bootstrap shape (empty lock only): agentUsage=%d err=%v", a, aerr)
	if aerr != nil {
		t.Errorf("the allowed bootstrap shape fails: %v", aerr)
	}
	if err := os.WriteFile(filepath.Join(zeSessionDir(data, "miner-1", zeOther), zeSeg+".raw"), bytes.Repeat([]byte("x"), 100), 0o600); err != nil {
		t.Fatal(err)
	}
	a, aerr = agentUsage(root, "miner-1")
	n, nerr := nodeUsage(root)
	t.Logf("no ledger + 100 B segment: agentUsage=%d err=%v; nodeUsage=%d err=%v", a, aerr, n, nerr)
	if aerr == nil || nerr == nil {
		t.Errorf("a ledger-less directory holding data is accounted by footprint instead of failing")
	}
	_ = root.Close()
	runner := &fakeRunner{
		serverOut: attachServer(),
		paneBy:    map[string]paneFacts{attachTarget: attachPane()},
		openOut:   openOutcome{opened: true, snapshot: []byte("\x1b[0mpane")},
	}
	_, err = Attach(context.Background(), attachDeps(runner, data, attachClock()), attachOpts())
	t.Logf("attach of the same agent: err=%v opens=%d", err, strings.Count(strings.Join(runner.calls, ","), "open"))
	if err == nil || errors.Is(err, ErrAttach) {
		t.Errorf("attach reserved over a malformed ledger-less directory")
	}
}

// zeFramesDoc writes a strict .frames for one frame covering raw.
func zeFinals(t *testing.T, dir string, raw []byte, zst []byte, writeZst bool) string {
	t.Helper()
	sum := sha256.Sum256(raw)
	rawSHA := hex.EncodeToString(sum[:])
	doc, err := json.Marshal(framesDoc{
		Schema: "gifford.capture.frames.v1", SegmentID: zeSeg, RawBytes: int64(len(raw)), RawSHA256: rawSHA,
		Frames: []frameEntry{{RawOffset: 0, RawLength: int64(len(raw)), Offset: 0, Length: int64(len(zst))}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, zeSeg+".frames"), doc, 0o600); err != nil {
		t.Fatal(err)
	}
	if writeZst {
		if err := os.WriteFile(filepath.Join(dir, zeSeg+".zst"), zst, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return rawSHA
}

// CAP-I06 (reuse candidate): a missing or junk .zst never verifies; a
// real one does.
func TestZEReuseNeedsRealZst(t *testing.T) {
	raw := bytes.Repeat([]byte("synthetic segment bytes\n"), 64)
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	good := enc.EncodeAll(raw, nil)
	_ = enc.Close()
	for _, c := range []struct {
		name     string
		zst      []byte
		writeZst bool
		want     bool
	}{{"missing .zst", good, false, false}, {"junk .zst", []byte("junk junk junk"), true, false}, {"real .zst", good, true, true}} {
		data := t.TempDir()
		root, err := openCaptureRoot(data, true)
		if err != nil {
			t.Fatal(err)
		}
		makeSession(t, root, storeTestAgent, storeTestSession)
		rawSHA := zeFinals(t, zeSessionDir(data, storeTestAgent, storeTestSession), raw, c.zst, c.writeZst)
		run := &ingestRun{root: root, opts: IngestOptions{Agent: storeTestAgent, Session: storeTestSession}}
		got := run.verifyFinals(zeSeg, int64(len(raw)), rawSHA)
		t.Logf("%s: verifyFinals=%v", c.name, got)
		if got != c.want {
			t.Errorf("%s: verifyFinals=%v, want %v", c.name, got, c.want)
		}
		_ = root.Close()
	}
}

// CAP-I06 (fresh path) and CAP-I07 run under strace from outside: one
// EOF ingest with compression, and one read that rotates many times.
func TestZEFreshCompression(t *testing.T) {
	skipUnlessAttach(t)
	data := t.TempDir()
	lim := ingestLimits()
	_ = ingestFixture(t, data, lim, controlRetain+lim.LogMaxBytes+lim.SnapshotMaxBytes)
	cause, err := runIngest(t, ingestDeps(), ingestOpts(data, nil), []byte("fresh compression probe \x1b[0m bytes"))
	names, _ := os.ReadDir(zeSessionDir(data, storeTestAgent, storeTestSession))
	var files []string
	for _, e := range names {
		files = append(files, e.Name())
	}
	t.Logf("cause=%s err=%v files=%v", cause, err, files)
}

func TestZEMultiRotationSync(t *testing.T) {
	skipUnlessAttach(t)
	data := t.TempDir()
	lim := ingestLimits()
	lim.ChunkMaxBytes = 512
	lim.FrameMaxBytes = 1024
	lim.SegmentMaxBytes = 2048
	_ = ingestFixture(t, data, lim, controlRetain+lim.LogMaxBytes+lim.SnapshotMaxBytes+65536)
	input := make([]byte, 16*1024)
	for i := range input {
		input[i] = byte(i)
	}
	cause, err := runIngest(t, ingestDeps(), ingestOpts(data, nil), input)
	n, total, _ := zeChunks(t, data)
	t.Logf("cause=%s err=%v chunks=%d bytes=%d", cause, err, n, total)
}

// CAP-I08: with valid chunk_max_bytes = 1, no committed chunk is larger
// than one byte (§8.3: it closes at chunk_max_bytes, the excess starts
// the next one; the open chunk is at most chunk_max_bytes).
func TestZETinyChunksObeyChunkMax(t *testing.T) {
	skipUnlessAttach(t)
	data := t.TempDir()
	lim := ingestLimits()
	lim.ChunkMaxBytes = 1
	_ = ingestFixture(t, data, lim, controlRetain+lim.LogMaxBytes+lim.SnapshotMaxBytes)
	cause, err := runIngest(t, ingestDeps(), ingestOpts(data, nil), bytes.Repeat([]byte("x"), 2048))
	n, total, maxLen := zeChunks(t, data)
	t.Logf("2048 B with chunk_max_bytes=1: cause=%s err=%v chunks=%d bytes=%d largest chunk=%d", cause, err, n, total, maxLen)
	if maxLen > lim.ChunkMaxBytes {
		t.Errorf("a committed chunk carries %d bytes, above chunk_max_bytes %d", maxLen, lim.ChunkMaxBytes)
	}
}

// CAP-I09: an existing ingest.log that is a symlink or a hard link to a
// sibling victim inside the capture root is refused before any write.
func TestZELogAliasRefused(t *testing.T) {
	skipUnlessAttach(t)
	for _, kind := range []string{"symlink", "hardlink"} {
		data := t.TempDir()
		lim := ingestLimits()
		root := ingestFixture(t, data, lim, controlRetain+lim.LogMaxBytes+lim.SnapshotMaxBytes)
		makeSession(t, root, storeTestAgent, zeOther)
		victim := filepath.Join(zeSessionDir(data, storeTestAgent, zeOther), "ingest.log")
		if err := os.WriteFile(victim, []byte("victim!\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		logPath := filepath.Join(zeSessionDir(data, storeTestAgent, storeTestSession), "ingest.log")
		var err error
		if kind == "symlink" {
			err = os.Symlink("../"+zeOther+"/ingest.log", logPath)
		} else {
			err = os.Link(victim, logPath)
		}
		if err != nil {
			t.Fatal(err)
		}
		cause, ierr := runIngest(t, ingestDeps(), ingestOpts(data, nil), []byte("x"))
		kept, _ := os.ReadFile(victim)
		t.Logf("%s ingest.log -> sibling victim: cause=%s err=%v victim now %d B", kind, cause, ierr, len(kept))
		if string(kept) != "victim!\n" {
			t.Errorf("%s: the victim changed", kind)
		}
	}
}

// CAP-I09 audit (§4.4: lock files are created O_CREATE|O_EXCL, an
// existing one is checked): Attach's no-replace probe opens
// claims.lock before any check. A dangling symlink inside the root
// makes it create its target in another session's directory.
func TestZEClaimsLockSymlinkCreatesTarget(t *testing.T) {
	skipUnlessAttach(t)
	data := t.TempDir()
	root, err := openCaptureRoot(data, true)
	if err != nil {
		t.Fatal(err)
	}
	makeSession(t, root, storeTestAgent2, zeOther2)
	target := filepath.Join(zeSessionDir(data, storeTestAgent2, zeOther2), "stop.request")
	if err := os.Symlink(storeTestAgent2+"/"+zeOther2+"/stop.request", filepath.Join(data, "captures", "claims.lock")); err != nil {
		t.Fatal(err)
	}
	runner := &fakeRunner{
		serverOut: attachServer(),
		paneBy:    map[string]paneFacts{attachTarget: attachPane()},
		openOut:   openOutcome{opened: true, snapshot: []byte("\x1b[0mpane")},
	}
	_, aerr := Attach(context.Background(), attachDeps(runner, data, attachClock()), attachOpts())
	fi, serr := os.Lstat(target)
	_, rerr := readStopRequest(root, storeTestAgent2, zeOther2)
	t.Logf("attach err=%v; other session stop.request after attach: stat err=%v size=%v; readStopRequest err=%v", aerr, serr, func() int64 {
		if fi == nil {
			return -1
		}
		return fi.Size()
	}(), rerr)
	_ = root.Close()
	if serr == nil {
		t.Errorf("attach created a file in another session's directory through claims.lock before any check")
	}
}

// CAP-I05 (62364): balances after the reservation grows, with a real
// root and ledger. The ledger holds only F and the agent quota leaves
// room for exactly one block: across three blocks of dense input the
// run may write at most that block (raw plus index), never F.
func TestZEBalanceAfterBlock(t *testing.T) {
	skipUnlessAttach(t)
	data := t.TempDir()
	lim := ingestLimits()
	ledger := controlRetain + lim.LogMaxBytes
	root := ingestFixture(t, data, lim, ledger)
	makeSession(t, root, storeTestAgent, zeOther)
	if err := writeLedger(root, storeTestAgent, zeOther, lim.AgentQuotaBytes-ledger-lim.ReserveBlockBytes-10); err != nil {
		t.Fatal(err)
	}
	cause, err := runIngest(t, ingestDeps(), ingestOpts(data, nil), bytes.Repeat([]byte("y"), int(3*lim.ReserveBlockBytes)))
	n, total, _ := zeChunks(t, data)
	_, _, dataBytes, ferr := footprint(root, storeTestAgent, storeTestSession)
	got, lerr := readLedger(root, storeTestAgent, storeTestSession)
	t.Logf("F-only ledger, one block of headroom: cause=%s err=%v chunks=%d raw=%d data footprint=%d (err %v) ledger=%d (err %v) F=%d block=%d",
		cause, err, n, total, dataBytes, ferr, got, lerr, ledger, lim.ReserveBlockBytes)
	if dataBytes > lim.ReserveBlockBytes+10 {
		t.Errorf("data footprint %d exceeds the data allowance %d: retained F was spent", dataBytes, lim.ReserveBlockBytes+10)
	}
}

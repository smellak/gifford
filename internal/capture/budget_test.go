package capture

import (
	"errors"
	"os"
	"testing"
)

// Repeated ledgerless entry names.
const (
	testSessionFile = "session.json"
	testLockFile    = "ingest.lock"
)

func testLimits() Limits {
	return Limits{
		ReserveBlockBytes: 50,
		LogMaxBytes:       100,
		AgentQuotaBytes:   1000,
		NodeQuotaBytes:    nodeAllowance + 2000,
	}
}

func TestClassifySessionFile(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind fileKind
		bad  bool
	}{
		{testSessionFile, fileControl, false},
		{"quota.json", fileControl, false},
		{"ingest.exit", fileControl, false},
		{"stop.request", fileControl, false},
		{testLockFile, fileControl, false},
		{"stop.lock", fileControl, false},
		{"ingest.log", fileLog, false},
		{"quota.json.tmp-abcdef0123456789", fileControl, false},
		{testSegment + ".zst.tmp-abcdef0123456789", fileData, false},
		{testSegment + ".frames.tmp-abcdef0123456789", fileData, false},
		{"initial-12-" + testNonce + ".ansi.tmp-abcdef0123456789", fileData, false},
		{testSegment + ".raw", fileData, false},
		{testSegment + ".idx", fileData, false},
		{testSegment + ".zst", fileData, false},
		{testSegment + ".frames", fileData, false},
		{"initial-12-" + testNonce + ".ansi", fileData, false},
		{"seg-BAD.raw", fileData, true},
		{"seg-01ARZ3NDEKTSV4RRFFQ69G5FAV.txt", fileData, true},
		{"initial-x-" + testNonce + ".ansi", fileData, true},
		{"initial-1-short.ansi", fileData, true},
		{"nope-" + testNonce + ".ansi", fileData, true},
		{"foo.tmp-xyz", fileData, true},
		{"evil.txt", fileData, true},
	} {
		kind, err := classifySessionFile(tc.name)
		if tc.bad {
			if err == nil {
				t.Errorf("%s: want error, got kind %d", tc.name, kind)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}
		if kind != tc.kind {
			t.Errorf("%s: kind = %d, want %d", tc.name, kind, tc.kind)
		}
	}
}

func TestFootprintBuckets(t *testing.T) {
	root, _ := openTestRoot(t)
	makeSession(t, root, testAgent, testSessionA)
	dir := testAgent + "/" + testSessionA
	putFile(t, root, dir, testSessionFile, []byte("0123456789"))
	putFile(t, root, dir, "ingest.log", []byte("01234"))
	putFile(t, root, dir, testSegment+".raw", make([]byte, 100))
	putFile(t, root, dir, "initial-0-"+testNonce+".ansi", make([]byte, 7))
	if err := writeLedger(root, testAgent, testSessionA, 10); err != nil {
		t.Fatal(err)
	}
	// A leftover publication temp counts as control.
	tf, err := root.Create(dir + "/quota.json.tmp-abcdef0123456789")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tf.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := tf.Close(); err != nil {
		t.Fatal(err)
	}
	ledgerSize := func() int64 {
		fi, err := root.Lstat(dir + "/quota.json")
		if err != nil {
			t.Fatal(err)
		}
		return fi.Size()
	}()
	control, log, data, err := footprint(root, testAgent, testSessionA)
	if err != nil {
		t.Fatal(err)
	}
	if want := int64(10) + ledgerSize + 3; control != want {
		t.Errorf("control = %d, want %d", control, want)
	}
	if log != 5 {
		t.Errorf("log = %d, want 5", log)
	}
	if data != 107 {
		t.Errorf("data = %d, want 107", data)
	}
}

func TestFootprintRejects(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, root *os.Root, dir, data string)
	}{
		{"unexpected", func(t *testing.T, root *os.Root, dir, _ string) {
			putFile(t, root, dir, "evil.txt", []byte("x"))
		}},
		{"symlink", func(t *testing.T, root *os.Root, dir, data string) {
			putFile(t, root, dir, testSessionFile, []byte("{}"))
			if err := os.Symlink(data+"/x", data+"/captures/"+dir+"/"+testSegment+".raw"); err != nil {
				t.Fatal(err)
			}
		}},
		{"subdir", func(t *testing.T, root *os.Root, dir, _ string) {
			putFile(t, root, dir, testSessionFile, []byte("{}"))
			if err := root.Mkdir(dir+"/sub", 0o700); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, data := openTestRoot(t)
			makeSession(t, root, testAgent, testSessionA)
			tc.setup(t, root, testAgent+"/"+testSessionA, data)
			if _, _, _, err := footprint(root, testAgent, testSessionA); !errors.Is(err, ErrUnsafePath) {
				t.Fatalf("footprint err = %v, want ErrUnsafePath", err)
			}
		})
	}
}

func TestReserveGrantAndCaps(t *testing.T) {
	root, _ := openTestRoot(t)
	skipUnlessLocks(t, root)
	lim := testLimits()
	makeSession(t, root, testAgent, testSessionA)
	if err := writeLedger(root, testAgent, testSessionA, 0); err != nil {
		t.Fatal(err)
	}
	ok, err := reserve(root, testAgent, testSessionA, 100, lim)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("grant refused under caps")
	}
	got, err := readLedger(root, testAgent, testSessionA)
	if err != nil {
		t.Fatal(err)
	}
	if got != 100 {
		t.Fatalf("ledger = %d, want 100", got)
	}
	au, err := agentUsage(root, testAgent)
	if err != nil {
		t.Fatal(err)
	}
	if au != 100 {
		t.Fatalf("agent usage = %d, want 100", au)
	}
	nu, err := nodeUsage(root)
	if err != nil {
		t.Fatal(err)
	}
	if nu != nodeAllowance+100 {
		t.Fatalf("node usage = %d, want %d", nu, nodeAllowance+100)
	}
	// Over the agent cap: refused, ledger untouched.
	ok, err = reserve(root, testAgent, testSessionA, 950, lim)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("grant over agent cap")
	}
	got, err = readLedger(root, testAgent, testSessionA)
	if err != nil {
		t.Fatal(err)
	}
	if got != 100 {
		t.Fatalf("ledger after refuse = %d, want 100", got)
	}
}

func TestReserveSecondAgent(t *testing.T) {
	root, _ := openTestRoot(t)
	skipUnlessLocks(t, root)
	lim := testLimits()
	makeSession(t, root, "miner-2", testSessionB)
	if err := writeLedger(root, "miner-2", testSessionB, 0); err != nil {
		t.Fatal(err)
	}
	ok, err := reserve(root, "miner-2", testSessionB, 60, lim)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("grant refused for second agent")
	}
	au, err := agentUsage(root, "miner-2")
	if err != nil {
		t.Fatal(err)
	}
	if au != 60 {
		t.Fatalf("agent usage = %d, want 60", au)
	}
}

func TestReserveNodeCap(t *testing.T) {
	root, _ := openTestRoot(t)
	skipUnlessLocks(t, root)
	lim := Limits{ReserveBlockBytes: 10, LogMaxBytes: 10, AgentQuotaBytes: 100000, NodeQuotaBytes: nodeAllowance + 150}
	makeSession(t, root, testAgent, testSessionA)
	if err := writeLedger(root, testAgent, testSessionA, 100); err != nil {
		t.Fatal(err)
	}
	ok, err := reserve(root, testAgent, testSessionA, 100, lim)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("grant over node cap")
	}
}

func TestReserveRequiresLedger(t *testing.T) {
	root, _ := openTestRoot(t)
	skipUnlessLocks(t, root)
	// Footprint but no ledger: unavailable, not granted.
	makeSession(t, root, testAgent, testSessionA)
	putFile(t, root, testAgent+"/"+testSessionA, testSessionFile, []byte("{}"))
	ok, err := reserve(root, testAgent, testSessionA, 10, testLimits())
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("grant without ledger")
	}
}

func TestReserveNegative(t *testing.T) {
	root, _ := openTestRoot(t)
	skipUnlessLocks(t, root)
	makeSession(t, root, testAgent, testSessionA)
	if _, err := reserve(root, testAgent, testSessionA, -1, testLimits()); err == nil {
		t.Fatal("want error for negative reservation")
	}
}

// TestUsageCountsBootstrapShape proves the only ledgerless directory
// the budget accounts is the abandoned-bootstrap shape: an empty
// ingest.lock and at most two small quota temps.
func TestUsageCountsBootstrapShape(t *testing.T) {
	root, _ := openTestRoot(t)
	makeSession(t, root, testAgent, testSessionA)
	makeSession(t, root, testAgent, testSessionB)
	if err := writeLedger(root, testAgent, testSessionA, 40); err != nil {
		t.Fatal(err)
	}
	if err := ensureLock(root, testAgent+"/"+testSessionB+"/ingest.lock"); err != nil {
		t.Fatal(err)
	}
	putFile(t, root, testAgent+"/"+testSessionB, "quota.json.tmp-0123456789abcdef", make([]byte, 100))
	au, err := agentUsage(root, testAgent)
	if err != nil {
		t.Fatal(err)
	}
	if au != 140 {
		t.Fatalf("agent usage = %d, want 140", au)
	}
	if _, err := nodeUsage(root); err != nil {
		t.Fatal(err)
	}
}

// TestUsageRejectsNonBootstrapLedgerless proves any other ledgerless
// content — session.json, segment parts, a third temp, a non-empty
// lock — leaves the budget unavailable instead of footprint-counted.
func TestUsageRejectsNonBootstrapLedgerless(t *testing.T) {
	for _, files := range []map[string][]byte{
		{testSessionFile: {}},
		{testLockFile: {}, testSessionFile: {}},
		{testLockFile: {}, "seg-01J00000000000000000000000.raw": make([]byte, 10)},
		{
			testLockFile:                      {},
			"quota.json.tmp-0000000000000000": make([]byte, 1),
			"quota.json.tmp-1111111111111111": make([]byte, 1),
			"quota.json.tmp-2222222222222222": make([]byte, 1),
		},
		{testLockFile: {1}},
	} {
		root, _ := openTestRoot(t)
		makeSession(t, root, testAgent, testSessionA)
		for name, b := range files {
			writeLedgerlessEntry(t, root, name, b)
		}
		if _, err := agentUsage(root, testAgent); err == nil {
			t.Fatalf("ledgerless %v counted instead of failing", files)
		}
		if _, err := nodeUsage(root); err == nil {
			t.Fatalf("ledgerless %v counted at node level", files)
		}
	}
}

// writeLedgerlessEntry stages one ledgerless directory entry: the
// lock through ensureLock (optionally non-empty), anything else
// through publishFile.
func writeLedgerlessEntry(t *testing.T, root *os.Root, name string, b []byte) {
	t.Helper()
	if name != testLockFile {
		putFile(t, root, testAgent+"/"+testSessionA, name, b)
		return
	}
	if err := ensureLock(root, testAgent+"/"+testSessionA+"/"+testLockFile); err != nil {
		t.Fatal(err)
	}
	if len(b) == 0 {
		return
	}
	f, err := root.OpenFile(testAgent+"/"+testSessionA+"/"+testLockFile, os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.Write(b); err != nil {
		t.Fatal(err)
	}
}

// TestUsageRejectsCorruptLedger proves a present but invalid ledger
// is unavailable: it errors instead of falling back to the
// footprint, which is reserved for ledgerless crash leftovers.
func TestUsageRejectsCorruptLedger(t *testing.T) {
	root, _ := openTestRoot(t)
	makeSession(t, root, testAgent, testSessionA)
	putFile(t, root, testAgent+"/"+testSessionA, "quota.json", []byte(`{"reserved_bytes":`))
	if _, err := agentUsage(root, testAgent); err == nil {
		t.Fatal("corrupt ledger counted as footprint")
	}
	if _, err := nodeUsage(root); err == nil {
		t.Fatal("corrupt ledger counted as footprint")
	}
	lim := ingestLimits()
	if quotaFits(root, testAgent, 1, lim) {
		t.Fatal("quota fits on an unavailable budget")
	}
}

func TestNodeUsageRejectsUnexpected(t *testing.T) {
	root, _ := openTestRoot(t)
	putFile(t, root, ".", "node-id", []byte("0123456789abcdef"))
	if _, err := nodeUsage(root); err != nil {
		t.Fatal(err)
	}
	putFile(t, root, ".", "zzz", []byte("x"))
	if _, err := nodeUsage(root); !errors.Is(err, ErrUnsafePath) {
		t.Fatalf("node usage err = %v, want ErrUnsafePath", err)
	}
}

func TestSettleLowersToFootprint(t *testing.T) {
	root, _ := openTestRoot(t)
	lim := testLimits()
	makeSession(t, root, testAgent, testSessionA)
	dir := testAgent + "/" + testSessionA
	putFile(t, root, dir, testSegment+".raw", make([]byte, 20))
	if err := writeLedger(root, testAgent, testSessionA, 200000); err != nil {
		t.Fatal(err)
	}
	if err := settle(root, testAgent, testSessionA, lim, true); err != nil {
		t.Fatal(err)
	}
	got, err := readLedger(root, testAgent, testSessionA)
	if err != nil {
		t.Fatal(err)
	}
	want := controlRetain + lim.LogMaxBytes + 20
	if got != want {
		t.Fatalf("settled = %d, want %d", got, want)
	}
}

func TestSettleKeepsBlockWhileLive(t *testing.T) {
	root, _ := openTestRoot(t)
	lim := testLimits()
	makeSession(t, root, testAgent, testSessionA)
	if err := writeLedger(root, testAgent, testSessionA, 200000); err != nil {
		t.Fatal(err)
	}
	// Live keeps one block: the ledger drops to target plus block.
	if err := settle(root, testAgent, testSessionA, lim, false); err != nil {
		t.Fatal(err)
	}
	got, err := readLedger(root, testAgent, testSessionA)
	if err != nil {
		t.Fatal(err)
	}
	if want := controlRetain + lim.LogMaxBytes + lim.ReserveBlockBytes; got != want {
		t.Fatalf("live settled = %d, want %d", got, want)
	}
	// Below target already: no write, no error.
	if err := writeLedger(root, testAgent, testSessionA, 10); err != nil {
		t.Fatal(err)
	}
	if err := settle(root, testAgent, testSessionA, lim, false); err != nil {
		t.Fatal(err)
	}
	got, err = readLedger(root, testAgent, testSessionA)
	if err != nil {
		t.Fatal(err)
	}
	if got != 10 {
		t.Fatalf("ledger = %d, want 10 (untouched)", got)
	}
}

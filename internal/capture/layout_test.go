package capture

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeClock is a fixed wall clock.
type fakeClock struct{ now time.Time }

func (f fakeClock) Now() time.Time { return f.now }

func TestULIDs(t *testing.T) {
	t.Parallel()
	clk := fakeClock{now: time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC)}
	var zeroEntropy [10]byte
	require.Equal(t, "00000000000000000000000000", encodeULID(0, zeroEntropy))
	seen := map[string]bool{}
	for range 1000 {
		id, err := newCaptureSessionID(clk)
		require.NoError(t, err)
		require.Regexp(t, `^cap-[0-9A-HJKMNP-TV-Z]{26}$`, id)
		require.False(t, seen[id])
		seen[id] = true
		sg, err := newSegmentID(clk)
		require.NoError(t, err)
		require.Regexp(t, `^seg-[0-9A-HJKMNP-TV-Z]{26}$`, sg)
	}
	require.NoError(t, validateCaptureSessionID("cap-01AAAAAAAAAAAAAAAAAAAAAAAA"))
	require.NoError(t, validateSegmentID("seg-01AAAAAAAAAAAAAAAAAAAAAAAA"))
	for _, bad := range []string{"", "cap-abc", "cap-01aaaaaaaa", "../x", "/a", "seg-01AAAAAAAAAAAAAAAAAAAAAAA 즈"} {
		require.ErrorIs(t, validateCaptureSessionID(bad), ErrUnsafePath, bad)
	}
	nonce, err := newNonce()
	require.NoError(t, err)
	require.Regexp(t, `^[0-9a-f]{32}$`, nonce)
	require.NoError(t, validateNonce(nonce))
	require.ErrorIs(t, validateNonce("nope"), ErrUnsafePath)
	n, err := parseSequence("2")
	require.NoError(t, err)
	require.Equal(t, int64(2), n)
	for _, bad := range []string{"", "-1", "1.5", "18446744073709551616"} {
		_, err := parseSequence(bad)
		require.Error(t, err, bad)
	}
}

func TestLimitsValidation(t *testing.T) {
	t.Parallel()
	require.NoError(t, DefaultLimits().Validate())
	cases := []struct {
		name   string
		mutate func(*Limits)
	}{
		{"chunk", func(l *Limits) { l.ChunkMaxBytes = 0 }},
		{"chunk frame", func(l *Limits) { l.ChunkMaxBytes = l.FrameMaxBytes + 1 }},
		{"frame segment", func(l *Limits) { l.FrameMaxBytes = l.SegmentMaxBytes + 1 }},
		{"frame ratio", func(l *Limits) { l.FrameMaxBytes = 1 }},
		{"segment", func(l *Limits) { l.SegmentMaxBytes = 1 << 31 }},
		{"segment age", func(l *Limits) { l.SegmentMaxAge = Duration(time.Hour * 25) }},
		{"chunk age", func(l *Limits) { l.ChunkMaxAge = Duration(2 * time.Second) }},
		{"flush", func(l *Limits) { l.FlushInterval = Duration(time.Second * 11) }},
		{"resize", func(l *Limits) { l.ResizeInterval = Duration(time.Millisecond) }},
		{"queue range", func(l *Limits) { l.QueueMaxBytes = 1 }},
		{"queue ratio", func(l *Limits) { l.QueueMaxBytes = 65536; l.ChunkMaxBytes = 1 << 20 }},
		{"reserve ratio", func(l *Limits) { l.ReserveBlockBytes = 1 }},
		{"reserve quota", func(l *Limits) { l.ReserveBlockBytes = l.AgentQuotaBytes + 1 }},
		{"snapshot", func(l *Limits) { l.SnapshotMaxBytes = 1 }},
		{"log", func(l *Limits) { l.LogMaxBytes = 1 }},
		{"agent quota", func(l *Limits) { l.AgentQuotaBytes = 1 }},
		{"node quota", func(l *Limits) { l.NodeQuotaBytes = l.AgentQuotaBytes }},
	}
	for _, tc := range cases {
		l := DefaultLimits()
		tc.mutate(&l)
		require.Error(t, l.Validate(), tc.name)
	}
	good := DefaultLimits()
	raw, err := json.Marshal(good)
	require.NoError(t, err)
	require.Contains(t, string(raw), `"segment_max_age":"1h0m0s"`)
	var back Limits
	require.NoError(t, json.Unmarshal(raw, &back))
	require.Equal(t, good, back)
}

// testRoot creates a data dir with a capture root and returns it.
func testRoot(t *testing.T) (string, *os.Root) {
	t.Helper()
	data := t.TempDir()
	root, err := openCaptureRoot(data, true)
	require.NoError(t, err)
	t.Cleanup(func() { _ = root.Close() })
	return data, root
}

func TestOpenCaptureRoot(t *testing.T) {
	t.Parallel()
	data := t.TempDir()
	_, err := openCaptureRoot(data, false)
	require.ErrorIs(t, err, ErrUnsafePath)
	root, err := openCaptureRoot(data, true)
	require.NoError(t, err)
	fi, err := os.Lstat(filepath.Join(data, "captures"))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o700), fi.Mode().Perm())
	_ = root.Close()

	root, err = openCaptureRoot(data, false)
	require.NoError(t, err)
	_ = root.Close()

	require.NoError(t, os.Chmod(filepath.Join(data, "captures"), 0o755))
	_, err = openCaptureRoot(data, false)
	require.ErrorIs(t, err, ErrUnsafePath)
}

func TestContainment(t *testing.T) {
	t.Parallel()
	sentinel := t.TempDir()
	data, root := testRoot(t)
	require.NoError(t, os.Mkdir(filepath.Join(data, "captures", "agent"), 0o700))
	good := filepath.Join(data, "captures", "agent", "f")
	require.NoError(t, os.WriteFile(good, []byte("x"), 0o600))
	_, err := openChecked(root, "agent/f", false)
	require.NoError(t, err)

	// Modes.
	for _, mode := range []os.FileMode{0o755, 0o644} {
		require.NoError(t, os.Chmod(good, mode))
		_, err := openChecked(root, "agent/f", false)
		require.ErrorIs(t, err, ErrUnsafePath)
	}
	require.NoError(t, os.Chmod(good, 0o600))

	// A second link is never legitimate.
	require.NoError(t, os.Link(good, filepath.Join(data, "captures", "agent", "g")))
	_, err = openChecked(root, "agent/f", false)
	require.ErrorIs(t, err, ErrUnsafePath)
	require.NoError(t, os.Remove(filepath.Join(data, "captures", "agent", "g")))

	// Symlinks, inside and outside the root.
	require.NoError(t, os.Symlink(good, filepath.Join(data, "captures", "agent", "link")))
	_, err = openChecked(root, "agent/link", false)
	require.ErrorIs(t, err, ErrUnsafePath)
	outside := filepath.Join(sentinel, "out")
	require.NoError(t, os.WriteFile(outside, []byte("y"), 0o600))
	require.NoError(t, os.Symlink(outside, filepath.Join(data, "captures", "evil")))
	_, err = openChecked(root, "evil", false)
	require.ErrorIs(t, err, ErrUnsafePath)
	require.Equal(t, []byte("y"), mustReadFile(t, outside))

	// Directories.
	_, err = openChecked(root, "agent", true)
	require.NoError(t, err)
	_, err = openChecked(root, "agent/f", true)
	require.ErrorIs(t, err, ErrUnsafePath)

	// Ids that escape.
	for _, bad := range []string{"../x", "/a", "a/../../b"} {
		_, err := openChecked(root, bad, false)
		require.Error(t, err, bad)
	}
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	return b
}

func writeControl(t *testing.T, data, rel, content string) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(data, "captures", rel), []byte(content), 0o600))
}

func goodSession() string {
	return `{"schema":"gifford.capture.session.v1","capture_session_id":"cap-01AAAAAAAAAAAAAAAAAAAAAAAA",` +
		`"agent_id":"agent-1","harness":"muse-code","node_id":"capture-0123456789abcdef0123456789abcdef",` +
		`"tmux":{"socket":"/tmp/sock","server_pid":1234,"server_start_time":1793231354,"session_id":"$0",` +
		`"session_name":"shell","window_index":3,"pane_index":0},"limits":` + goodLimits() + `}`
}

func goodLimits() string {
	raw, err := json.Marshal(DefaultLimits())
	if err != nil {
		panic(err)
	}
	return string(raw)
}

func TestControlFilesStrict(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		file   string
		limit  int64
		good   string
		fields func() controlFile
		bads   []string
	}{
		{"session", "session.json", 16384, goodSession(), func() controlFile { return &SessionMeta{} }, []string{
			`{}`,
			strings.Replace(goodSession(), `"harness":"muse-code"`, `"harness":"nope"`, 1),
			strings.Replace(goodSession(), `"socket":"/tmp/sock"`, `"socket":"rel"`, 1),
			strings.Replace(goodSession(), `"chunk_max_bytes":16384`, `"chunk_max_bytes":0`, 1),
		}},
		{testReasonQuota, "quota.json", 4096, `{"schema":"gifford.capture.quota.v1","reserved_bytes":131072}`, func() controlFile { return &quotaLedger{} }, []string{
			`{"schema":"gifford.capture.quota.v1","reserved_bytes":-1}`,
			`{"schema":"gifford.capture.quota.v1"}`,
		}},
		{"stop", "stop.request", 4096, `{"schema":"gifford.capture.stop.v1","reason":"manual","detail":"x","requested_at":"2026-10-29T00:00:00Z"}`, func() controlFile { return &stopRequest{} }, []string{
			`{"schema":"gifford.capture.stop.v1","reason":"force","detail":"x","requested_at":"2026-10-29T00:00:00Z"}`,
			`{"schema":"gifford.capture.stop.v1","reason":"manual","detail":"x","requested_at":"never"}`,
		}},
		{"exit", "ingest.exit", 4096, `{"schema":"gifford.capture.exit.v1","cause":"eof","at":"2026-10-29T00:00:00Z","pid":42,"attempt":"0123456789abcdef0123456789abcdef"}`, func() controlFile { return &ingestExit{} }, []string{
			`{"schema":"gifford.capture.exit.v1","cause":"maybe","at":"2026-10-29T00:00:00Z","pid":42,"attempt":"0123456789abcdef0123456789abcdef"}`,
			`{"schema":"gifford.capture.exit.v1","cause":"eof","at":"2026-10-29T00:00:00Z","pid":42,"attempt":"nope"}`,
		}},
	}
	for _, tc := range cases {
		data, root := testRoot(t)
		writeControl(t, data, tc.file, tc.good)
		require.NoError(t, readControl(root, tc.file, tc.limit, tc.fields()), tc.name)
		for i, bad := range tc.bads {
			writeControl(t, data, tc.file+".bad", bad)
			require.NoError(t, os.Rename(filepath.Join(data, "captures", tc.file+".bad"), filepath.Join(data, "captures", tc.file)))
			require.ErrorIs(t, readControl(root, tc.file, tc.limit, tc.fields()), ErrUnsafePath, "%s/%d", tc.name, i)
		}
		// Truncated, oversized, unknown field, trailing data, wrong schema.
		writeControl(t, data, tc.file, tc.good[:len(tc.good)/2])
		require.ErrorIs(t, readControl(root, tc.file, tc.limit, tc.fields()), ErrUnsafePath, tc.name)
		writeControl(t, data, tc.file, strings.Repeat("x", int(tc.limit)+1))
		require.ErrorIs(t, readControl(root, tc.file, tc.limit, tc.fields()), ErrUnsafePath, tc.name)
		writeControl(t, data, tc.file, strings.Replace(tc.good, `"schema"`, `"schema","zz":1`, 1))
		require.ErrorIs(t, readControl(root, tc.file, tc.limit, tc.fields()), ErrUnsafePath, tc.name)
		writeControl(t, data, tc.file, tc.good+" {}")
		require.ErrorIs(t, readControl(root, tc.file, tc.limit, tc.fields()), ErrUnsafePath, tc.name)
		writeControl(t, data, tc.file, strings.Replace(tc.good, "gifford.capture.", "gifford.capture.xx.", 1))
		require.ErrorIs(t, readControl(root, tc.file, tc.limit, tc.fields()), ErrUnsafePath, tc.name)
		writeControl(t, data, tc.file, tc.good)
		require.NoError(t, os.Chmod(filepath.Join(data, "captures", tc.file), 0o644))
		require.ErrorIs(t, readControl(root, tc.file, tc.limit, tc.fields()), ErrUnsafePath, tc.name)
	}
}

func TestNodeIDStrict(t *testing.T) {
	t.Parallel()
	data, root := testRoot(t)
	good := "capture-0123456789abcdef0123456789abcdef\n"
	writeControl(t, data, "node-id", good)
	got, err := readNodeID(root)
	require.NoError(t, err)
	require.Equal(t, strings.TrimSuffix(good, "\n"), got)
	for _, bad := range []string{"capture-xyz\n", "capture-0123456789abcdef0123456789abcdef", "capture-0123456789abcdef0123456789ABCDEF\n", ""} {
		writeControl(t, data, "node-id", bad)
		_, err := readNodeID(root)
		require.ErrorIs(t, err, ErrUnsafePath, "%q", bad)
	}
}

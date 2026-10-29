//go:build tmux

package capturetest

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/smellak/gifford/internal/capture"
	"github.com/smellak/gifford/internal/clock"
)

// minTmux is the oldest tmux the suite runs against.
const minTmuxMajor, minTmuxMinor = 3, 4

// TestMain gates the suite on tmux, then dispatches the in-binary
// helper and ingest modes. The gate fails, never skips.
func TestMain(m *testing.M) {
	if len(os.Args) > 2 && os.Args[1] == "capture" && os.Args[2] == "ingest" {
		runIngestMode(os.Args[3:])
	}
	if len(os.Args) > 1 && os.Args[1] == "helper" {
		runHelperMode(os.Args[2:])
	}
	out, err := exec.Command("tmux", "-V").Output()
	if err != nil {
		fmt.Fprintf(os.Stderr, "test/capture requires tmux: %v\n", err)
		os.Exit(1)
	}
	major, minor, err := parseTmuxVersion(strings.TrimSpace(string(out)))
	if err != nil || major < minTmuxMajor || (major == minTmuxMajor && minor < minTmuxMinor) {
		fmt.Fprintf(os.Stderr, "test/capture requires tmux >= 3.4, have %q\n", strings.TrimSpace(string(out)))
		os.Exit(1)
	}
	os.Exit(m.Run())
}

// parseTmuxVersion reads the "tmux X.Y" banner.
func parseTmuxVersion(banner string) (int, int, error) {
	rest, ok := strings.CutPrefix(banner, "tmux ")
	if !ok {
		return 0, 0, fmt.Errorf("bad tmux banner %q", banner)
	}
	major, minor, ok := strings.Cut(rest, ".")
	if !ok {
		return 0, 0, fmt.Errorf("bad tmux version %q", rest)
	}
	ma, err := strconv.Atoi(major)
	if err != nil {
		return 0, 0, err
	}
	mi, err := strconv.Atoi(strings.TrimRight(minor, "abcdefghijklmnopqrstuvwxyz"))
	if err != nil {
		return 0, 0, err
	}
	return ma, mi, nil
}

// testMono is a monotonic source for synthetic runs.
type testMono struct {
	start time.Time
}

// MonotonicNS returns nanoseconds since the run started.
func (m testMono) MonotonicNS() int64 {
	return time.Since(m.start).Nanoseconds()
}

// runIngestMode dispatches `capture ingest …` to the ingest entry
// point: tmux's pipe command runs this same binary, with the pane
// stream on stdin.
func runIngestMode(argv []string) {
	var opts capture.IngestOptions
	for i := 0; i < len(argv)-1; i++ {
		switch argv[i] {
		case "--session":
			opts.Session = argv[i+1]
		case "--agent":
			opts.Agent = argv[i+1]
		case "--data-dir":
			opts.Data = argv[i+1]
		case "--start-sequence":
			n, err := strconv.ParseInt(argv[i+1], 10, 64)
			if err != nil {
				fmt.Fprintf(os.Stderr, "ingest: %v\n", err)
				os.Exit(2)
			}
			opts.StartSequence = n
		case "--attempt":
			opts.Attempt = argv[i+1]
		}
	}
	if opts.Session == "" || opts.Agent == "" || opts.Data == "" || opts.Attempt == "" {
		fmt.Fprintln(os.Stderr, "ingest: missing session, agent, data-dir or attempt")
		os.Exit(2)
	}
	opts.In = os.Stdin
	// The runner stays nil: the ingest run never dials tmux, it only
	// reads the pipe and writes the store.
	deps := capture.IngestDeps{
		PID:   os.Getpid(),
		Clock: clock.System{},
		Mono:  testMono{start: time.Now()},
	}
	cause, err := capture.Ingest(context.Background(), deps, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ingest: %s: %v\n", cause, err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "ingest: %s\n", cause)
}

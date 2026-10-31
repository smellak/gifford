//go:build unix

package fuzz

import (
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"golang.org/x/term"
)

// The test binary runs again as a harness, the way gifford fuzz runs
// gifford again: harnessArg is the fake harness, deafArg one that draws
// its prompt and then reads keys without ever answering.
const (
	harnessArg = "fuzz-harness"
	deafArg    = "fuzz-deaf"
)

func TestMain(m *testing.M) {
	if len(os.Args) == 3 {
		switch os.Args[1] {
		case harnessArg:
			if err := Harness(os.Stdin, os.Stdout, os.Args[2]); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			os.Exit(0)
		case deafArg:
			if _, err := term.MakeRaw(int(os.Stdin.Fd())); err != nil {
				os.Exit(1)
			}
			_, _ = io.WriteString(os.Stdout, "fz ready\r\n"+freshArea+drawn)
			_, _ = io.Copy(io.Discard, os.Stdin)
			os.Exit(1)
		}
	}
	// A race-enabled harness sleeps a second at exit for late race
	// reports, and every scenario waits for its harness to exit.
	_ = os.Setenv("GORACE", strings.TrimSpace(os.Getenv("GORACE")+" atexit_sleep_ms=0"))
	os.Exit(m.Run())
}

// testHarness is a harness command: this test binary again, as mode.
func testHarness(t *testing.T, mode string) []string {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return []string{exe, mode}
}

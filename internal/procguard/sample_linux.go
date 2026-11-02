// Package procguard samples live process subtrees from /proc; see SampleTree.
package procguard

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
)

// ErrNoRoot names a root pid with no live /proc entry.
var ErrNoRoot = errors.New("procguard: root process is gone")

// SampleTree returns the entries of pid and all its descendants. Every entry
// carries pid, start time (stat field 22), comm and cmdline. Exe is never
// read: across users its readlink fails. A stat that vanishes mid-read is
// skipped; only a dead root is an error.
func SampleTree(pid int) ([]Entry, error) {
	root := filepath.Join("/proc", strconv.Itoa(pid), "stat")
	if _, err := os.Stat(root); err != nil {
		return nil, fmt.Errorf("%w: %d", ErrNoRoot, pid)
	}
	var out []Entry
	queue := []int{pid}
	seen := map[int]bool{pid: true}
	for len(queue) > 0 {
		p := queue[0]
		queue = queue[1:]
		if e, err := readEntry(p); err == nil {
			out = append(out, e)
		}
		for _, c := range childrenOf(p) {
			if !seen[c] {
				seen[c] = true
				queue = append(queue, c)
			}
		}
	}
	return out, nil
}

func readEntry(pid int) (Entry, error) {
	dir := filepath.Join("/proc", strconv.Itoa(pid))
	stat, err := os.ReadFile(dir + "/stat")
	if err != nil {
		return Entry{}, fmt.Errorf("procguard: read stat of %d: %w", pid, err)
	}
	start, err := parseStartTime(string(stat))
	if err != nil {
		return Entry{}, err
	}
	comm, _ := os.ReadFile(dir + "/comm")
	raw, _ := os.ReadFile(dir + "/cmdline")
	return Entry{
		PID:       pid,
		StartTime: start,
		Comm:      strings.TrimSpace(string(comm)),
		Program:   programName(raw),
	}, nil
}

// programName keeps only the basename of argv[0]. Arguments, where other
// users' secrets may hide, are dropped before the entry is built.
func programName(cmdline []byte) string {
	argv := strings.Split(string(cmdline), "\x00")
	if len(argv) == 0 || argv[0] == "" {
		return ""
	}
	return path.Base(argv[0])
}

// parseStartTime extracts stat field 22, the start time. Comm sits in
// parentheses and may hold spaces or parens itself, so fields split only
// after its close.
func parseStartTime(stat string) (uint64, error) {
	rest := stat[strings.LastIndex(stat, ")")+1:]
	fields := strings.Fields(rest)
	if len(fields) < 20 {
		return 0, fmt.Errorf("procguard: short stat %q", stat)
	}
	got, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("procguard: parse start time in %q: %w", stat, err)
	}
	return got, nil
}

func childrenOf(pid int) []int {
	pattern := filepath.Join("/proc", strconv.Itoa(pid), "task", "*", "children")
	var out []int
	for _, file := range mustGlob(pattern) {
		raw, err := os.ReadFile(file)
		if err != nil {
			continue
		}
		for _, f := range strings.Fields(string(raw)) {
			if c, err := strconv.Atoi(f); err == nil {
				out = append(out, c)
			}
		}
	}
	return out
}

func mustGlob(pattern string) []string {
	got, _ := filepath.Glob(pattern)
	return got
}

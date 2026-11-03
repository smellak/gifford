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

// SampleProcess returns one live process identity. A missing process is
// ErrNoRoot; malformed or unreadable live metadata remains a distinct error
// so recovery never mistakes unavailable evidence for process exit.
func SampleProcess(pid int) (Entry, error) {
	if pid <= 0 {
		return Entry{}, fmt.Errorf("%w: %d", ErrNoRoot, pid)
	}
	entry, err := readEntry(pid)
	if err == nil {
		return entry, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return Entry{}, fmt.Errorf("%w: %d", ErrNoRoot, pid)
	}
	return Entry{}, err
}

// SampleTree returns the entries of pid and all its descendants. Every entry
// carries pid, start time (stat field 22), comm, Program, Argv0 and a best-effort
// Exe readlink (empty when access is denied). The root is read first and a dead
// root is ErrNoRoot, never an empty sample; descendants that vanish mid-read
// are skipped.
func SampleTree(pid int) ([]Entry, error) {
	root, err := readEntry(pid)
	if err != nil {
		return nil, fmt.Errorf("%w: %d", ErrNoRoot, pid)
	}
	out := []Entry{root}
	queue := []int{pid}
	seen := map[int]bool{pid: true}
	for len(queue) > 0 {
		p := queue[0]
		queue = queue[1:]
		for _, c := range childrenOf(p) {
			if seen[c] {
				continue
			}
			seen[c] = true
			if e, err := readEntry(c); err == nil {
				out = append(out, e)
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
	e, err := parseStat(pid, string(stat))
	if err != nil {
		return Entry{}, err
	}
	comm, _ := os.ReadFile(dir + "/comm")
	raw, _ := os.ReadFile(dir + "/cmdline")
	e.Comm = strings.TrimSpace(string(comm))
	e.Program = programName(raw)
	e.Argv0 = argvZero(raw)
	e.Exe, _ = os.Readlink(dir + "/exe")
	return e, nil
}

// programName keeps only the basename of argv[0]. Arguments, where other
// users' secrets may hide, are dropped before the entry is built.
func programName(cmdline []byte) string {
	argv0 := argvZero(cmdline)
	if argv0 == "" {
		return ""
	}
	return path.Base(argv0)
}

// argvZero retains no argument after the executable name.
func argvZero(cmdline []byte) string {
	argv0, _, _ := strings.Cut(string(cmdline), "\x00")
	return argv0
}

// parseStat extracts one process row from a stat line in a single pass:
// state, ppid, pgrp and tpgid (fields 3, 4, 5 and 8) plus the start time
// (field 22). Comm sits in parentheses and may hold spaces or parens
// itself, so fields split only after its close.
func parseStat(pid int, stat string) (Entry, error) {
	end := strings.LastIndex(stat, ")")
	if end < 0 {
		return Entry{}, fmt.Errorf("procguard: no comm close in %q", stat)
	}
	rest := stat[end+1:]
	fields := strings.Fields(rest)
	if len(fields) < 20 {
		return Entry{}, fmt.Errorf("procguard: short stat %q", stat)
	}
	atoi := func(field string, index int) (int, error) {
		got, err := strconv.Atoi(fields[index])
		if err != nil {
			return 0, fmt.Errorf("procguard: parse %s in %q: %w", field, stat, err)
		}
		return got, nil
	}
	var e Entry
	var err error
	e.PID = pid
	e.State = fields[0]
	if e.PPID, err = atoi("ppid", 1); err != nil {
		return Entry{}, err
	}
	if e.Pgrp, err = atoi("pgrp", 2); err != nil {
		return Entry{}, err
	}
	if e.TPGID, err = atoi("tpgid", 5); err != nil {
		return Entry{}, err
	}
	start, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		return Entry{}, fmt.Errorf("procguard: parse start time in %q: %w", stat, err)
	}
	e.StartTime = start
	return e, nil
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

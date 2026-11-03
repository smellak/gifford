// Package procguard decides busy or free for tmux-attached panels from the
// live process tree, compared against a resting baseline. It is the P3
// process leg of the pre-write guard: a new process since the baseline means
// the agent is working. Screen and keyboard vote separately; any leg saying
// busy stops the write.
package procguard

import (
	"errors"
	"sync"
)

// ErrNoRoot names a root pid with no live process entry.
var ErrNoRoot = errors.New("procguard: root process is gone")

// Verdict is the guard outcome for one delivery attempt.
type Verdict int

const (
	// Free means no new process appeared since the resting baseline.
	Free Verdict = iota
	// Busy means a process is new after the baseline: an unseen pid, or a
	// seen pid with a different start time.
	Busy
	// Uncertain means the sample cannot prove either state: a baseline
	// member is gone without a confirmed rest, or the panel has no baseline.
	// Callers must not write and must not retry on their own.
	Uncertain
)

// Entry is one process row of a baseline or a fresh sample. Identity is PID
// plus start time (proc field 22); a reused pid with another start time is a
// different process. Comm and Program describe the shape but never identify.
// Program is only the basename of argv[0]: other users' argv may carry
// secrets, so arguments are never retained, logged or stored. Argv0 keeps
// only the program path for the measured auxiliary rule; Exe is an optional
// kernel cross-check, since its readlink is not available across users.
type Entry struct {
	PID       int
	StartTime uint64
	Comm      string
	Program   string
	Argv0     string
	Exe       string
	// PPID, Pgrp, State and TPGID come from the same single stat read
	// (fields 4, 5, 3 and 8). They let the tmux pre-write check build its
	// own process view from this entry instead of sampling /proc twice.
	PPID  int
	Pgrp  int
	State string
	TPGID int
}

// Guard keeps one resting baseline per panel. The zero value is ready; every
// method is safe for concurrent use.
type Guard struct {
	mu       sync.Mutex
	baseline map[string]map[int]Entry
}

// Rebaseline captures entries as the resting baseline of panel. It is only
// called at confirmed rest, so long-lived auxiliaries present here join the
// baseline instead of reporting busy forever.
func (g *Guard) Rebaseline(panel string, entries []Entry) {
	base := make(map[int]Entry, len(entries))
	for _, e := range entries {
		base[e.PID] = e
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.baseline == nil {
		g.baseline = make(map[string]map[int]Entry)
	}
	g.baseline[panel] = base
}

// Check compares a fresh sample of panel against its resting baseline.
func (g *Guard) Check(panel string, fresh []Entry) Verdict {
	g.mu.Lock()
	base, ok := g.baseline[panel]
	g.mu.Unlock()
	if !ok {
		return Uncertain
	}
	seen := make(map[int]uint64, len(fresh))
	for _, e := range fresh {
		seen[e.PID] = e.StartTime
		if old, found := base[e.PID]; !found || old.StartTime != e.StartTime {
			return Busy
		}
	}
	for pid := range base {
		if _, found := seen[pid]; !found {
			return Uncertain
		}
	}
	return Free
}

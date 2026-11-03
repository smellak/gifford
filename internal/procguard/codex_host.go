package procguard

import "path/filepath"

const (
	codexNativeProgram       = "codex"
	codexCodeModeHostProgram = "codex-code-mode-host"
	codexCodeModeHostComm    = "codex-code-mode"
)

// CodexWorkCensus also resolves the npm launcher registered as node/codex.
// Its one direct native child must be unambiguous before either warming a
// baseline or checking it. The bool is false when that launcher shape cannot
// be proved: the caller must vote Busy and must not establish a baseline.
// Registry verification owns the registered argv0/argv1 pair; this sample
// binds its PID/start and the native child's live path without retaining args.
func CodexWorkCensus(entries []Entry, harnessPID int, harnessStart uint64, harnessProgram, harnessScript string) ([]Entry, bool) {
	if harnessProgram != "node" {
		return WithoutCodexCodeModeHost(entries, harnessPID, harnessStart, harnessProgram), true
	}
	if harnessScript != codexNativeProgram || harnessPID <= 0 || harnessStart == 0 {
		return entries, false
	}
	registered := false
	var native Entry
	natives := 0
	for _, entry := range entries {
		if entry.PID == harnessPID && entry.StartTime == harnessStart {
			registered = true
		}
		if entry.PPID == harnessPID && entry.Comm == codexNativeProgram {
			native = entry
			natives++
		}
	}
	if !registered || natives != 1 || native.PID <= 0 || native.StartTime == 0 ||
		!filepath.IsAbs(native.Argv0) || filepath.Base(native.Argv0) != codexNativeProgram {
		return entries, false
	}
	return WithoutCodexCodeModeHost(entries, native.PID, native.StartTime, codexNativeProgram), true
}

// WithoutCodexCodeModeHost removes only the measured Codex auxiliary from
// the work census. Codex 0.154.0 starts this sibling executable lazily as a
// direct child of its native CLI; its birth alone is not a new tool. Bind
// the registered PID/start and program basename to its live absolute argv0,
// then require the child's exact sibling argv0, comm and direct parent.
// Across users exe cannot be read; when readable it must agree as well.
// Keep every descendant of the host: a tool it spawns must still vote Busy.
// Use this same projection for both the resting baseline and fresh samples.
func WithoutCodexCodeModeHost(entries []Entry, harnessPID int, harnessStart uint64, harnessProgram string) []Entry {
	if harnessPID <= 0 || harnessStart == 0 {
		return entries
	}
	var argv0 string
	for _, entry := range entries {
		if entry.PID == harnessPID && entry.StartTime == harnessStart {
			argv0 = entry.Argv0
			break
		}
	}
	if harnessProgram != codexNativeProgram || !filepath.IsAbs(argv0) || filepath.Base(argv0) != harnessProgram {
		return entries
	}
	hostExecutable := filepath.Join(filepath.Dir(argv0), codexCodeModeHostProgram)
	work := make([]Entry, 0, len(entries))
	for _, entry := range entries {
		if entry.PPID == harnessPID && entry.Argv0 == hostExecutable && entry.Comm == codexCodeModeHostComm &&
			(entry.Exe == "" || entry.Exe == hostExecutable) {
			continue
		}
		work = append(work, entry)
	}
	return work
}

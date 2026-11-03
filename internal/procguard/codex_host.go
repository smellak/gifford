package procguard

import "path/filepath"

const (
	codexCodeModeHostProgram = "codex-code-mode-host"
	codexCodeModeHostComm    = "codex-code-mode"
)

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
	if harnessProgram != "codex" || !filepath.IsAbs(argv0) || filepath.Base(argv0) != harnessProgram {
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

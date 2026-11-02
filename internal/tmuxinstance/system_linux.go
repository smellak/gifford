//go:build linux

package tmuxinstance

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
)

var (
	bootIDPattern    = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	machineIDPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)
)

type procSystem struct {
	proc string
	etc  string
}

func newSystem() system { return procSystem{proc: "/proc", etc: "/etc"} }

func (s procSystem) bootID() (string, error) {
	return readID(filepath.Join(s.proc, "sys", "kernel", "random", "boot_id"), bootIDPattern)
}

func (s procSystem) machineID() (string, error) {
	return readID(filepath.Join(s.etc, "machine-id"), machineIDPattern)
}

func readID(path string, pattern *regexp.Regexp) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", path, err)
	}
	id := strings.TrimSpace(string(raw))
	if !pattern.MatchString(id) {
		return "", fmt.Errorf("read %s: malformed: %w", path, ErrInvalidArgument)
	}
	return id, nil
}

// process reads stat, then status, then stat again: a start time that
// changed in between means the PID was reused while it was read.
func (s procSystem) process(pid int64) (procInfo, error) {
	if pid <= 0 {
		return procInfo{}, fmt.Errorf("process %d: %w", pid, ErrInvalidArgument)
	}
	dir := filepath.Join(s.proc, strconv.FormatInt(pid, 10))
	info, err := readStat(filepath.Join(dir, "stat"))
	if err != nil {
		return procInfo{}, err
	}
	raw, err := os.ReadFile(filepath.Join(dir, "status"))
	if errors.Is(err, fs.ErrNotExist) {
		return procInfo{}, fmt.Errorf("process %d: %w", pid, errProcessGone)
	}
	if err != nil {
		return procInfo{}, fmt.Errorf("read process %d status: %w", pid, err)
	}
	if info.uid, err = statusID(string(raw), "Uid:"); err != nil {
		return procInfo{}, fmt.Errorf("process %d: %w", pid, err)
	}
	if info.gid, err = statusID(string(raw), "Gid:"); err != nil {
		return procInfo{}, fmt.Errorf("process %d: %w", pid, err)
	}
	again, err := readStat(filepath.Join(dir, "stat"))
	if err != nil {
		return procInfo{}, err
	}
	if again.start != info.start {
		return procInfo{}, fmt.Errorf("process %d: %w", pid, errProcessGone)
	}
	return info, nil
}

func readStat(path string) (procInfo, error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return procInfo{}, fmt.Errorf("read %s: %w", path, errProcessGone)
	}
	if err != nil {
		return procInfo{}, fmt.Errorf("read %s: %w", path, err)
	}
	text := string(raw)
	open, closing := strings.IndexByte(text, '('), strings.LastIndexByte(text, ')')
	if open < 0 || closing < open {
		return procInfo{}, fmt.Errorf("parse %s: %w", path, ErrInvalidArgument)
	}
	fields := strings.Fields(text[closing+1:])
	if len(fields) < 20 {
		return procInfo{}, fmt.Errorf("parse %s: %w", path, ErrInvalidArgument)
	}
	if fields[0] == "Z" || fields[0] == "X" {
		return procInfo{}, fmt.Errorf("read %s: %w", path, errProcessGone)
	}
	ppid, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil || ppid < 0 {
		return procInfo{}, fmt.Errorf("parse %s parent: %w", path, ErrInvalidArgument)
	}
	start, err := strconv.ParseInt(fields[19], 10, 64)
	if err != nil || start < 0 {
		return procInfo{}, fmt.Errorf("parse %s start time: %w", path, ErrInvalidArgument)
	}
	return procInfo{ppid: ppid, start: start, comm: text[open+1 : closing]}, nil
}

// statusID returns the real id of a "Uid:" or "Gid:" line.
func statusID(status, key string) (uint32, error) {
	for line := range strings.SplitSeq(status, "\n") {
		rest, found := strings.CutPrefix(line, key)
		if !found {
			continue
		}
		fields := strings.Fields(rest)
		if len(fields) == 0 {
			break
		}
		id, err := strconv.ParseUint(fields[0], 10, 32)
		if err != nil {
			break
		}
		return uint32(id), nil
	}
	return 0, fmt.Errorf("status %s: %w", key, ErrInvalidArgument)
}

// children lists the direct children of every thread of pid.
func (s procSystem) children(pid int64) ([]int64, error) {
	taskDir := filepath.Join(s.proc, strconv.FormatInt(pid, 10), "task")
	tasks, err := os.ReadDir(taskDir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("process %d: %w", pid, errProcessGone)
	}
	if err != nil {
		return nil, fmt.Errorf("read process %d tasks: %w", pid, err)
	}
	seen := make(map[int64]struct{})
	var out []int64
	for _, task := range tasks {
		raw, err := os.ReadFile(filepath.Join(taskDir, task.Name(), "children"))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read process %d children: %w", pid, err)
		}
		for _, field := range strings.Fields(string(raw)) {
			child, err := strconv.ParseInt(field, 10, 64)
			if err != nil || child <= 0 {
				return nil, fmt.Errorf("parse process %d children: %w", pid, ErrInvalidArgument)
			}
			if _, dup := seen[child]; !dup {
				seen[child] = struct{}{}
				out = append(out, child)
			}
		}
	}
	return out, nil
}

func (procSystem) socketDir(socket string) (dirInfo, error) {
	dir := filepath.Dir(socket)
	fi, err := os.Lstat(dir)
	if err != nil {
		return dirInfo{}, fmt.Errorf("lstat socket directory %s: %w", dir, err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return dirInfo{}, fmt.Errorf("lstat socket directory %s: %w", dir, ErrInvalidArgument)
	}
	return dirInfo{
		uid: st.Uid, perm: fi.Mode().Perm(),
		dir: fi.IsDir(), symlink: fi.Mode()&fs.ModeSymlink != 0,
	}, nil
}

func (procSystem) lookupUser(name string) (uint32, error) {
	found, err := user.Lookup(name)
	if err != nil {
		return 0, fmt.Errorf("look up user %q: %w", name, err)
	}
	uid, err := strconv.ParseUint(found.Uid, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("look up user %q: uid %q: %w", name, found.Uid, ErrInvalidArgument)
	}
	return uint32(uid), nil
}

// fileOwner returns the uid that owns fi.
func fileOwner(fi fs.FileInfo) (uint32, bool) {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return st.Uid, true
}

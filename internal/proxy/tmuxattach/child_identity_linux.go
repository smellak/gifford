//go:build linux

package tmuxattach

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strconv"
)

func observeChildProcess(pid int) (PaneChild, error) {
	if pid <= 0 {
		return PaneChild{}, errors.New("observe child process: invalid pid")
	}
	raw, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return PaneChild{}, fmt.Errorf("observe child process: read stat: %w", err)
	}
	return parseChildProcessStat(pid, raw)
}

func parseChildProcessStat(pid int, raw []byte) (PaneChild, error) {
	if pid <= 0 {
		return PaneChild{}, errors.New("observe child process: invalid pid")
	}
	closing := bytes.LastIndexByte(raw, ')')
	if closing < 1 || closing+2 >= len(raw) || raw[closing+1] != ' ' {
		return PaneChild{}, errors.New("observe child process: malformed stat")
	}
	fields := bytes.Fields(raw[closing+2:])
	// After comm, fields begins with process field 3. Start time is field 22.
	if len(fields) < 20 {
		return PaneChild{}, errors.New("observe child process: short stat")
	}
	start, err := strconv.ParseInt(string(fields[19]), 10, 64)
	if err != nil || start <= 0 {
		return PaneChild{}, errors.New("observe child process: invalid start time")
	}
	return PaneChild{PID: int64(pid), Start: start}, nil
}

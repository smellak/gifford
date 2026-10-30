//go:build linux

package hub

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/smellak/gifford/internal/protocol"
)

type procSessionRuntime struct {
	root string
}

func newSessionProcessRuntime() sessionProcessRuntime {
	return &procSessionRuntime{root: "/proc"}
}

func (r *procSessionRuntime) bootID() (string, error) {
	raw, err := os.ReadFile(filepath.Join(r.root, "sys", "kernel", "random", "boot_id"))
	if err != nil {
		return "", fmt.Errorf("read session boot identity: %w", err)
	}
	bootID := strings.TrimSpace(string(raw))
	if err := validateBootID(bootID); err != nil {
		return "", err
	}
	return bootID, nil
}

func (r *procSessionRuntime) alive(process protocol.ProcessIdentity, bootID string) error {
	if err := process.Validate(); err != nil {
		return fmt.Errorf("validate session process: %w", err)
	}
	currentBoot, err := r.bootID()
	if err != nil {
		return err
	}
	if currentBoot != bootID {
		return fmt.Errorf("session boot identity changed: %w", errInvalidArgument)
	}
	start, err := readSessionStartTime(filepath.Join(r.root, strconv.FormatInt(process.PID, 10), "stat"))
	if err != nil {
		return err
	}
	if start != process.ProcessStartTime {
		return fmt.Errorf("session process identity changed: %w", errInvalidArgument)
	}
	return nil
}

func readSessionStartTime(path string) (int64, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, fmt.Errorf("read session process: %w", err)
	}
	closing := strings.LastIndexByte(string(raw), ')')
	if closing < 0 {
		return 0, fmt.Errorf("parse session process: %w", errInvalidArgument)
	}
	fields := strings.Fields(string(raw[closing+1:]))
	if len(fields) < 20 {
		return 0, fmt.Errorf("parse session process: %w", errInvalidArgument)
	}
	if fields[0] == "Z" || fields[0] == "X" {
		return 0, fmt.Errorf("session process exited: %w", errInvalidArgument)
	}
	start, err := strconv.ParseInt(fields[19], 10, 64)
	if err != nil || start < 0 {
		return 0, fmt.Errorf("parse session process start: %w", errInvalidArgument)
	}
	return start, nil
}

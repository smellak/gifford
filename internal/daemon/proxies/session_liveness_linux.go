//go:build linux

package proxies

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/smellak/gifford/internal/protocol"
)

type procSessionLiveness struct {
	root string
}

func newSessionLiveness() sessionLiveness {
	return &procSessionLiveness{root: "/proc"}
}

func (p *procSessionLiveness) alive(process protocol.ProcessIdentity, bootID string) error {
	if process.Validate() != nil || bootID == "" || len(bootID) > 128 {
		return fmt.Errorf("invalid detached process identity")
	}
	rawBoot, err := os.ReadFile(filepath.Join(p.root, "sys", "kernel", "random", "boot_id"))
	if err != nil {
		return fmt.Errorf("read detached boot identity: %w", err)
	}
	if strings.TrimSpace(string(rawBoot)) != bootID {
		return fmt.Errorf("detached boot identity changed")
	}
	raw, err := os.ReadFile(filepath.Join(p.root, strconv.FormatInt(process.PID, 10), "stat"))
	if err != nil {
		return fmt.Errorf("read detached process: %w", err)
	}
	closing := strings.LastIndexByte(string(raw), ')')
	if closing < 0 {
		return fmt.Errorf("parse detached process")
	}
	fields := strings.Fields(string(raw[closing+1:]))
	if len(fields) < 20 || fields[0] == "Z" || fields[0] == "X" {
		return fmt.Errorf("detached process exited")
	}
	start, err := strconv.ParseInt(fields[19], 10, 64)
	if err != nil || start != process.ProcessStartTime {
		return fmt.Errorf("detached process identity changed")
	}
	return nil
}

//go:build !linux

package orchestrator

import (
	"errors"

	"github.com/smellak/gifford/internal/procguard"
)

// defaultSampleTree fails closed off Linux: without /proc there is no process
// evidence, so no tmux delivery is authorized.
func defaultSampleTree(int) ([]procguard.Entry, error) { return nil, errors.ErrUnsupported }

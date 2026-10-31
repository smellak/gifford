//go:build !unix

package nodeproc

import (
	"time"

	"github.com/smellak/gifford/internal/xdg"
)

// Start fails with ErrUnsupported: the node runs in the background on Unix
// only.
func Start(xdg.Dirs, []string, time.Duration) (int, error) { return 0, ErrUnsupported }

// Stop fails with ErrUnsupported: the node runs in the background on Unix
// only.
func Stop(xdg.Dirs, []string, time.Duration) (int, error) { return 0, ErrUnsupported }

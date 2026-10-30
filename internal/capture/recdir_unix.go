//go:build unix

package capture

import (
	"fmt"

	"github.com/smellak/gifford/internal/proxy/session"
)

// recordable reports a system whose file modes keep recordings private.
const recordable = true

// checkRecordingsDir makes the check a run makes before it records.
func checkRecordingsDir(dir string) error {
	if err := session.CheckRecordingsDir(dir); err != nil {
		return fmt.Errorf("capture: %w", err)
	}
	return nil
}

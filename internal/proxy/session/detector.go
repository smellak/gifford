//go:build unix

package session

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/smellak/gifford/internal/detect"
	"github.com/smellak/gifford/internal/protocol"
)

// detectorHarnesses maps a measured Proxy profile to the detector profile
// measured for the same build. A harness absent here sends no verdict.
var detectorHarnesses = map[string]detect.Harness{
	"claude-code": detect.HarnessClaude,
	"codex":       detect.HarnessCodex,
	"muse-code":   detect.HarnessMuse,
	"opencode":    detect.HarnessOpenCode,
}

// detectorTable is read only: Classify never writes the table.
var detectorTable = detect.DefaultTable()

// detectorLocked reads the live screen with the detector, as the tmux route
// reads a pane dump: the emulator's ANSI render takes the place of
// `capture-pane -p -e`. Only a measured profile with a detector counterpart
// gives a verdict; a generic or unmeasured one gives none, so the Core keeps
// asking for approval. The verdict carries no screen text: Detail is a fixed
// code and ScreenDigest a hash of the visible text.
//
// IsCLI here means the bound harness is still running in the PTY: the Proxy
// started it, so no process walk is needed to find it; the Core's process
// guard checks the tree before a write. Callers hold s.mu and have settled
// the guard.
func (s *Session) detectorLocked() *protocol.StateObservationDetector {
	if !s.hasSnap || !s.running || s.obs.exited || !s.opts.Profile.Measured {
		return nil
	}
	harness, ok := detectorHarnesses[s.opts.Profile.Harness]
	if !ok {
		return nil
	}
	screen, err := s.emu.Render()
	if err != nil {
		return nil
	}
	snap := s.lastSnap
	state := detect.Classify(harness, detectorTable, []byte(screen), snap.Cols, snap.Rows, 0, nil)
	detail := state.Detail
	if len(detail) > 64 {
		detail = "detail-overflow"
	}
	sum := sha256.Sum256([]byte(snap.Text()))
	return &protocol.StateObservationDetector{
		Certain:      state.Certain,
		Busy:         state.Busy,
		BoxBusy:      state.BoxBusy,
		IsCLI:        true,
		Detail:       detail,
		ScreenDigest: protocol.Digest("sha256:" + hex.EncodeToString(sum[:])),
	}
}

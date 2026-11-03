package orchestrator

import (
	"crypto/sha256"
	"time"

	"github.com/smellak/gifford/internal/detect"
	"github.com/smellak/gifford/internal/protocol"
)

// The screen rule both tmux routes apply before a write. The box and the
// agent are separate questions. The box must be located by position and
// empty: an unlocated box is never typed into, however quiet the pane. The
// agent must show no busy mark while the pane output stays unchanged for the
// quiet window. The analyst route applies the rule before submitting a
// screen, so Glimmer still decides on top of it; the deterministic route
// applies it as its guards, so without Glimmer it decides alone.
const (
	tmuxBoxNotLocated  = "input box not located"
	tmuxBoxHoldsText   = "input box holds text"
	tmuxBusyMark       = "busy mark on screen"
	tmuxOutputChanging = "pane output changing"
)

// tmuxOutputLimit bounds the per-pane output memory. Losing it only restarts
// one quiet window.
const tmuxOutputLimit = 1024

// tmuxScreenRule is the rule's verdict on one capture. Each half carries the
// reason it failed, which the decision persists.
type tmuxScreenRule struct {
	boxFree     bool
	boxDetail   string
	agentQuiet  bool
	agentDetail string
}

func (v tmuxScreenRule) pass() bool {
	return v.boxFree && v.agentQuiet
}

// detail names the first failing half, the box first.
func (v tmuxScreenRule) detail() string {
	if !v.boxFree {
		return v.boxDetail
	}
	return v.agentDetail
}

// tmuxOutputSeen is the last visible text of one pane and when it appeared.
type tmuxOutputSeen struct {
	hash  [sha256.Size]byte
	since time.Time
}

// screenRule judges one capture of a registered pane. A screen the detector
// cannot read fails both halves: an unmeasured profile proves neither a free
// box nor an idle agent.
func (r *tmuxAttachRuntime) screenRule(
	instance protocol.ULID,
	state detect.State,
	screen []byte,
	now time.Time,
) tmuxScreenRule {
	quiet := r.outputQuiet(instance, screen, now)
	v := tmuxScreenRule{
		boxFree:    state.Certain && !state.BoxBusy,
		agentQuiet: state.Certain && !state.Busy && quiet,
	}
	switch {
	case !state.Certain:
		v.boxDetail, v.agentDetail = tmuxBoxNotLocated, tmuxBoxNotLocated
		return v
	case state.BoxBusy:
		v.boxDetail = tmuxBoxHoldsText
	}
	switch {
	case state.Busy:
		v.agentDetail = tmuxBusyMark
	case !quiet:
		v.agentDetail = tmuxOutputChanging
	}
	return v
}

// outputQuiet reports whether the pane's visible text has stayed the same for
// the quiet window. Every call records the capture, so changed text restarts
// the window. This memory schedules a write and never grants one.
func (r *tmuxAttachRuntime) outputQuiet(instance protocol.ULID, screen []byte, now time.Time) bool {
	hash := sha256.Sum256(tmuxStripScreen(screen))
	r.outputMu.Lock()
	defer r.outputMu.Unlock()
	seen, found := r.output[instance]
	if found && seen.hash == hash && !now.Before(seen.since) {
		return now.Sub(seen.since) >= r.quietWindow
	}
	if !found && len(r.output) >= tmuxOutputLimit {
		clear(r.output)
	}
	r.output[instance] = tmuxOutputSeen{hash: hash, since: now}
	return r.quietWindow <= 0
}

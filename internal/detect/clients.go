package detect

import (
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	// clientPaneID matches tmux pane ids (%42). Anything else on a
	// non-blank row means garbled transport, never another pane.
	clientPaneID = regexp.MustCompile(`^%\d+$`)
	// clientActivity matches whole epoch seconds. Empty (never acted)
	// is handled separately; anything else is garbled.
	clientActivity = regexp.MustCompile(`^[0-9]+$`)
)

// HumanActivity reads the E3 human signal from one pane: whether any tmux
// client currently on paneID showed keyboard activity within window.
//
// The input is the fixed-format output of
// `tmux list-clients -F '#{pane_id}\t#{client_activity}'`, one client per
// line, activity as whole epoch seconds, empty when the client never acted.
// Mere connection never counts: only recent keyboard activity blocks a
// write.
//
// Anything unverifiable fails closed with certain=false: a garbled row
// anywhere in the output (wrong field count, invalid pane id, or a
// timestamp that is neither empty nor whole epoch seconds), or a
// timestamp ahead of now. A garbled row cannot be ignored even when it
// names another pane: the transport itself is untrusted then. Zero valid
// rows for the target pane means no client sits on it. The caller must
// not write while uncertain.
func HumanActivity(output, paneID string, now time.Time, window time.Duration) (recent, certain bool) {
	if paneID == "" {
		return false, false
	}
	certain = true
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimRight(line, "\r")
		if strings.TrimSpace(line) == "" {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != 2 || !clientPaneID.MatchString(fields[0]) {
			return false, false
		}
		raw := strings.TrimSpace(fields[1])
		if raw != "" && !clientActivity.MatchString(raw) {
			return false, false
		}
		if fields[0] != paneID || raw == "" {
			continue
		}
		epoch, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || epoch <= 0 {
			if err != nil {
				return false, false
			}
			continue
		}
		last := time.Unix(epoch, 0)
		if last.After(now) {
			return false, false
		}
		if now.Sub(last) <= window {
			recent = true
		}
	}
	return recent, certain
}

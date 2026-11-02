package detect

import (
	"strconv"
	"strings"
	"time"
)

// HumanActivity reads the E3 human signal from one pane: whether any tmux
// client currently on paneID showed keyboard activity within window.
//
// The input is the fixed-format output of
// `tmux list-clients -F '#{pane_id}\t#{client_activity}'`, one client per
// line, activity as whole epoch seconds, empty when the client never acted.
// Mere connection never counts: only recent keyboard activity blocks a
// write, exactly like the CapyCom notifier's client_activity check.
//
// Anything unverifiable fails closed with certain=false: a garbled line
// for our pane, an unparsable timestamp, or a timestamp ahead of now. The
// caller must not write while uncertain.
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
		if fields[0] != paneID {
			continue
		}
		if len(fields) != 2 {
			return false, false
		}
		raw := strings.TrimSpace(fields[1])
		if raw == "" {
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

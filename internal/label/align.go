package label

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/smellak/gifford/internal/dataset"
	"github.com/smellak/gifford/internal/protocol"
)

// Source says where a label came from. A manual one wins over a hook
// one, and a hook one over a weak one.
type Source string

// Source values.
const (
	SourceManual Source = "manual"
	SourceHook   Source = "hook"
	SourceWeak   Source = "weak"
)

// AlignedLabel is one line of AlignedFile. It never holds the screen.
type AlignedLabel struct {
	WindowID protocol.ULID `json:"window_id"`
	Label    dataset.Label `json:"label"`
	Source   Source        `json:"source"`
}

// AlignStats count what one run did, never content.
type AlignStats struct {
	Windows   int `json:"windows"`
	Manual    int `json:"manual"`
	Hook      int `json:"hook"`
	Weak      int `json:"weak"`
	Unlabeled int `json:"unlabeled"`
	// BadHookLines and BadAnnotationLines are lines skipped as unreadable.
	BadHookLines       int `json:"bad_hook_lines"`
	BadAnnotationLines int `json:"bad_annotation_lines"`
}

// maxAlignWindows bounds one run; only ids and timing are kept per
// window.
const maxAlignWindows = 1 << 20

// Hook event names that move the hook timeline.
const (
	hookUserPromptSubmit = "UserPromptSubmit"
	hookPreToolUse       = "PreToolUse"
	hookPostToolUse      = "PostToolUse"
	hookNotification     = "Notification"
	hookStop             = "Stop"
)

// transition is a change of state somewhere in [lo, hi]; the state after
// it is label, or none when label is empty.
type transition struct {
	lo, hi time.Time
	label  dataset.Label
}

// interval is a labeled state, [from, to].
type interval struct {
	from, to time.Time
	label    dataset.Label
}

// window is what align keeps of a recording: no screen.
type window struct {
	id       protocol.ULID
	harness  protocol.Harness
	session  protocol.OpaqueID
	from, to time.Time
	anchor   time.Time
	quiet    time.Duration
	burst    int64
	children []string
}

// hookTransition maps one hook event to a transition, or false when the
// event does not move the state.
//
//	UserPromptSubmit           → THINKING_STREAMING
//	PreToolUse                 → RUNNING_TOOL
//	PostToolUse                → none (the tool ended; what follows is not known)
//	Notification of permission → WAITING_APPROVAL
//	Stop                       → IDLE_AT_PROMPT
//
// Any other event, and any other Notification, leaves the state alone.
func hookTransition(r HookRecord) (string, transition, bool) {
	var ev struct {
		Name             string `json:"hook_event_name"`
		SessionID        string `json:"session_id"`
		NotificationType string `json:"notification_type"`
		Message          string `json:"message"`
	}
	if json.Unmarshal(r.Event, &ev) != nil || r.ReceivedAt.IsZero() {
		return "", transition{}, false
	}
	t := transition{lo: r.ReceivedAt, hi: r.ReceivedAt}
	switch ev.Name {
	case hookUserPromptSubmit:
		t.label = dataset.LabelB
	case hookPreToolUse:
		t.label = dataset.LabelC
	case hookPostToolUse:
	case hookNotification:
		if !isPermissionNotification(ev.NotificationType, ev.Message) {
			return "", transition{}, false
		}
		t.label = dataset.LabelD
	case hookStop:
		t.label = dataset.LabelA
	default:
		return "", transition{}, false
	}
	return ev.SessionID, t, true
}

// isPermissionNotification tells a permission prompt from the other
// notifications (such as the idle reminder). Older events carry only
// the message.
func isPermissionNotification(kind, message string) bool {
	if kind != "" {
		return kind == "permission_prompt"
	}
	return strings.Contains(strings.ToLower(message), "permission")
}

// weakTransitions derives one session's transitions from its windows'
// activity, in time order:
//
//   - a window quiet for at least Silence ends the turn at its last
//     output: the state after is none;
//   - a window after such a quiet one, with at least BurstBytes in its
//     trailing 2 s, starts a turn (THINKING_STREAMING) somewhere between
//     the two anchors;
//   - a window with a new child process starts a tool (RUNNING_TOOL)
//     somewhere since the previous anchor.
func weakTransitions(cfg AlignConfig, ws []window) []transition {
	var out []transition
	for i, w := range ws {
		prevAnchor := w.from
		if i > 0 {
			prevAnchor = ws[i-1].anchor
		}
		if w.quiet >= cfg.Silence {
			last := w.anchor.Add(-w.quiet)
			out = append(out, transition{lo: last, hi: last})
		} else if i > 0 && ws[i-1].quiet >= cfg.Silence && w.burst >= cfg.BurstBytes {
			out = append(out, transition{lo: prevAnchor, hi: w.anchor.Add(-w.quiet), label: dataset.LabelB})
		}
		if len(w.children) > 0 {
			out = append(out, transition{lo: prevAnchor, hi: w.anchor, label: dataset.LabelC})
		}
	}
	return out
}

// intervals turns a timeline into its labeled states: each lasts from
// the end of its transition (and of every earlier one) to the start of
// the next, and at most
// maxLen. The last one, with no next transition, is dropped: its end is
// not known. Transitions whose uncertainty overlaps leave no state.
func intervals(ts []transition, maxLen time.Duration) []interval {
	ts = slices.Clone(ts)
	slices.SortStableFunc(ts, func(a, b transition) int { return a.lo.Compare(b.lo) })
	var out []interval
	var settled time.Time // the latest end of any transition so far
	for i := 0; i+1 < len(ts); i++ {
		if ts[i].hi.After(settled) {
			settled = ts[i].hi
		}
		if ts[i].label == "" {
			continue
		}
		// An earlier transition still uncertain past this one's end
		// delays the state.
		from, to := settled, ts[i+1].lo
		if lim := from.Add(maxLen); to.After(lim) {
			to = lim
		}
		if to.After(from) {
			out = append(out, interval{from: from, to: to, label: ts[i].label})
		}
	}
	return out
}

// labelIn finds the state that holds [from-margin, to+margin] whole, so
// no transition comes within margin of the window.
func labelIn(ivs []interval, from, to time.Time, margin time.Duration) (dataset.Label, bool) {
	lo, hi := from.Add(-margin), to.Add(margin)
	for _, iv := range ivs {
		if !iv.from.After(lo) && !iv.to.Before(hi) {
			return iv.label, true
		}
	}
	return "", false
}

// agreed is the label every timeline that has one agrees on; timelines
// that disagree give none.
func agreed(timelines [][]interval, from, to time.Time, margin time.Duration) (dataset.Label, bool) {
	var got dataset.Label
	for _, ivs := range timelines {
		l, ok := labelIn(ivs, from, to, margin)
		if !ok {
			continue
		}
		if got != "" && l != got {
			return "", false
		}
		got = l
	}
	return got, got != ""
}

// Align labels the windows from src: a manual annotation if there is
// one, else a hook label for a hook harness, else a weak label for the
// others. Hooks and weak labels are matched by wall clock, and a window
// within cfg.Margin of a transition gets neither. Hook events from
// concurrent sessions that disagree on a window label it not at all.
// The result is in window order and holds no screen.
func Align(ctx context.Context, cfg AlignConfig, src dataset.RecordingSource, hooks []HookRecord, anns []Annotation) ([]AlignedLabel, AlignStats, error) {
	if err := cfg.Validate(); err != nil {
		return nil, AlignStats{}, err
	}
	if src == nil {
		return nil, AlignStats{}, errors.New("align: no recording source")
	}
	ws, err := readWindows(ctx, cfg, src)
	if err != nil {
		return nil, AlignStats{}, err
	}

	manual := latestManual(anns)

	bySession := map[string][]transition{}
	for _, h := range hooks {
		if s, t, ok := hookTransition(h); ok {
			bySession[s] = append(bySession[s], t)
		}
	}
	sessions := make([]string, 0, len(bySession))
	for s := range bySession {
		sessions = append(sessions, s)
	}
	slices.Sort(sessions)
	hookTimelines := make([][]interval, 0, len(sessions))
	for _, s := range sessions {
		hookTimelines = append(hookTimelines, intervals(bySession[s], cfg.MaxInterval))
	}

	weakTimelines := map[protocol.OpaqueID][]interval{}
	weakWindows := map[protocol.OpaqueID][]window{}
	for _, w := range ws {
		if !slices.Contains(cfg.HookHarnesses, w.harness) {
			weakWindows[w.session] = append(weakWindows[w.session], w)
		}
	}
	for s, sw := range weakWindows {
		slices.SortStableFunc(sw, func(a, b window) int { return a.anchor.Compare(b.anchor) })
		weakTimelines[s] = intervals(weakTransitions(cfg, sw), cfg.MaxInterval)
	}

	st := AlignStats{Windows: len(ws)}
	out := make([]AlignedLabel, 0, len(ws))
	for _, w := range ws {
		if l, ok := manual[w.id]; ok {
			out = append(out, AlignedLabel{WindowID: w.id, Label: l, Source: SourceManual})
			st.Manual++
			continue
		}
		if w.from.IsZero() || w.to.IsZero() {
			st.Unlabeled++
			continue
		}
		if slices.Contains(cfg.HookHarnesses, w.harness) {
			if l, ok := agreed(hookTimelines, w.from, w.to, cfg.Margin); ok {
				out = append(out, AlignedLabel{WindowID: w.id, Label: l, Source: SourceHook})
				st.Hook++
				continue
			}
		} else if l, ok := labelIn(weakTimelines[w.session], w.from, w.to, cfg.Margin); ok {
			out = append(out, AlignedLabel{WindowID: w.id, Label: l, Source: SourceWeak})
			st.Weak++
			continue
		}
		st.Unlabeled++
	}
	return out, st, nil
}

// readWindows keeps the id, the timing and the activity of each
// recording, and drops the rest, the screen first.
func readWindows(ctx context.Context, cfg AlignConfig, src dataset.RecordingSource) ([]window, error) {
	var ws []window
	for {
		rec, err := src.Next(ctx)
		if errors.Is(err, io.EOF) {
			return ws, nil
		}
		if err != nil {
			return nil, fmt.Errorf("align: read window: %w", err)
		}
		if len(ws) == maxAlignWindows {
			return nil, errors.New("align: too many windows")
		}
		p, in := rec.Provenance, rec.Features
		w := window{
			id: rec.WindowID, harness: in.Harness, session: p.SessionID, anchor: p.AnchorAt,
			quiet:    seconds(in.SecondsSinceLastOutput),
			burst:    in.OutputBytes.Last2s,
			children: slices.Clone(in.NewChildProcesses),
		}
		if w.quiet < 0 {
			w.quiet = 0
		}
		switch cfg.EventWindow {
		case EventWindowSpan:
			w.from, w.to = p.WindowStart, p.WindowEnd
			if w.to.Before(w.from) {
				w.from, w.to = time.Time{}, time.Time{}
			}
		default:
			w.from, w.to = p.AnchorAt, p.AnchorAt
		}
		ws = append(ws, w)
	}
}

// latestManual keeps each window's most recent annotation; on a tie the
// later line wins.
func latestManual(anns []Annotation) map[protocol.ULID]dataset.Label {
	at := map[protocol.ULID]time.Time{}
	out := map[protocol.ULID]dataset.Label{}
	for _, a := range anns {
		if _, ok := a.Label.Phase(); !ok {
			continue
		}
		if prev, seen := at[a.WindowID]; seen && a.LabeledAt.Before(prev) {
			continue
		}
		at[a.WindowID] = a.LabeledAt
		out[a.WindowID] = a.Label
	}
	return out
}

// ReadHooks reads HooksFile in dir; unreadable lines are skipped and
// counted. A missing file has no events.
func ReadHooks(dir string) ([]HookRecord, int, error) {
	var out []HookRecord
	bad, err := readLines(dir, HooksFile, func(line []byte) error {
		var r HookRecord
		if err := json.Unmarshal(line, &r); err != nil {
			return fmt.Errorf("hook line: %w", err)
		}
		out = append(out, r)
		return nil
	})
	return out, bad, err
}

// ReadAnnotations reads AnnotationsFile in dir; unreadable lines are
// skipped and counted. A missing file has no annotations.
func ReadAnnotations(dir string) ([]Annotation, int, error) {
	var out []Annotation
	bad, err := readLines(dir, AnnotationsFile, func(line []byte) error {
		var a Annotation
		if err := json.Unmarshal(line, &a); err != nil {
			return fmt.Errorf("annotation line: %w", err)
		}
		if _, ok := a.Label.Phase(); !ok {
			return errors.New("bad label")
		}
		out = append(out, a)
		return nil
	})
	return out, bad, err
}

// SaveAligned replaces AlignedFile in dir with labels.
func SaveAligned(dir string, labels []AlignedLabel) error {
	var buf []byte
	for _, l := range labels {
		line, err := json.Marshal(l)
		if err != nil {
			return fmt.Errorf("encode aligned label: %w", err)
		}
		buf = append(append(buf, line...), '\n')
	}
	return replaceFile(dir, AlignedFile, buf)
}

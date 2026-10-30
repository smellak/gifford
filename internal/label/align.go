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

// AlignedLabel is one line of AlignedFile: the chosen label and its
// source, the hook and weak labels worked out apart, and the doubt that
// sends the window to a human. It never holds the screen.
type AlignedLabel struct {
	WindowID  protocol.ULID `json:"window_id"`
	Label     dataset.Label `json:"label,omitempty"`
	Source    Source        `json:"source,omitempty"`
	HookLabel dataset.Label `json:"hook_label,omitempty"`
	WeakLabel dataset.Label `json:"weak_label,omitempty"`
	Doubt     Doubt         `json:"doubt,omitempty"`

	// session and anchor order the doubtful queue; they are not saved.
	session protocol.OpaqueID
	anchor  time.Time
}

// AlignStats count what one run did, never content.
type AlignStats struct {
	Windows   int `json:"windows"`
	Manual    int `json:"manual"`
	Hook      int `json:"hook"`
	Weak      int `json:"weak"`
	Unlabeled int `json:"unlabeled"`
	// Discarded windows fell within the margin of a transition.
	Discarded int `json:"discarded"`
	// DiscardedWeakOnly are the discarded windows near a weak transition
	// and no hook one, also by harness: kept apart from the rest so the
	// rule can be weighed on real data.
	DiscardedWeakOnly          int                              `json:"discarded_weak_only"`
	DiscardedWeakOnlyByHarness map[protocol.Harness]int         `json:"discarded_weak_only_by_harness"`
	Doubtful                   DoubtCounts                      `json:"doubtful"`
	DoubtfulByHarness          map[protocol.Harness]DoubtCounts `json:"doubtful_by_harness"`
	// BadHookLines, BadAnnotationLines and BadWeakLines are skipped unreadable lines.
	BadHookLines       int `json:"bad_hook_lines"`
	BadAnnotationLines int `json:"bad_annotation_lines"`
	BadWeakLines       int `json:"bad_weak_lines"`
}

// maxAlignWindows bounds one run; only ids and timing are kept per
// window.
const maxAlignWindows = 1 << 20

// Hook event names that move the hook timeline.
const (
	hookUserPromptSubmit     = "UserPromptSubmit"
	hookPreToolUse           = "PreToolUse"
	hookPostToolUse          = "PostToolUse"
	hookNotification         = "Notification"
	hookPermission           = "PermissionRequest"
	hookStop                 = "Stop"
	hookStopFailure          = "StopFailure"
	hookInterrupt            = "Interrupt"
	hookSessionEnd           = "SessionEnd"
	museToolUseStart         = "ToolUseStart"
	openCodeAsked            = "permission.asked"
	openCodeReplied          = "permission.replied"
	openCodeQuestionAsked    = "question.asked"
	openCodeQuestionReplied  = "question.replied"
	openCodeQuestionRejected = "question.rejected"
	openCodeSessionError     = "session.error"
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
	id             protocol.ULID
	harness        protocol.Harness
	harnessVersion string
	session        protocol.OpaqueID
	capture        protocol.OpaqueID
	from, to       time.Time
	anchor         time.Time
	quiet          time.Duration
	burst          int64
	children       []string
}

type hookCaptureKey struct {
	harness protocol.Harness
	capture protocol.OpaqueID
	version string
}

type hookStreamKey struct {
	harness protocol.Harness
	capture protocol.OpaqueID
	native  protocol.OpaqueID
}

// linkedRoot returns the one native root session proved for the recording's
// exact capture, harness and measured version. Missing, ambiguous or
// contradictory evidence produces no link.
func linkedRoot(cfg AlignConfig, key hookCaptureKey, records []HookRecord) (protocol.OpaqueID, bool) {
	if !key.harness.Valid() || !ValidCaptureSessionID(key.capture) ||
		!waitEnabled(cfg.RootSessionSources, key.harness, key.version) {
		return "", false
	}
	roots := map[protocol.OpaqueID]bool{}
	for _, r := range records {
		if !rootEvidence(r) {
			continue
		}
		if r.Harness != key.harness {
			return "", false
		}
		native := protocol.OpaqueID(r.Event.SessionID)
		if !native.Valid() {
			return "", false
		}
		roots[native] = true
	}
	if len(roots) != 1 {
		return "", false
	}
	for root := range roots {
		return root, true
	}
	return "", false
}

func rootEvidence(r HookRecord) bool {
	// Only Claude can produce this source through RecordHook today. Treat a
	// forged or historical look-alike from another harness as contradictory
	// rather than silently letting it coexist with the proved root.
	return r.Event.Name == "SessionStart" && r.Event.Source == sessionStartStartup
}

// foldHookTimeline folds one already-linked native session in chronological
// order. A no-label transition cuts the preceding interval without claiming
// what follows. D and E are emitted only for an exact harness version enabled
// in cfg, and only after an observed wait; a question wait wins over an
// approval wait when both are pending.
func foldHookTimeline(cfg AlignConfig, harness protocol.Harness, version string, records []HookRecord) []transition {
	records = slices.Clone(records)
	slices.SortStableFunc(records, func(a, b HookRecord) int { return a.ReceivedAt.Compare(b.ReceivedAt) })
	approvalEnabled := waitEnabled(cfg.ApprovalWaitSources, harness, version)
	questionEnabled := waitEnabled(cfg.QuestionWaitSources, harness, version)
	var out []transition
	var pendingApproval, pendingQuestion bool
	add := func(at time.Time, label dataset.Label) {
		out = append(out, transition{lo: at, hi: at, label: label})
	}
	clearPending := func() { pendingApproval, pendingQuestion = false, false }
	for _, r := range records {
		ev := r.Event
		if r.ReceivedAt.IsZero() || r.Harness != harness || !harness.Valid() ||
			!protocol.OpaqueID(ev.SessionID).Valid() || ev.PostStop != nil && *ev.PostStop {
			continue
		}
		switch ev.Name {
		case hookUserPromptSubmit:
			add(r.ReceivedAt, dataset.LabelB)
			clearPending()
		case hookPreToolUse:
			add(r.ReceivedAt, dataset.LabelC)
			pendingApproval = false
			pendingQuestion = ev.ToolName != ""
		case museToolUseStart:
			if harness != protocol.HarnessMuseCode {
				continue
			}
			add(r.ReceivedAt, dataset.LabelC)
			clearPending()
		case hookPostToolUse, hookInterrupt, hookSessionEnd:
			add(r.ReceivedAt, "")
			clearPending()
		case openCodeReplied, openCodeQuestionReplied, openCodeQuestionRejected:
			if harness != protocol.HarnessOpencode {
				continue
			}
			add(r.ReceivedAt, "")
			clearPending()
		case hookStop:
			add(r.ReceivedAt, dataset.LabelA)
			clearPending()
		case hookStopFailure:
			add(r.ReceivedAt, dataset.LabelF)
			clearPending()
		case openCodeSessionError:
			if harness != protocol.HarnessOpencode {
				continue
			}
			if ev.ErrorType == openCodeAborted {
				add(r.ReceivedAt, "")
			} else {
				add(r.ReceivedAt, dataset.LabelF)
			}
			clearPending()
		case hookPermission:
			if harness != protocol.HarnessClaudeCode && harness != protocol.HarnessMuseCode {
				continue
			}
			// The request proves only that a possible wait began. It cuts
			// the prior state; D/E start at a later observed wait.
			add(r.ReceivedAt, "")
			pendingApproval = true
		case hookNotification:
			switch {
			case harness == protocol.HarnessMuseCode && ev.NotificationType == questionPrompt:
				if (pendingQuestion || pendingApproval) && questionEnabled {
					add(r.ReceivedAt, dataset.LabelE)
				} else {
					add(r.ReceivedAt, "")
				}
			case (harness == protocol.HarnessClaudeCode || harness == protocol.HarnessMuseCode) &&
				ev.NotificationType == permissionPrompt && pendingQuestion:
				if questionEnabled {
					add(r.ReceivedAt, dataset.LabelE)
				} else {
					add(r.ReceivedAt, "")
				}
			case (harness == protocol.HarnessClaudeCode || harness == protocol.HarnessMuseCode) &&
				ev.NotificationType == permissionPrompt && pendingApproval && approvalEnabled:
				add(r.ReceivedAt, dataset.LabelD)
			case (harness == protocol.HarnessClaudeCode || harness == protocol.HarnessMuseCode) &&
				ev.NotificationType == permissionPrompt:
				add(r.ReceivedAt, "")
			default:
				continue
			}
			clearPending()
		case openCodeAsked:
			if harness != protocol.HarnessOpencode {
				continue
			}
			switch {
			case pendingQuestion || ev.ToolName != "":
				if questionEnabled {
					add(r.ReceivedAt, dataset.LabelE)
				} else {
					add(r.ReceivedAt, "")
				}
			case approvalEnabled:
				add(r.ReceivedAt, dataset.LabelD)
			default:
				add(r.ReceivedAt, "")
			}
			clearPending()
		case openCodeQuestionAsked:
			if harness != protocol.HarnessOpencode {
				continue
			}
			if questionEnabled {
				add(r.ReceivedAt, dataset.LabelE)
			} else {
				add(r.ReceivedAt, "")
			}
			clearPending()
		}
	}
	return out
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
func labelIn(ivs []interval, from, to time.Time, margin time.Duration) dataset.Label {
	lo, hi := from.Add(-margin), to.Add(margin)
	for _, iv := range ivs {
		if !iv.from.After(lo) && !iv.to.Before(hi) {
			return iv.label
		}
	}
	return ""
}

// Doubt says why a window needs a human: it has no label, only a weak
// one, or a hook label and a weak one that disagree.
type Doubt string

// Doubt values, in the order annotate --doubtful asks them.
const (
	DoubtConflict  Doubt = "CONFLICT"
	DoubtUnlabeled Doubt = "UNLABELED"
	DoubtWeakOnly  Doubt = "WEAK_ONLY"
)

// doubtRank orders the queue: conflicts first, then unlabeled, then weak
// only.
func doubtRank(d Doubt) int {
	switch d {
	case DoubtConflict:
		return 0
	case DoubtUnlabeled:
		return 1
	default:
		return 2
	}
}

// DoubtCounts count the doubtful windows by reason.
type DoubtCounts struct {
	Conflict  int `json:"conflict"`
	Unlabeled int `json:"unlabeled"`
	WeakOnly  int `json:"weak_only"`
}

func (c *DoubtCounts) add(d Doubt) {
	switch d {
	case DoubtConflict:
		c.Conflict++
	case DoubtUnlabeled:
		c.Unlabeled++
	default:
		c.WeakOnly++
	}
}

// Align labels the windows from src. For each it works out the hook label
// (hook harnesses only) and the weak label apart, and chooses: a manual
// annotation if there is one, else the hook label, else the weak one.
// Hook labels are matched only inside a root link proved for the window's
// exact capture, harness and measured version; weak activity stays inside its
// recording session. Within those timelines labels are matched by wall clock. A window within
// cfg.Margin of any transition, hook or weak, is discarded before any
// label is chosen, a manual one included: it is left out of the result,
// is not doubtful, and counts only as Discarded. Hook
// events outside the linked native root never label or discard its windows.
// A window without a manual annotation is doubtful when it
// has no label, only a weak one, or a hook and a weak one that disagree.
// The result is in window order, discarded windows left out, and holds no
// screen.
func Align(ctx context.Context, cfg AlignConfig, src dataset.RecordingSource, hooks []HookRecord, anns []Annotation) ([]AlignedLabel, AlignStats, error) {
	return AlignWithWeak(ctx, cfg, src, hooks, anns, nil)
}

// AlignWithWeak is Align with audited records imported from WeakFile. The
// imported source has the same lowest priority as activity-derived weak labels;
// it never replaces manual or hook evidence, and disagreement is doubtful.
func AlignWithWeak(ctx context.Context, cfg AlignConfig, src dataset.RecordingSource, hooks []HookRecord, anns []Annotation, weak []WeakRecord) ([]AlignedLabel, AlignStats, error) {
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
	importedWeak := latestWeak(weak)

	byStream := map[hookStreamKey][]HookRecord{}
	byCapture := map[protocol.OpaqueID][]HookRecord{}
	for _, h := range hooks {
		key := hookStreamKey{
			harness: h.Harness, capture: h.CaptureSessionID, native: protocol.OpaqueID(h.Event.SessionID),
		}
		if key.harness.Valid() && ValidCaptureSessionID(key.capture) {
			byCapture[key.capture] = append(byCapture[key.capture], h)
			if key.native.Valid() {
				byStream[key] = append(byStream[key], h)
			}
		}
	}
	captures := map[hookCaptureKey]bool{}
	for _, w := range ws {
		if slices.Contains(cfg.HookHarnesses, w.harness) {
			captures[hookCaptureKey{harness: w.harness, capture: w.capture, version: w.harnessVersion}] = true
		}
	}
	hookTimelines := map[hookCaptureKey][]interval{}
	hookTransitions := map[hookCaptureKey][]transition{}
	for capture := range captures {
		root, ok := linkedRoot(cfg, capture, byCapture[capture.capture])
		if !ok {
			continue
		}
		records := byStream[hookStreamKey{harness: capture.harness, capture: capture.capture, native: root}]
		ts := foldHookTimeline(cfg, capture.harness, capture.version, records)
		hookTransitions[capture] = ts
		hookTimelines[capture] = intervals(ts, cfg.MaxInterval)
	}

	// Weak labels are worked out for every harness: on a hook harness
	// they are what a hook label is checked against.
	weakTimelines := map[protocol.OpaqueID][]interval{}
	weakTrans := map[protocol.OpaqueID][]transition{}
	weakWindows := map[protocol.OpaqueID][]window{}
	for _, w := range ws {
		weakWindows[w.session] = append(weakWindows[w.session], w)
	}
	for s, sw := range weakWindows {
		slices.SortStableFunc(sw, func(a, b window) int { return a.anchor.Compare(b.anchor) })
		weakTrans[s] = weakTransitions(cfg, sw)
		weakTimelines[s] = intervals(weakTrans[s], cfg.MaxInterval)
	}

	st := AlignStats{
		Windows:                    len(ws),
		DoubtfulByHarness:          map[protocol.Harness]DoubtCounts{},
		DiscardedWeakOnlyByHarness: map[protocol.Harness]int{},
	}
	out := make([]AlignedLabel, 0, len(ws))
	for _, w := range ws {
		al := AlignedLabel{WindowID: w.id, session: w.session, anchor: w.anchor}
		hookHarness := slices.Contains(cfg.HookHarnesses, w.harness)
		capture := hookCaptureKey{harness: w.harness, capture: w.capture, version: w.harnessVersion}
		timed := !w.from.IsZero() && !w.to.IsZero()
		// The margin is decided before any label, the manual one
		// included: a manual label does not make an ambiguous instant a
		// valid training window.
		nearWeak := timed && nearAny(weakTrans[w.session], w.from, w.to, cfg.Margin)
		nearHook := timed && hookHarness && nearAny(hookTransitions[capture], w.from, w.to, cfg.Margin)
		if nearWeak || nearHook {
			st.Discarded++
			// Counted apart: if S1 shows this eats too many windows, the
			// rule changes on this figure.
			if !nearHook {
				st.DiscardedWeakOnly++
				st.DiscardedWeakOnlyByHarness[countedHarness(w.harness)]++
			}
			continue
		}
		if timed {
			if hookHarness {
				al.HookLabel = labelIn(hookTimelines[capture], w.from, w.to, cfg.Margin)
			}
			al.WeakLabel = labelIn(weakTimelines[w.session], w.from, w.to, cfg.Margin)
		}
		weakConflict := false
		if imported, ok := importedWeak[w.id]; ok {
			if al.WeakLabel != "" && al.WeakLabel != imported {
				weakConflict = true
			} else {
				al.WeakLabel = imported
			}
		}
		if l, ok := manual[w.id]; ok {
			al.Label, al.Source = l, SourceManual
			st.Manual++
			out = append(out, al)
			continue
		}
		switch {
		case al.HookLabel != "":
			al.Label, al.Source = al.HookLabel, SourceHook
			st.Hook++
		case al.WeakLabel != "":
			al.Label, al.Source = al.WeakLabel, SourceWeak
			st.Weak++
		default:
			st.Unlabeled++
		}
		switch {
		case weakConflict || al.HookLabel != "" && al.WeakLabel != "" && al.HookLabel != al.WeakLabel:
			al.Doubt = DoubtConflict
		case al.Label == "":
			al.Doubt = DoubtUnlabeled
		case al.Source == SourceWeak:
			al.Doubt = DoubtWeakOnly
		}
		if al.Doubt != "" {
			st.Doubtful.add(al.Doubt)
			h := countedHarness(w.harness)
			c := st.DoubtfulByHarness[h]
			c.add(al.Doubt)
			st.DoubtfulByHarness[h] = c
		}
		out = append(out, al)
	}
	return out, st, nil
}

// latestWeak keeps each window's most recent valid imported weak record; on a
// labeled_at tie the later line wins.
func latestWeak(records []WeakRecord) map[protocol.ULID]dataset.Label {
	at := map[protocol.ULID]time.Time{}
	out := map[protocol.ULID]dataset.Label{}
	for _, untrusted := range records {
		r, err := normaliseWeakRecord(untrusted, false)
		if err != nil {
			continue
		}
		if prev, seen := at[r.WindowID]; seen && r.LabeledAt.Before(prev) {
			continue
		}
		at[r.WindowID] = r.LabeledAt
		out[r.WindowID] = r.Label
	}
	return out
}

// countedHarness keys the counts by a known harness name, never by a
// value a source made up.
func countedHarness(h protocol.Harness) protocol.Harness {
	if slices.Contains(protocol.Harness("").EnumValues(), string(h)) {
		return h
	}
	return "other"
}

// nearAny reports a transition closer than margin to [from, to]; one
// exactly margin away is clear, as in labelIn.
func nearAny(ts []transition, from, to time.Time, margin time.Duration) bool {
	lo, hi := from.Add(-margin), to.Add(margin)
	for _, t := range ts {
		if t.hi.After(lo) && t.lo.Before(hi) {
			return true
		}
	}
	return false
}

// DoubtfulQueue lists the doubtful windows the way annotate asks them:
// conflicts, then unlabeled, then weak only, each by session and time.
func DoubtfulQueue(labels []AlignedLabel) []protocol.ULID {
	var q []AlignedLabel
	for _, l := range labels {
		if l.Doubt != "" {
			q = append(q, l)
		}
	}
	slices.SortStableFunc(q, func(a, b AlignedLabel) int {
		if c := doubtRank(a.Doubt) - doubtRank(b.Doubt); c != 0 {
			return c
		}
		if c := strings.Compare(string(a.session), string(b.session)); c != 0 {
			return c
		}
		return a.anchor.Compare(b.anchor)
	})
	ids := make([]protocol.ULID, 0, len(q))
	for _, l := range q {
		ids = append(ids, l.WindowID)
	}
	return ids
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
			id: rec.WindowID, harness: in.Harness, harnessVersion: p.HarnessVersion,
			session: p.SessionID, capture: p.CaptureSessionID, anchor: p.AnchorAt,
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

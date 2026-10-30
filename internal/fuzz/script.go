package fuzz

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/smellak/gifford/internal/protocol"
)

// StepKind names one thing the human does.
type StepKind string

// Step kinds.
const (
	// StepType types printable runes, one write.
	StepType StepKind = "type"
	// StepEnter presses Enter.
	StepEnter StepKind = "enter"
	// StepBackspace presses backspace.
	StepBackspace StepKind = "backspace"
	// StepClearLine presses Ctrl-U.
	StepClearLine StepKind = "clear_line"
	// StepCancel presses Ctrl-C.
	StepCancel StepKind = "cancel"
	// StepPaste writes one piece of a bracketed paste.
	StepPaste StepKind = "paste"
	// StepEscape presses a key the harness ignores, an arrow or an Alt
	// key, or writes one piece of it.
	StepEscape StepKind = "escape"
	// StepPause stops typing for a while.
	StepPause StepKind = "pause"
)

// Step is one human action and its timing.
type Step struct {
	Kind StepKind
	// Keys is what the human writes; empty for a pause.
	Keys string
	// Wait is the time that passes before the step on the session's
	// clock: the rhythm of the keys, or the length of a pause. Nobody
	// waits for it.
	Wait time.Duration
	// Real is the real time the human waits before the step.
	Real time.Duration
	// Look makes the human wait, before pressing Enter, until the
	// screen shows Draft after the prompt.
	Look bool
	// Draft is the draft the human sees before the step.
	Draft string
}

// Instant names where an injector move falls among the human's steps.
type Instant string

// Instants.
const (
	// InstantRandom is after a step drawn at random.
	InstantRandom Instant = "random"
	// InstantPause is after a pause: idle on the session's clock,
	// while the next key may follow at once.
	InstantPause Instant = "pause"
	// InstantLineStart is as the human types the first key of a line.
	InstantLineStart Instant = "line_start"
	// InstantEnter is as the human presses Enter.
	InstantEnter Instant = "enter"
	// InstantAfterEnter is right after the human pressed Enter.
	InstantAfterEnter Instant = "after_enter"
	// InstantPasteStart is as a paste begins.
	InstantPasteStart Instant = "paste_start"
	// InstantPaste is inside a paste, between two of its pieces.
	InstantPaste Instant = "paste"
	// InstantCut is inside a key sequence the human wrote in two
	// pieces: an escape sequence, an Alt key or a rune.
	InstantCut Instant = "cut"
)

// Anchor places an injector move: as the human starts writing Step, or
// right after it, then Jitter of real time.
type Anchor struct {
	Step    int
	With    bool
	Jitter  time.Duration
	Instant Instant
}

// Gap is what lies between a prepare and the move that ends it.
type Gap string

// Gaps.
const (
	// GapNone ends the attempt right after its prepare.
	GapNone Gap = "none"
	// GapStep waits for the human to take one more step.
	GapStep Gap = "next_step"
	// GapSleep waits a short real time.
	GapSleep Gap = "sleep"
)

// Action is how an attempt ends once prepared.
type Action string

// Actions.
const (
	// ActionCommit commits the delivery.
	ActionCommit Action = "commit"
	// ActionCancel withdraws it.
	ActionCancel Action = "cancel"
	// ActionRace commits and cancels it at once.
	ActionRace Action = "commit_cancel"
)

// Plan is one delivery attempt: where it starts, what lies between its
// prepare and its end, and how it ends.
type Plan struct {
	At     Anchor
	Gap    Gap
	Sleep  time.Duration
	Action Action
}

// Notice is one line the injector delivers, and the plan of its first
// attempt, made the moment it arrives.
type Notice struct {
	Text string
	Plan Plan
}

// Script is one scenario, all drawn from the batch seed and the
// scenario's index: the human's steps, the notices and when each
// arrives, when the injector retries, and its limits.
type Script struct {
	Index int
	// Mode is how every notice is written: scenarios alternate typed
	// and bracketed_paste.
	Mode protocol.InjectMode
	// SubmitDelay is the session's wait between a line and its submit.
	SubmitDelay time.Duration
	Steps       []Step
	Notices     []Notice
	// Retries are the attempts made on the notices sent back to the
	// queue, oldest first, while the human types.
	Retries []Plan
	// Budget caps the attempts on each notice; one never delivered
	// within it stays queued.
	Budget int
}

// Keys is everything the human types, in order.
func (s Script) Keys() []byte {
	var b []byte
	for _, st := range s.Steps {
		b = append(b, st.Keys...)
	}
	return b
}

// String lists the script, one step or notice per line, for a failed
// scenario's directory.
func (s Script) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "index %d mode %s submit_delay %s budget %d\n", s.Index, s.Mode, s.SubmitDelay, s.Budget)
	for i, st := range s.Steps {
		fmt.Fprintf(&b, "step %d %s %q wait %s real %s", i, st.Kind, st.Keys, st.Wait, st.Real)
		if st.Look {
			fmt.Fprintf(&b, " look %q", st.Draft)
		}
		b.WriteByte('\n')
	}
	for i, n := range s.Notices {
		fmt.Fprintf(&b, "notice %d %q %s\n", i, n.Text, n.Plan)
	}
	for _, p := range s.Retries {
		fmt.Fprintf(&b, "retry %s\n", p)
	}
	return b.String()
}

// String is the plan on one line.
func (p Plan) String() string {
	with := "after"
	if p.At.With {
		with = "with"
	}
	return fmt.Sprintf("%s %s step %d jitter %s gap %s %s %s", p.At.Instant, with, p.At.Step, p.At.Jitter, p.Gap, p.Sleep, p.Action)
}

// Script limits.
const (
	// maxDraft keeps a draft on one screen line after the prompt.
	maxDraft = 56
	// noticeTag starts every notice. Nothing the human types has
	// capitals or brackets, so a human line can never pass for one.
	noticeTag = "[GIFFORD]"
	// alphabet is what the human types and pastes.
	alphabet = "abcdefghijklmnopqrstuvwxyz0123456789 .,-_/:;!?'éñü"
)

// submitDelays are the session waits between a line and its submit a
// scenario draws from.
var submitDelays = []time.Duration{
	time.Millisecond, 2 * time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond, 20 * time.Millisecond,
}

// escapes are the keys a human presses that the harness ignores. Each
// is one sequence a terminal writes at once, and a slow link may cut
// anywhere: in the escape sequence, after a lone escape, inside a rune.
var escapes = []string{
	"\x1b[A",    // up
	"\x1b[1;5D", // Ctrl+left
	"\x1bOB",    // down, as SS3
	"\x1bx",     // Alt+x
	"\x1bé",     // Alt+é
}

// rhythm is how one human types: the session-clock time each key
// takes, the real time before it, and how often keys come out together
// in one write.
type rhythm struct {
	keyMin, keyMax time.Duration
	realMax        time.Duration
	burst          int
}

// rhythms are the typists a scenario draws from.
var rhythms = []rhythm{
	{keyMin: 20 * time.Millisecond, keyMax: 80 * time.Millisecond, realMax: 0, burst: 50},
	{keyMin: 60 * time.Millisecond, keyMax: 250 * time.Millisecond, realMax: 200 * time.Microsecond, burst: 20},
	{keyMin: 150 * time.Millisecond, keyMax: 700 * time.Millisecond, realMax: time.Millisecond, burst: 5},
}

// NewScript draws scenario index of the batch seed: the same pair
// always gives the same script.
func NewScript(seed uint64, index int) Script {
	r := rand.New(rand.NewPCG(seed, uint64(index))) // #nosec G404 G115 -- a seeded script, not a secret; indexes are not negative
	mode := protocol.InjectModeTyped
	if index%2 == 1 {
		mode = protocol.InjectModeBracketedPaste
	}
	b := &builder{r: r, rhythm: rhythms[r.IntN(len(rhythms))]}
	lines := 2 + r.IntN(5)
	pasteLine := r.IntN(lines)
	for l := range lines {
		b.line(l == pasteLine)
	}
	s := Script{
		Index:       index,
		Mode:        mode,
		SubmitDelay: submitDelays[r.IntN(len(submitDelays))],
		Steps:       b.steps,
		Budget:      3 + r.IntN(6),
	}
	for i := range 1 + r.IntN(5) {
		s.Notices = append(s.Notices, Notice{
			Text: fmt.Sprintf("%s notice %d.%d %s", noticeTag, index, i, b.token(6)),
			Plan: b.plan(b.arrival()),
		})
	}
	for range 1 + r.IntN(4) {
		s.Retries = append(s.Retries, b.plan(b.anchor(InstantRandom, r.IntN(len(b.steps)), false)))
	}
	return s
}

// builder draws the human's steps, keeping the draft the editor would
// hold after them.
type builder struct {
	r      *rand.Rand
	rhythm rhythm
	steps  []Step
	ed     editor
	// lineStarts, enters, pauses, pasteStarts, pasteMids and cuts
	// index the steps each instant can fall on.
	lineStarts, enters, pauses, pasteStarts, pasteMids, cuts []int
}

// draft is what the editor holds after the steps so far.
func (b *builder) draft() string { return string(b.ed.draft) }

// add appends one step and runs its keys through the editor.
func (b *builder) add(st Step) int {
	st.Draft = b.draft()
	b.steps = append(b.steps, st)
	b.ed.feed([]byte(st.Keys))
	return len(b.steps) - 1
}

// between draws a duration in [lo, hi].
func (b *builder) between(lo, hi time.Duration) time.Duration {
	if hi <= lo {
		return lo
	}
	return lo + time.Duration(b.r.Int64N(int64(hi-lo)+1))
}

// key adds one key step at the human's rhythm.
func (b *builder) key(kind StepKind, keys string) int {
	return b.add(Step{
		Kind: kind,
		Keys: keys,
		Wait: b.between(b.rhythm.keyMin, b.rhythm.keyMax),
		Real: b.between(0, b.rhythm.realMax),
	})
}

// rest writes the rest of a key sequence cut in two: it follows the
// first piece at once, on the session's clock and in real time.
func (b *builder) rest(kind StepKind, keys string) int {
	return b.add(Step{
		Kind: kind,
		Keys: keys,
		Wait: b.between(0, 2*time.Millisecond),
		Real: b.between(0, b.rhythm.realMax),
	})
}

// cut writes keys in two pieces, cut anywhere inside, and keeps the
// first piece as an instant.
func (b *builder) cut(kind StepKind, keys string) {
	at := 1 + b.r.IntN(len(keys)-1)
	b.cuts = append(b.cuts, b.key(kind, keys[:at]))
	b.rest(kind, keys[at:])
}

// escape presses a key the harness ignores: half the time its sequence
// comes in two pieces.
func (b *builder) escape() {
	keys := escapes[b.r.IntN(len(escapes))]
	if b.r.IntN(2) == 0 {
		b.key(StepEscape, keys)
		return
	}
	b.cut(StepEscape, keys)
}

// pause stops typing for lo to hi on the session's clock, and a few
// milliseconds of real time at most.
func (b *builder) pause(lo, hi time.Duration) {
	b.pauses = append(b.pauses, b.add(Step{
		Kind: StepPause,
		Wait: b.between(lo, hi),
		Real: b.between(0, 3*time.Millisecond),
	}))
}

// line types one line and ends it with Enter or Ctrl-C; with paste,
// part of it is pasted.
func (b *builder) line(paste bool) {
	if b.r.IntN(2) == 0 {
		b.pause(400*time.Millisecond, 6*time.Second)
	}
	start := len(b.steps)
	target := 1 + b.r.IntN(40)
	for utf8.RuneCountInString(b.draft()) < target {
		n := utf8.RuneCountInString(b.draft())
		switch x := b.r.IntN(100); {
		case paste && x < 30:
			b.paste()
			paste = false
		case x < 10 && n > 0:
			for range 1 + b.r.IntN(min(4, n)) {
				b.key(StepBackspace, "\x7f")
			}
		case x < 13:
			b.key(StepClearLine, "\x15")
		case x < 18:
			b.pause(500*time.Millisecond, 3*time.Second)
		case x < 23:
			b.escape()
		default:
			b.word()
		}
	}
	if paste {
		b.paste()
	}
	for i := start; i < len(b.steps); i++ {
		if b.steps[i].Kind != StepPause {
			b.lineStarts = append(b.lineStarts, i)
			break
		}
	}
	if b.r.IntN(100) < 85 {
		b.enter(b.r.IntN(10) != 0)
	} else {
		b.key(StepCancel, "\x03")
	}
	if b.r.IntN(10) == 0 {
		b.enter(true)
	}
}

// enter presses Enter, looking at the screen first when look is set.
func (b *builder) enter(look bool) {
	i := b.key(StepEnter, "\r")
	b.steps[i].Look = look
	b.enters = append(b.enters, i)
}

// word types a few runes after a space, one write each or several
// together at the human's burst rate. A rune of several bytes typed
// alone may come in two pieces.
func (b *builder) word() {
	runes := []rune(b.token(2 + b.r.IntN(7)))
	if len(b.draft()) > 0 {
		runes = append([]rune{' '}, runes...)
	}
	runes = runes[:min(len(runes), maxDraft-utf8.RuneCountInString(b.draft()))]
	for len(runes) > 0 {
		n := 1
		if b.r.IntN(100) < b.rhythm.burst {
			n = min(len(runes), 2+b.r.IntN(3))
		}
		keys := string(runes[:n])
		if n == 1 && len(keys) > 1 && b.r.IntN(3) == 0 {
			b.cut(StepType, keys)
		} else {
			b.key(StepType, keys)
		}
		runes = runes[n:]
	}
}

// paste pastes a few runes in up to four pieces, cut anywhere: in the
// start marker, the text, a rune or the end marker.
func (b *builder) paste() {
	room := maxDraft - utf8.RuneCountInString(b.draft())
	if room < 1 {
		return
	}
	text := []byte(pasteStart + b.token(min(room, 3+b.r.IntN(22))) + pasteEnd)
	cuts := []int{1 + b.r.IntN(len(text))}
	for range b.r.IntN(3) {
		cuts = append(cuts, cuts[len(cuts)-1]+b.r.IntN(len(text)-cuts[len(cuts)-1]+1))
	}
	prev := 0
	for i, cut := range append(cuts, len(text)) {
		if cut == prev && i > 0 {
			continue
		}
		at := b.add(Step{Kind: StepPaste, Keys: string(text[prev:cut]), Real: b.between(0, b.rhythm.realMax)})
		if i == 0 {
			b.pasteStarts = append(b.pasteStarts, at)
		}
		if cut < len(text) {
			b.pasteMids = append(b.pasteMids, at)
		}
		prev = cut
	}
}

// token draws n runes of the human's alphabet, no space first.
func (b *builder) token(n int) string {
	letters := []rune(alphabet)
	out := make([]rune, 0, n)
	for len(out) < n {
		r := letters[b.r.IntN(len(letters))]
		if r == ' ' && len(out) == 0 {
			continue
		}
		out = append(out, r)
	}
	return string(out)
}

// anchor places a move on step.
func (b *builder) anchor(instant Instant, step int, with bool) Anchor {
	var jitter time.Duration
	if b.r.IntN(2) == 0 {
		jitter = b.between(0, 300*time.Microsecond)
	}
	return Anchor{Step: step, With: with, Jitter: jitter, Instant: instant}
}

// arrival draws where a notice arrives: at random, or at one of the
// worst instants the human's steps offer.
func (b *builder) arrival() Anchor {
	pick := func(steps []int) int { return steps[b.r.IntN(len(steps))] }
	for {
		switch x := b.r.IntN(100); {
		case x < 15 && len(b.pauses) > 0:
			return b.anchor(InstantPause, pick(b.pauses), false)
		case x < 35 && len(b.lineStarts) > 0:
			return b.anchor(InstantLineStart, pick(b.lineStarts), true)
		case x < 50 && len(b.enters) > 0:
			return b.anchor(InstantEnter, pick(b.enters), true)
		case x < 60 && len(b.enters) > 0:
			return b.anchor(InstantAfterEnter, pick(b.enters), false)
		case x < 70 && len(b.pasteStarts) > 0:
			return b.anchor(InstantPasteStart, pick(b.pasteStarts), true)
		case x < 80 && len(b.pasteMids) > 0:
			return b.anchor(InstantPaste, pick(b.pasteMids), false)
		case x < 90 && len(b.cuts) > 0:
			return b.anchor(InstantCut, pick(b.cuts), false)
		case x >= 90:
			return b.anchor(InstantRandom, b.r.IntN(len(b.steps)), false)
		}
	}
}

// plan draws what an attempt starting at a does after its prepare.
func (b *builder) plan(a Anchor) Plan {
	p := Plan{At: a, Gap: GapNone, Action: ActionCommit}
	switch x := b.r.IntN(100); {
	case x < 25:
		p.Gap = GapStep
	case x < 50:
		p.Gap = GapSleep
		p.Sleep = b.between(0, 2*time.Millisecond)
	}
	switch x := b.r.IntN(100); {
	case x < 10:
		p.Action = ActionCancel
	case x < 20:
		p.Action = ActionRace
	}
	return p
}

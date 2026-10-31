//go:build unix

package fuzz

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/creack/pty"

	"github.com/smellak/gifford/internal/profiles"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/proxy"
	"github.com/smellak/gifford/internal/proxy/draft"
	"github.com/smellak/gifford/internal/proxy/session"
)

// supported says the fuzzer runs here: it needs Unix PTYs.
const supported = true

// A scenario's files, kept when it fails.
const (
	recordFile   = "harness.rec"
	scriptFile   = "script.txt"
	eventsFile   = "events.jsonl"
	attemptsFile = "attempts.txt"
	verdictFile  = "verdict.txt"
)

// Scenario timing.
const (
	// proxyPatience bounds a look at the screen through the Proxy: an
	// echo takes milliseconds, so running out means something is wrong.
	proxyPatience = time.Second
	// baselinePatience is a glance: without the Proxy the screen often
	// never shows the draft the human meant.
	baselinePatience = 30 * time.Millisecond
	// drainPause is the session-clock time that passes before each
	// attempt once the human stopped, past the quiet period after a
	// submit.
	drainPause = time.Second
	// drainGap is the real time between those attempts.
	drainGap = time.Millisecond
	// teardownGrace bounds the wait for a session or harness to end
	// once the scenario's time is up.
	teardownGrace = 5 * time.Second
)

// cancelReason is the reason the injector gives its own cancels.
const cancelReason = string(protocol.CancelReasonManual)

// Scenario errors.
var (
	errTimeout     = errors.New("fuzz: scenario timed out")
	errNeverReady  = errors.New("fuzz: the harness never drew its prompt")
	errEndedEarly  = errors.New("fuzz: the harness ended before the scenario")
	errStuck       = errors.New("fuzz: the scenario did not end after its time was up")
	errHarnessExit = errors.New("fuzz: the harness did not exit cleanly")
)

// profileYAML is the fake harness's profile: measured, since its keys
// and its input line are known exactly.
const profileYAML = `harness: generic
measured: true
version_range: "1.0.0"
submit: "\r"
insert_newline: "\n"
clear_line: "\u0015"
cancel: "\u0003"
bracketed_paste: %t
extended_keyboard: none
input_line: %q
input_bounds:
  above: ["^fz-input-top$"]
  below: ["^fz-input-bottom$"]
`

// harnessProfile is the fake harness's profile, vetted like any
// profile file.
func harnessProfile(mode protocol.InjectMode) (profiles.Profile, error) {
	p, err := profiles.ParseFile("generic", fmt.Appendf(nil, profileYAML, mode == protocol.InjectModeBracketedPaste, InputLine))
	if err != nil {
		return profiles.Profile{}, fmt.Errorf("fuzz: harness profile: %w", err)
	}
	return p, nil
}

// harnessCommand is the fake harness command for a scenario directory.
func harnessCommand(cfg config, dir string) []string {
	return append(slices.Clone(cfg.harness), filepath.Join(dir, recordFile))
}

// runScenario runs one scenario, through the Proxy or, with baseline,
// without it, and judges it. A failed scenario keeps its directory.
func runScenario(ctx context.Context, cfg config, sc Script, baseline bool) result {
	res := result{index: sc.Index, notices: len(sc.Notices)}
	kind := "proxy"
	if baseline {
		kind = "baseline"
	}
	dir := filepath.Join(cfg.dir, fmt.Sprintf("%s-%d", kind, sc.Index))
	if err := os.Mkdir(dir, 0o700); err != nil {
		res.err = fmt.Errorf("fuzz: scenario dir: %w", err)
		return res
	}
	sctx, cancel := context.WithTimeout(ctx, cfg.timeout)
	defer cancel()
	var art artifacts
	var err error
	if baseline {
		art, err = runBaseline(sctx, cfg, sc, dir)
	} else {
		art, err = runProxy(sctx, cfg, sc, dir)
	}
	if errors.Is(sctx.Err(), context.DeadlineExceeded) && ctx.Err() == nil {
		err = errors.Join(fmt.Errorf("%w after %s", errTimeout, cfg.timeout), err)
	}
	res.blind = art.blind
	if err == nil {
		err = res.judge(sc, dir, art, baseline)
	}
	res.err = err
	if !res.failed(baseline) {
		if rerr := os.RemoveAll(dir); rerr != nil {
			res.err = fmt.Errorf("fuzz: scenario dir: %w", rerr)
		}
		return res
	}
	res.dir = dir
	keep(dir, sc, art, res)
	return res
}

// artifacts is what a scenario run leaves to judge.
type artifacts struct {
	events   []session.Event
	attempts []attempt
	done     []bool
	ids      map[protocol.OpaqueID]int
	blind    int
}

// judge reads the harness record and checks it, and through the Proxy
// the session's events and the injector's outcomes too.
func (r *result) judge(sc Script, dir string, art artifacts, baseline bool) error {
	raw, err := os.ReadFile(filepath.Join(dir, recordFile)) // #nosec G304 -- the scenario's own record file
	if err != nil {
		return fmt.Errorf("fuzz: harness record: %w", err)
	}
	records, err := ParseRecords(raw)
	if err != nil {
		return err
	}
	texts := make([]string, len(sc.Notices))
	for i, n := range sc.Notices {
		texts[i] = n.Text
	}
	lines := checkLines(records, Expected(sc.Keys()), texts)
	r.lines, r.mixed, r.lostKeys = lines.Lines, lines.Mixed, lines.LostKeys
	r.detail = append(r.detail, lines.Detail...)
	if baseline {
		for _, t := range texts {
			if lines.Seen[t] == 1 {
				r.delivered++
			}
		}
		return nil
	}
	var detail []string
	r.commits, r.openCommits, detail = checkCommits(art.events)
	r.detail = append(r.detail, detail...)
	r.attempts = len(art.attempts)
	committedEvents := make([]int, len(sc.Notices))
	for _, e := range art.events {
		if c, ok := e.Payload.(session.DeliveryCommitted); ok && e.Type == protocol.EventTypeInputDeliveryCommitted {
			if n, ok := art.ids[c.DeliveryID]; ok {
				committedEvents[n]++
			}
		}
	}
	for i, t := range texts {
		committed := art.done[i]
		if committed {
			r.delivered++
		} else {
			r.requeued++
		}
		want := 0
		if committed {
			want = 1
		}
		if lines.Seen[t] != want || committedEvents[i] != want {
			r.noticeFaults++
			r.detail = append(r.detail, fmt.Sprintf("notice %d: committed %t, received %d times, %d committed events",
				i, committed, lines.Seen[t], committedEvents[i]))
		}
	}
	for _, a := range art.attempts {
		if a.commit.outcome == session.DeliveryCommittedOutcome && a.cancel.outcome == session.DeliveryCancelledOutcome {
			r.noticeFaults++
			r.detail = append(r.detail, fmt.Sprintf("delivery %s: both committed and cancelled", a.id))
		}
	}
	return nil
}

// checkCommits follows the draft through the session's events and
// counts the deliveries that started writing, and those that started
// with the draft anything but closed.
func checkCommits(events []session.Event) (commits, open int, detail []string) {
	state := string(draft.StateClosed)
	for _, e := range events {
		switch e.Type {
		case protocol.EventTypeInputDraftOpened, protocol.EventTypeInputDraftUpdated, protocol.EventTypeInputDraftUnknown,
			protocol.EventTypeInputDraftSubmitted, protocol.EventTypeInputDraftCancelled, protocol.EventTypeInputDraftCleared:
			if d, ok := e.Payload.(session.InputDraft); ok {
				state = d.State
			}
		case protocol.EventTypeInputDeliveryStarted:
			commits++
			if state != string(draft.StateClosed) {
				open++
				id := protocol.OpaqueID("")
				if d, ok := e.Payload.(session.DeliveryStarted); ok {
					id = d.DeliveryID
				}
				detail = append(detail, fmt.Sprintf("commit with the draft %s: delivery %s started at event %d", state, id, e.Sequence))
			}
		default:
			// Other events leave the draft as it was.
		}
	}
	return commits, open, detail
}

// keep writes what a failed scenario needs to be read: its script, the
// session's events, the injector's attempts and the verdict. The
// harness record and the session recording are already there.
func keep(dir string, sc Script, art artifacts, res result) {
	_ = os.WriteFile(filepath.Join(dir, scriptFile), []byte(sc.String()), 0o600)
	_ = os.WriteFile(filepath.Join(dir, verdictFile), []byte(strings.Join(res.reasons(), "\n")+"\n"), 0o600)
	if len(art.events) > 0 {
		var b strings.Builder
		for _, e := range art.events {
			line, err := json.Marshal(e)
			if err != nil {
				continue
			}
			b.Write(line)
			b.WriteByte('\n')
		}
		_ = os.WriteFile(filepath.Join(dir, eventsFile), []byte(b.String()), 0o600)
	}
	if len(art.attempts) > 0 {
		var b strings.Builder
		for _, a := range art.attempts {
			fmt.Fprintln(&b, a)
		}
		_ = os.WriteFile(filepath.Join(dir, attemptsFile), []byte(b.String()), 0o600)
	}
}

// eventSink keeps a session's events in order.
type eventSink struct {
	mu     sync.Mutex
	events []session.Event
}

// Emit keeps one event.
func (s *eventSink) Emit(e session.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, e)
}

// all is every event kept.
func (s *eventSink) all() []session.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.events)
}

// waitDone waits for ch, past the scenario's deadline at most the
// teardown grace.
func waitDone(ctx context.Context, ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	case <-ctx.Done():
	}
	t := time.NewTimer(teardownGrace)
	defer t.Stop()
	select {
	case <-ch:
		return true
	case <-t.C:
		return false
	}
}

// runProxy runs the scenario through a real session: the human types on
// the outer terminal, the injector delivers through the session API,
// and the fake harness is the session's child.
func runProxy(ctx context.Context, cfg config, sc Script, dir string) (artifacts, error) {
	var art artifacts
	master, tty, err := pty.Open()
	if err != nil {
		return art, fmt.Errorf("fuzz: terminal: %w", err)
	}
	scr, err := newScreen()
	if err == nil {
		err = pty.Setsize(tty, &pty.Winsize{Cols: cols, Rows: rows})
	}
	if err != nil {
		_ = tty.Close()
		_ = master.Close()
		return art, fmt.Errorf("fuzz: terminal: %w", err)
	}
	read := make(chan struct{})
	go func() {
		defer close(read)
		scr.read(master)
	}()
	// The screen reads until the terminal closes; the session must be
	// done with it first.
	closeTerminal := func() {
		_ = tty.Close()
		if waitDone(ctx, read) {
			_ = master.Close()
		}
	}
	profile, err := harnessProfile(sc.Mode)
	if err != nil {
		closeTerminal()
		return art, err
	}
	clk := &virtualClock{}
	sink := &eventSink{}
	s, err := session.NewSession(session.Options{
		Command:     harnessCommand(cfg, dir),
		In:          tty,
		Out:         tty,
		DataDir:     dir,
		SessionID:   fmt.Sprintf("fuzz-%d", sc.Index),
		Profile:     profile,
		Clock:       clk,
		Sink:        sink,
		Notice:      io.Discard,
		SubmitDelay: sc.SubmitDelay,
	})
	if err != nil {
		closeTerminal()
		return art, fmt.Errorf("fuzz: session: %w", err)
	}
	// live ends when the session does, so nothing waits on a gone
	// harness.
	live, stopLive := context.WithCancel(ctx)
	defer stopLive()
	var exit proxy.Exit
	var runErr error
	ended := make(chan struct{})
	go func() {
		defer close(ended)
		exit, runErr = s.Run(ctx)
		stopLive()
	}()

	in := &injector{s: s, sc: sc, clock: clk}
	art.blind, err = play(live, scr, master, clk, sc, proxyPatience, in.run)
	art.events, art.attempts, art.done, art.ids = sink.all(), in.log, in.done, in.ids
	if !waitDone(ctx, ended) {
		return art, errors.Join(errStuck, err)
	}
	closeTerminal()
	art.events = sink.all()
	if err != nil {
		return art, errors.Join(err, runErr)
	}
	if runErr != nil {
		return art, fmt.Errorf("fuzz: session: %w", runErr)
	}
	if exit.Code != 0 || exit.Signal != 0 {
		return art, fmt.Errorf("%w: %s", errHarnessExit, exit)
	}
	return art, nil
}

// runBaseline runs the scenario without the Proxy: the human and the
// injector write to the same harness terminal, and each notice goes
// out the moment it arrives.
func runBaseline(ctx context.Context, cfg config, sc Script, dir string) (artifacts, error) {
	var art artifacts
	scr, err := newScreen()
	if err != nil {
		return art, err
	}
	command := harnessCommand(cfg, dir)
	cmd := exec.CommandContext(ctx, command[0], command[1:]...) // #nosec G204 -- the fuzzer's own harness
	master, err := pty.StartWithSize(cmd, &pty.Winsize{Cols: cols, Rows: rows})
	if err != nil {
		return art, fmt.Errorf("fuzz: start harness: %w", err)
	}
	read := make(chan struct{})
	go func() {
		defer close(read)
		scr.read(master)
	}()
	live, stopLive := context.WithCancel(ctx)
	defer stopLive()
	var waitErr error
	ended := make(chan struct{})
	go func() {
		defer close(ended)
		waitErr = cmd.Wait()
		stopLive()
	}()

	art.blind, err = play(live, scr, master, &virtualClock{}, sc, baselinePatience, func(ctx context.Context, at *progress) error {
		return typeNotices(ctx, master, sc, at)
	})
	if !waitDone(ctx, ended) {
		return art, errors.Join(errStuck, err)
	}
	if waitDone(ctx, read) {
		_ = master.Close()
	}
	if err != nil {
		return art, errors.Join(err, waitErr)
	}
	if waitErr != nil {
		return art, fmt.Errorf("%w: %w", errHarnessExit, waitErr)
	}
	return art, nil
}

// play runs one scenario on a harness that is starting: once the
// harness draws its prompt, the human types and the injector moves at
// the same time; once both are done, Ctrl-D ends the harness. It
// returns how many times the human pressed Enter blind.
func play(ctx context.Context, scr *screen, keys io.Writer, clk *virtualClock, sc Script, patience time.Duration,
	inject func(context.Context, *progress) error,
) (int, error) {
	if !scr.showsDraft(ctx, "", DefaultTimeout) {
		return 0, ended(ctx, errNeverReady)
	}
	at := newProgress()
	h := &human{keys: keys, screen: scr, clock: clk, at: at, patience: patience}
	var wg sync.WaitGroup
	var herr, ierr error
	wg.Go(func() { herr = h.run(ctx, sc.Steps) })
	wg.Go(func() { ierr = inject(ctx, at) })
	wg.Wait()
	if err := errors.Join(herr, ierr); err != nil {
		return h.blind, ended(ctx, err)
	}
	if _, err := io.WriteString(keys, theEnd); err != nil {
		return h.blind, fmt.Errorf("fuzz: end harness: %w", err)
	}
	return h.blind, nil
}

// theEnd ends the harness: Ctrl-D, after a paste end in case a mixed
// line cut the end of a paste and left the harness taking every key as
// pasted text. With no paste open, the harness ignores it.
const theEnd = pasteEnd + "\x04"

// ended names why a scenario stopped early: its harness ended, or its
// time ran out.
func ended(ctx context.Context, err error) error {
	if errors.Is(ctx.Err(), context.Canceled) {
		return errors.Join(errEndedEarly, err)
	}
	return err
}

// typeNotices writes each notice the moment it arrives, the way a
// terminal multiplexer types keys into a pane: the line, then Enter,
// with nothing to stop either.
func typeNotices(ctx context.Context, w io.Writer, sc Script, at *progress) error {
	order := make([]int, len(sc.Notices))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(i, j int) bool {
		return position(sc.Notices[order[i]].Plan.At) < position(sc.Notices[order[j]].Plan.At)
	})
	for _, i := range order {
		n := sc.Notices[i]
		if !at.reach(ctx, position(n.Plan.At)) || !sleep(ctx, n.Plan.At.Jitter) {
			return stopped(ctx)
		}
		line := n.Text
		if sc.Mode == protocol.InjectModeBracketedPaste {
			line = pasteStart + line + pasteEnd
		}
		if _, err := io.WriteString(w, line); err != nil {
			return fmt.Errorf("fuzz: notice %d: %w", i, err)
		}
		if _, err := w.Write([]byte{keySubmit}); err != nil {
			return fmt.Errorf("fuzz: notice %d: %w", i, err)
		}
	}
	return nil
}

// answer is the session's answer to an attempt's commit or cancel.
type answer struct {
	outcome session.DeliveryOutcome
	err     error
}

// String is the answer, or its error.
func (a answer) String() string {
	if a.err != nil {
		return "error: " + a.err.Error()
	}
	if a.outcome == "" {
		return "-"
	}
	return string(a.outcome)
}

// attempt is one delivery attempt and what the session answered.
type attempt struct {
	notice   int
	id       protocol.OpaqueID
	plan     Plan
	prepared session.DeliveryOutcome
	commit   answer
	cancel   answer
}

// String is the attempt on one line.
func (a attempt) String() string {
	return fmt.Sprintf("%s notice %d %s: prepare %s commit %s cancel %s", a.id, a.notice, a.plan, a.prepared, a.commit, a.cancel)
}

// injector delivers a script's notices through the session API at the
// script's instants. No Analyst stands in its way: it always tries, and
// sends back to the queue what the session refuses or it cancels.
type injector struct {
	s     *session.Session
	sc    Script
	clock *virtualClock
	at    *progress
	// queue holds the notices waiting for a retry, oldest first.
	queue []int
	// tries counts the attempts on each notice, done marks the ones
	// committed, and ids maps each delivery id to its notice.
	tries []int
	done  []bool
	ids   map[protocol.OpaqueID]int
	log   []attempt
}

// move is one injector move on the human's timeline: a notice that
// arrives, or a retry when notice is -1.
type move struct {
	notice int
	plan   Plan
}

// run makes every move as the human reaches it, then, once the human
// stopped, retries the queue while time passes on the session's clock.
func (in *injector) run(ctx context.Context, at *progress) error {
	in.at = at
	in.tries = make([]int, len(in.sc.Notices))
	in.done = make([]bool, len(in.sc.Notices))
	in.ids = make(map[protocol.OpaqueID]int)
	moves := make([]move, 0, len(in.sc.Notices)+len(in.sc.Retries))
	for i, n := range in.sc.Notices {
		moves = append(moves, move{notice: i, plan: n.Plan})
	}
	for _, p := range in.sc.Retries {
		moves = append(moves, move{notice: -1, plan: p})
	}
	sort.SliceStable(moves, func(i, j int) bool { return position(moves[i].plan.At) < position(moves[j].plan.At) })
	for _, m := range moves {
		if !at.reach(ctx, position(m.plan.At)) || !sleep(ctx, m.plan.At.Jitter) {
			return stopped(ctx)
		}
		n := m.notice
		if n < 0 {
			if len(in.queue) == 0 {
				continue
			}
			n, in.queue = in.queue[0], in.queue[1:]
		}
		if err := in.try(ctx, n, m.plan); err != nil {
			return err
		}
	}
	if !at.reach(ctx, done) {
		return stopped(ctx)
	}
	for len(in.queue) > 0 {
		n := in.queue[0]
		in.queue = in.queue[1:]
		in.clock.advance(drainPause)
		if !sleep(ctx, drainGap) {
			return stopped(ctx)
		}
		if err := in.try(ctx, n, Plan{Gap: GapNone, Action: ActionCommit}); err != nil {
			return err
		}
	}
	return nil
}

// try makes one attempt on notice n: prepare against the guard as it
// is now, then the plan's gap and its end. A refusal or a cancel sends
// the notice back to the queue while its budget lasts.
func (in *injector) try(ctx context.Context, n int, p Plan) error {
	in.tries[n]++
	a := attempt{notice: n, id: protocol.OpaqueID(fmt.Sprintf("n%d-a%d", n, in.tries[n])), plan: p}
	in.ids[a.id] = n
	defer func() { in.log = append(in.log, a) }()
	rep, err := in.s.PrepareDelivery(a.id, in.s.Generation(), in.s.GuardVersion())
	if err != nil {
		return fmt.Errorf("fuzz: prepare %s: %w", a.id, err)
	}
	a.prepared = rep.Outcome
	if rep.Outcome != session.DeliveryPreparedOutcome {
		in.requeue(n)
		return nil
	}
	switch p.Gap {
	case GapStep:
		if pos := in.at.at(); pos != done && !in.at.reach(ctx, pos+2) {
			return stopped(ctx)
		}
	case GapSleep:
		if !sleep(ctx, p.Sleep) {
			return stopped(ctx)
		}
	default:
		// GapNone ends the attempt at once.
	}
	committed, err := in.end(&a, p.Action)
	if err != nil {
		return err
	}
	if committed {
		in.done[n] = true
	} else {
		in.requeue(n)
	}
	return nil
}

// requeue sends notice n back to the queue while its budget lasts.
func (in *injector) requeue(n int) {
	if in.tries[n] < in.sc.Budget {
		in.queue = append(in.queue, n)
	}
}

// end ends a prepared attempt with action and reports whether the
// notice was committed. An answer the session must never give is an
// error.
func (in *injector) end(a *attempt, action Action) (bool, error) {
	line := in.sc.Notices[a.notice].Text
	switch action {
	case ActionCancel:
		rep, err := in.s.CancelDelivery(a.id, cancelReason)
		a.cancel = answer{rep.Outcome, err}
		if err != nil {
			return false, fmt.Errorf("fuzz: cancel %s: %w", a.id, err)
		}
		return false, nil
	case ActionRace:
		var wg sync.WaitGroup
		wg.Go(func() {
			rep, err := in.s.CommitDelivery(a.id, line, in.sc.Mode)
			a.commit = answer{rep.Outcome, err}
		})
		wg.Go(func() {
			rep, err := in.s.CancelDelivery(a.id, cancelReason)
			a.cancel = answer{rep.Outcome, err}
		})
		wg.Wait()
		// Whichever comes second finds the delivery taken or gone.
		if err := a.commit.err; err != nil && !errors.Is(err, session.ErrDeliveryUnknown) {
			return false, fmt.Errorf("fuzz: commit %s: %w", a.id, err)
		}
		if err := a.cancel.err; err != nil && !errors.Is(err, session.ErrDeliveryActive) && !errors.Is(err, session.ErrDeliveryUnknown) {
			return false, fmt.Errorf("fuzz: cancel %s: %w", a.id, err)
		}
		return a.commit.err == nil && a.commit.outcome == session.DeliveryCommittedOutcome, nil
	default:
		rep, err := in.s.CommitDelivery(a.id, line, in.sc.Mode)
		a.commit = answer{rep.Outcome, err}
		if err != nil {
			return false, fmt.Errorf("fuzz: commit %s: %w", a.id, err)
		}
		return rep.Outcome == session.DeliveryCommittedOutcome, nil
	}
}

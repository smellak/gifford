package tmuxattach

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/tmuxlock"
	"github.com/smellak/gifford/internal/tmuxlock/writerlog"
)

// This file is the journal half of the pane lifecycle: Begin, Step, Child and
// Finish keep the pane's cooperative .state record durable through one
// attempt while the transport holds the pane flock. Check, the ownership
// guard, is supplied separately and joined by NewJournaledLifecycle.

var (
	// ErrPaneOperationMissing means the delivery context does not name the
	// attempt's operation, so its op digest cannot be computed and no
	// attempt may begin.
	ErrPaneOperationMissing = errors.New("pane operation missing from delivery context")
	// ErrPaneAttemptNotBegun means a journal update arrived for a pane with
	// no open attempt.
	ErrPaneAttemptNotBegun = errors.New("pane attempt not begun")
	// ErrPaneIdentityChanged means the pane process no longer matches the
	// registered identity after the submit, so the attempt ends INCIERTO.
	ErrPaneIdentityChanged = errors.New("pane identity changed during attempt")
)

// PaneOperation names what one attempt announces, for the record's op
// digest: the recipient agent and its unread inbox. The caller of Deliver
// attaches it to the delivery context with WithPaneOperation.
type PaneOperation struct {
	Agent    string
	Unread   int64
	NewestID int64
}

type paneOperationKey struct{}

// WithPaneOperation returns a context carrying the attempt's operation.
func WithPaneOperation(ctx context.Context, operation PaneOperation) context.Context {
	return context.WithValue(ctx, paneOperationKey{}, operation)
}

// PaneChecker is the cooperative ownership guard run under the pane flock
// before an attempt begins.
type PaneChecker interface {
	Check(context.Context, Target, PaneObservation) error
}

// PaneJournal is the durable attempt journal of every pane one transport
// writes. It opens the pane's .state record at Begin and closes it at
// Finish; the pane flock serializes attempts on one pane.
type PaneJournal struct {
	schema   string
	policy   tmuxlock.RecordPolicy
	open     func(tmuxlock.PanelBinding) (tmuxlock.StateFile, error)
	writer   func() (tmuxlock.Writer, error)
	identity func(Target) error
	now      func() time.Time

	mu       sync.Mutex
	attempts map[tmuxlock.PanelBinding]*tmuxlock.Journal
}

// NewPaneJournal returns the journal for the production record layout.
// opSchema is the configured schema id of the op digest preimage; policy
// is the deployment's record policy, validated again at each Begin.
func NewPaneJournal(opSchema string, policy tmuxlock.RecordPolicy) (*PaneJournal, error) {
	if opSchema == "" {
		return nil, ErrInvalidOptions
	}
	if err := policy.Validate(); err != nil {
		return nil, fmt.Errorf("pane journal policy: %w", ErrInvalidOptions)
	}
	return &PaneJournal{
		schema: opSchema,
		policy: tmuxlock.RecordPolicy{
			PeerOwner: policy.PeerOwner, PeerWriters: append([]string(nil), policy.PeerWriters...),
		},
		open: tmuxlock.OpenStateFile, writer: tmuxlock.CurrentWriter,
		identity: sameProcessIdentity, now: clock.System{}.Now,
		attempts: map[tmuxlock.PanelBinding]*tmuxlock.Journal{},
	}, nil
}

type journaledLifecycle struct {
	check     PaneChecker
	journal   *PaneJournal
	writerLog *paneWriterLog
}

// NewJournaledLifecycle joins the ownership check and the attempt journal
// into the transport's single lifecycle hook.
func NewJournaledLifecycle(check PaneChecker, journal *PaneJournal) (PaneLifecycle, error) {
	if check == nil || journal == nil {
		return nil, ErrInvalidOptions
	}
	return &journaledLifecycle{check: check, journal: journal}, nil
}

// NewWriterLoggedLifecycle joins the ownership guard and state journal to a
// configured common writer log. A disabled client preserves the state-only
// path without opening or duplicating the pane lock descriptor.
func NewWriterLoggedLifecycle(
	check PaneChecker,
	journal *PaneJournal,
	client *writerlog.Client,
) (PaneLifecycle, error) {
	if check == nil || journal == nil || client == nil {
		return nil, ErrInvalidOptions
	}
	var files paneLockFileProvider
	if client.Enabled() {
		var ok bool
		files, ok = check.(paneLockFileProvider)
		if !ok {
			return nil, ErrInvalidOptions
		}
	}
	common, err := newPaneWriterLog(files, client)
	if err != nil {
		return nil, err
	}
	return &journaledLifecycle{check: check, journal: journal, writerLog: common}, nil
}

func paneBinding(target Target) tmuxlock.PanelBinding {
	return tmuxlock.PanelBinding{
		OwnerUID: target.Server.OwnerUID, Socket: target.Server.Socket, PaneID: string(target.Pane),
	}
}

func (l *journaledLifecycle) Check(
	ctx context.Context,
	target Target,
	observation PaneObservation,
) error {
	if err := l.check.Check(ctx, target, observation); err != nil {
		return fmt.Errorf("check pane ownership: %w", err)
	}
	return nil
}

func (l *journaledLifecycle) Begin(ctx context.Context, target Target) error {
	if err := l.journal.Begin(ctx, target); err != nil {
		return err
	}
	if l.writerLog == nil {
		return nil
	}
	attempt, err := l.journal.attempt(target)
	if err == nil {
		err = l.writerLog.Open(ctx, target, attempt.Attempt())
	}
	if err == nil {
		return nil
	}
	finishErr := l.journal.Finish(ctx, target, Result{
		Outcome: OutcomeNotWritten,
		Stage:   StageWrite,
	})
	return errors.Join(err, finishErr)
}

func (l *journaledLifecycle) Step(ctx context.Context, target Target, step PaneStep) error {
	return l.journal.Step(ctx, target, step)
}

func (l *journaledLifecycle) EffectBegin(
	_ context.Context,
	target Target,
	step PaneStep,
) error {
	if l.writerLog == nil {
		return nil
	}
	return l.writerLog.KeyBegin(target, step)
}

func (l *journaledLifecycle) Child(
	ctx context.Context,
	target Target,
	step PaneStep,
	child PaneChild,
) error {
	return l.journal.Child(ctx, target, step, child)
}

func (l *journaledLifecycle) EffectEnd(
	_ context.Context,
	target Target,
	step PaneStep,
	outcome PaneEffectOutcome,
) error {
	if l.writerLog == nil {
		return nil
	}
	return l.writerLog.KeyEnd(target, step, outcome)
}

func (l *journaledLifecycle) Finish(ctx context.Context, target Target, result Result) error {
	var commonErr error
	if l.writerLog != nil {
		commonErr = l.writerLog.Release(target)
		if errors.Is(commonErr, writerlog.ErrUncertain) ||
			(commonErr != nil && result.Outcome == OutcomeCommitted) {
			result.Outcome = OutcomeUncertain
		}
	}
	return errors.Join(commonErr, l.journal.Finish(ctx, target, result))
}

// Begin makes EN_CURSO durable before the first effect. Any error forbids
// the effect.
func (j *PaneJournal) Begin(ctx context.Context, target Target) error {
	operation, ok := ctx.Value(paneOperationKey{}).(PaneOperation)
	if !ok {
		return ErrPaneOperationMissing
	}
	binding := paneBinding(target)
	j.mu.Lock()
	defer j.mu.Unlock()
	if _, open := j.attempts[binding]; open {
		return fmt.Errorf("pane attempt already open: %w", ErrInvalidOptions)
	}
	writer, err := j.writer()
	if err != nil {
		return fmt.Errorf("pane journal writer: %w", err)
	}
	file, err := j.open(binding)
	if err != nil {
		return fmt.Errorf("open pane record: %w", err)
	}
	journal, err := tmuxlock.BeginAttempt(file, tmuxlock.LiveBinding{
		PanelBinding: binding, Host: target.Authority.Host, BootID: target.Authority.BootID,
		PanePID: target.Authority.PanePID, PaneStart: target.Authority.PaneStart,
	}, tmuxlock.Operation{
		Schema: j.schema, Agent: operation.Agent,
		Inbox: tmuxlock.Inbox{Unread: operation.Unread, NewestID: operation.NewestID},
	}, writer, j.policy, j.now)
	if err != nil {
		return errors.Join(err, file.Close())
	}
	j.attempts[binding] = journal
	return nil
}

func (j *PaneJournal) attempt(target Target) (*tmuxlock.Journal, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	journal, ok := j.attempts[paneBinding(target)]
	if !ok {
		return nil, ErrPaneAttemptNotBegun
	}
	return journal, nil
}

// Step makes the sub-step durable before its process starts.
func (j *PaneJournal) Step(_ context.Context, target Target, step PaneStep) error {
	journal, err := j.attempt(target)
	if err != nil {
		return err
	}
	if err := journal.Step(int(step)); err != nil {
		return fmt.Errorf("journal pane step %d: %w", step, err)
	}
	return nil
}

// Child makes the identity of an effect process durable right after it
// started. Only the text and the Enter steps launch effects; the checks
// after them are read-only captures and journal nothing.
func (j *PaneJournal) Child(_ context.Context, target Target, step PaneStep, child PaneChild) error {
	if step != PaneStepNoticeStart && step != PaneStepEnterStart {
		return nil
	}
	journal, err := j.attempt(target)
	if err != nil {
		return err
	}
	start := uint64(0)
	if child.Start > 0 {
		start = uint64(child.Start)
	}
	if err := journal.Child(child.PID, start); err != nil {
		return fmt.Errorf("journal pane child of step %d: %w", step, err)
	}
	return nil
}

// Finish makes the terminal phase durable before the pane flock is
// released: HECHO only for a committed delivery whose pane process is still
// the registered one, INCIERTO for every other end. A changed identity
// returns an error so the transport reports the delivery as uncertain.
func (j *PaneJournal) Finish(_ context.Context, target Target, result Result) error {
	binding := paneBinding(target)
	j.mu.Lock()
	journal, ok := j.attempts[binding]
	delete(j.attempts, binding)
	j.mu.Unlock()
	if !ok {
		return ErrPaneAttemptNotBegun
	}
	done := result.Outcome == OutcomeCommitted
	var identityErr error
	if done {
		if identityErr = j.identity(target); identityErr != nil {
			done = false
		}
	}
	return errors.Join(identityErr, journal.Finish(done), journal.Close())
}

// sameProcessIdentity checks the registered pane process is still alive
// with its start time, in the boot the registry recorded.
func sameProcessIdentity(target Target) error {
	authority := target.Authority
	start, err := tmuxlock.ProcessStart(authority.PanePID)
	if err != nil || authority.PaneStart <= 0 ||
		strconv.FormatUint(start, 10) != strconv.FormatInt(authority.PaneStart, 10) {
		return ErrPaneIdentityChanged
	}
	boot, err := tmuxlock.BootID()
	if err != nil || boot != authority.BootID {
		return ErrPaneIdentityChanged
	}
	return nil
}

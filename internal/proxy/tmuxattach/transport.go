package tmuxattach

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/smellak/gifford/internal/clock"
	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/tmuxhelper"
	"github.com/smellak/gifford/internal/tmuxlock"
	"github.com/smellak/gifford/internal/tmuxnotice"
)

const (
	defaultCallTimeout = 2 * time.Second
	maxCallTimeout     = 30 * time.Second
	maxCaptureBytes    = 1 << 20
	maxPaneDimension   = 4096
	commandCapturePane = "capture-pane"
	// A harness TUI repaints after it reads the keys, so a capture taken
	// right after they are sent can still show the box from before. The
	// transport re-captures until the guard accepts a capture or this much
	// time has passed, once after the notice and once after Enter.
	defaultSettleTimeout = 1500 * time.Millisecond
	maxSettleTimeout     = 10 * time.Second
	settlePollInterval   = 50 * time.Millisecond
	stableNoticeCaptures = 2
	// NoticePrefix starts every notice the transport may type. Should a
	// notice ever be left in, or submitted to, a shell that hosts the CLI,
	// the shell runs «:», which ignores its arguments, instead of a command
	// named after the notice.
	NoticePrefix = tmuxnotice.Prefix
	// FixedNoticeLine is the notice for agents with the Gifford MCP.
	FixedNoticeLine protocol.NoticeLine = NoticePrefix + "[GIFFORD] 1 pending message"
	// DefaultBridgeNoticeLine is the public-safe default for bridge agents.
	// A deployment may replace it, but must pass the exact replacement in
	// Options.AllowedNotices before Transport can write it.
	DefaultBridgeNoticeLine protocol.NoticeLine = NoticePrefix + "[GIFFORD] 1 pending message: check your inbox"
)

var (
	// ErrInvalidOptions means the transport cannot enforce its safety contract.
	ErrInvalidOptions = errors.New("tmux attach: invalid options")
	// ErrCommand means a bounded tmux command did not complete successfully.
	ErrCommand = errors.New("tmux attach: command failed")
	// ErrCommandTimeout means the tmux client exceeded its per-call deadline.
	ErrCommandTimeout = errors.New("tmux attach: command timed out")
	// ErrCaptureTooLarge means a pane exceeded the bounded capture buffer.
	ErrCaptureTooLarge = errors.New("tmux attach: capture too large")
	// ErrScreenUnstable means an accepted staged notice did not appear
	// identically in two consecutive captures before the bounded wait ended.
	ErrScreenUnstable = errors.New("tmux attach: screen did not stabilize")
)

// Outcome is the durable interpretation of one injection attempt.
type Outcome string

const (
	// OutcomeNotWritten proves no text was sent to the pane.
	OutcomeNotWritten Outcome = "NOT_WRITTEN"
	// OutcomeCommitted means the literal notice and Enter both completed.
	OutcomeCommitted Outcome = "COMMITTED"
	// OutcomeUncertain means tmux may have applied a command; callers must not retry.
	OutcomeUncertain Outcome = "UNCERTAIN"
)

// Stage names the last operation attempted without carrying pane content.
type Stage string

const (
	// StageLock means the transport was acquiring the shared pane lock.
	StageLock Stage = "LOCK"
	// StageCapture means the transport was taking fresh, escape-preserving evidence.
	StageCapture Stage = "CAPTURE"
	// StageRevalidate means the transport was checking identity and guards.
	StageRevalidate Stage = "REVALIDATE"
	// StageWrite means the literal notice command had started.
	StageWrite Stage = "WRITE"
	// StageVerify means the transport was checking the staged notice.
	StageVerify Stage = "VERIFY"
	// StageSubmit means the verified Enter command had started.
	StageSubmit Stage = "SUBMIT"
	// StageConfirm means Enter completed and the transport was confirming the
	// resulting box state from a fresh capture.
	StageConfirm Stage = "CONFIRM"
)

// Server identifies one registered tmux server and its operating-system owner.
type Server struct {
	OwnerUser string
	OwnerUID  uint32
	Socket    string
	// Bank is the helper registry bank authorizing this server. Empty
	// means the helper path refuses: a missing bank never falls back to
	// raw tmux once the helper is configured.
	Bank string
}

func (s Server) validate() error {
	if !validOwnerUser(s.OwnerUser) || s.OwnerUID == 0 || !filepath.IsAbs(s.Socket) ||
		!validArgument(s.Socket, 512) {
		return ErrInvalidOptions
	}
	if s.Bank != "" && !validBankID(s.Bank) {
		return ErrInvalidOptions
	}
	return nil
}

// Target is one registered pane in a server.
type Target struct {
	Server    Server
	Pane      protocol.TmuxPaneID
	Authority PaneAuthority
}

// PaneAuthority is the registry's complete process identity for a target.
// Legacy transports may leave it empty; the cooperative Check implementation
// requires every field.
type PaneAuthority struct {
	Host      string
	BootID    string
	PanePID   int64
	PaneStart int64
}

func (t Target) validate() error {
	if t.Server.validate() != nil || !validCanonicalPane(t.Pane) {
		return ErrInvalidOptions
	}
	return nil
}

func (t Target) liveBinding() tmuxlock.LiveBinding {
	return tmuxlock.LiveBinding{
		PanelBinding: tmuxlock.PanelBinding{
			OwnerUID: t.Server.OwnerUID, Socket: t.Server.Socket, PaneID: string(t.Pane),
		},
		Host: t.Authority.Host, BootID: t.Authority.BootID,
		PanePID: t.Authority.PanePID, PaneStart: t.Authority.PaneStart,
	}
}

func validCanonicalPane(pane protocol.TmuxPaneID) bool {
	return pane.Valid() && (len(pane) == 2 || pane[1] != '0')
}

func validOwnerUser(value string) bool {
	if len(value) == 0 || len(value) > 32 || value[0] != '_' && (value[0] < 'a' || value[0] > 'z') {
		return false
	}
	for index := 1; index < len(value); index++ {
		char := value[index]
		if char != '_' && char != '-' && (char < 'a' || char > 'z') && (char < '0' || char > '9') {
			return false
		}
	}
	return true
}

func validArgument(value string, maxRunes int) bool {
	if value == "" || utf8.RuneCountInString(value) > maxRunes {
		return false
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

// Request is one content-free notice injection.
type Request struct {
	Target Target
	Notice protocol.NoticeLine
}

// Result records whether retrying is safe. It deliberately contains no screen
// bytes or command diagnostics.
type Result struct {
	Outcome Outcome
	Stage   Stage
}

// PaneObservation is the identity tmux reports while the writer lock is held.
type PaneObservation struct {
	PaneID  protocol.TmuxPaneID
	PanePID int64
	Cols    int
	Rows    int
}

// LockedCapture is a read-only capture taken while the pane writer lock is
// held. It never writes to the pane: analyst passes use it so the capture
// is authoritative for its cycle. It carries no cursor position on purpose:
// a replayed cursor must never stand in for the live one, and the analyst
// only ever projects draft-closed screens, which do not consult it.
type LockedCapture struct {
	Screen      []byte
	Observation PaneObservation
}

// CaptureLocked captures the pane with escapes for the detector, with its
// geometry, while holding the pane writer lock, then releases the lock
// without writing anything. Consumers cut visible rows from the same bytes,
// so the detector and any screen comparison always read the same window.
func (t *Transport) CaptureLocked(ctx context.Context, target Target) (LockedCapture, error) {
	if ctx == nil || target.validate() != nil {
		return LockedCapture{}, ErrInvalidOptions
	}
	lock, err := t.locker.Lock(ctx, target)
	if err != nil {
		return LockedCapture{}, fmt.Errorf("lock tmux pane: %w", err)
	}
	defer func() { _ = lock.Unlock() }()
	screen, err := t.Capture(ctx, target)
	if err != nil {
		return LockedCapture{}, err
	}
	observation, err := t.observePane(ctx, target)
	if err != nil {
		return LockedCapture{}, err
	}
	if observation.PaneID != target.Pane {
		return LockedCapture{}, ErrCommand
	}
	return LockedCapture{
		Screen: screen, Observation: observation,
	}, nil
}

// Guard validates a fresh capture and then verifies the staged notice. Its
// implementation owns harness-specific parsing and process-identity checks.
type Guard interface {
	// Revalidate runs under the pane lock on the delivery-opening capture:
	// screen carries the escape-coded capture the detector reads. Guards
	// that pin an analyzed screen cut the same bytes the submission cut.
	Revalidate(context.Context, Target, PaneObservation, []byte, []byte) error
	VerifyNotice(context.Context, Target, []byte, protocol.NoticeLine) error
	// VerifySubmitted accepts only a fresh post-Enter screen that proves the
	// box is empty or the agent has started working. Any other state is
	// uncertain: the transport never retries or erases it.
	VerifySubmitted(context.Context, Target, []byte) error
	// Record persists the content-free result before the pane lock is
	// released. A failed record after writing makes the caller treat the
	// outcome as uncertain; it never authorizes a retry.
	Record(context.Context, Result) error
}

// Lock is held from the fresh capture through the Enter command.
type Lock interface {
	Unlock() error
}

// Locker serializes every automatic writer for one pane.
type Locker interface {
	Lock(context.Context, Target) (Lock, error)
}

type commandRunner interface {
	Run(context.Context, Server, ...string) ([]byte, error)
	Start(context.Context, Server, ...string) (runningEffect, error)
}

type runningEffect interface {
	Child() (PaneChild, error)
	Wait() error
}

// Options configure the tmux transport.
type Options struct {
	Locker         Locker
	Lifecycle      PaneLifecycle
	Timeout        time.Duration
	AllowedNotices []protocol.NoticeLine
	// HelperRevision is the manifest sha256 pinning the privileged
	// helper. Empty keeps the raw tmux path; set routes every command
	// through the helper and refuses servers without a bank.
	HelperRevision string
	// BridgeNotice is the configured bridge notice line, the full slot
	// for helper verbs. Empty selects DefaultBridgeNoticeLine. It must
	// differ from FixedNoticeLine, mirroring the manifest equal-slot
	// rejection on the helper side.
	BridgeNotice protocol.NoticeLine
	// SettleTimeout bounds each wait for the harness to repaint, after the
	// notice and after Enter. Zero selects the default.
	SettleTimeout time.Duration
}

// helperCaller runs one closed helper verb. The production
// implementation execs the privileged binary; tests stub frames.
type helperCaller interface {
	callHelper(ctx context.Context, server Server, verb, pane string) (tmuxhelper.ExitCode, []byte, error)
	startHelper(ctx context.Context, server Server, verb, pane string) (runningEffect, error)
}

// execHelperClient runs helper verbs through the privileged binary.
type execHelperClient struct {
	revision string
}

func (c execHelperClient) callHelper(ctx context.Context, server Server, verb, pane string) (tmuxhelper.ExitCode, []byte, error) {
	return helperClient(c).call(ctx, server, verb, pane)
}

func (c execHelperClient) startHelper(
	ctx context.Context,
	server Server,
	verb string,
	pane string,
) (runningEffect, error) {
	return helperClient(c).startEffect(ctx, server, verb, pane)
}

// Transport performs bounded capture and injection commands.
type Transport struct {
	locker         Locker
	timeout        time.Duration
	runner         commandRunner
	allowedNotices map[protocol.NoticeLine]struct{}
	helper         helperCaller
	helperRevision string
	bridgeNotice   protocol.NoticeLine
	lifecycle      PaneLifecycle
	settle         time.Duration
	settlePoll     time.Duration
	now            func() time.Time
}

func allowableNotice(notice protocol.NoticeLine) bool {
	return tmuxnotice.Valid(string(notice))
}

// New returns a transport backed by sudo and tmux clients.
func New(opts Options) (*Transport, error) {
	return newTransport(opts, newCommandRunner())
}

func newTransport(opts Options, runner commandRunner) (*Transport, error) {
	if opts.Locker == nil || runner == nil || opts.Timeout < 0 || opts.Timeout > maxCallTimeout ||
		opts.SettleTimeout < 0 || opts.SettleTimeout > maxSettleTimeout {
		return nil, ErrInvalidOptions
	}
	if opts.HelperRevision != "" && !validHex64(opts.HelperRevision) {
		return nil, ErrInvalidOptions
	}
	if opts.Timeout == 0 {
		opts.Timeout = defaultCallTimeout
	}
	if opts.SettleTimeout == 0 {
		opts.SettleTimeout = defaultSettleTimeout
	}
	if opts.Lifecycle == nil {
		opts.Lifecycle = noopPaneLifecycle{}
	}
	if len(opts.AllowedNotices) == 0 {
		opts.AllowedNotices = []protocol.NoticeLine{FixedNoticeLine, DefaultBridgeNoticeLine}
	}
	allowed := make(map[protocol.NoticeLine]struct{}, len(opts.AllowedNotices))
	for _, notice := range opts.AllowedNotices {
		if !allowableNotice(notice) {
			return nil, fmt.Errorf("allowed notice %q must start with %q and then use only "+
				"letters, digits, spaces and _.,:[]-: %w", notice, NoticePrefix, ErrInvalidOptions)
		}
		allowed[notice] = struct{}{}
	}
	bridgeNotice := opts.BridgeNotice
	if bridgeNotice == "" {
		bridgeNotice = DefaultBridgeNoticeLine
	}
	if !allowableNotice(bridgeNotice) {
		return nil, fmt.Errorf("bridge notice %q is not an allowable notice line: %w", bridgeNotice, ErrInvalidOptions)
	}
	if bridgeNotice == FixedNoticeLine {
		return nil, fmt.Errorf("bridge notice must differ from the fixed notice: %w", ErrInvalidOptions)
	}
	helper := helperCaller(execHelperClient{})
	if opts.HelperRevision != "" {
		helper = execHelperClient{revision: opts.HelperRevision}
	}
	return &Transport{
		locker: opts.Locker, timeout: opts.Timeout, runner: runner, allowedNotices: allowed,
		helper: helper, helperRevision: opts.HelperRevision, bridgeNotice: bridgeNotice,
		lifecycle: opts.Lifecycle,
		settle:    opts.SettleTimeout, settlePoll: settlePollInterval, now: clock.System{}.Now,
	}, nil
}

// helperConfigured reports whether every command routes through the
// privileged helper. Unset keeps the exact raw tmux path.
func (t *Transport) helperConfigured() bool {
	return t.helperRevision != ""
}

// helperDo runs one helper verb for server and maps the exit code. The
// revision comes from node config and the bank from the server record;
// a missing bank refuses without falling back to raw tmux.
func (t *Transport) helperDo(ctx context.Context, server Server, verb, pane string) ([]byte, error) {
	if server.Bank == "" {
		return nil, fmt.Errorf("helper bank missing: %w", ErrInvalidOptions)
	}
	callCtx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()
	code, body, err := t.helper.callHelper(callCtx, server, verb, pane)
	if err != nil {
		if errors.Is(callCtx.Err(), context.DeadlineExceeded) {
			return nil, ErrCommandTimeout
		}
		return nil, err
	}
	if err := helperError(code); err != nil {
		return nil, err
	}
	return bytes.Clone(body), nil
}

// helperNoticeVerb selects the closed verb sending notice. The slots
// are pinned: notice-short is exactly the fixed ordinary notice,
// notice-full is exactly the configured bridge line. Anything else
// refuses instead of guessing which registry entry to overwrite. The
// notice itself arrives from durable provenance upstream; this only
// maps the two configured literals onto verbs.
func (t *Transport) helperNoticeVerb(notice protocol.NoticeLine) (string, error) {
	switch notice {
	case FixedNoticeLine:
		return "notice-short", nil
	default:
		if notice == t.bridgeNotice {
			return "notice-full", nil
		}
		return "", ErrInvalidOptions
	}
}

// Capture reads only the visible pane and preserves terminal escapes for the
// harness detector. It never acquires the writer lock or changes pane state.
func (t *Transport) Capture(ctx context.Context, target Target) ([]byte, error) {
	if ctx == nil || target.validate() != nil {
		return nil, ErrInvalidOptions
	}
	if t.helperConfigured() {
		output, err := t.helperDo(ctx, target.Server, "capture", string(target.Pane))
		if err != nil {
			return nil, err
		}
		if len(output) > maxCaptureBytes {
			return nil, ErrCaptureTooLarge
		}
		return output, nil
	}
	return t.run(ctx, target.Server, commandCapturePane, "-p", "-e", "-t", string(target.Pane))
}

// ObservePane reads the live pane identity and geometry without acquiring the
// writer lock. Recovery calls it only while NodeWriter already retains the
// exact pane flock, avoiding a recursive lock acquisition.
func (t *Transport) ObservePane(ctx context.Context, target Target) (PaneObservation, error) {
	if ctx == nil || target.validate() != nil {
		return PaneObservation{}, ErrInvalidOptions
	}
	return t.observePane(ctx, target)
}

// ListPanes returns the stable pane id and leader pid pairs used by the node
// registry. The fixed format keeps sudoers rules narrow and parsing strict.
func (t *Transport) ListPanes(ctx context.Context, server Server) ([]PaneObservation, error) {
	if ctx == nil || server.validate() != nil {
		return nil, ErrInvalidOptions
	}
	if t.helperConfigured() {
		output, err := t.helperDo(ctx, server, "panes", "")
		if err != nil {
			return nil, err
		}
		return parsePaneObservations(output)
	}
	output, err := t.run(ctx, server, "list-panes", "-a", "-F", "#{pane_id}\t#{pane_pid}")
	if err != nil {
		return nil, err
	}
	return parsePaneObservations(output)
}

// ListClientActivity returns the fixed-format activity rows without client
// tty names or pane contents. The detector owns strict parsing: in particular,
// tmux may leave client_activity empty when a client has never acted, and that
// distinction must not be lost in the transport.
func (t *Transport) ListClientActivity(ctx context.Context, target Target) ([]byte, error) {
	if ctx == nil || target.validate() != nil {
		return nil, ErrInvalidOptions
	}
	if t.helperConfigured() {
		return t.helperDo(ctx, target.Server, "clients", "")
	}
	return t.run(ctx, target.Server, "list-clients", "-F", "#{pane_id}\t#{client_activity}")
}

// Deliver writes one literal notice after locking and revalidating a new
// capture. Once the first send command starts, every failure is UNCERTAIN and
// the caller must reconcile the pane instead of retrying or erasing text.
// After the notice and again after Enter it waits, still under the lock, for
// a capture the guard accepts, up to the settle timeout: it only reads while
// it waits and never types again.
func (t *Transport) Deliver(ctx context.Context, request Request, guard Guard) (result Result, err error) {
	result = Result{Outcome: OutcomeNotWritten, Stage: StageLock}
	if ctx == nil || request.Target.validate() != nil || !t.allowedNotice(request.Notice) || guard == nil {
		return result, ErrInvalidOptions
	}
	lock, err := t.locker.Lock(ctx, request.Target)
	if err != nil {
		return result, fmt.Errorf("lock pane: %w", err)
	}
	if lock == nil {
		return result, ErrInvalidOptions
	}
	// Keep unlock in its own defer so even a faulty recorder panic cannot
	// strand the process-local descriptor. LIFO still records first.
	defer func() {
		if unlockErr := lock.Unlock(); unlockErr != nil {
			err = errors.Join(err, fmt.Errorf("unlock pane: %w", unlockErr))
		}
	}()
	defer func() {
		recorded := result
		recordCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), t.timeout)
		recordErr := guard.Record(recordCtx, recorded)
		cancel()
		if recordErr != nil {
			if result.Outcome == OutcomeCommitted {
				result.Outcome = OutcomeUncertain
			}
			err = errors.Join(err, fmt.Errorf("record delivery result: %w", recordErr))
		}
	}()
	attemptBegun := false
	defer func() {
		if !attemptBegun {
			return
		}
		finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), t.timeout)
		finishErr := t.lifecycle.Finish(finishCtx, request.Target, result)
		cancel()
		if finishErr != nil {
			if result.Outcome == OutcomeCommitted {
				result.Outcome = OutcomeUncertain
			}
			err = errors.Join(err, fmt.Errorf("finish pane attempt: %w", finishErr))
		}
	}()

	result.Stage = StageCapture
	screen, err := t.Capture(ctx, request.Target)
	if err != nil {
		return result, err
	}
	observation, err := t.observePane(ctx, request.Target)
	if err != nil {
		return result, err
	}
	clients, err := t.ListClientActivity(ctx, request.Target)
	if err != nil {
		return result, err
	}
	result.Stage = StageRevalidate
	if err := guard.Revalidate(ctx, request.Target, observation, clients, screen); err != nil {
		return result, fmt.Errorf("revalidate pane: %w", err)
	}
	if err := t.lifecycle.Check(ctx, request.Target, observation); err != nil {
		return result, fmt.Errorf("check pane lifecycle: %w", err)
	}

	result.Stage = StageWrite
	var startNotice func() (runningEffect, error)
	if t.helperConfigured() {
		verb, err := t.helperNoticeVerb(request.Notice)
		if err != nil {
			return result, err
		}
		startNotice = func() (runningEffect, error) {
			return t.startHelperEffect(ctx, request.Target.Server, verb, string(request.Target.Pane))
		}
	} else {
		startNotice = func() (runningEffect, error) {
			return t.startRunnerEffect(ctx, request.Target.Server, "send-keys", "-l", "-t", string(request.Target.Pane), string(request.Notice))
		}
	}
	if err := t.lifecycle.Begin(ctx, request.Target); err != nil {
		return result, fmt.Errorf("begin pane attempt: %w", err)
	}
	attemptBegun = true
	result.Outcome = OutcomeUncertain
	if err := t.lifecycle.Step(ctx, request.Target, PaneStepNoticeStart); err != nil {
		return result, fmt.Errorf("record pane notice step: %w", err)
	}
	if err := t.executeEffect(ctx, request.Target, PaneStepNoticeStart, startNotice); err != nil {
		return result, fmt.Errorf("execute pane notice: %w", err)
	}
	result.Stage = StageVerify
	if err := t.lifecycle.Step(ctx, request.Target, PaneStepNoticeCheckStart); err != nil {
		return result, fmt.Errorf("record pane notice verification: %w", err)
	}
	if err := t.settleCapture(ctx, request.Target, stableNoticeCaptures, func(screen []byte) error {
		return guard.VerifyNotice(ctx, request.Target, screen, request.Notice)
	}); err != nil {
		return result, fmt.Errorf("verify staged notice: %w", err)
	}
	result.Stage = StageSubmit
	if err := t.lifecycle.Step(ctx, request.Target, PaneStepEnterStart); err != nil {
		return result, fmt.Errorf("record pane submit step: %w", err)
	}
	var startEnter func() (runningEffect, error)
	if t.helperConfigured() {
		startEnter = func() (runningEffect, error) {
			return t.startHelperEffect(ctx, request.Target.Server, "enter", string(request.Target.Pane))
		}
	} else {
		startEnter = func() (runningEffect, error) {
			return t.startRunnerEffect(ctx, request.Target.Server, "send-keys", "-t", string(request.Target.Pane), "Enter")
		}
	}
	if err := t.executeEffect(ctx, request.Target, PaneStepEnterStart, startEnter); err != nil {
		return result, fmt.Errorf("execute pane submit: %w", err)
	}
	result.Stage = StageConfirm
	if err := t.lifecycle.Step(ctx, request.Target, PaneStepResultCheckStart); err != nil {
		return result, fmt.Errorf("record pane result step: %w", err)
	}
	if err := t.settleCapture(ctx, request.Target, 1, func(screen []byte) error {
		return guard.VerifySubmitted(ctx, request.Target, screen)
	}); err != nil {
		return result, fmt.Errorf("verify submitted notice: %w", err)
	}
	result.Outcome = OutcomeCommitted
	return result, nil
}

func (t *Transport) executeEffect(
	ctx context.Context,
	target Target,
	step PaneStep,
	start func() (runningEffect, error),
) error {
	effect, err := start()
	if err != nil {
		return err
	}
	child, childErr := effect.Child()
	if childErr != nil {
		return errors.Join(
			fmt.Errorf("observe pane effect child: %w", childErr),
			effect.Wait(),
		)
	}
	if err := t.lifecycle.Child(ctx, target, step, child); err != nil {
		return errors.Join(
			fmt.Errorf("record pane effect child: %w", err),
			effect.Wait(),
		)
	}
	if err := effect.Wait(); err != nil {
		return fmt.Errorf("wait pane effect: %w", err)
	}
	return nil
}

type boundedEffect struct {
	ctx    context.Context
	cancel context.CancelFunc
	effect runningEffect
}

func (e *boundedEffect) Child() (PaneChild, error) {
	child, err := e.effect.Child()
	if err != nil {
		return PaneChild{}, fmt.Errorf("bounded effect child: %w", err)
	}
	return child, nil
}

func (e *boundedEffect) Wait() error {
	err := e.effect.Wait()
	timedOut := errors.Is(e.ctx.Err(), context.DeadlineExceeded)
	e.cancel()
	if timedOut {
		return ErrCommandTimeout
	}
	if err != nil {
		return fmt.Errorf("bounded effect wait: %w", err)
	}
	return nil
}

func (t *Transport) startRunnerEffect(
	ctx context.Context,
	server Server,
	args ...string,
) (runningEffect, error) {
	callCtx, cancel := context.WithTimeout(ctx, t.timeout)
	effect, err := t.runner.Start(callCtx, server, args...)
	if err != nil {
		cancel()
		if errors.Is(callCtx.Err(), context.DeadlineExceeded) {
			return nil, ErrCommandTimeout
		}
		return nil, fmt.Errorf("start bounded tmux effect: %w", err)
	}
	return &boundedEffect{ctx: callCtx, cancel: cancel, effect: effect}, nil
}

func (t *Transport) startHelperEffect(
	ctx context.Context,
	server Server,
	verb string,
	pane string,
) (runningEffect, error) {
	if server.Bank == "" {
		return nil, fmt.Errorf("helper bank missing: %w", ErrInvalidOptions)
	}
	callCtx, cancel := context.WithTimeout(ctx, t.timeout)
	effect, err := t.helper.startHelper(callCtx, server, verb, pane)
	if err != nil {
		cancel()
		if errors.Is(callCtx.Err(), context.DeadlineExceeded) {
			return nil, ErrCommandTimeout
		}
		return nil, err
	}
	return &boundedEffect{ctx: callCtx, cancel: cancel, effect: effect}, nil
}

// settleCapture re-captures the pane until check accepts the requested number
// of identical consecutive captures or the settle timeout passes. Rejected or
// changed captures reset the accepted run. Requiring two stable observations
// before Enter gives a literal burst an observable completion boundary without
// inventing another time delay. A failed capture or a cancelled context ends
// the wait at once.
func (t *Transport) settleCapture(
	ctx context.Context,
	target Target,
	confirmations int,
	check func([]byte) error,
) error {
	deadline := t.now().Add(t.settle)
	var previous []byte
	accepted := 0
	for {
		screen, err := t.Capture(ctx, target)
		if err != nil {
			return err
		}
		verdict := check(screen)
		switch {
		case verdict != nil:
			previous = nil
			accepted = 0
		case accepted > 0 && bytes.Equal(previous, screen):
			accepted++
		default:
			previous = bytes.Clone(screen)
			accepted = 1
		}
		remaining := deadline.Sub(t.now())
		if accepted >= confirmations {
			return nil
		}
		if remaining <= 0 {
			if verdict == nil {
				return ErrScreenUnstable
			}
			return verdict
		}
		timer := time.NewTimer(min(t.settlePoll, remaining))
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(verdict, ctx.Err())
		case <-timer.C:
		}
	}
}

func (t *Transport) allowedNotice(notice protocol.NoticeLine) bool {
	_, allowed := t.allowedNotices[notice]
	return allowed
}

func (t *Transport) observePane(ctx context.Context, target Target) (PaneObservation, error) {
	var output []byte
	var err error
	if t.helperConfigured() {
		output, err = t.helperDo(ctx, target.Server, "observe", string(target.Pane))
	} else {
		output, err = t.run(ctx, target.Server, "display-message", "-p", "-t", string(target.Pane),
			"#{pane_id}\t#{pane_pid}\t#{pane_width}\t#{pane_height}")
	}
	if err != nil {
		return PaneObservation{}, err
	}
	observation, err := parsePaneObservation(output)
	if err != nil || observation.PaneID != target.Pane {
		return PaneObservation{}, ErrCommand
	}
	return observation, nil
}

func parsePaneObservation(output []byte) (PaneObservation, error) {
	trimmed := strings.TrimSuffix(string(output), "\n")
	parts := strings.Split(trimmed, "\t")
	if trimmed == "" || strings.Contains(trimmed, "\n") || len(parts) != 4 {
		return PaneObservation{}, ErrCommand
	}
	pane := protocol.TmuxPaneID(parts[0])
	pid, pidErr := strconv.ParseInt(parts[1], 10, 64)
	cols, colsErr := strconv.Atoi(parts[2])
	rows, rowsErr := strconv.Atoi(parts[3])
	if !validCanonicalPane(pane) || pid <= 0 || pidErr != nil || colsErr != nil || rowsErr != nil ||
		cols <= 0 || rows <= 0 || cols > maxPaneDimension || rows > maxPaneDimension ||
		cols > maxCaptureBytes/rows {
		return PaneObservation{}, ErrCommand
	}
	return PaneObservation{PaneID: pane, PanePID: pid, Cols: cols, Rows: rows}, nil
}

func parsePaneObservations(output []byte) ([]PaneObservation, error) {
	rows, err := parseRows(output)
	if err != nil {
		return nil, err
	}
	result := make([]PaneObservation, 0, len(rows))
	seen := make(map[protocol.TmuxPaneID]bool, len(rows))
	for _, row := range rows {
		pane := protocol.TmuxPaneID(row[0])
		pid, parseErr := strconv.ParseInt(row[1], 10, 64)
		if !validCanonicalPane(pane) || pid <= 0 || parseErr != nil || seen[pane] {
			return nil, ErrCommand
		}
		seen[pane] = true
		result = append(result, PaneObservation{PaneID: pane, PanePID: pid})
	}
	return result, nil
}

func parseRows(output []byte) ([][2]string, error) {
	trimmed := strings.TrimSuffix(string(output), "\n")
	if trimmed == "" {
		return nil, nil
	}
	lines := strings.Split(trimmed, "\n")
	rows := make([][2]string, 0, len(lines))
	for _, line := range lines {
		parts := strings.Split(line, "\t")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return nil, ErrCommand
		}
		rows = append(rows, [2]string{parts[0], parts[1]})
	}
	return rows, nil
}

func (t *Transport) run(ctx context.Context, server Server, args ...string) ([]byte, error) {
	callCtx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()
	output, err := t.runner.Run(callCtx, server, args...)
	if errors.Is(callCtx.Err(), context.DeadlineExceeded) {
		return nil, ErrCommandTimeout
	}
	if err != nil {
		return nil, fmt.Errorf("run bounded tmux command: %w", err)
	}
	if len(output) > maxCaptureBytes {
		return nil, ErrCaptureTooLarge
	}
	return bytes.Clone(output), nil
}

func commandArguments(server Server, args ...string) []string {
	result := make([]string, 0, 7+len(args))
	result = append(result, "-n", "-u", server.OwnerUser, "--", "tmux", "-S", server.Socket)
	return append(result, args...)
}

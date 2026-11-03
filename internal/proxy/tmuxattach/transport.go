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

	"github.com/smellak/gifford/internal/protocol"
	"github.com/smellak/gifford/internal/tmuxnotice"
)

const (
	defaultCallTimeout = 2 * time.Second
	maxCallTimeout     = 30 * time.Second
	maxCaptureBytes    = 1 << 20
	maxPaneDimension   = 4096
	commandCapturePane = "capture-pane"
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
}

func (s Server) validate() error {
	if !validOwnerUser(s.OwnerUser) || s.OwnerUID == 0 || !filepath.IsAbs(s.Socket) ||
		!validArgument(s.Socket, 512) {
		return ErrInvalidOptions
	}
	return nil
}

// Target is one registered pane in a server.
type Target struct {
	Server Server
	Pane   protocol.TmuxPaneID
}

func (t Target) validate() error {
	if t.Server.validate() != nil || !validCanonicalPane(t.Pane) {
		return ErrInvalidOptions
	}
	return nil
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

// Guard validates a fresh capture and then verifies the staged notice. Its
// implementation owns harness-specific parsing and process-identity checks.
type Guard interface {
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
}

// Options configure the tmux transport.
type Options struct {
	Locker         Locker
	Timeout        time.Duration
	AllowedNotices []protocol.NoticeLine
}

// Transport performs bounded capture and injection commands.
type Transport struct {
	locker         Locker
	timeout        time.Duration
	runner         commandRunner
	allowedNotices map[protocol.NoticeLine]struct{}
}

func allowableNotice(notice protocol.NoticeLine) bool {
	return tmuxnotice.Valid(string(notice))
}

// New returns a transport backed by sudo and tmux clients.
func New(opts Options) (*Transport, error) {
	return newTransport(opts, newCommandRunner())
}

func newTransport(opts Options, runner commandRunner) (*Transport, error) {
	if opts.Locker == nil || runner == nil || opts.Timeout < 0 || opts.Timeout > maxCallTimeout {
		return nil, ErrInvalidOptions
	}
	if opts.Timeout == 0 {
		opts.Timeout = defaultCallTimeout
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
	return &Transport{
		locker: opts.Locker, timeout: opts.Timeout, runner: runner, allowedNotices: allowed,
	}, nil
}

// Capture reads only the visible pane and preserves terminal escapes for the
// harness detector. It never acquires the writer lock or changes pane state.
func (t *Transport) Capture(ctx context.Context, target Target) ([]byte, error) {
	if ctx == nil || target.validate() != nil {
		return nil, ErrInvalidOptions
	}
	return t.run(ctx, target.Server, commandCapturePane, "-p", "-e", "-t", string(target.Pane))
}

// ListPanes returns the stable pane id and leader pid pairs used by the node
// registry. The fixed format keeps sudoers rules narrow and parsing strict.
func (t *Transport) ListPanes(ctx context.Context, server Server) ([]PaneObservation, error) {
	if ctx == nil || server.validate() != nil {
		return nil, ErrInvalidOptions
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
	return t.run(ctx, target.Server, "list-clients", "-F", "#{pane_id}\t#{client_activity}")
}

// Deliver writes one literal notice after locking and revalidating a new
// capture. Once the first send command starts, every failure is UNCERTAIN and
// the caller must reconcile the pane instead of retrying or erasing text.
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

	result.Stage = StageWrite
	result.Outcome = OutcomeUncertain
	if _, err := t.run(ctx, request.Target.Server, "send-keys", "-l", "-t", string(request.Target.Pane), string(request.Notice)); err != nil {
		return result, err
	}
	result.Stage = StageVerify
	screen, err = t.Capture(ctx, request.Target)
	if err != nil {
		return result, err
	}
	if err := guard.VerifyNotice(ctx, request.Target, screen, request.Notice); err != nil {
		return result, fmt.Errorf("verify staged notice: %w", err)
	}

	result.Stage = StageSubmit
	if _, err := t.run(ctx, request.Target.Server, "send-keys", "-t", string(request.Target.Pane), "Enter"); err != nil {
		return result, err
	}
	result.Stage = StageConfirm
	screen, err = t.Capture(ctx, request.Target)
	if err != nil {
		return result, err
	}
	if err := guard.VerifySubmitted(ctx, request.Target, screen); err != nil {
		return result, fmt.Errorf("verify submitted notice: %w", err)
	}
	result.Outcome = OutcomeCommitted
	return result, nil
}

func (t *Transport) allowedNotice(notice protocol.NoticeLine) bool {
	_, allowed := t.allowedNotices[notice]
	return allowed
}

func (t *Transport) observePane(ctx context.Context, target Target) (PaneObservation, error) {
	output, err := t.run(ctx, target.Server, "display-message", "-p", "-t", string(target.Pane),
		"#{pane_id}\t#{pane_pid}\t#{pane_width}\t#{pane_height}")
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

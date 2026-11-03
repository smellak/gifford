package tmuxattach

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"

	"github.com/smellak/gifford/internal/tmuxhelper"
)

// helperSudo is the frozen invocation vector: passwordless sudo as the
// server owner, end of options, then the helper with its closed grammar.
var helperSudo = []string{"/usr/bin/sudo", "-n", "-u"}

// cappedStdout bounds one helper answer in memory: it keeps the first
// MaxFrameBytes+1 stdout bytes so an overlong answer is detected instead
// of buffered without end.
type cappedStdout struct {
	buf      bytes.Buffer
	overflow bool
}

func (c *cappedStdout) Write(data []byte) (int, error) {
	room := tmuxhelper.MaxFrameBytes + 1 - c.buf.Len()
	if room <= 0 {
		c.overflow = true
		return len(data), nil
	}
	kept := data
	if len(data) > room {
		kept = data[:room]
		c.overflow = true
	}
	wrote, writeErr := c.buf.Write(kept)
	if writeErr != nil {
		return wrote, fmt.Errorf("helper stdout: %w", writeErr)
	}
	return len(data), nil
}

// helperClient runs closed helper requests through the privileged
// binary. The revision comes from explicit node config and the bank
// from the server record, never deduced; the zero value refuses every
// call, which is the closed default for instances without a bank.
type helperClient struct {
	revision string
}

type helperInvocation struct {
	command  *exec.Cmd
	stdout   *cappedStdout
	child    PaneChild
	childErr error
	code     tmuxhelper.ExitCode
	body     []byte
}

type runningHelperEffect struct {
	invocation *helperInvocation
}

// call runs one verb against server and returns the helper exit code
// with its body. The process exit must equal the frame code; anything
// else is incoherent and fails closed.
func (c helperClient) call(ctx context.Context, server Server, verb, pane string) (tmuxhelper.ExitCode, []byte, error) {
	invocation, err := c.start(ctx, server, verb, pane)
	if err != nil {
		return 0, nil, err
	}
	if err := invocation.Wait(); err != nil {
		return 0, nil, err
	}
	return invocation.code, invocation.body, nil
}

func (c helperClient) startEffect(
	ctx context.Context,
	server Server,
	verb string,
	pane string,
) (runningEffect, error) {
	invocation, err := c.start(ctx, server, verb, pane)
	if err != nil {
		return nil, err
	}
	return &runningHelperEffect{invocation: invocation}, nil
}

func (c helperClient) start(
	ctx context.Context,
	server Server,
	verb string,
	pane string,
) (*helperInvocation, error) {
	if !validHex64(c.revision) {
		return nil, fmt.Errorf("helper unconfigured: %w", ErrInvalidOptions)
	}
	if server.Bank == "" || !validBankID(server.Bank) {
		return nil, fmt.Errorf("helper bank missing: %w", ErrInvalidOptions)
	}
	argv, err := helperArgv(c.revision, server.Bank, verb, pane)
	if err != nil {
		return nil, err
	}
	full := make([]string, 0, len(helperSudo)+3+len(argv))
	full = append(full, helperSudo...)
	full = append(full, server.OwnerUser, "--", helperBinary)
	full = append(full, argv...)
	// #nosec G204 -- every executable and prefix element is fixed; the
	// request argv elements are validated; no shell is involved.
	command := exec.CommandContext(ctx, full[0], full[1:]...)
	stdout := &cappedStdout{}
	command.Stdout = stdout
	if err := command.Start(); err != nil {
		return nil, fmt.Errorf("helper start: %w", err)
	}
	child, childErr := observeChildProcess(command.Process.Pid)
	return &helperInvocation{
		command: command, stdout: stdout, child: child, childErr: childErr,
	}, nil
}

func (i *helperInvocation) Child() (PaneChild, error) {
	return i.child, i.childErr
}

func (i *helperInvocation) Wait() error {
	waitErr := i.command.Wait()
	procCode, ok := exitCodeOf(waitErr)
	if !ok {
		return fmt.Errorf("helper run: %w", waitErr)
	}
	if i.stdout.overflow {
		return fmt.Errorf("helper frame too large: %w", ErrCaptureTooLarge)
	}
	code, body, err := joinHelperResult(procCode, i.stdout.buf.Bytes())
	if err != nil {
		return err
	}
	i.code = code
	i.body = body
	return nil
}

func (e *runningHelperEffect) Child() (PaneChild, error) {
	return e.invocation.Child()
}

func (e *runningHelperEffect) Wait() error {
	if err := e.invocation.Wait(); err != nil {
		return err
	}
	if err := helperError(e.invocation.code); err != nil {
		return err
	}
	if len(e.invocation.body) != 0 {
		return ErrCommand
	}
	return nil
}

// joinHelperResult joins a finished helper invocation: the process exit
// must equal the strict frame code, and the frame must decode with the
// owner decoder. It is pure so truncated, malformed and incoherent
// answers are table-tested without executing anything.
func joinHelperResult(procCode int, output []byte) (tmuxhelper.ExitCode, []byte, error) {
	if len(output) > tmuxhelper.MaxFrameBytes {
		return 0, nil, fmt.Errorf("helper frame too large: %w", ErrCaptureTooLarge)
	}
	code, body, err := tmuxhelper.DecodeFrame(output)
	if err != nil {
		return 0, nil, fmt.Errorf("helper frame: %w", err)
	}
	if int(code) != procCode {
		return 0, nil, errors.New("helper exit/frame mismatch")
	}
	return code, body, nil
}

// exitCodeOf extracts the process exit of a finished command. Anything
// that is not a wait status fails closed: only the frame may speak for
// the helper.
func exitCodeOf(runErr error) (int, bool) {
	if runErr == nil {
		return 0, true
	}
	var exitErr *exec.ExitError
	if !errors.As(runErr, &exitErr) {
		return 0, false
	}
	return exitErr.ExitCode(), true
}

// helperError maps a coherent helper exit code onto the transport
// surface: timeout and oversize keep their meaning, everything else
// that is not success is a plain command failure.
func helperError(code tmuxhelper.ExitCode) error {
	switch code {
	case tmuxhelper.ExitOK:
		return nil
	case tmuxhelper.ExitTimeout:
		return ErrCommandTimeout
	case tmuxhelper.ExitOutputLimit:
		return ErrCaptureTooLarge
	default:
		return ErrCommand
	}
}

// helperBinary is the fixed absolute path of the privileged closed tmux
// interface. No PATH lookup and no caller-controlled path: the sudoers
// shape belongs to the helper owner.
const helperBinary = "/usr/local/libexec/gifford-tmux"

// helperVerbs is the closed request grammar measured on the helper
// branch. The pane flag marks verbs that address one registered pane;
// the freeze may only shrink this table through helperArgv.
var helperVerbs = map[string]bool{
	"panes":        false,
	"observe":      true,
	"clients":      false,
	"capture":      true,
	"notice-full":  true,
	"notice-short": true,
	"enter":        true,
}

// helperArgv builds the helper request argv after the binary:
// "<verb> <sha256-manifest> <bank> [pane]". It is the single grammar
// point: a freeze only changes this function and its table test.
func helperArgv(revision, bank, verb, pane string) ([]string, error) {
	needsPane, ok := helperVerbs[verb]
	if !ok {
		return nil, fmt.Errorf("helper verb: %w", ErrInvalidOptions)
	}
	if !validHex64(revision) {
		return nil, fmt.Errorf("helper revision: %w", ErrInvalidOptions)
	}
	if !validBankID(bank) {
		return nil, fmt.Errorf("helper bank: %w", ErrInvalidOptions)
	}
	argv := []string{verb, revision, bank}
	if needsPane {
		if !validPaneID(pane) {
			return nil, fmt.Errorf("helper pane: %w", ErrInvalidOptions)
		}
		return append(argv, pane), nil
	}
	if pane != "" {
		return nil, fmt.Errorf("helper pane: %w", ErrInvalidOptions)
	}
	return argv, nil
}

func validHex64(value string) bool {
	if len(value) != 64 {
		return false
	}
	for index := range len(value) {
		char := value[index]
		digit := char >= '0' && char <= '9'
		lower := char >= 'a' && char <= 'f'
		if !digit && !lower {
			return false
		}
	}
	return true
}

func validBankID(value string) bool {
	if len(value) < 1 || len(value) > 32 {
		return false
	}
	for index := range len(value) {
		char := value[index]
		if char != '-' && (char < 'a' || char > 'z') && (char < '0' || char > '9') {
			return false
		}
	}
	return true
}

func validPaneID(value string) bool {
	if value == "%0" {
		return true
	}
	if len(value) < 2 || value[0] != '%' || value[1] < '1' || value[1] > '9' {
		return false
	}
	for index := 2; index < len(value); index++ {
		if value[index] < '0' || value[index] > '9' {
			return false
		}
	}
	return true
}

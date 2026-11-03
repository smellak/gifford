package tmuxattach

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"

	"github.com/smellak/gifford/internal/tmuxhelper"
)

// helperSudo is the frozen invocation vector: passwordless sudo as the
// server owner, end of options, then the helper with its closed grammar.
var helperSudo = []string{"/usr/bin/sudo", "-n", "-u"}

// helperFrameBytes caps one helper answer: the 1 MiB protocol body plus
// base64 expansion and the header line.
const helperFrameBytes = 2 << 20

// helperClient runs closed helper requests through the privileged
// binary. The revision comes from explicit node config and the bank
// from the server record, never deduced; the zero value refuses every
// call, which is the closed default for instances without a bank.
type helperClient struct {
	revision string
}

// call runs one verb against server and returns the helper exit code
// with its body. The process exit must equal the frame code; anything
// else is incoherent and fails closed.
func (c helperClient) call(ctx context.Context, server Server, verb, pane string) (tmuxhelper.ExitCode, []byte, error) { //nolint:unparam // pane varies once the transport switch passes registered panes
	if !validHex64(c.revision) {
		return 0, nil, fmt.Errorf("helper unconfigured: %w", ErrInvalidOptions)
	}
	if server.Bank == "" || !validBankID(server.Bank) {
		return 0, nil, fmt.Errorf("helper bank missing: %w", ErrInvalidOptions)
	}
	argv, err := helperArgv(c.revision, server.Bank, verb, pane)
	if err != nil {
		return 0, nil, err
	}
	full := make([]string, 0, len(helperSudo)+3+len(argv))
	full = append(full, helperSudo...)
	full = append(full, server.OwnerUser, "--", helperBinary)
	full = append(full, argv...)
	// #nosec G204 -- every executable and prefix element is fixed; the
	// request argv elements are validated; no shell is involved.
	command := exec.CommandContext(ctx, full[0], full[1:]...)
	var stdout bytes.Buffer
	command.Stdout = &stdout
	runErr := command.Run()
	procCode, ok := exitCodeOf(runErr)
	if !ok {
		return 0, nil, fmt.Errorf("helper run: %w", runErr)
	}
	if stdout.Len() > helperFrameBytes {
		return 0, nil, fmt.Errorf("helper frame too large: %w", ErrCaptureTooLarge)
	}
	return joinHelperResult(procCode, stdout.Bytes())
}

// joinHelperResult joins a finished helper invocation: the process exit
// must equal the frame code, and the frame must decode. It is pure so
// truncated, malformed and incoherent answers are table-tested without
// executing anything.
func joinHelperResult(procCode int, output []byte) (tmuxhelper.ExitCode, []byte, error) {
	if len(output) > helperFrameBytes {
		return 0, nil, fmt.Errorf("helper frame too large: %w", ErrCaptureTooLarge)
	}
	code, body, err := decodeHelperFrame(output)
	if err != nil {
		return 0, nil, err
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

// helperFrameVersion is the only output framing the node accepts from
// the helper.
const helperFrameVersion = "GTMX/1"

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

// decodeHelperFrame parses one GTMX/1 frame: "GTMX/1 <code> <len>\n"
// followed by the base64 body and a trailing newline. Truncated or
// malformed output is an error, never a guess.
func decodeHelperFrame(output []byte) (tmuxhelper.ExitCode, []byte, error) {
	line, rest, found := strings.Cut(string(output), "\n")
	if !found {
		return 0, nil, errors.New("helper frame without header")
	}
	fields := strings.Fields(line)
	if len(fields) != 3 || fields[0] != helperFrameVersion {
		return 0, nil, errors.New("helper frame without version")
	}
	code, err := strconv.Atoi(fields[1])
	if err != nil || code < int(tmuxhelper.ExitOK) || code > int(tmuxhelper.ExitBinding) {
		return 0, nil, errors.New("helper frame without code")
	}
	size, err := strconv.Atoi(fields[2])
	if err != nil || size < 0 {
		return 0, nil, errors.New("helper frame without size")
	}
	encoded, tail, found := strings.Cut(rest, "\n")
	if !found || tail != "" {
		return 0, nil, errors.New("helper frame truncated")
	}
	body, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return 0, nil, errors.New("helper frame without body")
	}
	if len(body) != size {
		return 0, nil, errors.New("helper frame size mismatch")
	}
	return tmuxhelper.ExitCode(code), body, nil
}
